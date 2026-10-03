from __future__ import annotations

import json
import sqlite3
import threading
from contextlib import contextmanager
from pathlib import Path
from typing import Any, Dict, List, Optional

from .audit import make_entry, utc_now
from .domain import ConflictError, NotFoundError
from .rules import ENTITY, ID_PREFIX, STATES


class _VersionConflict(Exception):
    """批次内部使用：版本竞争失败，由外层把批次标记为冲突。"""


class Repository:
    def __init__(self, db_path: str):
        self.db_path = str(db_path)
        Path(self.db_path).parent.mkdir(parents=True, exist_ok=True)
        self._lock = threading.RLock()
        self.conn = sqlite3.connect(self.db_path, check_same_thread=False)
        self.conn.row_factory = sqlite3.Row
        self.conn.execute("PRAGMA foreign_keys = ON")
        self.conn.execute("PRAGMA journal_mode = WAL")
        self._create_schema()
        self._migrate_schema()

    def _create_schema(self) -> None:
        statuses = ",".join("'" + s.replace("'", "''") + "'" for s in STATES)
        with self.conn:
            self.conn.executescript(f"""
                CREATE TABLE IF NOT EXISTS items (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    title TEXT NOT NULL,
                    description TEXT NOT NULL,
                    severity TEXT NOT NULL,
                    quantity REAL NOT NULL DEFAULT 0,
                    threshold REAL NOT NULL DEFAULT 1,
                    status TEXT NOT NULL CHECK(status IN ({statuses})),
                    version INTEGER NOT NULL DEFAULT 1,
                    external_ref TEXT,
                    created_by TEXT NOT NULL,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL
                );
                CREATE UNIQUE INDEX IF NOT EXISTS ux_items_external_ref
                    ON items(external_ref) WHERE external_ref IS NOT NULL;
                CREATE TABLE IF NOT EXISTS records (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    item_id INTEGER NOT NULL REFERENCES items(id) ON DELETE CASCADE,
                    kind TEXT NOT NULL,
                    detail TEXT NOT NULL,
                    status TEXT NOT NULL DEFAULT 'open'
                        CHECK(status IN ('open','closed')),
                    external_ref TEXT,
                    created_by TEXT NOT NULL,
                    created_at TEXT NOT NULL,
                    UNIQUE(item_id, external_ref)
                );
                CREATE TABLE IF NOT EXISTS batches (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    request_id TEXT NOT NULL UNIQUE,
                    item_id INTEGER NOT NULL REFERENCES items(id) ON DELETE CASCADE,
                    action TEXT NOT NULL,
                    actor TEXT NOT NULL,
                    status TEXT NOT NULL DEFAULT 'pending'
                        CHECK(status IN ('pending','committed','conflict','failed')),
                    payload TEXT NOT NULL DEFAULT '{{}}',
                    expected_version INTEGER,
                    result TEXT,
                    error TEXT,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL
                );
                CREATE INDEX IF NOT EXISTS ix_batches_item ON batches(item_id);
                CREATE TABLE IF NOT EXISTS audit_events (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    action TEXT NOT NULL,
                    entity_type TEXT NOT NULL,
                    entity_id INTEGER NOT NULL,
                    actor TEXT NOT NULL,
                    detail TEXT NOT NULL,
                    previous_hash TEXT NOT NULL,
                    entry_hash TEXT NOT NULL UNIQUE,
                    created_at TEXT NOT NULL,
                    batch_id INTEGER REFERENCES batches(id)
                );
            """)

    def _migrate_schema(self) -> None:
        # 旧库第一次升级：既有审计记录按原顺序补上批次归属，只写batch_id，不动哈希链
        with self._lock:
            columns = [row["name"] for row in self.conn.execute(
                "PRAGMA table_info(audit_events)").fetchall()]
            if "batch_id" in columns:
                return
            with self.conn:
                self.conn.execute(
                    "ALTER TABLE audit_events ADD COLUMN batch_id INTEGER REFERENCES batches(id)")
                rows = self.conn.execute(
                    "SELECT id, action, entity_id, actor, created_at FROM audit_events ORDER BY id"
                ).fetchall()
                for row in rows:
                    cur = self.conn.execute(
                        """INSERT INTO batches(request_id, item_id, action, actor, status,
                           payload, created_at, updated_at)
                           VALUES(?,?,?,?,'committed','{}',?,?)""",
                        ("legacy-audit-" + str(row["id"]), row["entity_id"], row["action"],
                         row["actor"], row["created_at"], row["created_at"]),
                    )
                    self.conn.execute("UPDATE audit_events SET batch_id=? WHERE id=?",
                                      (int(cur.lastrowid), row["id"]))

    @staticmethod
    def _item(row: sqlite3.Row) -> Dict[str, Any]:
        return dict(row)

    @staticmethod
    def _batch(row: sqlite3.Row) -> Dict[str, Any]:
        batch = dict(row)
        batch["payload"] = json.loads(batch["payload"]) if batch["payload"] else {}
        batch["result"] = json.loads(batch["result"]) if batch["result"] else None
        return batch

    @contextmanager
    def unit_of_work(self):
        # 批次从登记到完成串行执行，并发提交时先到者生效
        with self._lock:
            yield

    def create_item(self, request_id: str, title: str, description: str, severity: str,
                    quantity: float, threshold: float, external_ref: Optional[str],
                    actor: str, payload: Dict[str, Any],
                    audit_detail: Dict[str, Any]) -> Dict[str, Any]:
        now = utc_now()
        payload_json = json.dumps(payload, ensure_ascii=False, sort_keys=True)
        with self._lock:
            try:
                with self.conn:
                    cur = self.conn.execute(
                        """INSERT INTO items(title, description, severity, quantity, threshold,
                           status, version, external_ref, created_by, created_at, updated_at)
                           VALUES(?,?,?,?,?,?,?,?,?,?,?)""",
                        (title, description, severity, quantity, threshold, STATES[0], 1,
                         external_ref, actor, now, now),
                    )
                    item_id = int(cur.lastrowid)
                    item = dict(self.conn.execute(
                        "SELECT * FROM items WHERE id=?", (item_id,)).fetchone())
                    cur = self.conn.execute(
                        """INSERT INTO batches(request_id, item_id, action, actor, status,
                           payload, result, created_at, updated_at)
                           VALUES(?,?,?,?,'committed',?,?,?,?)""",
                        (request_id, item_id, "create", actor, payload_json,
                         json.dumps(item, ensure_ascii=False, sort_keys=True), now, now),
                    )
                    self._append_audit_locked("create", ENTITY, item_id, actor,
                                              audit_detail, int(cur.lastrowid))
            except sqlite3.IntegrityError as exc:
                raise ConflictError("external_ref已存在") from exc
        return self.get_item(item_id)

    def get_item(self, item_id: int) -> Dict[str, Any]:
        with self._lock:
            row = self.conn.execute("SELECT * FROM items WHERE id=?", (item_id,)).fetchone()
        if row is None:
            raise NotFoundError("项目不存在")
        return self._item(row)

    def list_items(self, status: Optional[str] = None) -> List[Dict[str, Any]]:
        sql = "SELECT * FROM items"
        params: tuple = ()
        if status:
            sql += " WHERE status=?"
            params = (status,)
        sql += " ORDER BY id DESC"
        with self._lock:
            rows = self.conn.execute(sql, params).fetchall()
        return [self._item(row) for row in rows]

    def begin_batch(self, request_id: str, item_id: int, action: str, actor: str,
                    payload: Dict[str, Any],
                    expected_version: Optional[int] = None) -> tuple:
        now = utc_now()
        payload_json = json.dumps(payload, ensure_ascii=False, sort_keys=True)
        with self._lock:
            row = self.conn.execute(
                "SELECT * FROM batches WHERE request_id=?", (request_id,)).fetchone()
            if row is not None:
                return self._batch(row), False
            try:
                with self.conn:
                    cur = self.conn.execute(
                        """INSERT INTO batches(request_id, item_id, action, actor, status,
                           payload, expected_version, created_at, updated_at)
                           VALUES(?,?,?,?,'pending',?,?,?,?)""",
                        (request_id, item_id, action, actor, payload_json,
                         expected_version, now, now),
                    )
                    batch_id = int(cur.lastrowid)
            except sqlite3.IntegrityError:
                row = self.conn.execute(
                    "SELECT * FROM batches WHERE request_id=?", (request_id,)).fetchone()
                if row is None:
                    raise NotFoundError("项目不存在")
                return self._batch(row), False
            row = self.conn.execute(
                "SELECT * FROM batches WHERE id=?", (batch_id,)).fetchone()
            return self._batch(row), True

    def find_batch(self, request_id: str) -> Optional[Dict[str, Any]]:
        with self._lock:
            row = self.conn.execute(
                "SELECT * FROM batches WHERE request_id=?", (request_id,)).fetchone()
        return self._batch(row) if row is not None else None

    def fail_batch(self, batch_id: int, error: str) -> None:
        now = utc_now()
        with self._lock, self.conn:
            self.conn.execute(
                """UPDATE batches SET status='failed', error=?, updated_at=?
                   WHERE id=? AND status='pending'""",
                (error, now, batch_id))

    def conflict_batch(self, batch_id: int, error: str) -> None:
        now = utc_now()
        with self._lock, self.conn:
            self.conn.execute(
                """UPDATE batches SET status='conflict', error=?, updated_at=?
                   WHERE id=? AND status='pending'""",
                (error, now, batch_id))

    def complete_transition_batch(self, batch_id: int, item_id: int, target: str,
                                  expected_version: int, actor: str,
                                  detail: Dict[str, Any]) -> Dict[str, Any]:
        now = utc_now()
        with self._lock:
            try:
                with self.conn:
                    cur = self.conn.execute(
                        """UPDATE items SET status=?, version=version+1, updated_at=?
                           WHERE id=? AND version=?""",
                        (target, now, item_id, expected_version),
                    )
                    if cur.rowcount == 0:
                        exists = self.conn.execute(
                            "SELECT 1 FROM items WHERE id=?", (item_id,)).fetchone()
                        if exists is None:
                            raise NotFoundError("项目不存在")
                        raise _VersionConflict()
                    item = dict(self.conn.execute(
                        "SELECT * FROM items WHERE id=?", (item_id,)).fetchone())
                    self._append_audit_locked("transition", ENTITY, item_id, actor,
                                              detail, batch_id)
                    self.conn.execute(
                        """UPDATE batches SET status='committed', result=?, updated_at=?
                           WHERE id=? AND status='pending'""",
                        (json.dumps(item, ensure_ascii=False, sort_keys=True), now, batch_id),
                    )
            except _VersionConflict:
                # 状态与审计都没有落库，只把批次标记为冲突
                self.conflict_batch(batch_id, "版本冲突，请刷新后重试")
                raise ConflictError("版本冲突，请刷新后重试")
        return self.get_item(item_id)

    def complete_record_batch(self, batch_id: int, item_id: int, kind: str, detail: str,
                              status: str, external_ref: Optional[str],
                              actor: str) -> Dict[str, Any]:
        now = utc_now()
        with self._lock:
            try:
                with self.conn:
                    cur = self.conn.execute(
                        """INSERT INTO records(item_id, kind, detail, status, external_ref,
                           created_by, created_at) VALUES(?,?,?,?,?,?,?)""",
                        (item_id, kind, detail, status, external_ref, actor, now),
                    )
                    record_id = int(cur.lastrowid)
                    record = dict(self.conn.execute(
                        "SELECT * FROM records WHERE id=?", (record_id,)).fetchone())
                    self._append_audit_locked("record", ENTITY, item_id, actor, {
                        "record_id": record_id, "kind": kind, "status": status,
                    }, batch_id)
                    self.conn.execute(
                        """UPDATE batches SET status='committed', result=?, updated_at=?
                           WHERE id=? AND status='pending'""",
                        (json.dumps(record, ensure_ascii=False, sort_keys=True), now, batch_id),
                    )
            except sqlite3.IntegrityError as exc:
                self.conflict_batch(batch_id, "记录唯一标识已存在")
                raise ConflictError("记录唯一标识已存在") from exc
            row = self.conn.execute(
                "SELECT * FROM records WHERE id=?", (record_id,)).fetchone()
            return dict(row)

    def _append_audit_locked(self, action: str, entity_type: str, entity_id: int,
                             actor: str, detail: dict, batch_id: int) -> None:
        # 同一批次只允许追加一次审计，按请求编号恢复时不得重复留痕
        row = self.conn.execute(
            "SELECT id FROM audit_events WHERE batch_id=?", (batch_id,)).fetchone()
        if row is not None:
            return
        row = self.conn.execute(
            "SELECT entry_hash FROM audit_events ORDER BY id DESC LIMIT 1").fetchone()
        previous = row["entry_hash"] if row else "GENESIS"
        event = make_entry(action, entity_type, entity_id, actor, detail, previous)
        self.conn.execute(
            """INSERT INTO audit_events(action, entity_type, entity_id, actor, detail,
               previous_hash, entry_hash, created_at, batch_id) VALUES(?,?,?,?,?,?,?,?,?)""",
            (event["action"], event["entity_type"], event["entity_id"], event["actor"],
             json.dumps(event["detail"], ensure_ascii=False, sort_keys=True),
             event["previous_hash"], event["entry_hash"], event["created_at"], batch_id),
        )

    def list_records(self, item_id: int) -> List[Dict[str, Any]]:
        self.get_item(item_id)
        with self._lock:
            rows = self.conn.execute(
                "SELECT * FROM records WHERE item_id=? ORDER BY id", (item_id,)
            ).fetchall()
        return [dict(row) for row in rows]

    def open_record_count(self, item_id: int) -> int:
        with self._lock:
            row = self.conn.execute(
                "SELECT COUNT(*) AS n FROM records WHERE item_id=? AND status='open'",
                (item_id,),
            ).fetchone()
        return int(row["n"])

    def list_batches(self, item_id: Optional[int] = None,
                     status: Optional[str] = None) -> List[Dict[str, Any]]:
        sql = "SELECT * FROM batches"
        conditions = []
        params: list = []
        if item_id is not None:
            conditions.append("item_id=?")
            params.append(item_id)
        if status is not None:
            conditions.append("status=?")
            params.append(status)
        if conditions:
            sql += " WHERE " + " AND ".join(conditions)
        sql += " ORDER BY id DESC"
        with self._lock:
            rows = self.conn.execute(sql, tuple(params)).fetchall()
        return [self._batch(row) for row in rows]

    def list_audit(self, entity_id: Optional[int] = None) -> List[Dict[str, Any]]:
        sql = ("SELECT a.*, b.request_id AS batch_request_id FROM audit_events a "
               "LEFT JOIN batches b ON b.id=a.batch_id")
        params: tuple = ()
        if entity_id is not None:
            sql += " WHERE a.entity_id=?"
            params = (entity_id,)
        sql += " ORDER BY a.id"
        with self._lock:
            rows = self.conn.execute(sql, params).fetchall()
        result = []
        for row in rows:
            item = dict(row)
            item["detail"] = json.loads(item["detail"])
            result.append(item)
        return result

    def verify_audit_chain(self) -> bool:
        from .audit import calculate_hash
        with self._lock:
            rows = self.conn.execute("SELECT * FROM audit_events ORDER BY id").fetchall()
        previous = "GENESIS"
        for row in rows:
            if row["previous_hash"] != previous:
                return False
            payload = {
                "action": row["action"], "entity_type": row["entity_type"],
                "entity_id": row["entity_id"], "actor": row["actor"],
                "detail": json.loads(row["detail"]), "created_at": row["created_at"],
            }
            if calculate_hash(previous, payload) != row["entry_hash"]:
                return False
            previous = row["entry_hash"]
        return True

    def close(self) -> None:
        with self._lock:
            self.conn.close()
