from __future__ import annotations

import json
import sqlite3
import threading
from pathlib import Path
from typing import Any, Dict, List, Optional

from .audit import make_entry, utc_now
from .domain import ConflictError, NotFoundError
from .rules import ENTITY, STATES


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
                CREATE TABLE IF NOT EXISTS audit_events (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    action TEXT NOT NULL,
                    entity_type TEXT NOT NULL,
                    entity_id INTEGER NOT NULL,
                    actor TEXT NOT NULL,
                    detail TEXT NOT NULL,
                    previous_hash TEXT NOT NULL,
                    entry_hash TEXT NOT NULL UNIQUE,
                    created_at TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS batches (
                    request_no TEXT PRIMARY KEY,
                    item_id INTEGER NOT NULL REFERENCES items(id) ON DELETE CASCADE,
                    stage TEXT NOT NULL DEFAULT 'feedback',
                    status TEXT NOT NULL DEFAULT 'pending'
                        CHECK(status IN ('pending','committed','conflict')),
                    version INTEGER,
                    record_id INTEGER,
                    feedback_kind TEXT,
                    feedback_detail TEXT,
                    feedback_status TEXT,
                    target_status TEXT,
                    actor TEXT NOT NULL,
                    authorizer TEXT,
                    conflict_reason TEXT,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL
                );
                CREATE INDEX IF NOT EXISTS idx_batches_item ON batches(item_id);
                CREATE TABLE IF NOT EXISTS schema_migrations (
                    version TEXT PRIMARY KEY,
                    applied_at TEXT NOT NULL
                );
            """)
        self._migrate()

    def _migrate(self) -> None:
        """一次性升级：为历史审计按原顺序补批次归属(batch_id)。

        batch_id 不参与哈希载荷，因此补齐后哈希链保持不变。
        仅对升级前已存在、batch_id 为空的历史数据执行一次。
        """
        with self._lock:
            cols = [r[1] for r in self.conn.execute("PRAGMA table_info(audit_events)").fetchall()]
            with self.conn:
                if "batch_id" not in cols:
                    self.conn.execute("ALTER TABLE audit_events ADD COLUMN batch_id TEXT")
                applied = self.conn.execute(
                    "SELECT 1 FROM schema_migrations WHERE version='batch_ownership'"
                ).fetchone()
                if applied is not None:
                    return
                legacy = self.conn.execute(
                    "SELECT id, entity_id, action FROM audit_events "
                    "WHERE batch_id IS NULL ORDER BY id"
                ).fetchall()
                batch_rows: Dict[str, Dict[str, Any]] = {}
                if legacy:
                    counters: Dict[int, List[Any]] = {}
                    for row in legacy:
                        eid = row["entity_id"]
                        state = counters.setdefault(eid, [0, None])
                        if row["action"] == "record":
                            state[0] += 1
                            state[1] = f"LEGACY-{eid}-{state[0]}"
                        elif state[1] is None:
                            state[0] = 1
                            state[1] = f"LEGACY-{eid}-1"
                        bno = state[1]
                        self.conn.execute(
                            "UPDATE audit_events SET batch_id=? WHERE id=?",
                            (bno, row["id"]),
                        )
                        batch_rows.setdefault(bno, {"item_id": eid})
                now = utc_now()
                for bno, info in batch_rows.items():
                    self.conn.execute(
                        """INSERT OR IGNORE INTO batches(request_no, item_id, stage, status,
                           actor, created_at, updated_at) VALUES(?,?,?,?,?,?,?)""",
                        (bno, info["item_id"], "legacy", "committed", "legacy", now, now),
                    )
                self.conn.execute(
                    "INSERT INTO schema_migrations(version, applied_at) VALUES(?,?)",
                    ("batch_ownership", now),
                )

    @staticmethod
    def _item(row: sqlite3.Row) -> Dict[str, Any]:
        return dict(row)

    def create_item(self, title: str, description: str, severity: str,
                    quantity: float, threshold: float, external_ref: Optional[str],
                    actor: str) -> Dict[str, Any]:
        now = utc_now()
        try:
            with self._lock, self.conn:
                cur = self.conn.execute(
                    """INSERT INTO items(title, description, severity, quantity, threshold,
                       status, version, external_ref, created_by, created_at, updated_at)
                       VALUES(?,?,?,?,?,?,?,?,?,?,?)""",
                    (title, description, severity, quantity, threshold, STATES[0], 1,
                     external_ref, actor, now, now),
                )
                item_id = int(cur.lastrowid)
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

    def transition_item(self, item_id: int, target: str, expected_version: int,
                        actor: str) -> Dict[str, Any]:
        now = utc_now()
        with self._lock, self.conn:
            cur = self.conn.execute(
                """UPDATE items SET status=?, version=version+1, updated_at=?
                   WHERE id=? AND version=?""",
                (target, now, item_id, expected_version),
            )
            if cur.rowcount == 0:
                exists = self.conn.execute("SELECT 1 FROM items WHERE id=?", (item_id,)).fetchone()
                if exists is None:
                    raise NotFoundError("项目不存在")
                raise ConflictError("版本冲突，请刷新后重试")
        return self.get_item(item_id)

    def add_record(self, item_id: int, kind: str, detail: str, status: str,
                   external_ref: Optional[str], actor: str) -> Dict[str, Any]:
        now = utc_now()
        self.get_item(item_id)
        try:
            with self._lock, self.conn:
                cur = self.conn.execute(
                    """INSERT INTO records(item_id, kind, detail, status, external_ref,
                       created_by, created_at) VALUES(?,?,?,?,?,?,?)""",
                    (item_id, kind, detail, status, external_ref, actor, now),
                )
                record_id = int(cur.lastrowid)
        except sqlite3.IntegrityError as exc:
            raise ConflictError("记录唯一标识已存在") from exc
        with self._lock:
            row = self.conn.execute("SELECT * FROM records WHERE id=?", (record_id,)).fetchone()
        return dict(row)

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

    # ------------------------------------------------------------------ #
    # 处置批次：现场反馈 + 总工授权
    # ------------------------------------------------------------------ #

    def create_batch_feedback(self, item_id: int, request_no: str, kind: str,
                              detail: str, status: str, external_ref: Optional[str],
                              actor: str) -> Dict[str, Any]:
        """批次前半程（值班员现场反馈）：记录、状态、审计、批次同生共死。

        幂等：同一 request_no 重复提交直接返回既有批次，不重复写记录/审计。
        """
        now = utc_now()
        with self._lock:
            existing = self.conn.execute(
                "SELECT * FROM batches WHERE request_no=?", (request_no,)
            ).fetchone()
            if existing is not None:
                existing = dict(existing)
                if existing["item_id"] != item_id:
                    raise ConflictError("请求编号已用于其他处置批次")
                return existing
            item = self.conn.execute(
                "SELECT * FROM items WHERE id=?", (item_id,)
            ).fetchone()
            if item is None:
                raise NotFoundError("项目不存在")
            item = dict(item)
            try:
                with self.conn:
                    cur = self.conn.execute(
                        """INSERT INTO records(item_id, kind, detail, status, external_ref,
                           created_by, created_at) VALUES(?,?,?,?,?,?,?)""",
                        (item_id, kind, detail, status, external_ref, actor, now),
                    )
                    record_id = int(cur.lastrowid)
                    # 值班员现场反馈同时完成“复核”：仅当仍为 draft 时推进到 checked。
                    cur = self.conn.execute(
                        """UPDATE items SET status='checked', version=version+1, updated_at=?
                           WHERE id=? AND status='draft'""",
                        (now, item_id),
                    )
                    advanced = cur.rowcount
                    new_version = item["version"] + 1 if advanced else item["version"]
                    self._append_audit(
                        "record", ENTITY, item_id, actor,
                        {"record_id": record_id, "kind": kind, "status": status,
                         "batch_request_no": request_no},
                        batch_id=request_no,
                    )
                    self.conn.execute(
                        """INSERT INTO batches(request_no, item_id, stage, status, version,
                           record_id, feedback_kind, feedback_detail, feedback_status,
                           target_status, actor, created_at, updated_at)
                           VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                        (request_no, item_id, "feedback", "pending", new_version,
                         record_id, kind, detail, status, "authorized", actor, now, now),
                    )
            except sqlite3.IntegrityError as exc:
                raise ConflictError("记录唯一标识已存在") from exc
        return self.get_batch(request_no)

    def authorize_batch(self, request_no: str, expected_version: int,
                        actor: str, target: str = "authorized") -> Dict[str, Any]:
        """批次后半程（总工授权）：先到者生效，失败不留痕，可按原请求编号恢复。

        - 批次与调度指令共用同一版本：以 items.version 做 compare-and-swap。
        - 状态与审计在同一事务内写入；冲突或异常时回滚，不写授权审计。
        - 已提交的批次重复授权直接返回，不重复追加审计。
        """
        now = utc_now()
        with self._lock:
            row = self.conn.execute(
                "SELECT * FROM batches WHERE request_no=?", (request_no,)
            ).fetchone()
            if row is None:
                raise NotFoundError("处置批次不存在")
            batch = dict(row)
            if batch["status"] == "committed":
                return batch
            item_row = self.conn.execute(
                "SELECT * FROM items WHERE id=?", (batch["item_id"],)
            ).fetchone()
            if item_row is None:
                raise NotFoundError("项目不存在")
            item = dict(item_row)
            try:
                with self.conn:
                    cur = self.conn.execute(
                        """UPDATE items SET status=?, version=version+1, updated_at=?
                           WHERE id=? AND version=? AND status='checked'""",
                        (target, now, item["id"], expected_version),
                    )
                    if cur.rowcount == 0:
                        if item["status"] != "checked":
                            reason = f"项目状态为{item['status']}，无法授权"
                        else:
                            reason = "版本冲突，请刷新后重试"
                        raise ConflictError(reason)
                    new_version = expected_version + 1
                    self._append_audit(
                        "transition", ENTITY, item["id"], actor,
                        {"from": "checked", "to": target,
                         "batch_request_no": request_no},
                        batch_id=request_no,
                    )
                    self.conn.execute(
                        """UPDATE batches SET status='committed', stage='authorize', version=?,
                           authorizer=?, target_status=?, conflict_reason=NULL, updated_at=?
                           WHERE request_no=?""",
                        (new_version, actor, target, now, request_no),
                    )
            except ConflictError as exc:
                # 冲突状态单独提交，确保“待恢复”标记落库；授权本身不留审计。
                with self.conn:
                    self.conn.execute(
                        """UPDATE batches SET status='conflict', conflict_reason=?, updated_at=?
                           WHERE request_no=?""",
                        (str(exc), now, request_no),
                    )
                raise
        return self.get_batch(request_no)

    def get_batch(self, request_no: str) -> Dict[str, Any]:
        with self._lock:
            row = self.conn.execute(
                "SELECT * FROM batches WHERE request_no=?", (request_no,)
            ).fetchone()
        if row is None:
            raise NotFoundError("处置批次不存在")
        return dict(row)

    def list_batches(self, status: Optional[str] = None,
                     item_id: Optional[int] = None) -> List[Dict[str, Any]]:
        sql = "SELECT * FROM batches WHERE 1=1"
        params: tuple = ()
        if status:
            sql += " AND status=?"
            params = (status,)
        if item_id is not None:
            sql += " AND item_id=?"
            params = (item_id,)
        sql += " ORDER BY created_at DESC, rowid DESC"
        with self._lock:
            rows = self.conn.execute(sql, params).fetchall()
        return [dict(row) for row in rows]

    # ------------------------------------------------------------------ #
    # 审计
    # ------------------------------------------------------------------ #

    def append_audit(self, action: str, entity_type: str, entity_id: int,
                     actor: str, detail: dict,
                     batch_id: Optional[str] = None) -> Dict[str, Any]:
        with self._lock, self.conn:
            return self._append_audit(action, entity_type, entity_id, actor, detail,
                                      batch_id=batch_id)

    def _append_audit(self, action: str, entity_type: str, entity_id: int,
                      actor: str, detail: dict,
                      batch_id: Optional[str] = None) -> Dict[str, Any]:
        """调用方须已持有 self._lock 并处于事务中；batch_id 不进入哈希载荷。"""
        row = self.conn.execute(
            "SELECT entry_hash FROM audit_events ORDER BY id DESC LIMIT 1"
        ).fetchone()
        previous = row["entry_hash"] if row else "GENESIS"
        event = make_entry(action, entity_type, entity_id, actor, detail, previous)
        cur = self.conn.execute(
            """INSERT INTO audit_events(action, entity_type, entity_id, actor, detail,
               previous_hash, entry_hash, created_at, batch_id)
               VALUES(?,?,?,?,?,?,?,?,?)""",
            (event["action"], event["entity_type"], event["entity_id"], event["actor"],
             json.dumps(event["detail"], ensure_ascii=False, sort_keys=True),
             event["previous_hash"], event["entry_hash"], event["created_at"], batch_id),
        )
        event["id"] = int(cur.lastrowid)
        if batch_id is not None:
            event["batch_id"] = batch_id
        return event

    def list_audit(self, entity_id: Optional[int] = None,
                  batch_id: Optional[str] = None) -> List[Dict[str, Any]]:
        sql = "SELECT * FROM audit_events WHERE 1=1"
        params: tuple = ()
        if entity_id is not None:
            sql += " AND entity_id=?"
            params = (entity_id,)
        if batch_id is not None:
            sql += " AND batch_id=?"
            params = params + (batch_id,)
        sql += " ORDER BY id"
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
