import json
import sqlite3
import tempfile
import threading
import unittest
from pathlib import Path

from src.audit import make_entry
from src.domain import ConflictError, PermissionDenied
from src.repository import Repository
from src.service import Service


class BatchTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.path = str(Path(self.tmp.name) / "test.db")
        self.repo = Repository(self.path)
        self.service = Service(self.repo)

    def tearDown(self):
        self.repo.close()
        self.tmp.cleanup()

    def _make_item(self, ref="BATCH-1"):
        return self.service.create_item(
            {"title": "batch item", "description": "flood control",
             "severity": "urgent", "quantity": 12, "threshold": 6,
             "external_ref": ref},
            "creator", "duty_officer")

    def _feedback(self, item_id, request_no, ref=None):
        payload = {"request_no": request_no, "kind": "evidence",
                   "detail": "on-site feedback", "status": "closed"}
        if ref:
            payload["external_ref"] = ref
        return self.service.submit_feedback(item_id, payload, "duty", "duty_officer")

    def test_feedback_then_authorize_commits_atomically(self):
        item = self._make_item()
        fb = self._feedback(item["id"], "B-1")
        self.assertEqual(fb["status"], "pending")
        self.assertEqual(fb["progress"], "feedback_done")
        self.assertTrue(fb["recoverable"])
        self.assertEqual(fb["version"], 2)
        self.assertEqual(fb["stage"], "feedback")

        auth = self.service.authorize("B-1", {"expected_version": 2},
                                      "chief", "chief_engineer")
        self.assertEqual(auth["status"], "committed")
        self.assertEqual(auth["progress"], "committed")
        self.assertFalse(auth["recoverable"])
        self.assertEqual(auth["version"], 3)
        self.assertEqual(auth["stage"], "authorize")

        stored = self.service.get_item(item["id"], "viewer")
        self.assertEqual(stored["status"], "authorized")
        self.assertEqual(stored["version"], 3)

        events = self.repo.list_audit(batch_id="B-1")
        self.assertEqual([e["action"] for e in events], ["record", "transition"])
        self.assertTrue(all(e["batch_id"] == "B-1" for e in events))
        self.assertTrue(self.repo.verify_audit_chain())

    def test_conflict_leaves_no_half_trace_and_recovers_by_request_no(self):
        item = self._make_item()
        self._feedback(item["id"], "B-2")

        # 后半程用过期版本号授权 → 冲突，状态与审计都不留痕
        with self.assertRaises(ConflictError):
            self.service.authorize("B-2", {"expected_version": 99},
                                   "chief", "chief_engineer")
        batch = self.service.get_batch("B-2", "viewer")
        self.assertEqual(batch["status"], "conflict")
        self.assertEqual(batch["progress"], "conflict")
        self.assertTrue(batch["recoverable"])
        self.assertTrue(batch["conflict_reason"])

        # 只有前半程一条反馈审计，没有授权审计（无半条痕迹）
        events = self.repo.list_audit(batch_id="B-2")
        self.assertEqual(len(events), 1)
        self.assertEqual(events[0]["action"], "record")

        # 按同一请求编号恢复，审计不重复追加
        auth = self.service.authorize("B-2", {"expected_version": 2},
                                      "chief", "chief_engineer")
        self.assertEqual(auth["status"], "committed")
        events = self.repo.list_audit(batch_id="B-2")
        self.assertEqual(len(events), 2)
        self.assertEqual([e["action"] for e in events], ["record", "transition"])
        self.assertTrue(self.repo.verify_audit_chain())

    def test_idempotent_feedback_and_authorize(self):
        item = self._make_item()
        fb1 = self._feedback(item["id"], "B-3", ref="EV-3")
        # 同一请求编号重复提交反馈 → 幂等，不重复写记录/审计
        fb2 = self._feedback(item["id"], "B-3", ref="EV-3")
        self.assertEqual(fb1["request_no"], fb2["request_no"])
        self.assertEqual(fb1["record_id"], fb2["record_id"])

        auth1 = self.service.authorize("B-3", {"expected_version": 2},
                                       "chief", "chief_engineer")
        # 同一请求编号重复授权 → 幂等返回，不重复追加审计
        auth2 = self.service.authorize("B-3", {"expected_version": 2},
                                       "chief", "chief_engineer")
        self.assertEqual(auth1["status"], auth2["status"])
        self.assertEqual(auth1["version"], auth2["version"])
        self.assertEqual(auth1["authorizer"], auth2["authorizer"])

        events = self.repo.list_audit(batch_id="B-3")
        self.assertEqual(len(events), 2)
        self.assertTrue(self.repo.verify_audit_chain())

    def test_feedback_request_no_reused_on_other_item_conflicts(self):
        item1 = self._make_item("X-1")
        item2 = self._make_item("X-2")
        self._feedback(item1["id"], "B-4")
        with self.assertRaises(ConflictError):
            self._feedback(item2["id"], "B-4")

    def test_concurrent_submissions_first_committer_wins(self):
        item = self._make_item()
        for bno in ("B-5A", "B-5B"):
            self._feedback(item["id"], bno)

        results = {}
        barrier = threading.Barrier(2)

        def do(bno):
            barrier.wait()
            try:
                r = self.service.authorize(bno, {"expected_version": 2},
                                           "chief", "chief_engineer")
                results[bno] = ("ok", r["status"])
            except ConflictError as exc:
                results[bno] = ("conflict", str(exc))

        t1 = threading.Thread(target=do, args=("B-5A",))
        t2 = threading.Thread(target=do, args=("B-5B",))
        t1.start()
        t2.start()
        t1.join()
        t2.join()

        statuses = [v[0] for v in results.values()]
        self.assertIn("ok", statuses)
        self.assertIn("conflict", statuses)
        committed = [b for b, v in results.items() if v[0] == "ok"][0]
        conflicted = [b for b, v in results.items() if v[0] == "conflict"][0]

        self.assertEqual(self.service.get_batch(committed, "viewer")["status"],
                         "committed")
        self.assertEqual(self.service.get_batch(conflicted, "viewer")["status"],
                         "conflict")
        stored = self.service.get_item(item["id"], "viewer")
        self.assertEqual(stored["status"], "authorized")
        self.assertEqual(stored["version"], 3)
        # 先到者两条审计，失利者只有反馈一条
        self.assertEqual(len(self.repo.list_audit(batch_id=committed)), 2)
        self.assertEqual(len(self.repo.list_audit(batch_id=conflicted)), 1)
        self.assertTrue(self.repo.verify_audit_chain())

    def test_list_distinguishes_progress_conflict_recoverable(self):
        item = self._make_item()
        self._feedback(item["id"], "B-6")
        self._feedback(item["id"], "B-7")
        self.service.authorize("B-6", {"expected_version": 2},
                               "chief", "chief_engineer")
        # B-7 授权时版本已被 B-6 推到 3，compare-and-swap 失败 → 冲突
        with self.assertRaises(ConflictError):
            self.service.authorize("B-7", {"expected_version": 2},
                                   "chief", "chief_engineer")

        batches = {b["request_no"]: b for b in self.service.list_batches("viewer")}
        self.assertEqual(batches["B-6"]["progress"], "committed")
        self.assertFalse(batches["B-6"]["recoverable"])
        self.assertEqual(batches["B-7"]["progress"], "conflict")
        self.assertTrue(batches["B-7"]["recoverable"])

        conflicts = self.service.list_batches("viewer", status="conflict")
        self.assertTrue(len(conflicts) >= 1)
        self.assertTrue(all(b["status"] == "conflict" for b in conflicts))
        self.assertTrue(all(b["progress"] == "conflict" for b in conflicts))

    def test_role_guards(self):
        item = self._make_item()
        with self.assertRaises(PermissionDenied):
            self.service.submit_feedback(item["id"],
                                         {"request_no": "B-8", "kind": "evidence",
                                          "detail": "d"},
                                         "attacker", "viewer")
        self._feedback(item["id"], "B-8")
        with self.assertRaises(PermissionDenied):
            self.service.authorize("B-8", {"expected_version": 2},
                                   "attacker", "viewer")

    def test_migration_backfills_batch_ownership_and_keeps_hash_chain(self):
        # 构造旧库：audit_events 无 batch_id 列，且已有历史审计
        old = str(Path(self.tmp.name) / "old.db")
        conn = sqlite3.connect(old)
        conn.executescript("""
            CREATE TABLE items (
                id INTEGER PRIMARY KEY AUTOINCREMENT, title TEXT NOT NULL,
                description TEXT NOT NULL, severity TEXT NOT NULL,
                quantity REAL NOT NULL DEFAULT 0, threshold REAL NOT NULL DEFAULT 1,
                status TEXT NOT NULL, version INTEGER NOT NULL DEFAULT 1,
                external_ref TEXT, created_by TEXT NOT NULL,
                created_at TEXT NOT NULL, updated_at TEXT NOT NULL);
            CREATE TABLE records (
                id INTEGER PRIMARY KEY AUTOINCREMENT, item_id INTEGER NOT NULL,
                kind TEXT NOT NULL, detail TEXT NOT NULL, status TEXT NOT NULL DEFAULT 'open',
                external_ref TEXT, created_by TEXT NOT NULL, created_at TEXT NOT NULL);
            CREATE TABLE audit_events (
                id INTEGER PRIMARY KEY AUTOINCREMENT, action TEXT NOT NULL,
                entity_type TEXT NOT NULL, entity_id INTEGER NOT NULL, actor TEXT NOT NULL,
                detail TEXT NOT NULL, previous_hash TEXT NOT NULL,
                entry_hash TEXT NOT NULL UNIQUE, created_at TEXT NOT NULL);
        """)
        now = "2026-01-01T00:00:00+00:00"
        conn.execute(
            "INSERT INTO items(title,description,severity,quantity,threshold,status,"
            "version,created_by,created_at,updated_at) VALUES(?,?,?,?,?,?,?,?,?,?)",
            ("old item", "old", "urgent", 5, 10, "draft", 1, "creator", now, now))
        prev = "GENESIS"
        for action, detail in [("create", {"title": "old item"}),
                               ("record", {"kind": "evidence"})]:
            ev = make_entry(action, "调度指令", 1, "creator", detail, prev)
            conn.execute(
                "INSERT INTO audit_events(action,entity_type,entity_id,actor,detail,"
                "previous_hash,entry_hash,created_at) VALUES(?,?,?,?,?,?,?,?)",
                (ev["action"], ev["entity_type"], ev["entity_id"], ev["actor"],
                 json.dumps(ev["detail"], ensure_ascii=False, sort_keys=True),
                 ev["previous_hash"], ev["entry_hash"], ev["created_at"]))
            prev = ev["entry_hash"]
        conn.commit()
        conn.close()

        # 打开旧库自动升级
        repo = Repository(old)
        # 哈希链保持不变
        self.assertTrue(repo.verify_audit_chain())
        # 历史审计按原顺序补上批次归属
        legacy = repo.list_audit()
        self.assertEqual(len(legacy), 2)
        self.assertTrue(all(e["batch_id"] for e in legacy))
        self.assertEqual(legacy[0]["batch_id"], "LEGACY-1-1")
        self.assertEqual(legacy[1]["batch_id"], "LEGACY-1-2")
        # 已生成遗留批次
        batches = {b["request_no"]: b for b in repo.list_batches()}
        self.assertIn("LEGACY-1-1", batches)
        self.assertIn("LEGACY-1-2", batches)
        self.assertEqual(batches["LEGACY-1-1"]["status"], "committed")
        self.assertEqual(batches["LEGACY-1-2"]["stage"], "legacy")

        # 升级只执行一次：重开后历史不重复分组，新审计不被归入遗留批次
        repo.close()
        repo2 = Repository(old)
        self.assertTrue(repo2.verify_audit_chain())
        svc = Service(repo2)
        item = svc.create_item(
            {"title": "new item", "description": "new", "severity": "routine",
             "external_ref": "NEW-1"},
            "creator", "duty_officer")
        new_events = repo2.list_audit(entity_id=item["id"])
        self.assertTrue(all(e["batch_id"] is None for e in new_events))
        repo2.close()


if __name__ == "__main__":
    unittest.main()
