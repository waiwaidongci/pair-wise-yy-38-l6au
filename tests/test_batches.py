import json
import sqlite3
import tempfile
import threading
import unittest
from pathlib import Path

from src.audit import make_entry
from src.domain import ConflictError
from src.repository import Repository
from src.rules import ENTITY
from src.service import Service


class BatchTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.repo = Repository(str(Path(self.tmp.name) / "test.db"))
        self.service = Service(self.repo)
        self.item = self.service.create_item(
            {"title": "batch item", "description": "batch scenarios",
             "severity": "urgent", "quantity": 5, "threshold": 10,
             "external_ref": "BATCH-1"}, "creator", "duty_officer")

    def tearDown(self):
        self.repo.close()
        self.tmp.cleanup()

    def test_transition_replay_same_request_id(self):
        first = self.service.transition(self.item["id"], "checked", 1, "rev",
                                        "duty_officer", request_id="T-1")
        second = self.service.transition(self.item["id"], "checked", 1, "rev",
                                         "duty_officer", request_id="T-1")
        self.assertEqual(first, second)
        self.assertEqual(second["version"], 2)
        events = [e for e in self.repo.list_audit(self.item["id"])
                  if e["action"] == "transition"]
        self.assertEqual(len(events), 1)
        self.assertEqual(events[0]["batch_request_id"], "T-1")

    def test_concurrent_transition_first_commit_wins(self):
        outcomes = []
        barrier = threading.Barrier(5)

        def worker(i):
            barrier.wait()
            try:
                self.service.transition(self.item["id"], "checked", 1,
                                        "actor-%d" % i, "duty_officer",
                                        request_id="RACE-%d" % i)
                outcomes.append("committed")
            except ConflictError:
                outcomes.append("conflict")

        threads = [threading.Thread(target=worker, args=(i,)) for i in range(5)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()
        self.assertEqual(outcomes.count("committed"), 1)
        self.assertEqual(outcomes.count("conflict"), 4)
        item = self.repo.get_item(self.item["id"])
        self.assertEqual(item["version"], 2)
        self.assertEqual(item["status"], "checked")
        batches = self.repo.list_batches(self.item["id"])
        self.assertEqual(sum(1 for b in batches if b["status"] == "committed"
                             and b["action"] == "transition"), 1)
        self.assertEqual(sum(1 for b in batches if b["status"] == "conflict"), 4)
        events = [e for e in self.repo.list_audit(self.item["id"])
                  if e["action"] == "transition"]
        self.assertEqual(len(events), 1)

    def test_pending_batch_recovers_by_request_id_without_duplicate_audit(self):
        # 模拟后半程失败：批次已登记但执行中断，留下待恢复项
        self.repo.begin_batch("REQ-7", self.item["id"], "transition", "rev",
                              {"target": "checked", "expected_version": 1,
                               "actor": "rev"}, 1)
        recovered = self.service.transition(self.item["id"], "checked", 1, "rev",
                                            "duty_officer", request_id="REQ-7")
        self.assertEqual(recovered["status"], "checked")
        self.assertEqual(recovered["version"], 2)
        again = self.service.transition(self.item["id"], "checked", 1, "rev",
                                        "duty_officer", request_id="REQ-7")
        self.assertEqual(again, recovered)
        events = [e for e in self.repo.list_audit(self.item["id"])
                  if e["action"] == "transition"]
        self.assertEqual(len(events), 1)
        batch = self.repo.find_batch("REQ-7")
        self.assertEqual(batch["status"], "committed")

    def test_record_replay_same_request_id(self):
        payload = {"kind": "feedback", "detail": "onsite feedback",
                   "status": "open", "external_ref": "FB-1"}
        first = self.service.add_record(self.item["id"], payload, "op",
                                        "duty_officer", request_id="REC-1")
        second = self.service.add_record(self.item["id"], payload, "op",
                                         "duty_officer", request_id="REC-1")
        self.assertEqual(first, second)
        self.assertEqual(len(self.repo.list_records(self.item["id"])), 1)
        events = [e for e in self.repo.list_audit(self.item["id"])
                  if e["action"] == "record"]
        self.assertEqual(len(events), 1)

    def test_record_duplicate_ref_marks_conflict_and_replays(self):
        payload = {"kind": "feedback", "detail": "dup", "status": "open",
                   "external_ref": "FB-2"}
        self.service.add_record(self.item["id"], payload, "op", "duty_officer",
                                request_id="REC-2")
        with self.assertRaises(ConflictError):
            self.service.add_record(self.item["id"], payload, "op",
                                    "duty_officer", request_id="REC-3")
        batch = self.repo.find_batch("REC-3")
        self.assertEqual(batch["status"], "conflict")
        with self.assertRaises(ConflictError):
            self.service.add_record(self.item["id"], payload, "op",
                                    "duty_officer", request_id="REC-3")
        events = [e for e in self.repo.list_audit(self.item["id"])
                  if e["action"] == "record"]
        self.assertEqual(len(events), 1)

    def test_create_replay_same_request_id(self):
        payload = {"title": "create replay", "description": "idem",
                   "severity": "attention", "quantity": 1, "threshold": 2,
                   "external_ref": "CR-1"}
        first = self.service.create_item(payload, "op", "duty_officer",
                                         request_id="C-1")
        second = self.service.create_item(payload, "op", "duty_officer",
                                          request_id="C-1")
        self.assertEqual(first, second)
        self.assertEqual(len(self.repo.list_items()), 2)
        events = [e for e in self.repo.list_audit(first["id"])
                  if e["action"] == "create"]
        self.assertEqual(len(events), 1)

    def test_request_id_bound_to_original_payload(self):
        self.service.transition(self.item["id"], "checked", 1, "rev",
                                "duty_officer", request_id="M-1")
        with self.assertRaises(ConflictError):
            self.service.transition(self.item["id"], "checked", 2, "rev",
                                    "duty_officer", request_id="M-1")

    def test_batch_list_distinguishes_progress_conflict_and_recovery(self):
        self.service.transition(self.item["id"], "checked", 1, "rev",
                                "duty_officer", request_id="L-OK")
        with self.assertRaises(ConflictError):
            self.service.transition(self.item["id"], "authorized", 1, "chief",
                                    "chief_engineer", request_id="L-LOST")
        self.repo.begin_batch("L-PENDING", self.item["id"], "record", "op",
                              {"kind": "k"})
        result = self.service.list_batches("viewer", item_id=self.item["id"])
        batches = {b["request_id"]: b for b in result["batches"]}
        self.assertEqual(batches["L-OK"]["category"], "progress")
        self.assertEqual(batches["L-OK"]["result"]["version"], 2)
        self.assertEqual(batches["L-LOST"]["category"], "conflict")
        self.assertEqual(batches["L-PENDING"]["category"], "recovery")
        self.assertTrue(batches["L-PENDING"]["recovery_required"])
        summary = result["summary"]
        self.assertEqual(summary["committed"], 2)
        self.assertEqual(summary["conflict"], 1)
        self.assertEqual(summary["pending"], 1)
        self.assertEqual(summary["recovery_required"], 1)


class MigrationTest(unittest.TestCase):
    def test_legacy_audit_rows_get_batch_attribution_in_order(self):
        tmp = tempfile.TemporaryDirectory()
        db_path = str(Path(tmp.name) / "legacy.db")
        conn = sqlite3.connect(db_path)
        conn.row_factory = sqlite3.Row
        conn.executescript("""
            CREATE TABLE items (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                title TEXT NOT NULL,
                description TEXT NOT NULL,
                severity TEXT NOT NULL,
                quantity REAL NOT NULL DEFAULT 0,
                threshold REAL NOT NULL DEFAULT 1,
                status TEXT NOT NULL,
                version INTEGER NOT NULL DEFAULT 1,
                external_ref TEXT,
                created_by TEXT NOT NULL,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL
            );
            CREATE TABLE audit_events (
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
        """)
        conn.execute(
            """INSERT INTO items(title, description, severity, quantity, threshold,
               status, version, external_ref, created_by, created_at, updated_at)
               VALUES('legacy item','legacy','urgent',5,10,'draft',1,'LEG-1','op',
               '2026-09-01T00:00:00+00:00','2026-09-01T00:00:00+00:00')""")
        previous = "GENESIS"
        steps = [("create", "duty-1"), ("record", "duty-2"), ("transition", "chief-1")]
        for index, (action, actor) in enumerate(steps, start=1):
            event = make_entry(action, ENTITY, 1, actor, {"step": index}, previous)
            conn.execute(
                """INSERT INTO audit_events(action, entity_type, entity_id, actor,
                   detail, previous_hash, entry_hash, created_at)
                   VALUES(?,?,?,?,?,?,?,?)""",
                (event["action"], event["entity_type"], event["entity_id"],
                 event["actor"],
                 json.dumps(event["detail"], ensure_ascii=False, sort_keys=True),
                 event["previous_hash"], event["entry_hash"], event["created_at"]))
            previous = event["entry_hash"]
        conn.commit()
        conn.close()

        repo = Repository(db_path)
        try:
            events = repo.list_audit()
            self.assertEqual(len(events), 3)
            self.assertEqual([e["batch_request_id"] for e in events],
                             ["legacy-audit-1", "legacy-audit-2", "legacy-audit-3"])
            batches = sorted(repo.list_batches(), key=lambda b: b["id"])
            self.assertEqual([b["action"] for b in batches],
                             ["create", "record", "transition"])
            self.assertEqual([b["actor"] for b in batches],
                             ["duty-1", "duty-2", "chief-1"])
            self.assertTrue(all(b["status"] == "committed" for b in batches))
            self.assertEqual([b["created_at"] for b in batches],
                             [e["created_at"] for e in events])
            self.assertTrue(repo.verify_audit_chain())
            # 迁移只跑一次：再次打开不会重复补批次
            repo.close()
            repo = Repository(db_path)
            self.assertEqual(len(repo.list_batches()), 3)
            self.assertTrue(repo.verify_audit_chain())
        finally:
            repo.close()
            tmp.cleanup()


if __name__ == "__main__":
    unittest.main()
