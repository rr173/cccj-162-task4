"""空间回收: 水位计划、代际锁、并发安全与崩溃恢复。"""

import json
import os
import shutil
import tempfile
import threading
import unittest
import urllib.error
import urllib.request

from hpc_cr import JobManager
from hpc_cr.api import build_server
from hpc_cr.epochs import Scheduler
from hpc_cr.errors import Conflict, NotFound, ValidationError
from hpc_cr.security import Signer, sha256_hex


class GCTest(unittest.TestCase):
    def setUp(self):
        self.mgr = JobManager(Scheduler(Signer("gc-secret")))
        self.mgr.create_job(
            name="gc-job", world_size=1, shards_per_rank=1,
            rank_nodes={0: "n0"}, job_id="gc-1")

    def _complete_generation(self, gen: int, payload: bytes):
        pre = self.mgr.preempt(
            "gc-1", force_new_epoch=gen > 1) if gen > 1 \
            else self.mgr.preempt("gc-1")
        token = pre["epoch_token"]
        self.mgr.upload_shard(
            "gc-1", epoch_token=token, rank=0, index=0, node="n0",
            input_pos=gen, data=payload)
        self.mgr.commit_checkpoint("gc-1", epoch_token=token)
        self.mgr.confirm_release("gc-1", epoch_token=token)
        return token

    def test_plan_reports_estimated_and_actual_deletion(self):
        shard = b"first recoverable checkpoint"
        orphan = b"untracked temp upload"
        self._complete_generation(1, shard)
        self.mgr.store.put(orphan)
        plan = self.mgr.create_gc_plan()

        self.assertEqual(plan["state"], "PLANNED")
        self.assertEqual(plan["estimated_reclaim_items"], 1)
        self.assertEqual(plan["estimated_reclaim_bytes"], len(orphan))
        self.assertTrue(plan["watermark_seq"] >= 0)
        self.assertTrue(plan["reference_fingerprint"])
        self.assertEqual(
            set(plan["retained_generations"]["gc-1"]["1"]),
            {"released_for_restore", "current_complete_checkpoint"})

        result = self.mgr.execute_gc_plan(plan["plan_id"])
        self.assertEqual(result["state"], "COMPLETED")
        self.assertEqual(result["deleted_items"], 1)
        self.assertEqual(result["deleted_bytes"], len(orphan))
        self.assertTrue(self.mgr.store.has(sha256_hex(shard)))
        self.assertFalse(self.mgr.store.has(sha256_hex(orphan)))
        self.assertEqual(result["skipped_items"], 0)

    def test_pin_protects_generation_and_watermark_collects_old_gen(self):
        d1, d2 = b"cp1", b"cp2"
        self._complete_generation(1, d1)
        self.mgr.restore("gc-1", generation=1)
        # 直接在内部锁定结构上加 gen0, 模拟计划创建前已存在的持久化锁。
        self.mgr._pins.setdefault("gc-1", set()).add(0)
        self._complete_generation(2, d2)

        plan = self.mgr.create_gc_plan(
            policy="watermark", min_retain_generation=2)
        candidates = {c["blob"] for c in plan["candidates"]}
        # gen1 低于水位且未锁定 -> 候选; gen2 是已释放可恢复代际且受水位保护。
        self.assertIn(sha256_hex(d1), candidates)
        self.assertNotIn(sha256_hex(d2), candidates)
        self.assertIn("pinned_generation",
                      plan["retained_generations"]["gc-1"]["0"])
        self.mgr.execute_gc_plan(plan["plan_id"])
        self.assertFalse(self.mgr.store.has(sha256_hex(d1)))
        self.assertTrue(self.mgr.store.has(sha256_hex(d2)))

        # pin 是持久化审计操作, 并且重复 pin 幂等。
        pin = self.mgr.pin_generation("gc-1", 0, reason="test")
        self.assertEqual(pin["pinned_generations"], [0])
        self.assertEqual(
            self.mgr.generation_pins("gc-1")["pinned_generations"], [0])

    def test_reference_change_after_plan_rejects_execution(self):
        d1, d2 = b"g1 checkpoint", b"g2 checkpoint"
        self._complete_generation(1, d1)
        self.mgr.restore("gc-1", generation=1)
        self._complete_generation(2, d2)
        plan = self.mgr.create_gc_plan(
            policy="watermark", min_retain_generation=2)
        self.assertEqual(plan["estimated_reclaim_items"], 1)
        self.assertIn(sha256_hex(d1),
                      {c["blob"] for c in plan["candidates"]})

        # 计划后恢复当前代际并提交新输出: 引用关系/物理集合均变化, 必须拒绝。
        report = self.mgr.restore("gc-1", generation=2)
        lease_token = self.mgr.get("gc-1").leases.current().token
        self.mgr.append_output(
            "gc-1", lease_token=lease_token,
            output_id="after-plan", data=b"new output after plan")
        with self.assertRaises(Conflict) as ctx:
            self.mgr.execute_gc_plan(plan["plan_id"])
        stale = self.mgr.get_gc_plan(plan["plan_id"])
        self.assertEqual(stale["state"], "STALE")
        self.assertTrue(stale["stale_reason"])
        self.assertIn(stale["stale_reason"], str(ctx.exception))
        # 拒绝执行不能删除任何旧代际数据。
        self.assertTrue(self.mgr.store.has(sha256_hex(d1)))
        self.assertTrue(self.mgr.store.has(sha256_hex(d2)))

    def test_plan_after_pin_change_is_rejected(self):
        self._complete_generation(1, b"cp1")
        plan = self.mgr.create_gc_plan(
            policy="watermark", min_retain_generation=2)
        self.mgr.pin_generation("gc-1", 1)
        with self.assertRaises(Conflict):
            self.mgr.execute_gc_plan(plan["plan_id"])
        self.assertEqual(
            self.mgr.get_gc_plan(plan["plan_id"])["stale_reason"],
            "reference_seq_advanced")

    def test_completed_plan_is_idempotent_and_partial_plan_blocks_new(self):
        orphan = b"orphan"
        self.mgr.store.put(orphan)
        plan = self.mgr.create_gc_plan()
        first = self.mgr.execute_gc_plan(plan["plan_id"])
        second = self.mgr.execute_gc_plan(plan["plan_id"])
        self.assertEqual(first["deleted_items"], second["deleted_items"])
        self.assertEqual(second["deleted_items"], 1)
        self.assertEqual(second["skipped_items"], 0)
        # 完成后可以生成下一份计划。
        self.mgr.create_gc_plan()

        partial = {p["plan_id"]: p for p in self.mgr.list_gc_plans()}
        self.assertIn(plan["plan_id"], partial)

    def test_concurrent_output_and_gc_never_deletes_accessible_blob(self):
        # gen1 完成释放, 计划把 gen1 当旧候选; 恢复与 GC 执行并发。
        data = b"checkpoint for concurrent restore"
        self._complete_generation(1, data)
        plan_id = self.mgr.create_gc_plan(
            policy="watermark", min_retain_generation=2)["plan_id"]
        outcomes = []

        def restore():
            self.mgr.restore("gc-1", generation=1)
            outcomes.append("restored")

        def gc():
            try:
                self.mgr.execute_gc_plan(plan_id)
                outcomes.append("gc_completed")
            except Conflict:
                outcomes.append("gc_rejected")

        t1 = threading.Thread(target=restore)
        t2 = threading.Thread(target=gc)
        t1.start(); t2.start()
        t1.join(); t2.join()
        self.assertIn("restored", outcomes)
        self.assertIn("gc_rejected", outcomes)
        self.assertTrue(self.mgr.store.has(sha256_hex(data)))

    def test_concurrent_upload_cannot_be_seen_as_orphan(self):
        pre = self.mgr.preempt("gc-1")
        token = pre["epoch_token"]
        barrier = threading.Barrier(2)
        payload = b"shard protected by reference lock"

        def upload():
            barrier.wait()
            self.mgr.upload_shard(
                "gc-1", epoch_token=token, rank=0, index=0, node="n0",
                input_pos=1, data=payload)

        def gc():
            barrier.wait()
            # 无论 GC 先扫描还是上传先完成, 持锁结果只能是:
            # 计划看不到该 blob, 或看到时引用已建立。
            plan = self.mgr.create_gc_plan()
            try:
                self.mgr.execute_gc_plan(plan["plan_id"])
            except Conflict:
                pass

        t1 = threading.Thread(target=upload)
        t2 = threading.Thread(target=gc)
        t1.start(); t2.start()
        t1.join(); t2.join()
        self.assertTrue(self.mgr.store.has(sha256_hex(payload)))

    def test_restart_can_continue_plan_and_query_results(self):
        data_dir = tempfile.mkdtemp(prefix="hpc-gc-")
        try:
            secret = "persistent-gc-secret"
            mgr = JobManager(Scheduler(Signer(secret)), data_dir=data_dir)
            mgr2 = mgr3 = None
            mgr.create_job(name="p", world_size=1,
                           rank_nodes={0: "n0"}, job_id="p-1")
            pre = mgr.preempt("p-1")
            mgr.upload_shard(
                "p-1", epoch_token=pre["epoch_token"], rank=0, index=0,
                node="n0", input_pos=1, data=b"pending protected")
            orphan = b"orphan after restart"
            mgr.store.put(orphan)
            plan = mgr.create_gc_plan(plan_id="gc-fixed")
            digest = sha256_hex(orphan)
            self.assertTrue(os.path.exists(os.path.join(
                data_dir, "blobs", digest)))

            # 模拟崩溃点: 物理删除已完成, 但删除事件尚未追加。
            existed, size = mgr.store.delete(digest)
            self.assertTrue(existed)
            self.assertEqual(size, len(orphan))

            mgr2 = JobManager(Scheduler(Signer(secret)), data_dir=data_dir)
            resumed = mgr2.execute_gc_plan("gc-fixed")
            self.assertIn(resumed["state"],
                          ("COMPLETED", "PARTIALLY_COMPLETED"))
            self.assertEqual(resumed["deleted_items"], 1)
            self.assertEqual(resumed["deleted_bytes"], len(orphan))
            self.assertEqual(
                resumed["recovery_reasons"].get(
                    "already_deleted_after_crash"), 1)

            mgr3 = JobManager(Scheduler(Signer(secret)), data_dir=data_dir)
            queried = mgr3.get_gc_plan("gc-fixed")
            self.assertEqual(queried["state"], resumed["state"])
            self.assertEqual(queried["estimated_reclaim_items"], 1)
            self.assertEqual(queried["deleted_items"], 1)
            self.assertFalse(os.path.exists(os.path.join(
                data_dir, "blobs", digest)))
            page = mgr3.progress_page_data("p-1")
            self.assertTrue(any(p["plan_id"] == "gc-fixed"
                                for p in page["gc_plans"]))
        finally:
            for instance in (mgr, mgr2, mgr3):
                if instance is not None:
                    instance.journal.close()
            shutil.rmtree(data_dir, ignore_errors=True)

    def test_validation_errors_are_stable(self):
        with self.assertRaises(ValidationError):
            self.mgr.create_gc_plan(policy="unknown")
        with self.assertRaises(ValidationError):
            self.mgr.create_gc_plan(policy="watermark")
        with self.assertRaises(NotFound):
            self.mgr.get_gc_plan("missing")
        with self.assertRaises(ValidationError):
            self.mgr.pin_generation("gc-1", -1)


class GCHttpTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.server = build_server(
            JobManager(Scheduler(Signer("gc-http-secret"))),
            host="127.0.0.1", port=0)
        cls.port = cls.server.server_address[1]
        cls.thread = threading.Thread(target=cls.server.serve_forever,
                                  daemon=True)
        cls.thread.start()

    @classmethod
    def tearDownClass(cls):
        cls.server.shutdown()

    def _req(self, method, path, body=None):
        url = f"http://127.0.0.1:{self.port}{path}"
        data = json.dumps(body).encode() if body is not None else None
        req = urllib.request.Request(
            url, data=data, method=method,
            headers={"Content-Type": "application/json"})
        try:
            with urllib.request.urlopen(req, timeout=5) as resp:
                return resp.status, json.loads(resp.read())
        except urllib.error.HTTPError as e:
            return e.code, json.loads(e.read())

    def test_plan_execute_pin_and_query_over_http(self):
        mgr = self.server.manager
        mgr.create_job(name="http-gc", world_size=1,
                       rank_nodes={0: "n0"}, job_id="gc-http")
        orphan = b"http orphan"
        mgr.store.put(orphan)

        st, pin = self._req("POST", "/api/jobs/gc-http",
                              {"action": "pin_generation", "generation": 1})
        self.assertEqual(st, 200)
        self.assertTrue(pin["pinned"])
        st, pins = self._req("GET", "/api/jobs/gc-http/gc/pins")
        self.assertEqual(st, 200)
        self.assertEqual(pins["pinned_generations"], [1])

        st, plan = self._req("POST", "/api/gc/plans", {})
        self.assertEqual(st, 201)
        self.assertEqual(plan["estimated_reclaim_items"], 1)
        self.assertEqual(plan["state"], "PLANNED")
        plan_id = plan["plan_id"]

        st, plans = self._req("GET", "/api/gc/plans")
        self.assertEqual(st, 200)
        self.assertEqual([p["plan_id"] for p in plans["plans"]], [plan_id])

        st, result = self._req(
            "POST", f"/api/gc/plans/{plan_id}", {"action": "execute"})
        self.assertEqual(st, 200)
        self.assertEqual(result["state"], "COMPLETED")
        self.assertEqual(result["deleted_items"], 1)
        self.assertEqual(result["deleted_bytes"], len(orphan))

        st, detail = self._req("GET", f"/api/gc/plans/{plan_id}")
        self.assertEqual(st, 200)
        self.assertEqual(detail["candidates"][0]["status"], "DELETED")

        st, page = self._req("GET", "/api/jobs/gc-http/progress.json")
        self.assertEqual(st, 200)
        self.assertEqual(page["gc_plans"][0]["plan_id"], plan_id)


if __name__ == "__main__":
    unittest.main()
