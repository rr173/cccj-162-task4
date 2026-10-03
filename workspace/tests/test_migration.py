"""可恢复检查点迁移: 冻结清单、断点续传、原子发布、保留释放与 GC 联动。"""

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
from hpc_cr.storage import ContentStore


class MigrationTestBase(unittest.TestCase):
    def setUp(self):
        self.dest = ContentStore()
        self.mgr = JobManager(Scheduler(Signer("mig-secret")),
                              destinations={"archive": self.dest})
        self.mgr.create_job(
            name="mig-job", world_size=2, shards_per_rank=2,
            rank_nodes={0: "n0", 1: "n1"}, job_id="mig-1")

    def _complete_generation(self, gen: int, payloads: dict, *,
                             outputs: dict | None = None):
        """完成一代检查点并释放; payloads: {(rank, index): bytes}"""
        pre = self.mgr.preempt("mig-1", force_new_epoch=gen > 1)
        token = pre["epoch_token"]
        for (rank, index), data in payloads.items():
            self.mgr.upload_shard(
                "mig-1", epoch_token=token, rank=rank, index=index,
                node=f"n{rank}", input_pos=100 + rank * 10 + index,
                data=data)
        self.mgr.commit_checkpoint("mig-1", epoch_token=token)
        self.mgr.confirm_release("mig-1", epoch_token=token)
        if outputs:
            self.mgr.restore("mig-1", generation=gen)
            lease = self.mgr.get("mig-1").leases.current().token
            for output_id, data in outputs.items():
                self.mgr.append_output(
                    "mig-1", lease_token=lease, output_id=output_id,
                    data=data)
        return token

    def _gen1_payloads(self):
        return {(r, i): f"gen1-r{r}-s{i}".encode()
                for r in range(2) for i in range(2)}


class MigrationFlowTest(MigrationTestBase):
    def test_create_freezes_manifest_and_run_publishes_atomically(self):
        payloads = self._gen1_payloads()
        self._complete_generation(
            1, payloads, outputs={"out-1": b"final output 1"})

        task = self.mgr.create_migration(
            "mig-1", generation=1, destination="archive")
        self.assertEqual(task["state"], "PLANNED")
        self.assertFalse(task["idempotent_replay"])
        # 4 个分片 + 1 个输出 = 5 个清单项 (内容互不相同)
        self.assertEqual(task["planned_items"], 5)
        expected_bytes = sum(len(p) for p in payloads.values()) + \
            len(b"final output 1")
        self.assertEqual(task["planned_bytes"], expected_bytes)
        self.assertTrue(task["manifest_digest"])
        # 创建即冻结: 尚未传输, 目标存储为空, 无发布记录
        self.assertEqual(self.dest.blob_count(), 0)
        self.assertEqual(self.mgr.published_generations(), [])

        done = self.mgr.run_migration(task["task_id"])
        self.assertEqual(done["state"], "PUBLISHED")
        self.assertEqual(done["transferred_items"], 5)
        self.assertEqual(done["transferred_bytes"], expected_bytes)
        self.assertEqual(done["reused_items"], 0)
        self.assertEqual(done["failed_items"], 0)
        self.assertEqual(done["pending_items"], 0)
        # 全部载荷到达目标存储且可校验
        for data in list(payloads.values()) + [b"final output 1"]:
            digest = sha256_hex(data)
            self.assertTrue(self.dest.verify(digest))
        # 原子发布: 恰好一条可恢复代际记录, 内容完整
        published = self.mgr.published_generations("mig-1")
        self.assertEqual(len(published), 1)
        rec = published[0]
        self.assertEqual(rec["generation"], 1)
        self.assertEqual(rec["destination"], "archive")
        self.assertEqual(rec["manifest_digest"], task["manifest_digest"])
        self.assertEqual(len(rec["items"]), 5)
        # 已发布任务重复 run 幂等
        again = self.mgr.run_migration(task["task_id"])
        self.assertEqual(again["state"], "PUBLISHED")
        self.assertEqual(again["transferred_items"], 5)

    def test_duplicate_create_and_run_continue_same_task(self):
        self._complete_generation(1, self._gen1_payloads())
        fixed = self.mgr.create_migration(
            "mig-1", generation=1, destination="archive",
            task_id="mig-fixed")
        self.assertFalse(fixed["idempotent_replay"])
        # 同 (作业, 代际, 目标) 的重复创建返回同一活动任务
        dup = self.mgr.create_migration(
            "mig-1", generation=1, destination="archive")
        self.assertTrue(dup["idempotent_replay"])
        self.assertEqual(dup["task_id"], "mig-fixed")
        # 显式 task_id 重投同样幂等
        refired = self.mgr.create_migration(
            "mig-1", generation=1, destination="archive",
            task_id="mig-fixed")
        self.assertTrue(refired["idempotent_replay"])
        self.assertEqual(len(self.mgr.list_migrations()), 1)
        # task_id 不能绑定到不同的三元组
        self.mgr.register_destination("other", ContentStore())
        with self.assertRaises(Conflict):
            self.mgr.create_migration(
                "mig-1", generation=1, destination="other",
                task_id="mig-fixed")

        # 分块运行: 中断后继续同一任务
        part = self.mgr.run_migration("mig-fixed", max_items=2)
        self.assertEqual(part["state"], "TRANSFERRING")
        self.assertEqual(part["transferred_items"], 2)
        self.assertEqual(part["pending_items"], 2)
        rest = self.mgr.run_migration("mig-fixed")
        self.assertEqual(rest["state"], "PUBLISHED")
        self.assertEqual(rest["transferred_items"], 4)
        # 重复 run 已发布任务不再改变计数
        self.assertEqual(
            self.mgr.run_migration("mig-fixed")["transferred_items"], 4)

    def test_receiver_reuses_existing_objects_by_digest(self):
        payloads = self._gen1_payloads()
        self._complete_generation(1, payloads)
        # 目标存储预先持有部分内容 (上次搬迁/其他渠道已送达)
        reused_blob, _ = sha256_hex(payloads[(0, 0)]), None
        self.dest.put(payloads[(0, 0)])
        self.dest.put(payloads[(1, 1)])

        task = self.mgr.create_migration(
            "mig-1", generation=1, destination="archive")
        done = self.mgr.run_migration(task["task_id"])
        self.assertEqual(done["state"], "PUBLISHED")
        self.assertEqual(done["reused_items"], 2)
        self.assertEqual(done["transferred_items"], 2)
        statuses = sorted(it["status"] for it in done["items"])
        self.assertEqual(statuses,
                         ["REUSED", "REUSED", "TRANSFERRED", "TRANSFERRED"])

        # 第二个任务 (同代际同目标已发布, 允许新建) 全部复用
        task2 = self.mgr.create_migration(
            "mig-1", generation=1, destination="archive")
        done2 = self.mgr.run_migration(task2["task_id"])
        self.assertEqual(done2["transferred_items"], 0)
        self.assertEqual(done2["reused_items"], 4)

    def test_manifest_change_prevents_publish(self):
        payloads = self._gen1_payloads()
        self._complete_generation(
            1, payloads, outputs={"out-1": b"output before freeze"})
        task = self.mgr.create_migration(
            "mig-1", generation=1, destination="archive")
        # 冻结后清单变化: 同代际追加新输出 (作业仍处于恢复运行态)
        lease = self.mgr.get("mig-1").leases.current().token
        self.mgr.append_output(
            "mig-1", lease_token=lease, output_id="out-2",
            data=b"output after freeze")

        done = self.mgr.run_migration(task["task_id"])
        self.assertEqual(done["state"], "FAILED")
        self.assertEqual(done["state_reason"], "manifest_changed")
        self.assertEqual(self.mgr.published_generations(), [])
        # 临时保留已释放, 失败任务清理了目标端新写入的 blob
        self.assertEqual(self.mgr._migration_holds, {})
        self.assertEqual(self.dest.blob_count(), 0)
        self.assertEqual(done["cleanup"]["deleted_items"], 5)
        # 失败任务不能继续 run; 新任务可重新创建并完成
        with self.assertRaises(Conflict):
            self.mgr.run_migration(task["task_id"])
        retry = self.mgr.create_migration(
            "mig-1", generation=1, destination="archive")
        self.assertEqual(retry["planned_items"], 6)  # 含 out-2
        self.assertEqual(
            self.mgr.run_migration(retry["task_id"])["state"], "PUBLISHED")

    def test_digest_error_prevents_publish(self):
        payloads = self._gen1_payloads()
        self._complete_generation(1, payloads)
        task = self.mgr.create_migration(
            "mig-1", generation=1, destination="archive")
        # 源端 bit-rot: 摘要不符, 绝不能产生可恢复代际
        victim = sha256_hex(payloads[(1, 0)])
        self.mgr.store.corrupt(victim)

        done = self.mgr.run_migration(task["task_id"])
        self.assertEqual(done["state"], "FAILED")
        self.assertEqual(done["state_reason"], "source_digest_mismatch")
        self.assertEqual(done["failed_items"], 1)
        failed = [it for it in done["items"] if it["status"] == "FAILED"]
        self.assertEqual([it["blob"] for it in failed], [victim])
        self.assertEqual(self.mgr.published_generations(), [])
        # 已传输项被清理, 目标存储不留半成品
        self.assertEqual(self.dest.blob_count(), 0)
        self.assertEqual(self.mgr._migration_holds, {})

    def test_missing_source_blob_fails_task(self):
        payloads = self._gen1_payloads()
        self._complete_generation(1, payloads)
        task = self.mgr.create_migration(
            "mig-1", generation=1, destination="archive")
        victim = sha256_hex(payloads[(0, 1)])
        self.mgr.store.delete(victim)
        done = self.mgr.run_migration(task["task_id"])
        self.assertEqual(done["state"], "FAILED")
        self.assertEqual(done["state_reason"], "source_missing")
        self.assertEqual(self.mgr.published_generations(), [])

    def test_cancel_releases_hold_and_cleans_destination(self):
        payloads = self._gen1_payloads()
        self._complete_generation(1, payloads)
        task = self.mgr.create_migration(
            "mig-1", generation=1, destination="archive")
        self.mgr.run_migration(task["task_id"], max_items=2)
        self.assertEqual(self.dest.blob_count(), 2)
        self.assertTrue(self.mgr._migration_holds)

        cancelled = self.mgr.cancel_migration(task["task_id"],
                                              reason="cluster_offline")
        self.assertEqual(cancelled["state"], "CANCELLED")
        self.assertEqual(cancelled["cancel_reason"], "cluster_offline")
        self.assertEqual(cancelled["cleanup"]["deleted_items"], 2)
        self.assertEqual(self.dest.blob_count(), 0)
        self.assertEqual(self.mgr._migration_holds, {})
        self.assertEqual(self.mgr.published_generations(), [])
        # 重复取消幂等; 已取消任务不能 run
        again = self.mgr.cancel_migration(task["task_id"])
        self.assertEqual(again["state"], "CANCELLED")
        with self.assertRaises(Conflict):
            self.mgr.run_migration(task["task_id"])
        # 取消后可重新创建同代际任务
        retry = self.mgr.create_migration(
            "mig-1", generation=1, destination="archive")
        self.assertFalse(retry["idempotent_replay"])

    def test_validation_and_state_errors(self):
        self._complete_generation(1, self._gen1_payloads())
        with self.assertRaises(ValidationError):
            self.mgr.create_migration(
                "mig-1", generation=1, destination="nowhere")
        with self.assertRaises(NotFound):
            self.mgr.create_migration("mig-1", generation=9,
                                      destination="archive")
        # 只有 COMPLETE 检查点可迁移: gen2 只开不交
        self.mgr.restore("mig-1", generation=1)
        self.mgr.preempt("mig-1", force_new_epoch=True)
        with self.assertRaises(Conflict):
            self.mgr.create_migration("mig-1", generation=2,
                                      destination="archive")
        with self.assertRaises(NotFound):
            self.mgr.get_migration("mig-missing")
        with self.assertRaises(NotFound):
            self.mgr.run_migration("mig-missing")
        with self.assertRaises(ValidationError):
            self.mgr.register_destination("", ContentStore())


class MigrationGCTest(MigrationTestBase):
    """迁移临时保留与空间回收的联动。"""

    def _make_collectable_gen1(self):
        """gen1 完成并恢复, 再完成 gen2: gen1 成为可回收历史代际。"""
        p1 = self._gen1_payloads()
        self._complete_generation(1, p1)
        self.mgr.restore("mig-1", generation=1)
        p2 = {(r, i): f"gen2-r{r}-s{i}".encode()
              for r in range(2) for i in range(2)}
        self._complete_generation(2, p2)
        return p1

    def test_hold_protects_payloads_from_gc(self):
        payloads = self._make_collectable_gen1()
        task = self.mgr.create_migration(
            "mig-1", generation=1, destination="archive")
        # 迁移持有期间: gen1 的 blob 不进入 GC 候选
        plan = self.mgr.create_gc_plan(
            policy="watermark", min_retain_generation=2)
        held = {sha256_hex(p) for p in payloads.values()}
        candidates = {c["blob"] for c in plan["candidates"]}
        self.assertFalse(held & candidates)
        self.assertTrue(
            set(plan["migration_holds"]) == held)
        result = self.mgr.execute_gc_plan(plan["plan_id"])
        self.assertEqual(result["state"], "COMPLETED")
        for p in payloads.values():
            self.assertTrue(self.mgr.store.has(sha256_hex(p)))

        # 迁移完成后保留释放: 新计划可以回收 gen1
        self.mgr.run_migration(task["task_id"])
        self.assertEqual(self.mgr._migration_holds, {})
        plan2 = self.mgr.create_gc_plan(
            policy="watermark", min_retain_generation=2)
        self.assertTrue(held & {c["blob"] for c in plan2["candidates"]})
        self.mgr.execute_gc_plan(plan2["plan_id"])
        for p in payloads.values():
            self.assertFalse(self.mgr.store.has(sha256_hex(p)))

    def test_existing_gc_plan_goes_stale_when_migration_starts(self):
        self._make_collectable_gen1()
        plan = self.mgr.create_gc_plan(
            policy="watermark", min_retain_generation=2)
        self.mgr.create_migration("mig-1", generation=1,
                                  destination="archive")
        with self.assertRaises(Conflict):
            self.mgr.execute_gc_plan(plan["plan_id"])
        self.assertEqual(
            self.mgr.get_gc_plan(plan["plan_id"])["state"], "STALE")

    def test_failed_migration_does_not_block_gc(self):
        payloads = self._make_collectable_gen1()
        task = self.mgr.create_migration(
            "mig-1", generation=1, destination="archive")
        self.mgr.store.corrupt(sha256_hex(payloads[(0, 0)]))
        result = self.mgr.run_migration(task["task_id"])
        self.assertEqual(result["state"], "FAILED")
        # 保留已释放: GC 计划立即可生成 (损坏 blob 仍是物理存在, 可被回收)
        plan = self.mgr.create_gc_plan(
            policy="watermark", min_retain_generation=2)
        self.assertEqual(plan["state"], "PLANNED")
        self.mgr.execute_gc_plan(plan["plan_id"])

    def test_concurrent_gc_and_migration_never_deletes_held_blob(self):
        payloads = self._make_collectable_gen1()
        task = self.mgr.create_migration(
            "mig-1", generation=1, destination="archive")
        barrier = threading.Barrier(2)
        outcomes = []

        def gc():
            barrier.wait()
            try:
                plan = self.mgr.create_gc_plan(
                    policy="watermark", min_retain_generation=2)
                self.mgr.execute_gc_plan(plan["plan_id"])
                outcomes.append("gc_completed")
            except Conflict:
                outcomes.append("gc_rejected")

        def migrate():
            barrier.wait()
            self.mgr.run_migration(task["task_id"])
            outcomes.append("migrated")

        t1, t2 = threading.Thread(target=gc), threading.Thread(target=migrate)
        t1.start(); t2.start()
        t1.join(); t2.join()
        self.assertIn("migrated", outcomes)
        # 无论交错如何: 迁移已发布, 目标端内容完整可校验
        self.assertEqual(
            self.mgr.get_migration(task["task_id"])["state"], "PUBLISHED")
        for p in payloads.values():
            self.assertTrue(self.dest.verify(sha256_hex(p)))


class MigrationRestartTest(unittest.TestCase):
    def test_restart_resumes_same_task_and_publishes_once(self):
        data_dir = tempfile.mkdtemp(prefix="hpc-mig-")
        dest_dir = tempfile.mkdtemp(prefix="hpc-mig-dest-")
        secret = "mig-restart-secret"
        mgr = mgr2 = mgr3 = None
        try:
            dest = ContentStore(dest_dir)
            mgr = JobManager(Scheduler(Signer(secret)), data_dir=data_dir,
                             destinations={"archive": dest})
            mgr.create_job(name="p", world_size=2, shards_per_rank=1,
                           rank_nodes={0: "n0", 1: "n1"}, job_id="p-1")
            pre = mgr.preempt("p-1")
            token = pre["epoch_token"]
            for r in range(2):
                mgr.upload_shard(
                    "p-1", epoch_token=token, rank=r, index=0,
                    node=f"n{r}", input_pos=7 + r,
                    data=f"payload-r{r}".encode())
            mgr.commit_checkpoint("p-1", epoch_token=token)
            mgr.confirm_release("p-1", epoch_token=token)

            task = mgr.create_migration("p-1", generation=1,
                                        destination="archive",
                                        task_id="mig-restart")
            part = mgr.run_migration("mig-restart", max_items=1)
            self.assertEqual(part["state"], "TRANSFERRING")
            self.assertEqual(part["transferred_items"], 1)
            # 模拟崩溃窗口: 第二项已物理写入目标存储, 但传输事件未落盘。
            pending = [it for it in part["items"]
                       if it["status"] == "PENDING"]
            self.assertEqual(len(pending), 1)
            for r in range(2):
                data = f"payload-r{r}".encode()
                if sha256_hex(data) == pending[0]["blob"]:
                    ContentStore(dest_dir).put(data)
            mgr.journal.close()

            # 服务重启: 重放后继续同一任务 (目标存储同目录重新注册)
            mgr2 = JobManager(Scheduler(Signer(secret)), data_dir=data_dir,
                              destinations={"archive": ContentStore(dest_dir)})
            resumed_view = mgr2.get_migration("mig-restart")
            self.assertEqual(resumed_view["state"], "TRANSFERRING")
            self.assertEqual(resumed_view["transferred_items"], 1)
            done = mgr2.run_migration("mig-restart")
            self.assertEqual(done["state"], "PUBLISHED")
            # 已落盘事件的第一项保持 TRANSFERRED; 崩溃窗口写入的第二项
            # 在目标端去重 -> 记为复用, 任务仍是同一个
            self.assertEqual(done["transferred_items"], 1)
            self.assertEqual(done["reused_items"], 1)
            self.assertEqual(len(mgr2.published_generations("p-1")), 1)
            mgr2.journal.close()

            # 再次重启: 终态与发布记录仍可查询, 重复 run 幂等
            mgr3 = JobManager(Scheduler(Signer(secret)), data_dir=data_dir,
                              destinations={"archive": ContentStore(dest_dir)})
            view = mgr3.get_migration("mig-restart")
            self.assertEqual(view["state"], "PUBLISHED")
            self.assertEqual(view["transferred_items"], 1)
            self.assertEqual(view["reused_items"], 1)
            self.assertEqual(len(mgr3.published_generations("p-1")), 1)
            self.assertEqual(
                mgr3.run_migration("mig-restart")["state"], "PUBLISHED")
            page = mgr3.progress_page_data("p-1")
            self.assertEqual(page["migrations"][0]["task_id"], "mig-restart")
            self.assertEqual(len(page["published_migrations"]), 1)
            # 目标存储内容完整
            dest_check = ContentStore(dest_dir)
            for r in range(2):
                self.assertTrue(
                    dest_check.verify(sha256_hex(f"payload-r{r}".encode())))
        finally:
            for instance in (mgr, mgr2, mgr3):
                if instance is not None:
                    instance.journal.close()
            shutil.rmtree(data_dir, ignore_errors=True)
            shutil.rmtree(dest_dir, ignore_errors=True)

    def test_restart_after_cancel_keeps_hold_released(self):
        data_dir = tempfile.mkdtemp(prefix="hpc-mig-cancel-")
        mgr = mgr2 = None
        try:
            mgr = JobManager(Scheduler(Signer("s")), data_dir=data_dir,
                             destinations={"a": ContentStore()})
            mgr.create_job(name="p", world_size=1, rank_nodes={0: "n0"},
                           job_id="p-1")
            pre = mgr.preempt("p-1")
            mgr.upload_shard("p-1", epoch_token=pre["epoch_token"], rank=0,
                             index=0, node="n0", input_pos=1, data=b"x")
            mgr.commit_checkpoint("p-1", epoch_token=pre["epoch_token"])
            mgr.confirm_release("p-1", epoch_token=pre["epoch_token"])
            task = mgr.create_migration("p-1", generation=1, destination="a")
            mgr.cancel_migration(task["task_id"], reason="offload_aborted")
            mgr.journal.close()

            mgr2 = JobManager(Scheduler(Signer("s")), data_dir=data_dir,
                              destinations={"a": ContentStore()})
            view = mgr2.get_migration(task["task_id"])
            self.assertEqual(view["state"], "CANCELLED")
            self.assertEqual(view["cancel_reason"], "offload_aborted")
            self.assertEqual(mgr2._migration_holds, {})
            with self.assertRaises(Conflict):
                mgr2.run_migration(task["task_id"])
        finally:
            for instance in (mgr, mgr2):
                if instance is not None:
                    instance.journal.close()
            shutil.rmtree(data_dir, ignore_errors=True)


class MigrationHttpTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.dest = ContentStore()
        cls.server = build_server(
            JobManager(Scheduler(Signer("mig-http-secret")),
                       destinations={"archive": cls.dest}),
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

    def test_migration_api_and_progress_page(self):
        mgr = self.server.manager
        mgr.create_job(name="http-mig", world_size=1, shards_per_rank=1,
                       rank_nodes={0: "n0"}, job_id="mig-http")
        pre = mgr.preempt("mig-http")
        mgr.upload_shard("mig-http", epoch_token=pre["epoch_token"], rank=0,
                         index=0, node="n0", input_pos=3, data=b"http blob")
        mgr.commit_checkpoint("mig-http", epoch_token=pre["epoch_token"])
        mgr.confirm_release("mig-http", epoch_token=pre["epoch_token"])

        # 创建 (幂等) -> 分块运行 -> 完成 -> 查询
        st, task = self._req("POST", "/api/migrations", {
            "job_id": "mig-http", "generation": 1, "destination": "archive"})
        self.assertEqual(st, 201)
        self.assertEqual(task["state"], "PLANNED")
        self.assertEqual(task["planned_items"], 1)
        st, dup = self._req("POST", "/api/migrations", {
            "job_id": "mig-http", "generation": 1, "destination": "archive"})
        self.assertEqual(dup["task_id"], task["task_id"])
        self.assertTrue(dup["idempotent_replay"])

        st, done = self._req("POST", f"/api/migrations/{task['task_id']}",
                             {"action": "run"})
        self.assertEqual(st, 200)
        self.assertEqual(done["state"], "PUBLISHED")
        self.assertEqual(done["transferred_items"], 1)
        self.assertTrue(self.dest.verify(sha256_hex(b"http blob")))

        st, detail = self._req("GET", f"/api/migrations/{task['task_id']}")
        self.assertEqual(st, 200)
        self.assertEqual(detail["published"]["generation"], 1)
        st, listing = self._req("GET", "/api/migrations")
        self.assertEqual(listing["destinations"], ["archive"])
        self.assertEqual(
            [t["task_id"] for t in listing["migrations"]], [task["task_id"]])
        st, per_job = self._req("GET", "/api/jobs/mig-http/migrations")
        self.assertEqual(len(per_job["published"]), 1)

        # 进度页包含迁移区块与计数
        st, page = self._req("GET", "/api/jobs/mig-http/progress.json")
        self.assertEqual(page["migrations"][0]["state"], "PUBLISHED")
        req = urllib.request.Request(
            f"http://127.0.0.1:{self.port}/ui/jobs/mig-http")
        with urllib.request.urlopen(req, timeout=5) as resp:
            html = resp.read().decode()
        self.assertIn("检查点迁移任务", html)
        self.assertIn("已发布可恢复代际", html)
        self.assertIn("PUBLISHED", html)

        # 未知目标存储 / 未知任务
        st, err = self._req("POST", "/api/migrations", {
            "job_id": "mig-http", "generation": 1, "destination": "nope"})
        self.assertEqual(st, 400)
        st, err = self._req("GET", "/api/migrations/mig-missing")
        self.assertEqual(st, 404)


if __name__ == "__main__":
    unittest.main()
