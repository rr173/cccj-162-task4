"""可恢复检查点迁移: 冻结清单、临时保留、按摘要复用、全量校验发布、
中断/重复/重启续传、失败不发布、取消释放保留、与 GC 互不干扰。
"""

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


class MigrationTestBase(unittest.TestCase):
    world_size = 2

    def setUp(self):
        self.srcdir = tempfile.mkdtemp(prefix="mig-src-")
        self.dstdir = tempfile.mkdtemp(prefix="mig-dst-")
        self.mgr = JobManager(
            Scheduler(Signer("migration-secret")), data_dir=self.srcdir)
        self.mgr.create_job(
            name="mig-job", world_size=self.world_size,
            shards_per_rank=1,
            rank_nodes={r: f"n{r}" for r in range(self.world_size)},
            job_id="mj-1")

    def tearDown(self):
        self.mgr.journal.close()
        shutil.rmtree(self.srcdir, ignore_errors=True)
        shutil.rmtree(self.dstdir, ignore_errors=True)
    def _complete_gen1(self, payloads=None, release=False):
        pre = self.mgr.preempt("mj-1")
        token = pre["epoch_token"]
        payloads = payloads or {
            r: f"shard-r{r}-gen1".encode() for r in range(self.world_size)}
        for r, data in payloads.items():
            self.mgr.upload_shard(
                "mj-1", epoch_token=token, rank=r, index=0,
                node=f"n{r}", input_pos=r + 1, data=data)
        self.mgr.commit_checkpoint("mj-1", epoch_token=token)
        if release:
            self.mgr.confirm_release("mj-1", epoch_token=token)
        return token

    def _register_and_create(self, dest="cluster-b", mig_id=None):
        self.mgr.register_destination(dest, data_dir=self.dstdir)
        return self.mgr.create_migration(
            "mj-1", generation=1, destination_id=dest,
            migration_id=mig_id)

    def _migrate(self, mig_id, **kw):
        return self.mgr.transfer_migration(mig_id, **kw)


class MigrationHappyPathTest(MigrationTestBase):
    def test_freeze_requires_complete_checkpoint(self):
        pre = self.mgr.preempt("mj-1")
        self.mgr.register_destination("cluster-b", data_dir=self.dstdir)
        # PENDING (只传了一个分片) -> 拒绝导出, 无任务/无保留
        self.mgr.upload_shard(
            "mj-1", epoch_token=pre["epoch_token"], rank=0, index=0,
            node="n0", input_pos=1, data=b"only-one")
        with self.assertRaises(Conflict):
            self.mgr.create_migration(
                "mj-1", generation=1, destination_id="cluster-b")
        self.assertEqual(self.mgr.list_migrations(), [])
        self.assertEqual(self.mgr.migration_holds(), {})

    def test_unknown_generation_and_destination(self):
        self._complete_gen1()
        with self.assertRaises(NotFound):
            self.mgr.create_migration(
                "mj-1", generation=1, destination_id="nope")
        self.mgr.register_destination("cluster-b", data_dir=self.dstdir)
        with self.assertRaises(Conflict):
            self.mgr.create_migration(
                "mj-1", generation=99, destination_id="cluster-b")

    def test_full_migration_publishes_generation(self):
        self._complete_gen1()
        mig = self._register_and_create(mig_id="m-1")
        self.assertEqual(mig["state"], "PLANNED")
        self.assertEqual(mig["total_items"], self.world_size)
        self.assertIn("manifest_digest", mig)
        self.assertEqual(mig["done_items"], 0)
        self.assertEqual(len(self.mgr.migration_holds()), self.world_size)

        result = self._migrate("m-1")
        self.assertEqual(result["state"], "PUBLISHED")
        self.assertEqual(result["transferred_items"], self.world_size)
        self.assertEqual(result["reused_items"], 0)
        self.assertEqual(result["failed_items"], 0)
        self.assertTrue(result["publication"])

        published = self.mgr.list_published_generations("cluster-b")
        self.assertEqual(len(published), 1)
        self.assertEqual(published[0]["source_generation"], 1)
        # 发布后临时保留立即释放
        self.assertEqual(self.mgr.migration_holds(), {})
        # 载荷物理存在于接收端
        dst = self.mgr._destinations["cluster-b"]
        for o in result["objects"]:
            self.assertTrue(dst.has(o["blob"]))

    def test_repeated_create_is_idempotent_for_active_task(self):
        self._complete_gen1()
        first = self._register_and_create(mig_id="m-1")
        again = self.mgr.create_migration(
            "mj-1", generation=1, destination_id="cluster-b")
        self.assertTrue(again["idempotent_replay"])
        self.assertEqual(again["migration_id"], first["migration_id"])
        # 保留没有被重复添加
        self.assertEqual(len(self.mgr.migration_holds()), self.world_size)

    def test_duplicate_transfer_requests_do_not_double_transfer(self):
        self._complete_gen1()
        self._register_and_create(mig_id="m-1")
        r1 = self._migrate("m-1")
        r2 = self._migrate("m-1")  # 已发布: 幂等返回
        r3 = self.mgr.finalize_migration("m-1")
        self.assertEqual(r1["state"], "PUBLISHED")
        self.assertEqual(r2["state"], "PUBLISHED")
        self.assertEqual(r3["state"], "PUBLISHED")
        self.assertEqual(len(self.mgr.list_published_generations()), 1)

    def test_destination_reuses_existing_objects_by_digest(self):
        self._complete_gen1()
        self.mgr.register_destination("cluster-b", data_dir=self.dstdir)
        dst = self.mgr._destinations["cluster-b"]
        # 接收端已存在 r0 的同摘要内容 (例如别的作业迁来的共享分片)
        r0_blob = sha256_hex(b"shard-r0-gen1")
        dst.put(b"shard-r0-gen1")
        mig = self.mgr.create_migration(
            "mj-1", generation=1, destination_id="cluster-b",
            migration_id="m-1")
        result = self._migrate("m-1")
        self.assertEqual(result["state"], "PUBLISHED")
        self.assertEqual(result["reused_items"], 1)
        self.assertEqual(result["transferred_items"], self.world_size - 1)
        reused = [o for o in result["objects"] if o["status"] == "REUSED"]
        self.assertEqual([o["object_id"] for o in reused], ["r0:s0"])
        self.assertTrue(dst.has(r0_blob))


class MigrationResumeTest(MigrationTestBase):
    def test_interrupted_transfer_resumes_same_task(self):
        self._complete_gen1()
        mig = self._register_and_create(mig_id="m-1")
        # 只传 r0, 中断 (auto_finalize=False)
        part = self._migrate(
            "m-1", auto_finalize=False, object_ids=["r0:s0"])
        self.assertEqual(part["state"], "TRANSFERRING")
        self.assertEqual(part["transferred_items"], 1)
        self.assertIsNone(part["publication"])
        # 重复同一对象: 已 REUSED/TRANSFERRED 跳过
        again = self._migrate(
            "m-1", auto_finalize=False, object_ids=["r0:s0"])
        self.assertEqual(again["transferred_items"], 1)
        # 续传剩余对象后一次性发布
        done = self._migrate("m-1")
        self.assertEqual(done["state"], "PUBLISHED")
        self.assertEqual(done["done_items"], self.world_size)

    def test_restart_resumes_inflight_task_and_releases_holds_on_finish(self):
        secret = "migration-restart-secret"
        self.mgr.journal.close()
        self.mgr = JobManager(
            Scheduler(Signer(secret)), data_dir=self.srcdir)
        self._complete_gen1()
        self.mgr.register_destination("cluster-b", data_dir=self.dstdir)
        self.mgr.create_migration(
            "mj-1", generation=1, destination_id="cluster-b",
            migration_id="fixed-mig")
        self._migrate(
            "fixed-mig", auto_finalize=False, object_ids=["r0:s0"])
        self.mgr.journal.close()

        mgr2 = JobManager(Scheduler(Signer(secret)), data_dir=self.srcdir)
        try:
            # 目标磁盘后端自动重建, 保留按活跃任务重建
            self.assertEqual(
                [d["destination_id"] for d in mgr2.list_destinations()],
                ["cluster-b"])
            t = mgr2.get_migration("fixed-mig")
            self.assertEqual(t["state"], "TRANSFERRING")
            self.assertEqual(t["transferred_items"], 1)
            self.assertEqual(len(mgr2.migration_holds()), self.world_size)
            done = mgr2.transfer_migration("fixed-mig")
            self.assertEqual(done["state"], "PUBLISHED")
            self.assertEqual(done["transferred_items"], self.world_size)
            self.assertEqual(mgr2.migration_holds(), {})
            self.assertEqual(
                len(mgr2.list_published_generations("cluster-b")), 1)
        finally:
            mgr2.journal.close()

    def test_restart_cancelled_task_stays_cancelled_without_holds(self):
        secret = "migration-cancel-restart"
        self.mgr.journal.close()
        self.mgr = JobManager(
            Scheduler(Signer(secret)), data_dir=self.srcdir)
        self._complete_gen1()
        self.mgr.register_destination("cluster-b", data_dir=self.dstdir)
        self.mgr.create_migration(
            "mj-1", generation=1, destination_id="cluster-b",
            migration_id="m-cancel")
        self.mgr.cancel_migration("m-cancel")
        self.mgr.journal.close()

        mgr2 = JobManager(Scheduler(Signer(secret)), data_dir=self.srcdir)
        try:
            t = mgr2.get_migration("m-cancel")
            self.assertEqual(t["state"], "CANCELLED")
            self.assertEqual(mgr2.migration_holds(), {})
            # 终态任务上的操作全部幂等
            self.assertEqual(
                mgr2.transfer_migration("m-cancel")["state"], "CANCELLED")
            self.assertEqual(
                mgr2.cancel_migration("m-cancel")["state"], "CANCELLED")
        finally:
            mgr2.journal.close()


class MigrationFailureTest(MigrationTestBase):
    def test_manifest_change_after_freeze_fails_and_releases_holds(self):
        self._complete_gen1(release=True)
        mig = self._register_and_create(mig_id="m-1")
        # 全部传完但先不发布
        self._migrate("m-1", auto_finalize=False)
        # 冻结后源代际产生新输出 -> 清单变化
        self.mgr.restore("mj-1", generation=1)
        lease = self.mgr.get("mj-1").leases.current().token
        self.mgr.append_output(
            "mj-1", lease_token=lease, output_id="late-output",
            data=b"output added after freeze")
        with self.assertRaises(Conflict):
            self.mgr.transfer_migration("m-1")
        t = self.mgr.get_migration("m-1")
        self.assertEqual(t["state"], "FAILED")
        self.assertEqual(t["reason"], "manifest_changed")
        self.assertFalse(self.mgr.list_published_generations("cluster-b"))
        self.assertEqual(self.mgr.migration_holds(), {})

    def test_corrupt_destination_reused_blob_fails_finalization(self):
        self._complete_gen1()
        self.mgr.register_destination("cluster-b", data_dir=self.dstdir)
        dst = self.mgr._destinations["cluster-b"]
        dst.put(b"shard-r0-gen1")
        digest_r0 = sha256_hex(b"shard-r0-gen1")
        dst.corrupt(digest_r0)
        self.mgr.create_migration(
            "mj-1", generation=1, destination_id="cluster-b",
            migration_id="m-1")
        # 传输阶段发现"复用"对象已损坏 -> 传输项失败但任务可修复续传
        r = self._migrate("m-1", auto_finalize=False)
        self.assertEqual(r["failed_items"], 1)
        self.assertEqual(r["state"], "TRANSFERRING")
        # 带失败项的终检拒绝发布, 且不产生可恢复代际
        final = self.mgr.finalize_migration("m-1")
        self.assertEqual(final["state"], "FAILED")
        self.assertFalse(self.mgr.list_published_generations("cluster-b"))
        # 失败后释放保留; 修复目标对象后, 用一个新任务完成迁移
        dst.delete(digest_r0)
        dst.put(b"shard-r0-gen1")
        self.mgr.create_migration(
            "mj-1", generation=1, destination_id="cluster-b",
            migration_id="m-2")
        done = self.mgr.transfer_migration("m-2")
        self.assertEqual(done["state"], "PUBLISHED")
        self.assertEqual(done["reused_items"], self.world_size)

    def test_corrupt_repaired_via_source_resumes_and_publishes(self):
        self._complete_gen1()
        self.mgr.register_destination("cluster-b", data_dir=self.dstdir)
        dst = self.mgr._destinations["cluster-b"]
        digest_r0 = sha256_hex(b"shard-r0-gen1")
        dst.put(b"shard-r0-gen1")
        dst.corrupt(digest_r0)
        self.mgr.create_migration(
            "mj-1", generation=1, destination_id="cluster-b",
            migration_id="m-1")
        # 首轮: 损坏的复用项标记失败, 其余对象照常传输, 但不发布
        r = self._migrate("m-1", auto_finalize=False)
        self.assertEqual(r["failed_items"], 1)
        self.assertEqual(r["transferred_items"], 1)
        # 显式重试失败项: 用源端正确字节原子修复损坏副本
        r = self._migrate("m-1", object_ids=["r0:s0"])
        self.assertEqual(r["state"], "PUBLISHED")
        statuses = {o["object_id"]: o["status"] for o in r["objects"]}
        self.assertEqual(statuses["r0:s0"], "TRANSFERRED")
        self.assertTrue(dst.verify(digest_r0))
        self.assertEqual(
            len(self.mgr.list_published_generations("cluster-b")), 1)

    def test_corrupt_source_payload_at_finalize_blocks_publication(self):
        self._complete_gen1()
        self._register_and_create(mig_id="m-1")
        # 全部对象传输后损坏源端 (finalize 前清单未变, 但目标摘要仍正确);
        # 这里改为损坏目标物理文件模拟 bit-rot: 终检必须重读重算。
        self._migrate("m-1", auto_finalize=False)
        dst = self.mgr._destinations["cluster-b"]
        blob = dst.inventory()[0][0]
        dst.corrupt(blob)
        final = self.mgr.finalize_migration("m-1")
        self.assertEqual(final["state"], "FAILED")
        self.assertEqual(final["reason"], "destination_digest_mismatch")
        self.assertFalse(self.mgr.list_published_generations("cluster-b"))

    def test_cancel_releases_holds_and_never_publishes(self):
        self._complete_gen1()
        self._register_and_create(mig_id="m-1")
        self._migrate("m-1", auto_finalize=False, object_ids=["r0:s0"])
        cancelled = self.mgr.cancel_migration("m-1")
        self.assertEqual(cancelled["state"], "CANCELLED")
        self.assertEqual(self.mgr.migration_holds(), {})
        self.assertFalse(self.mgr.list_published_generations())
        # 取消后无法再发布: finalize/transfer 对终态任务幂等返回 CANCELLED
        final = self.mgr.finalize_migration("m-1")
        self.assertEqual(final["state"], "CANCELLED")
        self.assertEqual(
            self.mgr.transfer_migration("m-1")["state"], "CANCELLED")

    def test_validation_on_unknown_objects(self):
        self._complete_gen1()
        self._register_and_create(mig_id="m-1")
        with self.assertRaises(ValidationError):
            self._migrate("m-1", object_ids=["r9:s9"])


class MigrationGCTest(MigrationTestBase):
    def test_gc_plan_excludes_held_blobs_and_stales_after_hold_change(self):
        token = self._complete_gen1(release=True)
        self.mgr.restore("mj-1", generation=1)
        pre2 = self.mgr.preempt("mj-1", force_new_epoch=True)
        token2 = pre2["epoch_token"]
        for r in range(self.world_size):
            self.mgr.upload_shard(
                "mj-1", epoch_token=token2, rank=r, index=0,
                node=f"n{r}", input_pos=r + 10,
                data=f"shard-r{r}-gen2".encode())
        self.mgr.commit_checkpoint("mj-1", epoch_token=token2)

        # gen1 低于水位本应可回收
        before = self.mgr.create_gc_plan(
            policy="watermark", min_retain_generation=2)
        self.assertEqual(before["estimated_reclaim_items"], self.world_size)

        # 创建迁移 -> 推进水位, 旧计划 STALE
        self._register_and_create(mig_id="m-1")
        with self.assertRaises(Conflict):
            self.mgr.execute_gc_plan(before["plan_id"])
        self.assertEqual(
            self.mgr.get_gc_plan(before["plan_id"])["state"], "STALE")

        # 活跃迁移期间新计划不把保留 blob 当候选
        during = self.mgr.create_gc_plan(
            policy="watermark", min_retain_generation=2)
        self.assertEqual(during["estimated_reclaim_items"], 0)
        self.assertEqual(
            during["migration_held_blob_count"], self.world_size)

        # 发布释放保留后, gen1 blob 恢复可回收 (源端仍可被正常 GC)
        self._migrate("m-1")
        with self.assertRaises(Conflict):
            self.mgr.execute_gc_plan(during["plan_id"])
        after = self.mgr.create_gc_plan(
            policy="watermark", min_retain_generation=2)
        result = self.mgr.execute_gc_plan(after["plan_id"])
        self.assertEqual(result["state"], "COMPLETED")
        self.assertEqual(result["deleted_items"], self.world_size)
        # 接收端副本依然完好
        dst = self.mgr._destinations["cluster-b"]
        for r in range(self.world_size):
            self.assertTrue(dst.has(sha256_hex(f"shard-r{r}-gen1".encode())))

    def test_cancelled_migration_does_not_block_normal_gc(self):
        self._complete_gen1(release=True)
        self.mgr.restore("mj-1", generation=1)
        pre2 = self.mgr.preempt("mj-1", force_new_epoch=True)
        token2 = pre2["epoch_token"]
        for r in range(self.world_size):
            self.mgr.upload_shard(
                "mj-1", epoch_token=token2, rank=r, index=0,
                node=f"n{r}", input_pos=r + 10,
                data=f"gen2-{r}".encode())
        self.mgr.commit_checkpoint("mj-1", epoch_token=token2)
        self._register_and_create(mig_id="m-1")
        self.mgr.cancel_migration("m-1")
        plan = self.mgr.create_gc_plan(
            policy="watermark", min_retain_generation=2)
        self.assertEqual(plan["estimated_reclaim_items"], self.world_size)
        result = self.mgr.execute_gc_plan(plan["plan_id"])
        self.assertEqual(result["deleted_items"], self.world_size)


class AdoptTest(MigrationTestBase):
    def _publish(self, mig_id="m-1", dest="cluster-b"):
        self._complete_gen1()
        self.mgr.register_destination(dest, data_dir=self.dstdir)
        self.mgr.create_migration(
            "mj-1", generation=1, destination_id=dest, migration_id=mig_id)
        self.mgr.transfer_migration(mig_id)
        return self.mgr.list_published_generations(dest)[0]

    def _new_receiver(self):
        recv_dir = tempfile.mkdtemp(prefix="recv-meta-")
        mgr = JobManager(
            Scheduler(Signer("other-cluster-secret")), data_dir=recv_dir)
        mgr.register_destination("cluster-b", data_dir=self.dstdir)
        return mgr, recv_dir

    def test_adopt_manifest_and_restore_same_generation(self):
        published = self._publish()
        recv, recv_dir = self._new_receiver()
        try:
            res = recv.adopt_generation(
                manifest=published["manifest"], destination_id="cluster-b")
            self.assertTrue(res["adopted"])
            self.assertTrue(res["job_created"])
            self.assertEqual(res["checkpoint"]["state"], "COMPLETE")
            self.assertEqual(res["generation"], 1)
            job = recv.get("mj-1")
            self.assertEqual(job.state, "RELEASED")
            self.assertEqual(job.released_generation, 1)
            report = recv.restore("mj-1", generation=1)
            self.assertTrue(report.success)
            self.assertEqual(report.shards_verified, self.world_size)
        finally:
            recv.journal.close()
            shutil.rmtree(recv_dir, ignore_errors=True)

    def test_adopt_by_manifest_is_idempotent_conflict_on_repeat(self):
        published = self._publish()
        recv, recv_dir = self._new_receiver()
        try:
            res = recv.adopt_generation(
                manifest=published["manifest"], destination_id="cluster-b")
            self.assertTrue(res["adopted"])
            with self.assertRaises(Conflict):
                recv.adopt_generation(
                    manifest=published["manifest"],
                    destination_id="cluster-b")
        finally:
            recv.journal.close()
            shutil.rmtree(recv_dir, ignore_errors=True)

    def test_adopt_refused_on_missing_destination_content(self):
        published = self._publish()
        empty_dst = tempfile.mkdtemp(prefix="empty-dst-")
        recv_dir = tempfile.mkdtemp(prefix="recv-")
        recv = JobManager(
            Scheduler(Signer("s3")), data_dir=recv_dir)
        try:
            recv.register_destination("empty", data_dir=empty_dst)
            with self.assertRaises(Conflict):
                recv.adopt_generation(
                    manifest=published["manifest"],
                    destination_id="empty")
            self.assertNotIn("mj-1", {j.job_id for j in recv.list_jobs()})
        finally:
            recv.journal.close()
            shutil.rmtree(recv_dir, ignore_errors=True)
            shutil.rmtree(empty_dst, ignore_errors=True)

    def test_adopt_survives_receiver_restart_and_can_restore(self):
        published = self._publish()
        recv_dir = tempfile.mkdtemp(prefix="recv-persist-")
        secret = "receiver-secret"
        recv = JobManager(Scheduler(Signer(secret)), data_dir=recv_dir)
        try:
            recv.register_destination("cluster-b", data_dir=self.dstdir)
            recv.adopt_generation(
                manifest=published["manifest"], destination_id="cluster-b")
            recv.journal.close()
            recv2 = JobManager(Scheduler(Signer(secret)), data_dir=recv_dir)
            recv2.register_destination("cluster-b", data_dir=self.dstdir)
            try:
                job = recv2.get("mj-1")
                self.assertEqual(job.state, "RELEASED")
                cp = job.cpm.get(1)
                self.assertEqual(cp.state, "COMPLETE")
                report = recv2.restore("mj-1", generation=1)
                self.assertTrue(report.success)
            finally:
                recv2.journal.close()
        finally:
            shutil.rmtree(recv_dir, ignore_errors=True)


class MigrationConcurrencyTest(MigrationTestBase):
    def test_concurrent_transfers_publish_once_and_keep_consistent_counts(self):
        self._complete_gen1()
        self.mgr = JobManager(Scheduler(Signer("mig-conc")))
        self.mgr.create_job(
            name="j", world_size=self.world_size,
            rank_nodes={r: f"n{r}" for r in range(self.world_size)},
            job_id="mj-1")
        token = self.mgr.preempt("mj-1")["epoch_token"]
        for r in range(self.world_size):
            self.mgr.upload_shard(
                "mj-1", epoch_token=token, rank=r, index=0,
                node=f"n{r}", input_pos=r + 1,
                data=f"conc-{r}".encode())
        self.mgr.commit_checkpoint("mj-1", epoch_token=token)
        self.mgr.register_destination("cluster-b")  # 内存目标
        self.mgr.create_migration(
            "mj-1", generation=1, destination_id="cluster-b",
            migration_id="m-1")
        errors = []

        def worker():
            try:
                self.mgr.transfer_migration("m-1")
            except Exception as exc:  # noqa: BLE001
                errors.append(exc)

        threads = [threading.Thread(target=worker) for _ in range(8)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        self.assertEqual(errors, [])
        t = self.mgr.get_migration("m-1")
        self.assertEqual(t["state"], "PUBLISHED")
        self.assertEqual(t["done_items"], self.world_size)
        self.assertEqual(
            t["transferred_items"] + t["reused_items"], self.world_size)
        self.assertEqual(len(self.mgr.list_published_generations()), 1)

    def test_concurrent_transfer_and_gc_never_deletes_held_blob(self):
        self._complete_gen1(release=True)
        self.mgr.restore("mj-1", generation=1)
        pre2 = self.mgr.preempt("mj-1", force_new_epoch=True)
        token2 = pre2["epoch_token"]
        for r in range(self.world_size):
            self.mgr.upload_shard(
                "mj-1", epoch_token=token2, rank=r, index=0,
                node=f"n{r}", input_pos=r + 10,
                data=f"gen2-{r}".encode())
        self.mgr.commit_checkpoint("mj-1", epoch_token=token2)
        self._register_and_create(mig_id="m-1")
        outcomes = []

        def transfer():
            r = self.mgr.transfer_migration("m-1")
            outcomes.append(("mig", r["state"]))

        def gc_loop():
            # 迁移进行中反复尝试生成/执行计划: held blob 绝不允许被删
            try:
                plan = self.mgr.create_gc_plan(
                    policy="watermark", min_retain_generation=2)
                self.mgr.execute_gc_plan(plan["plan_id"])
                outcomes.append(("gc", "executed"))
            except Conflict:
                outcomes.append(("gc", "rejected"))

        threads = [threading.Thread(target=transfer)]
        threads += [threading.Thread(target=gc_loop) for _ in range(4)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        self.assertIn(("mig", "PUBLISHED"), outcomes)
        for r in range(self.world_size):
            # 迁移期间源代际 blob 始终可达; 发布后是否回收取决于执行时序,
            # 但绝不可能在任务仍活跃、blob 被 hold 时被删除。
            dst = self.mgr._destinations["cluster-b"]
            self.assertTrue(
                dst.has(sha256_hex(f"shard-r{r}-gen1".encode())))
        t = self.mgr.get_migration("m-1")
        self.assertEqual(t["state"], "PUBLISHED")


class MigrationHttpTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.dstdir = tempfile.mkdtemp(prefix="mig-http-dst-")
        cls.server = build_server(
            JobManager(Scheduler(Signer("mig-http-secret"))),
            host="127.0.0.1", port=0)
        cls.port = cls.server.server_address[1]
        cls.thread = threading.Thread(target=cls.server.serve_forever,
                                      daemon=True)
        cls.thread.start()

    @classmethod
    def tearDownClass(cls):
        cls.server.shutdown()
        shutil.rmtree(cls.dstdir, ignore_errors=True)

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
        import base64
        mgr = self.server.manager
        mgr.create_job(name="http-mig", world_size=1,
                       rank_nodes={0: "n0"}, job_id="mig-http")
        pre = mgr.preempt("mig-http")
        mgr.upload_shard(
            "mig-http", epoch_token=pre["epoch_token"], rank=0, index=0,
            node="n0", input_pos=1, data=b"http-migration-shard")
        mgr.commit_checkpoint("mig-http", epoch_token=pre["epoch_token"])

        st, dest = self._req("POST", "/api/migration-destinations", {
            "destination_id": "cluster-b", "data_dir": self.dstdir})
        self.assertEqual(st, 201)
        st, dests = self._req("GET", "/api/migration-destinations")
        self.assertEqual(st, 200)
        self.assertEqual(
            [d["destination_id"] for d in dests["destinations"]],
            ["cluster-b"])

        st, mig = self._req("POST", "/api/migrations", {
            "job_id": "mig-http", "generation": 1,
            "destination_id": "cluster-b", "migration_id": "mig-http-1"})
        self.assertEqual(st, 201)
        self.assertEqual(mig["state"], "PLANNED")
        self.assertEqual(mig["total_items"], 1)

        st, running = self._req(
            "POST", "/api/migrations/mig-http-1",
            {"action": "transfer", "auto_finalize": False})
        self.assertEqual(st, 200)
        self.assertEqual(running["transferred_items"], 1)

        st, done = self._req(
            "POST", "/api/migrations/mig-http-1",
            {"action": "finalize"})
        self.assertEqual(st, 200)
        self.assertEqual(done["state"], "PUBLISHED")

        st, lst = self._req("GET", "/api/migrations?job_id=mig-http")
        self.assertEqual(st, 200)
        self.assertEqual(lst["migrations"][0]["migration_id"], "mig-http-1")
        st, pub = self._req(
            "GET", "/api/published-generations?job_id=mig-http")
        self.assertEqual(st, 200)
        self.assertEqual(pub["published"][0]["source_generation"], 1)

        # 接收集群 (本服务内第二接收端目录) 迁移并用发布清单 adopt
        recv_dir = tempfile.mkdtemp(prefix="mig-adopt-dst-")
        st, _ = self._req("POST", "/api/migration-destinations", {
            "destination_id": "cluster-c", "data_dir": recv_dir})
        self.assertEqual(st, 201)
        st, migc = self._req("POST", "/api/migrations", {
            "job_id": "mig-http", "generation": 1,
            "destination_id": "cluster-c", "migration_id": "mig-to-c"})
        self.assertEqual(st, 201)
        st, done = self._req("POST", "/api/migrations/mig-to-c",
                             {"action": "transfer"})
        self.assertEqual(st, 200)
        self.assertEqual(done["state"], "PUBLISHED")
        st, pubc = self._req(
            "GET", "/api/published-generations?destination_id=cluster-c")
        st, adopted = self._req("POST", "/api/adoptions", {
            "manifest": pubc["published"][0]["manifest"],
            "destination_id": "cluster-c",
            "target_job_id": "mig-http-copy", "create_job": True})
        self.assertEqual(st, 201)
        self.assertTrue(adopted["adopted"])
        self.assertEqual(adopted["checkpoint"]["state"], "COMPLETE")
        shutil.rmtree(recv_dir, ignore_errors=True)

        st, page = self._req("GET", "/api/jobs/mig-http/progress.json")
        self.assertEqual(st, 200)
        self.assertEqual(page["migrations"][0]["state"], "PUBLISHED")
        self.assertEqual(
            page["published_generations"][0]["manifest"]["total_items"], 1)

        # 取消路径
        st, mig2 = self._req("POST", "/api/migrations", {
            "job_id": "mig-http", "generation": 1,
            "destination_id": "cluster-b", "migration_id": "mig-http-2"})
        self.assertEqual(st, 201)
        st, cancelled = self._req(
            "POST", "/api/migrations/mig-http-2", {"action": "cancel"})
        self.assertEqual(st, 200)
        self.assertEqual(cancelled["state"], "CANCELLED")

        # UI 页面包含迁移区块
        with urllib.request.urlopen(
                "http://127.0.0.1:"
                f"{self.port}/ui/jobs/mig-http", timeout=5) as resp:
            html = resp.read().decode()
        self.assertIn("检查点迁移", html)
        self.assertIn("mig-http-1", html)


if __name__ == "__main__":
    unittest.main()
