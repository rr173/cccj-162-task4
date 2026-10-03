"""作业生命周期: 抢占 -> 失联 -> 补齐 -> 全量校验 -> 释放 -> 恢复。"""

import unittest

from hpc_cr import JobManager
from hpc_cr.checkpoints import COMPLETE, PENDING
from hpc_cr.epochs import Scheduler
from hpc_cr.errors import (
    Conflict,
    FenceViolation,
    InvalidToken,
)
from hpc_cr.jobs import ABORTED, PREEMPTING, RELEASED, RUNNING
from hpc_cr.security import Signer


def make_mgr():
    mgr = JobManager(Scheduler(Signer("unit-secret")))
    nodes = {r: f"node-{r // 2}" for r in range(4)}
    job = mgr.create_job(name="sim", world_size=4, shards_per_rank=2,
                         rank_nodes=nodes, job_id="sim-1")
    return mgr, job


def shard_bytes(r, i, pos):
    return f"r{r}s{i}@{pos}".encode()


class LifecycleTest(unittest.TestCase):
    def setUp(self):
        self.mgr, self.job = make_mgr()

    def _preempt(self, **kw):
        return self.mgr.preempt("sim-1", **kw)

    def _dump_all(self, token):
        for r in range(4):
            for i in range(2):
                self.mgr.upload_shard(
                    "sim-1", epoch_token=token, rank=r, index=i,
                    node=f"node-{r // 2}", input_pos=1000 + r * 10 + i,
                    data=shard_bytes(r, i, 1000 + r * 10 + i))

    def test_happy_path_preempt_release_restore(self):
        out = self._preempt(grace_seconds=60)
        self.assertEqual(self.job.state, PREEMPTING)
        token = out["epoch_token"]
        self.assertEqual(out["checkpoint"]["state"], PENDING)

        # 抢占即吊销旧租约: gen0 lease 已 REVOKED
        gen0_lease = self.job.leases.get(0)
        self.assertEqual(gen0_lease.state, "REVOKED")

        self._dump_all(token)
        commit = self.mgr.commit_checkpoint("sim-1", epoch_token=token)
        self.assertEqual(commit["checkpoint"]["state"], COMPLETE)

        # 重复 commit 幂等
        again = self.mgr.commit_checkpoint("sim-1", epoch_token=token)
        self.assertTrue(again["already_complete"])

        rel = self.mgr.confirm_release("sim-1", epoch_token=token)
        self.assertTrue(rel["released"])
        self.assertEqual(self.job.state, RELEASED)

        # 恢复: 同代际 + 全量重校验 + 新租约
        report = self.mgr.restore("sim-1", generation=1)
        self.assertTrue(report.success)
        self.assertEqual(report.shards_verified, 8)
        self.assertEqual(report.lease["generation"], 1)
        self.assertEqual(report.lease["state"], "ACTIVE")
        self.assertEqual(self.job.state, RUNNING)
        self.assertEqual(self.job.output_generation, 1)
        # 输入位点全部回传
        self.assertEqual(report.input_positions["r0:s0"], 1000)
        self.assertEqual(report.input_positions["r3:s1"], 1031)
        # 报告可序列化为 dict / markdown
        md = report.to_markdown()
        self.assertIn("恢复验证报告", md)
        self.assertIn("验证通过", md)

    def test_duplicate_preempt_notice_is_idempotent(self):
        out1 = self._preempt()
        out2 = self._preempt()
        self.assertTrue(out1["preempted"])
        self.assertTrue(out2["idempotent_replay"])
        self.assertEqual(out1["epoch"]["epoch"], out2["epoch"]["epoch"])
        self.assertEqual(out1["epoch_token"], out2["epoch_token"])

    def test_lost_node_blocks_completeness_until_recovered(self):
        out = self._preempt()
        token = out["epoch_token"]
        self.mgr.mark_node_lost("sim-1", "node-1")  # ranks 2,3

        # 失联节点的分片不被接受
        with self.assertRaises(Conflict):
            self.mgr.upload_shard(
                "sim-1", epoch_token=token, rank=2, index=0,
                node="node-1", input_pos=5, data=b"late?")
        for r in (0, 1):
            for i in range(2):
                self.mgr.upload_shard(
                    "sim-1", epoch_token=token, rank=r, index=i,
                    node=f"node-{r // 2}", input_pos=7,
                    data=shard_bytes(r, i, 7))
        with self.assertRaises(Conflict):
            self.mgr.commit_checkpoint("sim-1", epoch_token=token)
        with self.assertRaises(Conflict):
            self.mgr.confirm_release("sim-1", epoch_token=token)

        # 分区恢复 -> 心跳复活 -> 补齐 -> 完整提交
        self.mgr.heartbeat("sim-1", "node-1")
        for r in (2, 3):
            for i in range(2):
                self.mgr.upload_shard(
                    "sim-1", epoch_token=token, rank=r, index=i,
                    node="node-1", input_pos=9,
                    data=shard_bytes(r, i, 9))
        cp = self.mgr.commit_checkpoint("sim-1", epoch_token=token)
        self.assertEqual(cp["checkpoint"]["state"], COMPLETE)

    def test_release_refused_on_incomplete_then_abort(self):
        out = self._preempt()
        token = out["epoch_token"]
        # 一片都不交
        with self.assertRaises(Conflict):
            self.mgr.confirm_release("sim-1", epoch_token=token)
        res = self.mgr.abort_epoch("sim-1")
        self.assertTrue(res["aborted"])
        self.assertEqual(self.job.state, ABORTED)
        self.assertEqual(res["checkpoint"]["state"], "INCOMPLETE")
        # 放弃后不能再恢复
        with self.assertRaises(Conflict):
            self.mgr.restore("sim-1", generation=1)

    def test_restore_must_use_same_generation(self):
        out = self._preempt()
        self._dump_all(out["epoch_token"])
        self.mgr.commit_checkpoint("sim-1", epoch_token=out["epoch_token"])
        self.mgr.confirm_release("sim-1", epoch_token=out["epoch_token"])

        # 不存在的代际 -> 失败恢复报告 (MISSING), 作业维持 RELEASED
        report = self.mgr.restore("sim-1", generation=99)
        self.assertFalse(report.success)
        self.assertEqual(report.checkpoint_state, "MISSING")

        # 恢复 gen1 后再次抢占产生 gen2 并释放; 此时回指旧代际 gen1 必须栅栏
        self.mgr.restore("sim-1", generation=1)
        pre2 = self.mgr.preempt("sim-1", force_new_epoch=True)
        for r in range(4):
            for i in range(2):
                self.mgr.upload_shard(
                    "sim-1", epoch_token=pre2["epoch_token"], rank=r, index=i,
                    node=f"node-{r // 2}", input_pos=2000 + r,
                    data=shard_bytes(r, i, 2000 + r))
        self.mgr.commit_checkpoint("sim-1",
                                   epoch_token=pre2["epoch_token"])
        self.mgr.confirm_release("sim-1", epoch_token=pre2["epoch_token"])
        with self.assertRaises(FenceViolation):
            self.mgr.restore("sim-1", generation=1)

    def test_restore_revalidates_every_shard_and_rejects_on_rot(self):
        out = self._preempt()
        self._dump_all(out["epoch_token"])
        self.mgr.commit_checkpoint("sim-1", epoch_token=out["epoch_token"])
        self.mgr.confirm_release("sim-1", epoch_token=out["epoch_token"])
        # 提交后存储损坏: 恢复期的再次重校验必须抓住
        cp = self.job.cpm.get(1)
        self.mgr.store.corrupt(cp.shards["r2:s1"].blob)
        report = self.mgr.restore("sim-1", generation=1)
        self.assertFalse(report.success)
        self.assertEqual(report.shards_failed, ["r2:s1"])
        self.assertEqual(self.job.state, RELEASED)  # 未恢复成功, 状态不变
        md = report.to_markdown()
        self.assertIn("验证失败", md)

    def test_restore_idempotent_and_relocation_report(self):
        out = self._preempt()
        self._dump_all(out["epoch_token"])
        self.mgr.commit_checkpoint("sim-1", epoch_token=out["epoch_token"])
        self.mgr.confirm_release("sim-1", epoch_token=out["epoch_token"])
        placement = {r: f"node-{r // 2}" for r in range(4)}
        placement[3] = "node-9"
        r1 = self.mgr.restore("sim-1", generation=1, new_rank_nodes=placement)
        self.assertTrue(r1.success)
        self.assertEqual(r1.relocated_ranks, [3])
        r2 = self.mgr.restore("sim-1", generation=1, new_rank_nodes=placement)
        self.assertIs(r1, r2)  # 幂等: 同一报告, 不重发租约

    def test_epoch_token_rejects_tamper_and_wrong_epoch(self):
        out = self._preempt()
        with self.assertRaises(InvalidToken):
            self.mgr.commit_checkpoint("sim-1", epoch_token="garbage")
        # 第二个作业的令牌不能用在本作业
        self.mgr.create_job(name="other", world_size=1, job_id="other-1")
        other = self.mgr.preempt("other-1")
        with self.assertRaises(FenceViolation):
            self.mgr.commit_checkpoint(
                "sim-1", epoch_token=other["epoch_token"])

    def test_second_preemption_supersedes_old_pending(self):
        out1 = self._preempt()
        self.mgr.upload_shard("sim-1", epoch_token=out1["epoch_token"],
                              rank=0, index=0, node="node-0",
                              input_pos=1, data=b"a")
        out2 = self.mgr.preempt("sim-1", reason="urgent",
                                force_new_epoch=True)
        self.assertEqual(out2["epoch"]["epoch"], 2)
        cps = {c.generation: c for c in self.job.cpm.list_all()}
        self.assertEqual(cps[1].state, "SUPERSEDED")
        self.assertEqual(cps[2].state, PENDING)
        # 旧纪元令牌不能写新代际
        with self.assertRaises(FenceViolation):
            self.mgr.upload_shard(
                "sim-1", epoch_token=out1["epoch_token"], rank=0, index=0,
                node="node-0", input_pos=1, data=b"a")


if __name__ == "__main__":
    unittest.main()
