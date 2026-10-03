"""检查点聚合语义测试: 内容寻址、重复分片、全量重校验门。"""

import unittest

from hpc_cr.checkpoints import (
    COMPLETE,
    INCOMPLETE,
    PENDING,
    SUPERSEDED,
    CheckpointManager,
    shard_id,
)
from hpc_cr.errors import Conflict, FenceViolation, ValidationError
from hpc_cr.storage import ContentStore


def procs(n):
    return [{"rank": r, "node": f"node-{r}"} for r in range(n)]


class CheckpointTest(unittest.TestCase):
    def setUp(self):
        self.store = ContentStore()
        self.cpm = CheckpointManager("job-A", self.store)
        self.cpm.open(epoch=1, generation=1, world_size=3,
                      shards_per_rank=2, processes=procs(3))

    def _put(self, r, i, pos, data=None):
        return self.cpm.put_shard(
            generation=1, rank=r, index=i,
            data=data or f"payload-r{r}-s{i}@pos{pos}".encode(),
            node=f"node-{r}", input_pos=pos)

    def test_incomplete_until_every_shard_verified(self):
        cp = self.cpm.get(1)
        self.assertEqual(cp.state, PENDING)
        for r in range(3):
            for i in range(2):
                if (r, i) == (2, 1):
                    continue
                self._put(r, i, 100 + r)
        # 缺一片 -> 提交拒绝, 永远不 COMPLETE
        with self.assertRaises(Conflict) as ctx:
            self.cpm.commit(1)
        self.assertIn("missing", str(ctx.exception))
        self.assertEqual(self.cpm.get(1).state, PENDING)

        self._put(2, 1, 102)
        cp = self.cpm.commit(1)
        self.assertEqual(cp.state, COMPLETE)
        self.assertEqual(len(cp.manifest["shards"]), 6)
        self.assertTrue(cp.manifest["manifest_digest"])

    def test_duplicate_shard_idempotent_but_conflicting_payload_rejected(self):
        s1, new1 = self._put(0, 0, 50)
        s2, new2 = self._put(0, 0, 50)  # 完全相同的重传
        self.assertTrue(new1)
        self.assertFalse(new2)
        self.assertEqual(s1.blob, s2.blob)
        # 同分片不同内容/位点 -> 脑裂写入必须拒绝
        with self.assertRaises(Conflict):
            self.cpm.put_shard(generation=1, rank=0, index=0,
                               data=b"different bytes",
                               node="node-0", input_pos=999)

    def test_wrong_node_writer_fenced(self):
        with self.assertRaises(FenceViolation):
            self.cpm.put_shard(generation=1, rank=0, index=0,
                               data=b"x", node="node-evil", input_pos=1)

    def test_cas_dedupes_identical_shards_across_generations(self):
        data = b"identical payload"
        self.cpm.put_shard(generation=1, rank=0, index=0, data=data,
                           node="node-0", input_pos=1)
        between = self.store.blob_count()
        # 新抢占纪元接管: gen1 被 SUPERSEDED, gen2 打开
        self.cpm.open(epoch=2, generation=2, world_size=3,
                      shards_per_rank=2, processes=procs(3))
        self.cpm.supersede_pending(2)
        self.cpm.put_shard(generation=2, rank=0, index=0, data=data,
                           node="node-0", input_pos=1)
        # 相同载荷跨代际复用同一 blob, 存储不膨胀
        self.assertEqual(self.store.blob_count(), between)

    def test_commit_recomputes_hash_and_catches_corruption(self):
        for r in range(3):
            for i in range(2):
                self._put(r, i, 10 + r)
        cp = self.cpm.get(1)
        blob = cp.shards[shard_id(1, 0)].blob
        # 模拟磁盘 bit-rot: 客户端自报哈希无法发现, 提交期重算必须发现
        self.store.corrupt(blob)
        with self.assertRaises(Conflict) as ctx:
            self.cpm.commit(1)
        self.assertIn("corrupt", str(ctx.exception))
        self.assertEqual(self.cpm.get(1).state, PENDING)

    def test_superseded_and_abandoned_never_complete(self):
        self._put(0, 0, 1)
        self.cpm.open(epoch=2, generation=2, world_size=3,
                      shards_per_rank=2, processes=procs(3))
        self.cpm.supersede_pending(2)
        self.assertEqual(self.cpm.get(1).state, SUPERSEDED)
        with self.assertRaises(Conflict):
            self.cpm.commit(1)
        # 放弃的检查点
        self.cpm.mark_incomplete(2)
        self.assertEqual(self.cpm.get(2).state, INCOMPLETE)
        with self.assertRaises(Conflict):
            self.cpm.commit(2)

    def test_open_requires_full_process_set(self):
        with self.assertRaises(ValidationError):
            self.cpm.open(epoch=9, generation=9, world_size=3,
                          shards_per_rank=1,
                          processes=[{"rank": 0, "node": "n0"},
                                     {"rank": 2, "node": "n2"}])

    def test_open_idempotent_same_epoch(self):
        cp1 = self.cpm.get(1)
        cp2 = self.cpm.open(epoch=1, generation=1, world_size=3,
                            shards_per_rank=2, processes=procs(3))
        self.assertIs(cp1, cp2)
        with self.assertRaises(Conflict):
            self.cpm.open(epoch=2, generation=1, world_size=3,
                          shards_per_rank=2, processes=procs(3))


if __name__ == "__main__":
    unittest.main()
