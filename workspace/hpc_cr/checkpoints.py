"""检查点聚合与分片状态机。

检查点生命周期::

    (open) -> PENDING --全部分片到位且逐个重算哈希成功--> COMPLETE (可用)
                |                                        ^
                | 部分分片缺失/损坏: commit 被拒绝,       | 重复 commit:
                | 永远停留 PENDING, 绝不提前可用          | 幂等返回
                v
           INCOMPLETE (调度器放弃该纪元) / SUPERSEDED (更新的纪元接管)

关键设计:
  * 期望分片集合在 open() 时由进程集合 × shards_per_rank 固定下来,
    之后不能增删, 防止"用部分分片凑数提交"。
  * 分片 ID 形如 ``r3:s0``; 重复分片: 内容/位点一致则幂等重传成功,
    不一致直接 409 (重复检查点里的脑裂写入)。
  * commit() 不信任任何上传时记录, 对每个 blob 重新从 CAS 读出并重算
    sha256; 任一缺失/损坏即整体拒绝。
"""

from __future__ import annotations

import threading
import time
from dataclasses import dataclass, field

from .errors import Conflict, FenceViolation, NotFound, ValidationError
from .security import canonical_json
from .storage import ContentStore

PENDING = "PENDING"
COMPLETE = "COMPLETE"
INCOMPLETE = "INCOMPLETE"
SUPERSEDED = "SUPERSEDED"


def shard_id(rank: int, index: int) -> str:
    return f"r{rank}:s{index}"


@dataclass
class Shard:
    rank: int
    index: int
    node: str
    blob: str
    size: int
    input_pos: int
    uploaded_ts: float

    def ref(self) -> dict:
        return {"rank": self.rank, "index": self.index, "blob": self.blob,
                "size": self.size, "input_pos": self.input_pos,
                "node": self.node}


@dataclass
class Checkpoint:
    job_id: str
    epoch: int
    generation: int
    world_size: int
    shards_per_rank: int
    processes: tuple[tuple[int, str], ...]  # (rank, node) 排序后
    created_ts: float
    state: str = PENDING
    completed_ts: float | None = None
    shards: dict[str, Shard] = field(default_factory=dict)
    manifest: dict | None = None

    @property
    def expected_total(self) -> int:
        return self.world_size * self.shards_per_rank

    def missing(self) -> list[str]:
        return [
            shard_id(r, i)
            for r in range(self.world_size)
            for i in range(self.shards_per_rank)
            if shard_id(r, i) not in self.shards
        ]

    def progress(self) -> dict:
        return {
            "received": len(self.shards),
            "expected": self.expected_total,
            "missing": self.missing(),
        }

    def to_dict(self) -> dict:
        return {
            "checkpoint_id": f"cp-gen{self.generation}",
            "job_id": self.job_id,
            "epoch": self.epoch,
            "generation": self.generation,
            "state": self.state,
            "world_size": self.world_size,
            "shards_per_rank": self.shards_per_rank,
            "processes": [{"rank": r, "node": n} for r, n in self.processes],
            "created_ts": self.created_ts,
            "completed_ts": self.completed_ts,
            **self.progress(),
        }


class CheckpointManager:
    """单个作业的全部代际检查点。"""

    def __init__(self, job_id: str, store: ContentStore):
        self.job_id = job_id
        self._store = store
        self._lock = threading.RLock()
        self._cps: dict[int, Checkpoint] = {}

    # ------------------------------------------------------------------ open
    def open(
        self,
        *,
        epoch: int,
        generation: int,
        world_size: int,
        shards_per_rank: int,
        processes: list[dict] | list[tuple[int, str]],
        now: float | None = None,
    ) -> Checkpoint:
        if world_size <= 0 or shards_per_rank <= 0:
            raise ValidationError("world_size/shards_per_rank must be positive")
        proc_tuple = sorted(
            (p["rank"], p["node"]) if isinstance(p, dict) else p
            for p in processes
        )
        ranks = [r for r, _ in proc_tuple]
        if ranks != list(range(world_size)):
            raise ValidationError(
                f"process set must cover ranks 0..{world_size - 1}, got {ranks}"
            )
        with self._lock:
            existing = self._cps.get(generation)
            if existing is not None:
                if existing.epoch == epoch:
                    return existing  # 同一纪元重复 open -> 幂等
                raise Conflict(
                    f"generation {generation} already bound to epoch "
                    f"{existing.epoch}"
                )
            cp = Checkpoint(
                job_id=self.job_id,
                epoch=epoch,
                generation=generation,
                world_size=world_size,
                shards_per_rank=shards_per_rank,
                processes=tuple(proc_tuple),
                created_ts=now or time.time(),
            )
            self._cps[generation] = cp
            return cp

    def get(self, generation: int) -> Checkpoint:
        with self._lock:
            cp = self._cps.get(generation)
            if cp is None:
                raise NotFound(f"no checkpoint for generation {generation}")
            return cp

    def list_all(self) -> list[Checkpoint]:
        with self._lock:
            return [self._cps[g] for g in sorted(self._cps)]

    def supersede_pending(self, keep_generation: int) -> None:
        """新抢占纪元接管时, 旧的未完成检查点作废 (已完成的永不动)。"""
        with self._lock:
            for g, cp in self._cps.items():
                if g != keep_generation and cp.state == PENDING:
                    cp.state = SUPERSEDED

    def mark_incomplete(self, generation: int) -> Checkpoint:
        with self._lock:
            cp = self.get(generation)
            if cp.state == COMPLETE:
                raise Conflict("cannot mark a complete checkpoint incomplete")
            cp.state = INCOMPLETE
            return cp

    # --------------------------------------------------------------- upload
    def put_shard(
        self,
        *,
        generation: int,
        rank: int,
        index: int,
        data: bytes,
        node: str,
        input_pos: int,
        now: float | None = None,
    ) -> tuple[Shard, bool]:
        """写入一个分片。返回 (分片记录, 是否新写入)。

        调用方 (JobManager) 已完成纪元令牌与代际栅栏校验。
        """
        sid = shard_id(rank, index)
        with self._lock:
            cp = self.get(generation)
            if cp.state != PENDING:
                raise Conflict(
                    f"checkpoint gen{generation} is {cp.state}, "
                    "shard upload rejected"
                )
            if not (0 <= rank < cp.world_size):
                raise ValidationError(f"rank {rank} out of process set")
            if not (0 <= index < cp.shards_per_rank):
                raise ValidationError(
                    f"shard index {index} out of 0..{cp.shards_per_rank - 1}"
                )
            assigned_node = dict(cp.processes)[rank]
            if node != assigned_node:
                raise FenceViolation(
                    f"shard {sid} must be written by {assigned_node}, "
                    f"not {node} (process set is immutable)"
                )
            digest, size, _ = self._store.put(data)
            old = cp.shards.get(sid)
            if old is not None:
                if old.blob == digest and old.input_pos == input_pos:
                    return old, False  # 幂等重传
                raise Conflict(
                    f"shard {sid} already uploaded with different content/pos"
                )
            shard = Shard(
                rank=rank,
                index=index,
                node=node,
                blob=digest,
                size=size,
                input_pos=input_pos,
                uploaded_ts=now or time.time(),
            )
            cp.shards[sid] = shard
            return shard, True

    # ---------------------------------------------------------------- commit
    def commit(self, generation: int, now: float | None = None) -> Checkpoint:
        """全量重校验并尝试完成检查点。

        COMPLETE -> 幂等返回; 缺片或任一 blob 重算哈希不符 -> Conflict,
        检查点保持 PENDING。
        """
        with self._lock:
            cp = self.get(generation)
            if cp.state == COMPLETE:
                return cp
            if cp.state != PENDING:
                raise Conflict(
                    f"cannot commit checkpoint in state {cp.state}"
                )
            missing = cp.missing()
            corrupt: list[str] = []
            for sid, sh in cp.shards.items():
                if not self._store.verify(sh.blob):
                    corrupt.append(sid)
            if missing or corrupt:
                raise Conflict(
                    "checkpoint cannot be marked available: "
                    f"missing={missing}, corrupt={corrupt}"
                )
            cp.state = COMPLETE
            cp.completed_ts = now or time.time()
            cp.manifest = {
                "job_id": self.job_id,
                "epoch": cp.epoch,
                "generation": cp.generation,
                "world_size": cp.world_size,
                "shards_per_rank": cp.shards_per_rank,
                "processes": [
                    {"rank": r, "node": n} for r, n in cp.processes
                ],
                "shards": [
                    cp.shards[shard_id(r, i)].ref()
                    for r in range(cp.world_size)
                    for i in range(cp.shards_per_rank)
                ],
                "manifest_digest": "",  # 下面填充
            }
            cp.manifest["manifest_digest"] = canonical_json(
                {k: v for k, v in cp.manifest.items() if k != "manifest_digest"}
            ).hex()[:32]
            return cp

    # ------------------------------------------------------- replay support
    def restore_after_preempt(
        self,
        *,
        epoch: int,
        generation: int,
        world_size: int,
        shards_per_rank: int,
        created_ts: float,
        processes: list[tuple[int, str]] | None = None,
    ) -> Checkpoint:
        """事件重放: 抢占事件对应的 PENDING 检查点骨架。

        若该代际已有检查点 (重放幂等) 直接返回。分片/完成状态由后续
        shard_uploaded / checkpoint_complete 事件补齐。
        """
        with self._lock:
            existing = self._cps.get(generation)
            if existing is not None:
                return existing
            cp = Checkpoint(
                job_id=self.job_id,
                epoch=epoch,
                generation=generation,
                world_size=world_size,
                shards_per_rank=shards_per_rank,
                processes=tuple(sorted(processes or [])),
                created_ts=created_ts,
            )
            self._cps[generation] = cp
            return cp

    def restore_shard(
        self,
        *,
        generation: int,
        rank: int,
        index: int,
        node: str,
        blob: str,
        size: int,
        input_pos: int,
        uploaded_ts: float,
    ) -> None:
        """事件重放: 按已提交事实回填分片 (不触碰 blob 存储, blob 独立持久)。"""
        with self._lock:
            cp = self.get(generation)
            cp.shards[shard_id(rank, index)] = Shard(
                rank=rank, index=index, node=node, blob=blob, size=size,
                input_pos=input_pos, uploaded_ts=uploaded_ts)

    def restore_complete(
        self, generation: int, *, completed_ts: float,
        manifest: dict | None = None,
    ) -> None:
        """事件重放: 检查点进入 COMPLETE, 复用持久化清单。

        重放不重新校验 blob (重启后 blob 同样持久, 恢复/提交路径会再做
        全量重校验); 但缺失 blob 的场景下不影响状态重建。
        """
        with self._lock:
            cp = self.get(generation)
            cp.state = COMPLETE
            cp.completed_ts = completed_ts
            if manifest is not None:
                cp.manifest = manifest

    def import_complete(
        self,
        *,
        epoch: int,
        generation: int,
        world_size: int,
        shards_per_rank: int,
        processes: list[tuple[int, str]],
        created_ts: float,
        completed_ts: float,
        manifest: dict,
    ) -> Checkpoint:
        """接收端按迁移发布清单构建一个已 COMPLETE 的检查点骨架。

        与 open() 不同: 导入代际从未在本集群经历抢占, 直接以冻结清单的
        COMPLETE 形态存在; 分片由调用方在 *全量内容校验通过后* 用
        restore_shard() 回填。调用方必须先保证不存在同代际检查点。
        """
        with self._lock:
            if generation in self._cps:
                raise Conflict(
                    f"generation {generation} already exists in job "
                    f"{self.job_id}; adopt is only allowed once")
            cp = Checkpoint(
                job_id=self.job_id,
                epoch=epoch,
                generation=generation,
                world_size=world_size,
                shards_per_rank=shards_per_rank,
                processes=tuple(sorted(processes)),
                created_ts=created_ts,
            )
            cp.state = COMPLETE
            cp.completed_ts = completed_ts
            cp.manifest = manifest
            self._cps[generation] = cp
            return cp
