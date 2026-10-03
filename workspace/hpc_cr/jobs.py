"""作业聚合根: 协调抢占纪元、检查点、输出租约与恢复全流程。

线程模型: 每个 Job 一把 RLock; 子管理器 (CheckpointManager/LeaseManager)
各有内部锁, 加锁顺序固定为 Job -> 子管理器, 避免死锁。

持久化: 所有状态变更通过 JobManager._emit 追加为顺序化事件 (单调 seq,
见 persistence.Journal); 服务重启后 _replay() 严格按 seq 重放重建作业、
检查点、租约、输出条目、审计与 **安全事件**。凭证令牌从不入库 (自描述
可验, 重放时用同一 HMAC 密钥重新派生)。
"""

from __future__ import annotations

import dataclasses
import hashlib
import threading
import time
import uuid
from contextlib import contextmanager
from dataclasses import dataclass, field

from .checkpoints import (
    COMPLETE,
    PENDING,
    CheckpointManager,
    shard_id,
)
from .epochs import Scheduler
from .errors import (
    Conflict,
    CpError,
    FenceViolation,
    InvalidToken,
    NotFound,
    ValidationError,
)
from .gc import (
    COMPLETED as GC_COMPLETED,
    PARTIALLY_COMPLETED as GC_PARTIALLY_COMPLETED,
    PLANNED as GC_PLANNED,
    POLICY_RECOVERABLE,
    RUNNING as GC_RUNNING,
    STALE as GC_STALE,
    build_snapshot,
    new_plan_id,
    plan_public,
)
from .migration import (
    ACTIVE_STATES as MIG_ACTIVE,
    CANCELLED as MIG_CANCELLED,
    FAILED as MIG_FAILED,
    ITEM_DONE as MIG_ITEM_DONE,
    ITEM_FAILED as MIG_ITEM_FAILED,
    ITEM_PLANNED as MIG_ITEM_PLANNED,
    ITEM_REUSED as MIG_ITEM_REUSED,
    ITEM_TRANSFERRED as MIG_ITEM_TRANSFERRED,
    KIND_OUTPUT as MIG_KIND_OUTPUT,
    KIND_SHARD as MIG_KIND_SHARD,
    PLANNED as MIG_PLANNED,
    PUBLISHED as MIG_PUBLISHED,
    REASON_CANCELLED,
    REASON_DEST_DIGEST,
    REASON_INCOMPLETE,
    REASON_MANIFEST_CHANGED,
    REASON_SOURCE_DIGEST,
    TERMINAL_STATES as MIG_TERMINAL,
    TRANSFERRING as MIG_TRANSFERRING,
    current_manifest_digest,
    freeze_manifest,
    make_task,
    new_migration_id,
    publication_from_task,
    publication_key,
    recount,
    task_public,
    verify_manifest,
)
from .leases import LeaseManager
from .persistence import Journal
from .security import sha256_hex
from .storage import ContentStore, OutputLog

# 作业状态
RUNNING = "RUNNING"
PREEMPTING = "PREEMPTING"
RELEASED = "RELEASED"
ABORTED = "ABORTED"

# 进程 / 节点状态
ALIVE = "ALIVE"
LOST = "LOST"
UP = "UP"

AUDIT_LIMIT = 500

# 统一安全事件名 (审计流 + 持久化事件类型共用)
SECURITY_DENIED = "security_authz_denied"

# GC 相关事件。带 gc_plan_id 的事件不归属具体作业; pin/unpin 绑定作业。
GC_PLAN_CREATED = "gc_plan_created"
GC_RUN_STARTED = "gc_run_started"
GC_BLOB_DELETED = "gc_blob_deleted"
GC_RUN_FINISHED = "gc_run_finished"
GC_PLAN_STALE = "gc_plan_stale"
GC_GENERATION_PIN = "gc_generation_pin"
GC_GENERATION_UNPIN = "gc_generation_unpin"

# 迁移生命周期事件。带 migration_id 的事件不归属具体作业 (除
# generation_adopted: 接收端集群的接收作业); 发布/取消/失败/创建/采用
# 会改变引用/保留关系, 进入 GC 水位; 逐项传输事件不影响引用关系。
MIG_DEST_REGISTERED = "migration_destination_registered"
MIG_TASK_CREATED = "migration_created"
MIG_TASK_STARTED = "migration_started"
MIG_OBJECT_DONE = "migration_object_done"
MIG_TASK_PUBLISHED = "migration_published"
MIG_TASK_FAILED = "migration_failed"
MIG_TASK_CANCELLED = "migration_cancelled"
GENERATION_ADOPTED = "generation_adopted"

# 会改变"仍可访问数据引用关系/保留代际"的事件。GC 计划水位只跟踪这些事件。
_REFERENCE_EVENTS = frozenset({
    "preempt_issued",
    "shard_uploaded",
    "checkpoint_complete",
    "resources_released",
    "epoch_abandoned",
    "output_committed",
    "restore_verified",
    GC_GENERATION_PIN,
    GC_GENERATION_UNPIN,
    MIG_TASK_CREATED,
    MIG_TASK_PUBLISHED,
    MIG_TASK_FAILED,
    MIG_TASK_CANCELLED,
    GENERATION_ADOPTED,
})

_MIG_TASK_EVENTS = frozenset({
    MIG_TASK_CREATED,
    MIG_TASK_STARTED,
    MIG_OBJECT_DONE,
    MIG_TASK_PUBLISHED,
    MIG_TASK_FAILED,
    MIG_TASK_CANCELLED,
})


@dataclass
class Process:
    rank: int
    node: str
    incarnation: int = 1
    state: str = ALIVE
    last_heartbeat: float = field(default_factory=time.time)
    last_input_pos: int = 0
    last_progress_ts: float = field(default_factory=time.time)

    def to_dict(self) -> dict:
        return {
            "rank": self.rank,
            "node": self.node,
            "incarnation": self.incarnation,
            "state": self.state,
            "last_heartbeat": self.last_heartbeat,
            "last_input_pos": self.last_input_pos,
            "last_progress_ts": self.last_progress_ts,
        }


@dataclass
class Node:
    node_id: str
    state: str = UP
    last_heartbeat: float = field(default_factory=time.time)
    ranks: list[int] = field(default_factory=list)

    def to_dict(self) -> dict:
        return {
            "node_id": self.node_id,
            "state": self.state,
            "last_heartbeat": self.last_heartbeat,
            "ranks": list(self.ranks),
        }


@dataclass
class RestoreReport:
    """恢复验证报告: 只有全部检查项通过 success 才为 True。"""

    job_id: str
    requested_generation: int
    success: bool
    ts: float
    checkpoint_state: str
    manifest_digest: str | None = None
    shards_total: int = 0
    shards_verified: int = 0
    shards_failed: list[str] = field(default_factory=list)
    input_positions: dict[str, int] = field(default_factory=dict)
    old_processes: list[dict] = field(default_factory=list)
    new_processes: list[dict] = field(default_factory=list)
    process_set_match: bool = False
    relocated_ranks: list[int] = field(default_factory=list)
    lease: dict | None = None
    warnings: list[str] = field(default_factory=list)
    error: str | None = None
    # 截至报告生成时该作业的安全事件 (授权拒绝证据), 随报告持久化,
    # 重启后仍可在验证报告中查询; 读取接口会补充报告之后的新事件。
    security_events: list[dict] = field(default_factory=list)

    def to_dict(self) -> dict:
        return {
            "job_id": self.job_id,
            "requested_generation": self.requested_generation,
            "success": self.success,
            "ts": self.ts,
            "checkpoint_state": self.checkpoint_state,
            "manifest_digest": self.manifest_digest,
            "shards_total": self.shards_total,
            "shards_verified": self.shards_verified,
            "shards_failed": list(self.shards_failed),
            "input_positions": dict(self.input_positions),
            "old_processes": list(self.old_processes),
            "new_processes": list(self.new_processes),
            "process_set_match": self.process_set_match,
            "relocated_ranks": list(self.relocated_ranks),
            "lease": self.lease,
            "warnings": list(self.warnings),
            "error": self.error,
            "security_events": list(self.security_events),
        }

    def to_markdown(self) -> str:
        return render_report_md(self)


def render_report_md(r: RestoreReport) -> str:
    lines = [
        f"# 恢复验证报告 — 作业 {r.job_id}",
        "",
        f"- 生成时间: {time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(r.ts))}",
        f"- 请求恢复代际: gen{r.requested_generation}",
        f"- 检查点状态: {r.checkpoint_state}",
        f"- 结论: **{'✅ 验证通过, 允许恢复' if r.success else '❌ 验证失败, 拒绝恢复'}**",
    ]
    if r.error:
        lines += [f"- 失败原因: `{r.error}`"]
    lines += [
        "",
        "## 检查点完整性",
        "",
        f"- 清单摘要 (manifest digest): `{r.manifest_digest or '-'}`",
        f"- 分片总数: {r.shards_total}",
        f"- 重算哈希通过: {r.shards_verified}",
        f"- 校验失败: {r.shards_failed or '无'}",
        f"- 进程集合一致 (rank 0..N-1): {'是' if r.process_set_match else '否'}",
        "",
        "## 输入位点 (恢复后 seek 位置)",
        "",
    ]
    if r.input_positions:
        lines += [
            "| 分片 | rank | 序号 | 输入位点 |",
            "|---|---|---|---|",
        ]
        for sid, pos in sorted(
            r.input_positions.items(),
            key=lambda kv: (int(kv[0].split(":")[0][1:]), kv[1]),
        ):
            rank, idx = sid.split(":")
            lines.append(f"| {sid} | {rank[1:]} | {idx[1:]} | {pos} |")
    else:
        lines.append("(无)")
    lines += ["", "## 进程放置", "", "| rank | 旧节点 | 新节点 | 备注 |", "|---|---|---|---|"]
    old = {p["rank"]: p["node"] for p in r.old_processes}
    relocated = set(r.relocated_ranks)
    for p in r.new_processes:
        rk = p["rank"]
        note = "迁移重调度" if rk in relocated else "同节点"
        lines.append(f"| {rk} | {old.get(rk, '-')} | {p['node']} | {note} |")
    if r.lease:
        lines += [
            "",
            "## 输出租约",
            "",
            f"- 租约 ID: `{r.lease['lease_id']}`",
            f"- 绑定代际: gen{r.lease['generation']}",
            f"- 状态: {r.lease['state']}",
            f"- 有效期至: {time.strftime('%H:%M:%S', time.localtime(r.lease['expires_ts']))}",
        ]
    if r.warnings:
        lines += ["", "## 警告", ""]
        lines += [f"- {w}" for w in r.warnings]
    if r.security_events:
        lines += [
            "",
            f"## 安全事件 (截至报告生成, {len(r.security_events)} 条)",
            "",
            "| seq | 时间 | 凭证 | 操作 | 原因 |",
            "|---|---|---|---|---|",
        ]
        for e in r.security_events:
            lines.append(
                f"| {e.get('seq', '-')} | "
                f"{time.strftime('%H:%M:%S', time.localtime(e['ts']))} | "
                f"{e.get('credential', '-')} | {e.get('operation', '-')} | "
                f"{e.get('reason', '-')} |"
            )
    lines.append("")
    return "\n".join(lines)


class Job:
    def __init__(
        self,
        job_id: str,
        name: str,
        world_size: int,
        shards_per_rank: int,
        rank_nodes: dict[int, str],
        store: ContentStore,
        scheduler: Scheduler,
        manager: "JobManager",
        now: float | None = None,
        lease_ttl: float = 300.0,
    ):
        self.job_id = job_id
        self.name = name
        self.world_size = world_size
        self.shards_per_rank = shards_per_rank
        self.created_ts = now or time.time()
        self.state = RUNNING
        self.store = store
        self.scheduler = scheduler
        self.mgr = manager
        self.lock = threading.RLock()

        self.nodes: dict[str, Node] = {}
        self.processes: dict[int, Process] = {}
        for rank in range(world_size):
            node = rank_nodes[rank]
            self.processes[rank] = Process(
                rank=rank, node=node, last_heartbeat=self.created_ts,
                last_progress_ts=self.created_ts)
            n = self.nodes.setdefault(node, Node(
                node_id=node, last_heartbeat=self.created_ts))
            if rank not in n.ranks:
                n.ranks.append(rank)

        self.cpm = CheckpointManager(job_id, store)
        self.leases = LeaseManager(job_id, scheduler)
        self.outputs = OutputLog()
        self.audit: list[dict] = []

        self.current_epoch = 0
        self.current_generation = 0          # 抢占纪元代际 (0 = 初始)
        self.output_generation = 0           # 当前允许输出的代际
        self.epoch_token: str | None = None
        self.released_generation: int | None = None
        self.restored_generation: int | None = None
        self.released_ts: float | None = None
        self.aborted_ts: float | None = None
        self._last_restore: RestoreReport | None = None
        self._lease_ttl = lease_ttl

        # 初始代际 gen0 的输出租约 (事件由 JobManager.create_job 统一发出)
        self.leases.grant(0, lease_ttl)

    # ------------------------------------------------------------- utility
    def _log(self, event: str, detail: dict | None = None,
             ts: float | None = None):
        """审计事件入口: 实时模式追加顺序化日志, 重放模式仅追加内存审计。"""
        return self.mgr._record(self.job_id, event, detail or {}, ts=ts)

    def epoch_info(self):
        return self.scheduler.current_epoch(self.job_id)

    def snapshot(self) -> dict:
        with self.lock:
            ep = self.epoch_info()
            cur_cp = None
            if self.current_generation:
                cps = {c.generation: c for c in self.cpm.list_all()}
                if self.current_generation in cps:
                    cur_cp = cps[self.current_generation].to_dict()
            return {
                "job_id": self.job_id,
                "name": self.name,
                "state": self.state,
                "world_size": self.world_size,
                "shards_per_rank": self.shards_per_rank,
                "created_ts": self.created_ts,
                "current_epoch": self.current_epoch,
                "current_generation": self.current_generation,
                "output_generation": self.output_generation,
                "released_generation": self.released_generation,
                "restored_generation": self.restored_generation,
                "epoch": ep.to_dict() if ep else None,
                "epoch_grace_remaining": (
                    max(0.0, ep.deadline_ts - time.time()) if ep else None
                ),
                "released_ts": self.released_ts,
                "aborted_ts": self.aborted_ts,
                "current_checkpoint": cur_cp,
                "active_lease": self.leases.current().to_dict()
                if self.leases.current() else None,
                "outputs_total": len(self.outputs.list()),
                "security_events_total": len(
                    self.mgr.security_events(self.job_id)),
            }


class JobManager:
    """作业注册表 + 全部编排用例。

    data_dir 为 None 时纯内存 (测试/临时演示); 给定目录则事件顺序化落盘,
    构造时自动重放, 服务重启后作业/检查点/租约/输出/审计/安全事件均可
    查询且可继续写入。
    """

    def __init__(self, scheduler: Scheduler, data_dir: str | None = None):
        self.scheduler = scheduler
        self._lock = threading.Lock()
        self._jobs: dict[str, Job] = {}
        self.journal = Journal(data_dir)
        self.store = ContentStore(data_dir)
        self._security: list[dict] = []
        self._recording = False

        # 全局引用临界区锁: 计划生成/执行与所有可能新增引用的写操作互斥。
        # 固定顺序为 reference -> migration -> GC state -> 各 Job.lock。
        self._reference_lock = threading.RLock()
        self._migration_lock = threading.RLock()
        self._gc_lock = threading.RLock()
        self._pins: dict[str, set[int]] = {}
        self._gc_plans: dict[str, dict] = {}
        self._reference_seq = 0

        # 检查点迁移状态
        # 目标内容存储: destination_id -> ContentStore (重放后自动重建磁盘后端)
        self._destinations: dict[str, ContentStore] = {}
        self._dest_dirs: dict[str, str | None] = {}
        # 迁移任务: migration_id -> task dict
        self._migrations: dict[str, dict] = {}
        # 临时保留: blob -> [{"migration_id", "source_job_id", ...}, ...]
        self._migration_holds: dict[str, list[dict]] = {}
        # 已发布可恢复代际目录: publication_key -> record
        self._published: dict[tuple, dict] = {}
        self._replay()
        self._recording = True

    @contextmanager
    def _reference_section(self):
        """持有全局引用读/写临界区。

        GC 删除计划扫描和执行用同一把锁; 分片/输出上传在完成 CAS 写入并
        建立元数据引用期间也持有它, 因此扫描不可能看到"刚落盘但尚未引用"
        的新 blob。
        """
        with self._reference_lock:
            yield

    @contextmanager
    def _gc_world(self):
        """全局 GC 快照/执行临界区: reference -> migration -> GC -> 全部作业锁。

        引用锁在持锁期间也冻结作业注册表 (create_job 同样先取引用锁), 因此
        这里直接读 _jobs, 不再获取注册表锁, 避免与 restore 等
        "Job.lock -> 注册表锁" 的既有顺序相反。
        """
        with self._reference_lock, self._migration_lock, self._gc_lock:
            jobs = [self._jobs[jid] for jid in sorted(self._jobs)]
            acquired = []
            try:
                for job in jobs:
                    job.lock.acquire()
                    acquired.append(job)
                yield
            finally:
                for job in reversed(acquired):
                    job.lock.release()

    def _migration_holds_payload(self) -> dict[str, list[dict]]:
        """调用方持有 migration 锁 (或更大的临界区)。"""
        return {
            digest: [dict(h) for h in holds]
            for digest, holds in self._migration_holds.items()
        }

    # ===================================================== event plumbing
    def _record(
        self, job_id: str, event: str, detail: dict, *,
        ts: float | None = None,
    ) -> dict:
        """追加一条审计事件; 实时模式同时落盘 (顺序化), 重放模式只入内存。

        安全事件 (SECURITY_DENIED) 额外进入全局安全事件流。detail 必须
        JSON 可序列化且不含任何凭证原文。
        """
        detail = dict(detail)
        # job_id 作为每条事件的顶层路由字段, 重放据此归属到作业
        detail["job_id"] = job_id
        detail.setdefault("ts", ts or time.time())
        if self._recording:
            evt = self.journal.append(event, detail)
        else:
            evt = {"seq": None, "ts": detail["ts"], "type": event,
                   **{k: v for k, v in detail.items() if k != "ts"}}
        job = self._jobs.get(job_id)
        if job is not None:
            job.audit.append({"ts": evt["ts"], "event": event,
                              "detail": {k: v for k, v in evt.items()
                                         if k not in ("seq", "ts", "type")}})
            if len(job.audit) > AUDIT_LIMIT:
                del job.audit[:-AUDIT_LIMIT]
        if event == SECURITY_DENIED:
            self._security.append(evt)
        if event in _REFERENCE_EVENTS and isinstance(evt.get("seq"), int):
            self._reference_seq = evt["seq"]
        return evt

    def _record_global(self, event: str, detail: dict, *,
                       ts: float | None = None) -> dict:
        """追加不归属单个作业的全局事件 (目前为 GC 生命周期事件)。"""
        detail = dict(detail)
        detail.setdefault("ts", ts or time.time())
        if self._recording:
            evt = self.journal.append(event, detail)
        else:
            evt = {"seq": None, "ts": detail["ts"], "type": event,
                   **{k: v for k, v in detail.items() if k != "ts"}}
        if event in _REFERENCE_EVENTS and isinstance(evt.get("seq"), int):
            self._reference_seq = evt["seq"]
        return evt

    def security_events(self, job_id: str | None = None) -> list[dict]:
        """按 seq 顺序返回安全事件 (可按作业过滤)。"""
        with self._lock:
            events = list(self._security)
        if job_id is not None:
            events = [e for e in events if e.get("job_id") == job_id]
        return sorted(events, key=lambda e: e.get("seq") or 0)

    # ------------------------------------------------------------ create
    def create_job(
        self,
        *,
        name: str,
        world_size: int,
        shards_per_rank: int = 1,
        rank_nodes: dict[int, str] | None = None,
        job_id: str | None = None,
        lease_ttl: float = 300.0,
    ) -> Job:
        if world_size <= 0 or shards_per_rank <= 0:
            raise ValidationError("world_size/shards_per_rank must be positive")
        rank_nodes = rank_nodes or {
            r: f"node-{r // 8}" for r in range(world_size)
        }
        if set(rank_nodes) != set(range(world_size)):
            raise ValidationError("rank_nodes must map every rank 0..N-1")
        job_id = job_id or f"job-{uuid.uuid4().hex[:10]}"
        with self._reference_lock, self._lock:
            if job_id in self._jobs:
                raise Conflict(f"job {job_id} already exists")
            job = Job(
                job_id=job_id,
                name=name,
                world_size=world_size,
                shards_per_rank=shards_per_rank,
                rank_nodes=rank_nodes,
                store=self.store,
                scheduler=self.scheduler,
                manager=self,
                lease_ttl=lease_ttl,
            )
            self._jobs[job_id] = job
            self._record(job_id, "job_created", {
                "name": name, "world_size": world_size,
                "shards_per_rank": shards_per_rank,
                "rank_nodes": {str(r): n for r, n in rank_nodes.items()},
                "lease_ttl": lease_ttl, "created_ts": job.created_ts,
            }, ts=job.created_ts)
            lease = job.leases.get(0)
            self._record(job_id, "lease_granted", {
                "generation": 0, "lease_id": lease.lease_id,
                "issued_ts": lease.issued_ts, "expires_ts": lease.expires_ts,
                "state": lease.state,
            }, ts=lease.issued_ts)
            return job

    def get(self, job_id: str) -> Job:
        job = self._jobs.get(job_id)
        if job is None:
            raise NotFound(f"unknown job {job_id}")
        return job

    def list_jobs(self) -> list[Job]:
        with self._lock:
            return list(self._jobs.values())

    # -------------------------------------------------- security denials
    def _deny(
        self,
        job: Job,
        exc: CpError,
        *,
        credential: str,
        operation: str,
        expected_generation: int,
        output_id: str | None = None,
        data: bytes | None = None,
    ) -> dict:
        """统一记录一次授权失败: 稳定 reason + 只含安全字段, 无凭证原文。"""
        if isinstance(exc, InvalidToken):
            default_reason = "invalid_credential"
        else:
            default_reason = "not_authorized"
        record = {
            "job_id": job.job_id,
            "credential": credential,         # output_lease | epoch_token
            "operation": operation,
            "reason": getattr(exc, "reason", None) or default_reason,
            "http_status": exc.http_status,
            "error": exc.error_code,
            "expected_generation": expected_generation,
            "claimed": getattr(exc, "claimed", None) or {},
        }
        if output_id is not None:
            record["output_id"] = output_id
        if data is not None:
            # 输出载荷摘要不是凭证; 只取短摘要用于关联, 令牌本身绝不记录
            record["attempted_blob"] = hashlib.sha256(data).hexdigest()[:12]
        return self._record(job.job_id, SECURITY_DENIED, record)

    # ---------------------------------------------------------- preempt
    def preempt(
        self,
        job_id: str,
        *,
        reason: str = "scheduler_reclaim",
        kind: str = "all",
        nodes: list[str] | None = None,
        grace_seconds: float = 30.0,
        force_new_epoch: bool = False,
    ) -> dict:
        """调度器回收节点: 签发抢占纪元。

        抢占通知可能因重试重复投递: 当作业已处于 PREEMPTING 且未显式声明
        新抢占 (``force_new_epoch=True``) 时, 原样回传当前纪元, 不递增、
        不轮换令牌。调度器确定要再次回收 (新的一轮抢占) 时传
        force_new_epoch=True, 旧的未完成检查点被 SUPERSEDED。
        """
        job = self.get(job_id)
        with self._reference_section(), job.lock:
            ep = self.scheduler.current_epoch(job_id)
            if (
                not force_new_epoch
                and ep is not None
                and job.state == PREEMPTING
                and job.current_epoch == ep.epoch
            ):
                job._log("preempt_duplicate", {"epoch": ep.epoch})
                return {
                    "preempted": False,
                    "idempotent_replay": True,
                    "epoch": ep.to_dict(),
                    "checkpoint": job.cpm.get(ep.generation).to_dict(),
                    "epoch_token": ep.token,
                }
            if job.state in (RELEASED, ABORTED):
                raise Conflict(f"job is {job.state}, cannot preempt")

            ep = self.scheduler.issue_preemption(
                job_id,
                reason=reason,
                kind=kind,
                nodes=nodes,
                grace_seconds=grace_seconds,
            )
            job.current_epoch = ep.epoch
            job.current_generation = ep.generation
            job.state = PREEMPTING
            job.epoch_token = ep.token

            job.cpm.supersede_pending(ep.generation)
            job.cpm.open(
                epoch=ep.epoch,
                generation=ep.generation,
                world_size=job.world_size,
                shards_per_rank=job.shards_per_rank,
                processes=[job.processes[r].to_dict() for r in
                           sorted(job.processes)],
                now=ep.issued_ts,
            )
            # 抢占瞬间吊销当前输出租约: 旧进程的迟到写入立即失去资格
            revoked = job.leases.revoke_generation(job.output_generation)
            if revoked is not None and revoked.state == "REVOKED":
                self._record(job_id, "lease_revoked", {
                    "generation": revoked.generation,
                    "lease_id": revoked.lease_id,
                    "revoked_ts": revoked.revoked_ts,
                }, ts=revoked.revoked_ts)
            job._log(
                "preempt_issued",
                {
                    "epoch_obj": ep.to_dict(),
                    "epoch": ep.epoch,
                    "generation": ep.generation,
                    "kind": kind,
                    "nodes": list(ep.nodes),
                    "grace_seconds": grace_seconds,
                    "revoked_lease": revoked.lease_id if revoked else None,
                },
                ts=ep.issued_ts,
            )
            return {
                "preempted": True,
                "idempotent_replay": False,
                "epoch": ep.to_dict(),
                "checkpoint": job.cpm.get(ep.generation).to_dict(),
                "epoch_token": ep.token,
            }

    # ----------------------------------------------------- node progress
    def heartbeat(self, job_id: str, node_id: str, now: float | None = None):
        job = self.get(job_id)
        ts = now or time.time()
        with job.lock:
            node = job.nodes.get(node_id)
            if node is None:
                raise NotFound(f"unknown node {node_id}")
            node.last_heartbeat = ts
            recovered = False
            if node.state == LOST:
                node.state = UP
                recovered = True
            for rank in node.ranks:
                proc = job.processes[rank]
                proc.last_heartbeat = ts
                if proc.state == LOST:
                    proc.state = ALIVE
                    proc.incarnation += 1
            if recovered:
                job._log("node_recovered", {"node": node_id}, ts=ts)
            else:
                job._log("node_heartbeat", {"node": node_id}, ts=ts)
            return node.to_dict()

    def report_progress(self, job_id: str, rank: int, input_pos: int):
        job = self.get(job_id)
        with job.lock:
            proc = job.processes.get(rank)
            if proc is None:
                raise NotFound(f"unknown rank {rank}")
            ts = time.time()
            proc.last_input_pos = input_pos
            proc.last_progress_ts = ts
            job._log("progress_reported", {"rank": rank,
                                           "input_pos": input_pos}, ts=ts)
            return proc.to_dict()

    def mark_node_lost(self, job_id: str, node_id: str):
        """节点失联: 节点与其上全部 rank 标记 LOST, 阻塞其分片。"""
        job = self.get(job_id)
        with job.lock:
            node = job.nodes.get(node_id)
            if node is None:
                raise NotFound(f"unknown node {node_id}")
            node.state = LOST
            lost_ranks = []
            for rank in node.ranks:
                proc = job.processes[rank]
                proc.state = LOST
                lost_ranks.append(rank)
            job._log("node_lost", {"node": node_id, "ranks": lost_ranks})
            return {"node": node.to_dict(), "lost_ranks": lost_ranks}

    # ------------------------------------------------------- shard write
    def _check_epoch_token(self, job: Job, token: str | None, *,
                           operation: str):
        if not token:
            from .leases import REASON_MISSING_TOKEN, stable_denial
            deny = InvalidToken(
                stable_denial(REASON_MISSING_TOKEN, credential="epoch_token"),
                reason=REASON_MISSING_TOKEN)
            self._deny(job, deny, credential="epoch_token",
                       operation=operation,
                       expected_generation=job.current_generation)
            raise deny
        try:
            payload = self.scheduler.verify_epoch_token(
                token, job_id=job.job_id, epoch=job.current_epoch
            )
            if payload["generation"] != job.current_generation:
                deny = FenceViolation(
                    "epoch credential is not valid for the current generation",
                    reason="stale_generation")
                deny.claimed = {
                    "generation": payload.get("generation"),
                    "expected_generation": job.current_generation}
                raise deny
            return payload
        except CpError as exc:
            # 统一授权失败处理: 伪造/跨作业/旧纪元令牌同样留下安全事件
            if isinstance(exc, (InvalidToken, FenceViolation)):
                self._deny(job, exc, credential="epoch_token",
                           operation=operation,
                           expected_generation=job.current_generation)
            raise

    def upload_shard(
        self,
        job_id: str,
        *,
        epoch_token: str,
        rank: int,
        index: int,
        node: str,
        input_pos: int,
        data: bytes,
    ) -> dict:
        job = self.get(job_id)
        with self._reference_section(), job.lock:
            self._check_epoch_token(job, epoch_token,
                                    operation="upload_shard")
            if job.state != PREEMPTING:
                raise Conflict(f"job is {job.state}, not accepting shards")
            proc = job.processes.get(rank)
            if proc is None:
                raise NotFound(f"unknown rank {rank}")
            if proc.state == LOST:
                raise Conflict(
                    f"rank {rank} on {node} is marked LOST; shard cannot be "
                    "accepted until the node recovers/heartbeats"
                )
            shard, is_new = job.cpm.put_shard(
                generation=job.current_generation,
                rank=rank,
                index=index,
                data=data,
                node=node,
                input_pos=input_pos,
            )
            cp = job.cpm.get(job.current_generation)
            if is_new:
                job._log("shard_uploaded", {
                    "shard": shard_id(rank, index),
                    "generation": job.current_generation,
                    "rank": rank, "index": index, "node": node,
                    "blob": shard.blob, "size": shard.size,
                    "input_pos": input_pos,
                    "uploaded_ts": shard.uploaded_ts,
                }, ts=shard.uploaded_ts)
            else:
                job._log("shard_upload_duplicate", {
                    "shard": shard_id(rank, index),
                    "generation": job.current_generation,
                    "input_pos": input_pos})
            return {"shard": shard.ref(), "stored": is_new,
                    "checkpoint": cp.to_dict()}

    # ------------------------------------------------------------- commit
    def commit_checkpoint(self, job_id: str, *, epoch_token: str) -> dict:
        """提交检查点: 全部分片重校验通过才可 COMPLETE。"""
        job = self.get(job_id)
        with self._reference_section(), job.lock:
            self._check_epoch_token(job, epoch_token,
                                    operation="commit_checkpoint")
            gen = job.current_generation
            cp_before = job.cpm.get(gen)
            if cp_before.state == COMPLETE:
                job._log("commit_duplicate", {"generation": gen})
                return {"checkpoint": cp_before.to_dict(), "already_complete": True}
            if cp_before.state != PENDING:
                raise Conflict(
                    f"checkpoint gen{gen} is {cp_before.state}, cannot commit"
                )
            cp = job.cpm.commit(gen)
            job._log("checkpoint_complete", {
                "generation": gen, "shards": len(cp.shards),
                "completed_ts": cp.completed_ts, "manifest": cp.manifest},
                ts=cp.completed_ts)
            return {"checkpoint": cp.to_dict(), "already_complete": False}

    # ------------------------------------------------------------- release
    def confirm_release(self, job_id: str, *, epoch_token: str) -> dict:
        """确认释放资源 — 只有当前纪元检查点 COMPLETE 才允许。"""
        job = self.get(job_id)
        with self._reference_section(), job.lock:
            self._check_epoch_token(job, epoch_token,
                                    operation="confirm_release")
            gen = job.current_generation
            cp = job.cpm.get(gen)
            if cp.state != COMPLETE:
                raise Conflict(
                    "resource release refused: checkpoint gen"
                    f"{gen} is {cp.state}, not COMPLETE; missing/verified "
                    f"shards: {cp.missing()}"
                )
            ts = time.time()
            job.state = RELEASED
            job.released_generation = gen
            job.released_ts = ts
            revoked = job.leases.revoke_generation(job.output_generation)
            if revoked is not None and revoked.state == "REVOKED":
                self._record(job_id, "lease_revoked", {
                    "generation": revoked.generation,
                    "lease_id": revoked.lease_id,
                    "revoked_ts": revoked.revoked_ts,
                }, ts=revoked.revoked_ts)
            job._log("resources_released", {"generation": gen}, ts=ts)
            return {"released": True, "generation": gen,
                    "released_ts": ts, "job": job.snapshot()}

    def abort_epoch(self, job_id: str, *, reason: str = "grace_expired") -> dict:
        """调度器在宽限期用尽后放弃: 检查点 INCOMPLETE, 作业 ABORTED。"""
        job = self.get(job_id)
        with self._reference_section(), job.lock:
            gen = job.current_generation
            cp = job.cpm.get(gen)
            if cp.state == COMPLETE:
                raise Conflict("checkpoint already complete; release instead")
            ts = time.time()
            job.cpm.mark_incomplete(gen)
            job.state = ABORTED
            job.aborted_ts = ts
            revoked = job.leases.revoke_generation(job.output_generation)
            if revoked is not None and revoked.state == "REVOKED":
                self._record(job_id, "lease_revoked", {
                    "generation": revoked.generation,
                    "lease_id": revoked.lease_id,
                    "revoked_ts": revoked.revoked_ts,
                }, ts=revoked.revoked_ts)
            job._log("epoch_abandoned", {"generation": gen, "reason": reason},
                     ts=ts)
            return {"aborted": True, "generation": gen,
                    "checkpoint": cp.to_dict()}

    # -------------------------------------------------------------- output
    def append_output(
        self,
        job_id: str,
        *,
        lease_token: str | None,
        output_id: str,
        data: bytes,
        rank: int | None = None,
    ) -> dict:
        """提交最终输出。

        授权失败统一处理: 缺失/伪造/超期/已吊销/跨作业/旧代际凭证一律
        得到稳定错误 (401/412 + reason + 固定消息), 并写入一条不含凭证
        原文的顺序化安全事件; 请求在授权前终止, OutputLog 不发生任何
        写入 —— 重复并发请求不可能增加结果条目。
        """
        job = self.get(job_id)
        with self._reference_section(), job.lock:
            gen = job.output_generation
            try:
                lease = job.leases.authorize(
                    lease_token, expected_generation=gen)
            except CpError as exc:
                self._deny(job, exc, credential="output_lease",
                           operation="append_output",
                           expected_generation=gen,
                           output_id=output_id, data=data)
                raise
            digest, size, _stored = job.store.put(data)
            ts = time.time()
            rec, is_new = job.outputs.append(
                generation=gen, output_id=output_id, blob=digest, size=size,
                rank=rank, ts=ts)
            if is_new:
                job._log("output_committed", {
                    "output_id": output_id, "generation": gen,
                    "blob": digest, "size": size, "rank": rank, "ts": ts}, ts=ts)
            else:
                job._log("output_duplicate", {
                    "output_id": output_id, "generation": gen,
                    "blob": digest[:12], "rank": rank})
            return {"record": rec.to_dict(), "delivered": is_new,
                    "generation": gen, "lease_id": lease.lease_id}

    def list_outputs(self, job_id: str, generation: int | None = None):
        collected = self._collected_blob_plans()
        job = self.get(job_id)
        with job.lock:
            outputs = [r.to_dict() for r in job.outputs.list(generation)]
        for out in outputs:
            plan_id = collected.get(out["blob"])
            if plan_id:
                out["collected_by"] = plan_id
                out["physically_available"] = False
        return outputs

    # =============================================================== GC
    @staticmethod
    def _normalize_generation_map(
        value: dict | None, *, field_name: str
    ) -> dict[str, list[int]]:
        if value is None:
            return {}
        if not isinstance(value, dict):
            raise ValidationError(f"{field_name} must be an object")
        result: dict[str, list[int]] = {}
        for job_id, gens in value.items():
            if not isinstance(job_id, str) or not isinstance(gens, list):
                raise ValidationError(
                    f"{field_name} must map job id to generation list")
            try:
                normalized = sorted({int(g) for g in gens})
            except (TypeError, ValueError) as exc:
                raise ValidationError(
                    f"{field_name} generations must be integers") from exc
            if any(g < 0 for g in normalized):
                raise ValidationError(
                    f"{field_name} generations must be non-negative")
            result[job_id] = normalized
        return result

    def pin_generation(
        self, job_id: str, generation: int, *, reason: str = "manual",
        unpin: bool = False,
    ) -> dict:
        """锁定/解锁需要保留的代际。任何变更都会使既有计划水位过期。"""
        if generation < 0:
            raise ValidationError("generation must be non-negative")
        job = self.get(job_id)
        with self._reference_lock, self._gc_lock, job.lock:
            pins = self._pins.setdefault(job_id, set())
            existed = generation in pins
            if unpin:
                pins.discard(generation)
                if not pins:
                    self._pins.pop(job_id, None)
            else:
                pins.add(generation)
            event = GC_GENERATION_UNPIN if unpin else GC_GENERATION_PIN
            self._record(job_id, event, {
                "generation": generation, "reason": reason,
                "pinned_generations": sorted(self._pins.get(job_id, [])),
                "changed": existed if unpin else not existed,
            })
            return {"job_id": job_id, "generation": generation,
                    "pinned": not unpin,
                    "pinned_generations": sorted(self._pins.get(job_id, []))}

    def generation_pins(self, job_id: str | None = None) -> dict:
        self.get(job_id) if job_id is not None else None
        with self._gc_lock:
            pins = {k: sorted(v) for k, v in sorted(self._pins.items())}
        if job_id is not None:
            return {"job_id": job_id,
                    "pinned_generations": pins.get(job_id, [])}
        return {"pins": pins}

    def create_gc_plan(
        self,
        *,
        policy: str = POLICY_RECOVERABLE,
        min_retain_generation: int | None = None,
        retain_generations: dict[str, list[int]] | None = None,
        include_pins: bool = True,
        plan_id: str | None = None,
        now: float | None = None,
    ) -> dict:
        """生成带 journal 水位和引用指纹的回收计划; 不删除任何数据。"""
        if policy not in ("recoverable", "all_complete", "watermark"):
            raise ValidationError("unknown gc policy")
        if policy == "watermark" and min_retain_generation is None:
            raise ValidationError(
                "watermark policy requires min_retain_generation")
        if min_retain_generation is not None and min_retain_generation < 0:
            raise ValidationError("min_retain_generation must be non-negative")
        retain = self._normalize_generation_map(
            retain_generations, field_name="retain_generations")
        for job_id in retain:
            self.get(job_id)

        with self._gc_world():
            active = [p for p in self._gc_plans.values()
                      if p["state"] in (GC_PLANNED, GC_RUNNING,
                                        GC_PARTIALLY_COMPLETED)]
            if active:
                raise Conflict(
                    f"gc plan {active[0]['plan_id']} is still "
                    f"{active[0]['state']}; finish or retry it first")
            jobs = self.list_jobs()
            pins = {k: list(v) for k, v in self._pins.items()} \
                if include_pins else {}
            holds = self._migration_holds_payload()
            snapshot = build_snapshot(
                jobs, inventory=self.store.inventory(), pins=pins,
                policy=policy,
                min_retain_generation=min_retain_generation,
                retain_generations=retain,
                migration_holds=holds)
            ts = now or time.time()
            plan = {
                "plan_id": plan_id or new_plan_id(),
                "state": GC_PLANNED,
                "created_ts": ts,
                "started_ts": None,
                "finished_ts": None,
                "watermark_seq": self._reference_seq,
                "completion_seq": None,
                **snapshot,
                "deleted_items": 0,
                "deleted_bytes": 0,
                "skipped_items": 0,
                "skip_reasons": {},
                "recovery_reasons": {},
                "error": None,
                "stale_reason": None,
            }
            self._gc_plans[plan["plan_id"]] = plan
            public = plan_public(plan)
            self._record_global(GC_PLAN_CREATED, {"plan": public}, ts=ts)
            return public

    def get_gc_plan(self, plan_id: str) -> dict:
        with self._gc_lock:
            plan = self._gc_plans.get(plan_id)
            if plan is None:
                raise NotFound(f"unknown gc plan {plan_id}")
            return plan_public(plan)

    def list_gc_plans(self, job_id: str | None = None) -> list[dict]:
        with self._gc_lock:
            plans = [plan_public(self._gc_plans[pid])
                     for pid in sorted(self._gc_plans)]
        if job_id is None:
            return plans
        return [p for p in plans
                if job_id in p.get("retained_generations", {})
                or any(c["references"] and any(
                    r.get("job_id") == job_id for r in c["references"])
                    for c in p["candidates"])]

    def _candidate_index(self, plan: dict) -> dict[str, dict]:
        return {c["blob"]: c for c in plan["candidates"]}

    def _plan_is_stale(self, plan: dict) -> str | None:
        """在全局引用锁和全部作业锁内调用; 返回稳定的过期原因。"""
        if self._reference_seq < int(plan["watermark_seq"]):
            return "journal_rewound"
        if self._reference_seq != int(plan["watermark_seq"]):
            return "reference_seq_advanced"
        jobs = self.list_jobs()
        current = build_snapshot(
            jobs,
            inventory=self.store.inventory(),
            pins=plan.get("pins", {}),
            policy=plan["policy"],
            min_retain_generation=plan.get("min_retain_generation"),
            retain_generations=plan.get("retain_generations"),
            migration_holds=self._migration_holds_payload(),
        )
        if current["reference_fingerprint"] != plan["reference_fingerprint"]:
            return "reference_fingerprint_changed"
        current_retained = {
            ref["blob"]
            for ref in current.get("references", [])
            if str(ref["generation"]) in current.get(
                "retained_generations", {}).get(ref["job_id"], {})
        }
        planned = {c["blob"]: c for c in plan["candidates"]}
        physical = {digest for digest, _ in self.store.inventory()}
        extra = (physical - current_retained - set(planned)
                 - set(self._migration_holds))
        if extra:
            return "physical_blob_set_changed"
        # 崩溃可能发生在"物理已 unlink、删除事件未 fsync"之间; 续跑时允许
        # 计划内的部分 blob 已不存在, 但不允许已删除 blob 重新出现。
        deleted = {digest for digest, c in planned.items()
                   if c["status"] == "DELETED"}
        if deleted & physical:
            return "deleted_blob_reappeared"
        return None

    def _mark_plan_stale(self, plan: dict, reason: str, *,
                         ts: float | None = None) -> dict:
        ts = ts or time.time()
        plan["state"] = GC_STALE
        plan["finished_ts"] = ts
        plan["stale_reason"] = reason
        public = plan_public(plan)
        self._record_global(GC_PLAN_STALE, {
            "plan_id": plan["plan_id"], "reason": reason,
            "watermark_seq": plan["watermark_seq"],
            "current_seq": self._reference_seq,
            "plan": public,
        }, ts=ts)
        return public

    def _record_skip(self, plan: dict, candidate: dict, reason: str) -> None:
        candidate["status"] = "SKIPPED"
        candidate["skip_reason"] = reason
        plan["skipped_items"] += 1
        plan["skip_reasons"][reason] = plan["skip_reasons"].get(reason, 0) + 1

    def execute_gc_plan(self, plan_id: str) -> dict:
        """按计划删除; 水位/引用/物理集合发生变化则拒绝执行。

        RUNNING 计划可在进程重启后继续。每个 blob 先物理删除、再追加删除
        事件; 若崩溃发生在两者之间, 重试会把该 blob 记为
        already_deleted_after_crash 而不是再次暴露给任何引用路径。
        """
        with self._gc_world():
            plan = self._gc_plans.get(plan_id)
            if plan is None:
                raise NotFound(f"unknown gc plan {plan_id}")
            if plan["state"] == GC_STALE:
                raise Conflict(
                    f"gc plan {plan_id} is stale: {plan.get('stale_reason')}")
            if plan["state"] == GC_COMPLETED:
                return plan_public(plan)
            if plan["state"] not in (GC_PLANNED, GC_RUNNING,
                                     GC_PARTIALLY_COMPLETED):
                raise Conflict(
                    f"gc plan {plan_id} cannot execute in state "
                    f"{plan['state']}")

            stale_reason = self._plan_is_stale(plan)
            if stale_reason is not None:
                public = self._mark_plan_stale(plan, stale_reason)
                raise Conflict(
                    f"gc plan {plan_id} rejected: {stale_reason}",
                )

            is_resume = plan["state"] in (GC_RUNNING, GC_PARTIALLY_COMPLETED)
            if not is_resume:
                plan["state"] = GC_RUNNING
                plan["started_ts"] = time.time()
                self._record_global(GC_RUN_STARTED, {
                    "plan_id": plan_id,
                    "watermark_seq": plan["watermark_seq"]})

            errors: list[dict] = []
            for candidate in plan["candidates"]:
                if candidate["status"] == "DELETED":
                    continue
                if candidate["status"] == "SKIPPED":
                    continue
                digest = candidate["blob"]
                try:
                    existed, freed = self.store.delete(digest)
                except OSError as exc:
                    candidate["status"] = "FAILED"
                    candidate["skip_reason"] = "delete_failed"
                    plan["skipped_items"] += 1
                    plan["skip_reasons"]["delete_failed"] = \
                        plan["skip_reasons"].get("delete_failed", 0) + 1
                    errors.append({"blob": digest, "error": str(exc)})
                    continue
                if existed:
                    candidate["status"] = "DELETED"
                    candidate["deleted_ts"] = time.time()
                    plan["deleted_items"] += 1
                    plan["deleted_bytes"] += freed
                    recovery_reason = None
                else:
                    # 仅可能出现在"物理删除已成功、删除事件未及落盘"的
                    # 崩溃恢复; 该项仍计入实际删除项, 并单独记录恢复原因。
                    candidate["status"] = "DELETED"
                    candidate["deleted_ts"] = time.time()
                    plan["deleted_items"] += 1
                    plan["deleted_bytes"] += candidate["size"]
                    recovery_reason = "already_deleted_after_crash"
                    plan["recovery_reasons"][recovery_reason] = \
                        plan["recovery_reasons"].get(recovery_reason, 0) + 1
                self._record_global(GC_BLOB_DELETED, {
                    "plan_id": plan_id, "blob": digest,
                    "size": candidate["size"], "existed": existed,
                    "reason": recovery_reason,
                    "kind": candidate["kind"],
                    "references": candidate["references"],
                }, ts=candidate["deleted_ts"])

            ts = time.time()
            plan["finished_ts"] = ts
            plan["completion_seq"] = self._reference_seq
            if errors:
                plan["state"] = GC_PARTIALLY_COMPLETED
                plan["error"] = "one or more blob deletions failed"
            else:
                plan["state"] = GC_COMPLETED
                plan["error"] = None
            public = plan_public(plan)
            self._record_global(GC_RUN_FINISHED, {
                "plan_id": plan_id, "state": plan["state"],
                "deleted_items": plan["deleted_items"],
                "deleted_bytes": plan["deleted_bytes"],
                "skipped_items": plan["skipped_items"],
                "skip_reasons": plan["skip_reasons"],
                "recovery_reasons": plan["recovery_reasons"],
                "errors": errors,
                "completion_seq": plan["completion_seq"],
                "plan": public,
            }, ts=ts)
            return public

    def _collected_blob_plans(self) -> dict[str, str]:
        with self._gc_lock:
            return {
                c["blob"]: p["plan_id"]
                for p in self._gc_plans.values()
                if p["state"] in (GC_COMPLETED, GC_PARTIALLY_COMPLETED,
                                  GC_RUNNING)
                for c in p["candidates"] if c["status"] == "DELETED"
            }

    # ====================================================== checkpoint migration
    def register_destination(
        self, destination_id: str, *,
        data_dir: str | None = None, store: ContentStore | None = None,
        reason: str = "cluster_relocation",
    ) -> dict:
        """注册接收端内容存储。

        传入显式 ``store`` 用于测试; 否则以 ``data_dir`` 打开磁盘后端
        (重放重建时也是同一规则), 两者皆空为临时内存存储。重复注册同一
        destination_id 幂等返回已有视图 (目录一致时不轮换后端)。
        """
        with self._reference_lock, self._migration_lock:
            existing = self._destinations.get(destination_id)
            if existing is not None:
                return self._destination_view(destination_id, existing)
            if store is not None:
                dst = store
                data_dir = getattr(store, "_data_dir", data_dir)
            else:
                dst = ContentStore(data_dir)
            self._destinations[destination_id] = dst
            self._dest_dirs[destination_id] = data_dir
            self._record_global(MIG_DEST_REGISTERED, {
                "destination_id": destination_id,
                "data_dir": data_dir, "persistent": data_dir is not None,
                "reason": reason,
            })
            return self._destination_view(destination_id, dst)

    @staticmethod
    def _destination_view(destination_id: str, store: ContentStore) -> dict:
        blobs, total = 0, 0
        for digest, size in store.inventory():
            blobs += 1
            total += size
        return {"destination_id": destination_id,
                "persistent": getattr(store, "_data_dir", None) is not None,
                "data_dir": getattr(store, "_data_dir", None),
                "blob_count": blobs, "blob_bytes": total}

    def list_destinations(self) -> list[dict]:
        with self._migration_lock:
            return [self._destination_view(did, st)
                    for did, st in sorted(self._destinations.items())]

    def _add_holds(self, task: dict) -> None:
        for obj in task["objects"]:
            self._migration_holds.setdefault(obj["blob"], []).append({
                "migration_id": task["migration_id"],
                "source_job_id": task["source_job_id"],
                "source_generation": task["source_generation"],
                "object_id": obj["object_id"],
                "kind": obj["kind"],
            })

    def _release_holds(self, migration_id: str) -> int:
        released = 0
        for digest, holds in list(self._migration_holds.items()):
            kept = [h for h in holds if h["migration_id"] != migration_id]
            released += len(holds) - len(kept)
            if kept:
                self._migration_holds[digest] = kept
            else:
                self._migration_holds.pop(digest, None)
        return released

    def create_migration(
        self,
        job_id: str,
        *,
        generation: int,
        destination_id: str,
        migration_id: str | None = None,
        reason: str = "cluster_relocation",
    ) -> dict:
        """导出端冻结对象清单并临时保留涉及载荷 (可恢复迁移任务起点)。

        对同一 (job, generation, destination) 的活跃任务重复请求幂等:
        原样返回已创建任务, 不重新冻结、不重复保留。
        """
        with self._reference_lock, self._migration_lock:
            job = self.get(job_id)
            if destination_id not in self._destinations:
                raise NotFound(
                    f"unknown migration destination {destination_id!r}; "
                    "register it first")
            for existing in self._migrations.values():
                if (existing["source_job_id"] == job_id
                        and existing["source_generation"] == generation
                        and existing["destination_id"] == destination_id
                        and existing["state"] in MIG_ACTIVE):
                    view = task_public(existing)
                    view["idempotent_replay"] = True
                    return view
            with job.lock:
                # freeze_manifest 会对源载荷逐个重读重算哈希; 失败时不加保留
                manifest = freeze_manifest(job, generation)
                ts = time.time()
                task = make_task(
                    migration_id=migration_id or new_migration_id(),
                    job=job, generation=generation,
                    destination_id=destination_id, manifest=manifest,
                    watermark_seq=self._reference_seq, now=ts)
            self._migrations[task["migration_id"]] = task
            self._add_holds(task)
            public = task_public(task)
            self._record_global(MIG_TASK_CREATED, {
                "migration_id": task["migration_id"],
                "job_id": job_id,
                "source_job_id": job_id,
                "source_generation": generation,
                "destination_id": destination_id,
                "reason": reason,
                "watermark_seq": task["watermark_seq"],
                "manifest_digest": task["manifest_digest"],
                "total_items": task["total_items"],
                "total_bytes": task["total_bytes"],
                "held_items": len(task["objects"]),
                "task": public,
            }, ts=ts)
            public["idempotent_replay"] = False
            return public

    def _get_task(self, migration_id: str) -> dict:
        task = self._migrations.get(migration_id)
        if task is None:
            raise NotFound(f"unknown migration task {migration_id}")
        return task

    def _finish_failed(self, task: dict, reason: str, message: str, *,
                       release: bool, ts: float) -> None:
        """任务终态失败: 不发布可恢复代际, 释放临时保留。"""
        task["state"] = MIG_FAILED
        task["reason"] = reason
        task["error"] = message
        task["finished_ts"] = ts
        task["completion_seq"] = self._reference_seq
        released = self._release_holds(task["migration_id"]) if release else 0
        self._record_global(MIG_TASK_FAILED, {
            "migration_id": task["migration_id"],
            "job_id": task["source_job_id"],
            "source_job_id": task["source_job_id"],
            "source_generation": task["source_generation"],
            "destination_id": task["destination_id"],
            "reason": reason, "error": message,
            "released_holds": released,
            "task": task_public(task),
        }, ts=ts)

    def transfer_migration(
        self, migration_id: str, *,
        object_ids: list[str] | None = None,
        limit: int | None = None,
        auto_finalize: bool = True,
    ) -> dict:
        """推进迁移: 接收端按摘要复用已有对象, 缺失对象才传输。

        - 中断/重复请求/重启后继续同一任务: TRANSFERRED/REUSED 对象跳过;
        - 每项在源端和目标端各做一次重读重算哈希, 摘要错误标记该项失败;
        - ``object_ids`` 只传指定项; ``limit`` 限制本次处理的待办项数;
        - 全部对象完成后默认立即全量校验并一次性发布 (finalize)。
        """
        with self._reference_lock, self._migration_lock:
            task = self._get_task(migration_id)
            if task["state"] in MIG_TERMINAL:
                return task_public(task)
            job = self.get(task["source_job_id"])
            dst = self._destinations[task["destination_id"]]

            # 清单冻结后源端代际若产生新输出 -> 清单已变化, 不得继续/发布
            with job.lock:
                fresh_digest = current_manifest_digest(
                    job, task["source_generation"])
            if fresh_digest != task["manifest_digest"]:
                self._finish_failed(
                    task, REASON_MANIFEST_CHANGED,
                    "source manifest changed after freeze: outputs added to "
                    "the generation; the frozen migration cannot publish",
                    release=True, ts=time.time())
                raise Conflict(task["error"])

            wanted = set(object_ids) if object_ids is not None else None
            if wanted is not None:
                unknown = wanted - {o["object_id"] for o in task["objects"]}
                if unknown:
                    raise ValidationError(
                        f"unknown migration objects: {sorted(unknown)}")
            # 普通轮次只处理从未完成 (PLANNED) 的对象; ITEM_FAILED 项必须由
            # 调用方在 object_ids 中显式点名重试 (摘要错误不得被静默吞掉)。
            pending = [
                o for o in task["objects"]
                if (o["status"] == MIG_ITEM_PLANNED
                    or (wanted is not None
                        and o["object_id"] in wanted
                        and o["status"] not in MIG_ITEM_DONE))
                and (wanted is None or o["object_id"] in wanted)
            ]
            if limit is not None and limit >= 0:
                pending = pending[:limit]

            if task["state"] == MIG_PLANNED and pending:
                task["state"] = MIG_TRANSFERRING
                task["started_ts"] = time.time()
                self._record_global(MIG_TASK_STARTED, {
                    "migration_id": migration_id,
                    "job_id": task["source_job_id"],
                    "source_job_id": task["source_job_id"],
                    "source_generation": task["source_generation"],
                    "destination_id": task["destination_id"],
                    "started_ts": task["started_ts"],
                }, ts=task["started_ts"])

            manifest_by_id = {o["object_id"]: o
                              for o in task["manifest"]["objects"]}
            for item in pending:
                retry_of_failed = item["status"] == MIG_ITEM_FAILED
                if retry_of_failed:
                    item["last_error"] = None
                digest = item["blob"]
                item["attempts"] += 1

                # 源端始终是权威: 重读重算 (不信任冻结时刻); 源坏 -> 项失败
                try:
                    data = job.store.get(digest)
                except NotFound:
                    item["status"] = MIG_ITEM_FAILED
                    item["last_error"] = REASON_SOURCE_DIGEST
                    continue
                if sha256_hex(data) != digest:
                    item["status"] = MIG_ITEM_FAILED
                    item["last_error"] = REASON_SOURCE_DIGEST
                    continue

                if dst.has(digest) and dst.verify(digest):
                    # 接收端已有同摘要且内容完好 -> 按摘要复用
                    item["status"] = MIG_ITEM_REUSED
                    item["completed_ts"] = time.time()
                    recount(task)
                    self._emit_object_done(task, item, "reused")
                    continue
                if dst.has(digest) and not retry_of_failed:
                    # 首次遇到损坏的"同键"对象: 摘要错误, 项失败 ——
                    # 绝不在本轮静默替换冒充; 客户端显式重试该项才会修复。
                    item["status"] = MIG_ITEM_FAILED
                    item["last_error"] = REASON_DEST_DIGEST
                    continue
                # 缺失对象, 或显式重试失败项: 用源端正确字节写入/原子修复
                was_corrupt = dst.has(digest)
                try:
                    if was_corrupt:
                        dst.rewrite(digest, data)
                    else:
                        stored_digest, _size, _new = dst.put(data)
                        if stored_digest != digest:
                            raise Conflict("destination digest mismatch")
                except (OSError, Conflict, ValidationError):
                    item["status"] = MIG_ITEM_FAILED
                    item["last_error"] = REASON_DEST_DIGEST
                    continue
                if not dst.verify(digest):
                    item["status"] = MIG_ITEM_FAILED
                    item["last_error"] = REASON_DEST_DIGEST
                    continue
                item["status"] = MIG_ITEM_TRANSFERRED
                item["last_error"] = None
                item["completed_ts"] = time.time()
                self._emit_object_done(
                    task, item,
                    "repaired" if was_corrupt else "transferred",
                    manifest_by_id[item["object_id"]])

            recount(task)
            public = task_public(task)
            if auto_finalize and all(
                    o["status"] in MIG_ITEM_DONE for o in task["objects"]):
                self._finalize_locked(task)
                return task_public(task)
            return public

    def _emit_object_done(self, task: dict, item: dict, outcome: str,
                          manifest_obj: dict | None = None) -> None:
        """逐项传输/复用事件 (不含载荷; 断点续传与重启据此重建进度)。"""
        self._record_global(MIG_OBJECT_DONE, {
            "migration_id": task["migration_id"],
            "job_id": task["source_job_id"],
            "source_job_id": task["source_job_id"],
            "source_generation": task["source_generation"],
            "destination_id": task["destination_id"],
            "object_id": item["object_id"],
            "kind": item["kind"],
            "blob": item["blob"],
            "size": item["size"],
            "outcome": outcome,
            "status": item["status"],
            "attempts": item["attempts"],
        }, ts=item["completed_ts"])

    def finalize_migration(self, migration_id: str) -> dict:
        """全量内容校验通过后才一次性发布可恢复代际; 否则任务失败。"""
        with self._reference_lock, self._migration_lock:
            task = self._get_task(migration_id)
            if task["state"] in MIG_TERMINAL:
                return task_public(task)
            self._finalize_locked(task)
            return task_public(task)

    def _finalize_locked(self, task: dict) -> None:
        """发布闸门: 清单重检 + 全部对象在目标端重读重算哈希。

        任一对象缺失/摘要错误, 或仍有未完成/失败项, 任务 FAILED 且绝不
        写入发布目录; 临时保留随失败释放。
        """
        ts = time.time()
        job = self.get(task["source_job_id"])
        dst = self._destinations[task["destination_id"]]
        with job.lock:
            fresh_digest = current_manifest_digest(
                job, task["source_generation"])
        if fresh_digest != task["manifest_digest"]:
            self._finish_failed(
                task, REASON_MANIFEST_CHANGED,
                "source manifest changed after freeze; no restorable "
                "generation is published", release=True, ts=ts)
            return

        bad: list[str] = []
        not_done = []
        for item in task["objects"]:
            if item["status"] not in MIG_ITEM_DONE:
                not_done.append(item["object_id"])
            elif not dst.verify(item["blob"]):
                bad.append(item["object_id"])
                if item["status"] != MIG_ITEM_FAILED:
                    item["status"] = MIG_ITEM_FAILED
                    item["last_error"] = REASON_DEST_DIGEST
        recount(task)
        if not_done or bad:
            reason = REASON_INCOMPLETE if not_done else REASON_DEST_DIGEST
            self._finish_failed(
                task, reason,
                f"final verification failed: unverified={bad[:10]} "
                f"pending={not_done[:10]}; no restorable generation published",
                release=True, ts=ts)
            return

        # 全部校验通过 —— 原子发布: 发布目录写入与任务终态/保留释放同一临界区
        record = publication_from_task(task, ts=ts)
        self._published[publication_key(record)] = record
        task["state"] = MIG_PUBLISHED
        task["reason"] = None
        task["error"] = None
        task["finished_ts"] = ts
        task["published_ts"] = ts
        task["completion_seq"] = self._reference_seq
        task["publication"] = {
            k: v for k, v in record.items() if k != "manifest"}
        released = self._release_holds(task["migration_id"])
        self._record_global(MIG_TASK_PUBLISHED, {
            "migration_id": task["migration_id"],
            "job_id": task["source_job_id"],
            "source_job_id": task["source_job_id"],
            "source_generation": task["source_generation"],
            "destination_id": task["destination_id"],
            "manifest_digest": task["manifest_digest"],
            "total_items": task["total_items"],
            "transferred_items": task["transferred_items"],
            "reused_items": task["reused_items"],
            "released_holds": released,
            "publication": task["publication"],
            "publication_manifest": record["manifest"],
            "published_ts": ts,
            "task": task_public(task),
        }, ts=ts)

    def cancel_migration(self, migration_id: str, *,
                         reason: str = REASON_CANCELLED) -> dict:
        """取消活跃迁移: 不发布代际, 立即释放本任务的临时保留。

        已复制到接收端的对象作为无引用 orphan 留给正常 GC; 取消终态幂等。
        """
        with self._reference_lock, self._migration_lock:
            task = self._get_task(migration_id)
            if task["state"] in MIG_TERMINAL:
                return task_public(task)
            ts = time.time()
            task["state"] = MIG_CANCELLED
            task["reason"] = reason
            task["error"] = "migration cancelled before publication"
            task["finished_ts"] = ts
            task["completion_seq"] = self._reference_seq
            released = self._release_holds(task["migration_id"])
            self._record_global(MIG_TASK_CANCELLED, {
                "migration_id": task["migration_id"],
                "job_id": task["source_job_id"],
                "source_job_id": task["source_job_id"],
                "source_generation": task["source_generation"],
                "destination_id": task["destination_id"],
                "reason": reason,
                "released_holds": released,
                "task": task_public(task),
            }, ts=ts)
            return task_public(task)

    def get_migration(self, migration_id: str) -> dict:
        with self._migration_lock:
            return task_public(self._get_task(migration_id))

    def list_migrations(self, job_id: str | None = None) -> list[dict]:
        with self._migration_lock:
            tasks = [task_public(t) for t in self._migrations.values()]
        if job_id is not None:
            tasks = [t for t in tasks if t["source_job_id"] == job_id]
        tasks.sort(key=lambda t: t["created_ts"] or 0.0)
        return tasks

    def list_published_generations(
        self, destination_id: str | None = None,
        source_job_id: str | None = None,
    ) -> list[dict]:
        """已发布可恢复代际目录 (接收端据此 adopt, 不需要源集群在线)。"""
        with self._migration_lock:
            records = []
            for rec in self._published.values():
                if destination_id is not None \
                        and rec["destination_id"] != destination_id:
                    continue
                if source_job_id is not None \
                        and rec["source_job_id"] != source_job_id:
                    continue
                records.append(dict(rec))
        records.sort(key=lambda r: (r["destination_id"], r["source_job_id"],
                                    r["source_generation"]))
        return records

    def migration_holds(self) -> dict[str, list[dict]]:
        with self._migration_lock:
            return self._migration_holds_payload()

    # ------------------------------------------------------------- restore
    def restore(
        self,
        job_id: str,
        *,
        generation: int,
        new_rank_nodes: dict[int, str] | None = None,
        lease_ttl: float | None = None,
    ) -> RestoreReport:
        """从指定代际检查点恢复: 同代际 + 全量重校验 + 新输出租约。

        对同一代际重复调用幂等, 直接返回已生成的报告 (不重发租约)。
        """
        job = self.get(job_id)
        ttl = lease_ttl or job._lease_ttl
        with self._reference_section(), job.lock:
            # 仅当请求代际同时是 *当前已释放代际* 且已恢复过, 才幂等短路;
            # 再次抢占释放后回指旧代际必须继续走向栅栏检查
            if (
                job.restored_generation == generation
                and job.released_generation == generation
            ):
                return job._last_restore  # type: ignore[return-value]
            if job.state not in (RELEASED, RUNNING):
                raise Conflict(
                    f"restore requires RELEASED job (current {job.state})"
                )
            existing = {c.generation for c in job.cpm.list_all()}
            if generation not in existing:
                return self._failed_report(
                    job, generation, checkpoint_state="MISSING",
                    error=f"no checkpoint exists for generation {generation}")
            if job.released_generation != generation:
                raise FenceViolation(
                    "restore generation mismatch: checkpoint was released "
                    f"at gen{job.released_generation}, requested gen"
                    f"{generation}. Must restore the exact same checkpoint "
                    "generation.")
            cp = job.cpm.get(generation)

            if cp.state != COMPLETE:
                return self._failed_report(
                    job, generation, checkpoint_state=cp.state,
                    error=f"checkpoint is {cp.state}, only COMPLETE "
                          "checkpoints can be restored")

            # 恢复时再次对每一个分片做内容重校验 (不信任提交时刻的结论)
            failed, positions = [], {}
            for r in range(cp.world_size):
                for i in range(cp.shards_per_rank):
                    sid = shard_id(r, i)
                    sh = cp.shards[sid]
                    if not job.store.verify(sh.blob):
                        failed.append(sid)
                    else:
                        positions[sid] = sh.input_pos
            if failed:
                return self._failed_report(
                    job, generation, checkpoint_state=cp.state,
                    error="post-hoc shard verification failed",
                    failed=failed, cp=cp)

            new_rank_nodes = new_rank_nodes or {
                r: n for r, n in cp.processes
            }
            if set(new_rank_nodes) != set(range(cp.world_size)):
                raise ValidationError("new placement must cover all ranks")

            old_procs = [{"rank": r, "node": n} for r, n in cp.processes]
            new_procs = [
                {"rank": r, "node": new_rank_nodes[r]}
                for r in sorted(new_rank_nodes)
            ]
            relocated = [
                r for r, n in cp.processes if new_rank_nodes[r] != n
            ]
            warnings = []
            if relocated:
                warnings.append(
                    f"{len(relocated)} 个 rank 被重新放置到不同节点: "
                    f"ranks {relocated}"
                )

            # 同代际恢复: 输出代际沿用 checkpoint 代际, 但签发全新租约;
            # 旧 lease 已在抢占/释放时吊销, 旧进程令牌不可能通过新栅栏
            lease = job.leases.grant(generation, ttl)
            self._record(job_id, "lease_granted", {
                "generation": generation, "lease_id": lease.lease_id,
                "issued_ts": lease.issued_ts, "expires_ts": lease.expires_ts,
                "state": lease.state,
            }, ts=lease.issued_ts)
            ts = time.time()
            self._apply_placement(job, new_rank_nodes, ts)
            job.output_generation = generation
            job.state = RUNNING
            job.restored_generation = generation

            report = RestoreReport(
                job_id=job.job_id,
                requested_generation=generation,
                success=True,
                ts=ts,
                checkpoint_state=cp.state,
                manifest_digest=(cp.manifest or {}).get("manifest_digest"),
                shards_total=cp.expected_total,
                shards_verified=cp.expected_total,
                shards_failed=[],
                input_positions=positions,
                old_processes=old_procs,
                new_processes=new_procs,
                process_set_match=True,
                relocated_ranks=relocated,
                lease=lease.to_dict(),
                warnings=warnings,
                security_events=self.security_events(job_id),
            )
            job._last_restore = report
            job._log("restore_verified", {
                "generation": generation, "shards": cp.expected_total,
                "lease_id": lease.lease_id, "relocated": relocated,
                "report": report.to_dict()}, ts=ts)
            return report

    @staticmethod
    def _apply_placement(job: Job, new_rank_nodes: dict[int, str],
                         ts: float) -> None:
        """按新放置重建节点/进程视图 (新的 incarnation)。"""
        for r, n in new_rank_nodes.items():
            old_node = job.processes[r].node
            proc = job.processes[r]
            proc.node = n
            proc.state = ALIVE
            proc.incarnation += 1
            proc.last_heartbeat = ts
            node = job.nodes.setdefault(n, Node(node_id=n))
            if r not in node.ranks:
                node.ranks.append(r)
            node.state = UP
            node.last_heartbeat = ts
            if old_node != n and old_node in job.nodes:
                old = job.nodes[old_node]
                old.ranks = [x for x in old.ranks if x != r]

    def _failed_report(
        self, job: Job, generation: int, *, checkpoint_state: str,
        error: str, failed: list[str] | None = None, cp=None,
    ) -> RestoreReport:
        positions, total = {}, 0
        if cp is not None:
            total = cp.expected_total
            for r in range(cp.world_size):
                for i in range(cp.shards_per_rank):
                    sid = shard_id(r, i)
                    if sid in cp.shards and sid not in (failed or []):
                        positions[sid] = cp.shards[sid].input_pos
        ts = time.time()
        report = RestoreReport(
            job_id=job.job_id,
            requested_generation=generation,
            success=False,
            ts=ts,
            checkpoint_state=checkpoint_state,
            manifest_digest=(
                (cp.manifest or {}).get("manifest_digest") if cp else None
            ),
            shards_total=total,
            shards_verified=len(positions),
            shards_failed=failed or [],
            input_positions=positions,
            old_processes=(
                [{"rank": r, "node": n} for r, n in cp.processes]
                if cp else []
            ),
            error=error,
            security_events=self.security_events(job.job_id),
        )
        job._last_restore = report
        job._log("restore_rejected", {"generation": generation,
                                      "error": error,
                                      "report": report.to_dict()}, ts=ts)
        return report

    def last_restore_report_dict(self, job_id: str) -> dict:
        """报告读取: 持久化报告 + 补充报告生成之后该作业的全部安全事件。"""
        return self.last_restore_report(job_id).to_dict()

    def last_restore_report(self, job_id: str) -> RestoreReport:
        job = self.get(job_id)
        if job._last_restore is None:
            raise NotFound(f"no restore attempt for job {job_id}")
        # 返回时补充报告生成之后新发生的安全事件: 冻结的判定结论不变,
        # 但授权拒绝证据始终完整可查 (页面/报告/重启后一致)。
        report = self._enrich_report_security(job._last_restore)
        return report

    def _enrich_report_security(self, report: RestoreReport) -> RestoreReport:
        events = self.security_events(report.job_id)
        return dataclasses.replace(report, security_events=events)

    # ----------------------------------------------- destination-side adopt
    def adopt_generation(
        self,
        *,
        manifest: dict | None = None,
        migration_id: str | None = None,
        destination_id: str | None = None,
        target_job_id: str | None = None,
        name: str | None = None,
        create_job: bool = True,
        lease_ttl: float = 300.0,
        now: float | None = None,
    ) -> dict:
        """接收端把已发布代际落地为可同代际恢复的 COMPLETE 检查点。

        两种清单来源: 直接给出发布清单 (跨集群: 从源端 published 目录取回),
        或给出本集群的 ``migration_id`` 由发布目录解析。adopt 前对 *全部*
        对象在本地内容存储重读重算哈希; 任一缺失/不符即拒绝, 不产生任何
        检查点/输出/作业状态。目标作业不存在时可显式创建。

        锁序: reference → migration (仅解析清单, 随即释放) → registry →
        Job.lock; migration 与 registry 绝不嵌套, 与 GC 世界的
        reference → migration → gc → jobs → registry 顺序一致。
        """
        with self._reference_lock:
            resolved = self._resolve_adopt_manifest(
                manifest=manifest, migration_id=migration_id,
                destination_id=destination_id)
            manifest = resolved["manifest"]
            dest_store = resolved["store"]
            job_id = target_job_id or manifest["source_job_id"]
            cp_info = manifest["checkpoint"]
            generation = int(cp_info["generation"])

            # 全量闸门: 本地存储必须包含全部对象且摘要逐一相符
            bad, missing = [], []
            for obj in manifest["objects"]:
                if not dest_store.has(obj["blob"]):
                    missing.append(obj["object_id"])
                elif not dest_store.verify(obj["blob"]):
                    bad.append(obj["object_id"])
            if missing or bad:
                raise Conflict(
                    "adopt refused: destination content verification "
                    f"failed: missing={missing[:10]} corrupt={bad[:10]}")
            if not verify_manifest(manifest):
                raise Conflict(
                    "adopt refused: manifest digest mismatch")

            with self._lock:
                job = self._jobs.get(job_id)
                created = False
                if job is None:
                    if not create_job:
                        raise NotFound(
                            f"unknown target job {job_id}; pass create_job=True")
                    rank_nodes = {
                        int(p[0]) if isinstance(p, list) else p["rank"]:
                        p[1] if isinstance(p, list) else p["node"]
                        for p in cp_info["processes"]
                    }
                    job = Job(
                        job_id=job_id,
                        name=name or manifest.get("source_job_id", job_id),
                        world_size=int(cp_info["world_size"]),
                        shards_per_rank=int(cp_info["shards_per_rank"]),
                        rank_nodes=rank_nodes,
                        store=dest_store,
                        scheduler=self.scheduler,
                        manager=self,
                        lease_ttl=lease_ttl,
                    )
                    self._jobs[job_id] = job
                    created = True
                    self._record(job_id, "job_created", {
                        "name": job.name, "world_size": job.world_size,
                        "shards_per_rank": job.shards_per_rank,
                        "rank_nodes": {str(r): n for r, n in rank_nodes.items()},
                        "lease_ttl": lease_ttl, "created_ts": job.created_ts,
                        "adopted_from_migration": resolved.get("migration_id"),
                        "adopt_destination_id": resolved.get("destination_id"),
                    }, ts=job.created_ts)
                    lease0 = job.leases.get(0)
                    self._record(job_id, "lease_granted", {
                        "generation": 0, "lease_id": lease0.lease_id,
                        "issued_ts": lease0.issued_ts,
                        "expires_ts": lease0.expires_ts,
                        "state": lease0.state,
                    }, ts=lease0.issued_ts)

                # 作业内变更在 Job.lock 内完成; 注意不能在持有注册表锁
                # self._lock 时调用 job.snapshot() (后者经 security_events
                # 再次取不可重入的注册表锁)。
                with job.lock:
                    existing = {c.generation for c in job.cpm.list_all()}
                    if generation in existing:
                        raise Conflict(
                            f"generation {generation} already exists in job "
                            f"{job_id}; adopt is only allowed once per generation")
                    if job.world_size != int(cp_info["world_size"]) or \
                            job.shards_per_rank != int(cp_info["shards_per_rank"]):
                        raise Conflict(
                            "adopt refused: target job world size/shards mismatch")
                    ts = now or time.time()
                    cp = job.cpm.import_complete(
                        epoch=int(cp_info["epoch"]),
                        generation=generation,
                        world_size=int(cp_info["world_size"]),
                        shards_per_rank=int(cp_info["shards_per_rank"]),
                        processes=[
                            (p[0], p[1]) if isinstance(p, list)
                            else (p["rank"], p["node"])
                            for p in cp_info["processes"]
                        ],
                        created_ts=float(cp_info.get("completed_ts") or ts),
                        completed_ts=float(cp_info.get("completed_ts") or ts),
                        manifest=self._cp_manifest_for_adopt(manifest),
                    )
                    for obj in manifest["objects"]:
                        if obj["kind"] == MIG_KIND_SHARD:
                            job.cpm.restore_shard(
                                generation=generation,
                                rank=int(obj["rank"]), index=int(obj["index"]),
                                node=obj["node"], blob=obj["blob"],
                                size=int(obj["size"]),
                                input_pos=int(obj["input_pos"]),
                                uploaded_ts=float(cp_info.get("completed_ts") or ts))
                        else:
                            job.outputs.restore(
                                generation=generation,
                                output_id=obj["object_id"], blob=obj["blob"],
                                size=int(obj["size"]), rank=obj.get("rank"),
                                ts=float(obj.get("output_ts") or ts))
                    # 到达与 confirm_release 相同的状态: RELEASED + 可恢复代际
                    revoked = job.leases.revoke_generation(job.output_generation)
                    if revoked is not None and revoked.state == "REVOKED":
                        self._record(job_id, "lease_revoked", {
                            "generation": revoked.generation,
                            "lease_id": revoked.lease_id,
                            "revoked_ts": revoked.revoked_ts,
                        }, ts=revoked.revoked_ts)
                    job.state = RELEASED
                    job.current_generation = generation
                    job.current_epoch = int(cp_info["epoch"])
                    job.released_generation = generation
                    job.released_ts = ts
                    adopted_outputs = [o for o in manifest["objects"]
                                       if o["kind"] == MIG_KIND_OUTPUT]
                    self._record(job_id, GENERATION_ADOPTED, {
                        "generation": generation,
                        "migration_id": resolved.get("migration_id"),
                        "destination_id": resolved.get("destination_id"),
                        "manifest_digest": manifest["manifest_digest"],
                        "shards": cp.expected_total,
                        "outputs": len(adopted_outputs),
                        "total_items": manifest["total_items"],
                        "total_bytes": manifest["total_bytes"],
                        "adopted_ts": ts,
                        "created_job": created,
                        # 自包含清单: 重放顺序在 shard/output 恢复事件之前,
                        # 仅凭本事件即可完整重建 COMPLETE 检查点与输出。
                        "manifest": manifest,
                    }, ts=ts)
                    cp_dict = cp.to_dict()
                    n_outputs = len(adopted_outputs)
            # 快照在注册表锁外构造 (job 已发布, snapshot 自身取 Job.lock)
            return {
                "adopted": True,
                "job_id": job_id,
                "job_created": created,
                "generation": generation,
                "checkpoint": cp_dict,
                "outputs": n_outputs,
                "manifest_digest": manifest["manifest_digest"],
                "job": job.snapshot(),
            }

    @staticmethod
    def _cp_manifest_for_adopt(manifest: dict) -> dict:
        """由发布清单重建与 commit() 等价的检查点 manifest。"""
        cp_info = manifest["checkpoint"]
        shards = []
        for obj in sorted(
                (o for o in manifest["objects"]
                 if o["kind"] == MIG_KIND_SHARD),
                key=lambda o: (int(o["rank"]), int(o["index"]))):
            shards.append({
                "rank": obj["rank"], "index": obj["index"],
                "blob": obj["blob"], "size": obj["size"],
                "input_pos": obj["input_pos"], "node": obj["node"],
            })
        return {
            "job_id": cp_info["job_id"],
            "epoch": cp_info["epoch"],
            "generation": cp_info["generation"],
            "world_size": cp_info["world_size"],
            "shards_per_rank": cp_info["shards_per_rank"],
            "processes": [
                {"rank": p[0], "node": p[1]} if isinstance(p, list)
                else {"rank": p["rank"], "node": p["node"]}
                for p in cp_info["processes"]
            ],
            "shards": shards,
            "manifest_digest": cp_info.get("checkpoint_manifest_digest"),
        }

    def _resolve_adopt_manifest(
        self, *, manifest: dict | None, migration_id: str | None,
        destination_id: str | None,
    ) -> dict:
        """解析 adopt 清单来源, 并确定目标内容存储。

        由 migration_id 解析时短暂获取 migration 锁 (调用方持有 reference
        锁但不持有 registry 锁, 保持锁序固定)。
        """
        if manifest is not None:
            if not isinstance(manifest, dict) or "objects" not in manifest:
                raise ValidationError("manifest must be a published manifest")
            store = (self._destinations[destination_id]
                     if destination_id in self._destinations else self.store)
            return {"manifest": manifest, "store": store,
                    "destination_id": destination_id,
                    "migration_id": migration_id}
        if migration_id is None:
            raise ValidationError(
                "adopt requires manifest or migration_id")
        with self._migration_lock:
            task = self._migrations.get(migration_id)
            if task is None:
                raise NotFound(f"unknown migration task {migration_id}")
            if task["state"] != MIG_PUBLISHED:
                raise Conflict(
                    f"migration {migration_id} is {task['state']}; only "
                    "PUBLISHED migrations can be adopted")
            dest_id = destination_id or task["destination_id"]
            store = self._destinations.get(dest_id)
            if store is None:
                raise NotFound(
                    f"destination store {dest_id!r} is not registered")
            return {"manifest": task["manifest"], "store": store,
                    "destination_id": dest_id, "migration_id": migration_id}

    # ------------------------------------------------------------ progress
    def progress_page_data(self, job_id: str) -> dict:
        """节点进度页面/进度 API 的聚合数据。"""
        job = self.get(job_id)
        # 固定加锁顺序: GC state -> Job.lock; 先在 Job 锁外读取 GC 视图。
        gc_plans = self.list_gc_plans(job_id)
        collected = self._collected_blob_plans()
        migrations = self.list_migrations(job_id)
        held = {}
        for digest, hs in self.migration_holds().items():
            mine = [h for h in hs if h["source_job_id"] == job_id]
            if mine:
                held[digest] = mine
        published = self.list_published_generations(source_job_id=job_id)
        with job.lock:
            cps = [c.to_dict() for c in job.cpm.list_all()]
            shard_index: dict[str, dict] = {}
            for c in job.cpm.list_all():
                for sid, sh in c.shards.items():
                    item = {
                        **sh.ref(),
                        "generation": c.generation,
                        "uploaded_ts": sh.uploaded_ts,
                    }
                    if sh.blob in collected:
                        item["collected_by"] = collected[sh.blob]
                        item["physically_available"] = False
                    shard_index[f"gen{c.generation}:{sid}"] = item
            outputs = []
            for rec in job.outputs.list():
                item = rec.to_dict()
                if rec.blob in collected:
                    item["collected_by"] = collected[rec.blob]
                    item["physically_available"] = False
                outputs.append(item)
            result = {
                "job": job.snapshot(),
                "nodes": [n.to_dict() for n in
                          sorted(job.nodes.values(), key=lambda n: n.node_id)],
                "processes": [job.processes[r].to_dict()
                              for r in sorted(job.processes)],
                "checkpoints": cps,
                "shards": shard_index,
                "outputs": outputs,
                "audit": list(reversed(job.audit)),
                "security_events": list(
                    reversed(self.security_events(job_id))),
                "gc_plans": gc_plans,
                "migrations": migrations,
                "migration_holds": held,
                "published_generations": published,
                "server_ts": time.time(),
            }
        return result

    # ============================================================ replay
    def _replay(self) -> None:
        """按 seq 顺序重放全部持久化事件, 重建内存状态。"""
        for evt in self.journal.replay():
            self._apply(evt)
        # 临时保留不单独持久化: 按重放后仍处于活跃态的任务重建;
        # 终态 (发布/失败/取消) 事件已经意味着保留释放。
        for task in self._migrations.values():
            if task["state"] in MIG_ACTIVE:
                self._add_holds(task)

    def _apply(self, evt: dict) -> None:
        etype = evt["type"]
        if etype in _REFERENCE_EVENTS and isinstance(evt.get("seq"), int):
            self._reference_seq = evt["seq"]
        detail = {k: v for k, v in evt.items()
                  if k not in ("seq", "ts", "type")}
        ts = evt.get("ts") or time.time()
        job_id = detail.get("job_id")

        if etype in (GC_PLAN_CREATED, GC_RUN_STARTED, GC_BLOB_DELETED,
                     GC_RUN_FINISHED, GC_PLAN_STALE):
            self._apply_gc_event(etype, detail, ts)
            return

        if etype in _MIG_TASK_EVENTS or etype == MIG_DEST_REGISTERED:
            self._apply_migration_event(etype, detail, ts)
            return

        if etype == "job_created":
            rank_nodes = {int(r): n for r, n in
                          detail["rank_nodes"].items()}
            adopt_dest = detail.get("adopt_destination_id")
            job_store = (self._destinations.get(adopt_dest)
                         if adopt_dest else self.store)
            if job_store is None:
                # 目标为重启后未重注册的内存存储; adopt 已结束的作业仍应可读,
                # 退回本地存储 (blob 缺失会在恢复校验时如实暴露)。
                job_store = self.store
            job = Job(
                job_id=job_id, name=detail["name"],
                world_size=detail["world_size"],
                shards_per_rank=detail["shards_per_rank"],
                rank_nodes=rank_nodes, store=job_store,
                scheduler=self.scheduler, manager=self,
                now=detail.get("created_ts", ts),
                lease_ttl=detail.get("lease_ttl", 300.0),
            )
            self._jobs[job_id] = job
            self._audit_only(job, etype, ts, detail)
            return

        if etype == GC_GENERATION_PIN:
            pin_job = self._jobs.get(job_id)
            if pin_job is not None:
                self._pins.setdefault(job_id, set()).add(detail["generation"])
                self._audit_only(pin_job, etype, ts, detail)
            return
        if etype == GC_GENERATION_UNPIN:
            pin_job = self._jobs.get(job_id)
            if pin_job is not None:
                self._pins.get(job_id, set()).discard(detail["generation"])
                if not self._pins.get(job_id):
                    self._pins.pop(job_id, None)
                self._audit_only(pin_job, etype, ts, detail)
            return

        job = self._jobs.get(job_id)
        if job is None:
            return  # 无法归属的历史事件忽略

        if etype == "lease_granted":
            job.leases.restore_grant(
                generation=detail["generation"],
                lease_id=detail["lease_id"],
                issued_ts=detail["issued_ts"],
                expires_ts=detail["expires_ts"],
                state=detail.get("state", "ACTIVE"))
        elif etype == "lease_revoked":
            job.leases.restore_revoke(detail["generation"],
                                      detail.get("revoked_ts", ts))
        elif etype == "preempt_issued":
            ep_dict = detail["epoch_obj"]
            ep = self.scheduler.restore_epoch(ep_dict)
            job.current_epoch = ep.epoch
            job.current_generation = ep.generation
            job.state = PREEMPTING
            job.epoch_token = ep.token
            job.cpm.supersede_pending(ep.generation)
            job.cpm.restore_after_preempt(
                epoch=ep.epoch, generation=ep.generation,
                world_size=job.world_size,
                shards_per_rank=job.shards_per_rank,
                created_ts=ep.issued_ts,
                processes=sorted(
                    (r, p.node) for r, p in job.processes.items()))
        elif etype == "node_lost":
            node = job.nodes.get(detail["node"])
            if node is not None:
                node.state = LOST
                for rank in node.ranks:
                    job.processes[rank].state = LOST
        elif etype == "node_recovered":
            node = job.nodes.get(detail["node"])
            if node is not None:
                node.state = UP
                node.last_heartbeat = ts
                for rank in node.ranks:
                    proc = job.processes[rank]
                    proc.last_heartbeat = ts
                    if proc.state == LOST:
                        proc.state = ALIVE
                        proc.incarnation += 1
        elif etype == "node_heartbeat":
            node = job.nodes.get(detail["node"])
            if node is not None:
                node.last_heartbeat = ts
                for rank in node.ranks:
                    job.processes[rank].last_heartbeat = ts
        elif etype == "progress_reported":
            proc = job.processes.get(detail["rank"])
            if proc is not None:
                proc.last_input_pos = detail["input_pos"]
                proc.last_progress_ts = ts
        elif etype == "shard_uploaded":
            job.cpm.restore_shard(
                generation=detail["generation"], rank=detail["rank"],
                index=detail["index"], node=detail["node"],
                blob=detail["blob"], size=detail["size"],
                input_pos=detail["input_pos"],
                uploaded_ts=detail.get("uploaded_ts", ts))
        elif etype == "checkpoint_complete":
            job.cpm.restore_complete(
                detail["generation"],
                completed_ts=detail.get("completed_ts", ts),
                manifest=detail.get("manifest"))
        elif etype == "resources_released":
            gen = detail["generation"]
            job.state = RELEASED
            job.released_generation = gen
            job.released_ts = ts
        elif etype == "epoch_abandoned":
            gen = detail["generation"]
            job.cpm.mark_incomplete(gen)
            job.state = ABORTED
            job.aborted_ts = ts
        elif etype == "output_committed":
            job.outputs.restore(
                generation=detail["generation"],
                output_id=detail["output_id"], blob=detail["blob"],
                size=detail["size"], rank=detail.get("rank"),
                ts=detail.get("ts", ts))
        elif etype == "restore_verified":
            rep = RestoreReport(**detail["report"])
            placement = {p["rank"]: p["node"] for p in rep.new_processes}
            self._apply_placement(job, placement, rep.ts)
            job.output_generation = rep.requested_generation
            job.state = RUNNING
            job.restored_generation = rep.requested_generation
            job._last_restore = rep
        elif etype == "restore_rejected":
            job._last_restore = RestoreReport(**detail["report"])
        elif etype == GENERATION_ADOPTED:
            # 自包含重建: adopt 事件携带完整发布清单, 检查点/输出以本事件
            # 为准 (后续 shard_uploaded/output_committed 恢复事件只补审计)。
            gen = detail["generation"]
            manifest = detail.get("manifest")
            if manifest is not None and gen not in \
                    {c.generation for c in job.cpm.list_all()}:
                cp_info = manifest["checkpoint"]
                job.cpm.import_complete(
                    epoch=int(cp_info["epoch"]),
                    generation=gen,
                    world_size=int(cp_info["world_size"]),
                    shards_per_rank=int(cp_info["shards_per_rank"]),
                    processes=[
                        (p[0], p[1]) if isinstance(p, list)
                        else (p["rank"], p["node"])
                        for p in cp_info["processes"]
                    ],
                    created_ts=float(cp_info.get("completed_ts") or ts),
                    completed_ts=float(cp_info.get("completed_ts") or ts),
                    manifest=self._cp_manifest_for_adopt(manifest),
                )
                for obj in manifest["objects"]:
                    if obj["kind"] == MIG_KIND_SHARD:
                        job.cpm.restore_shard(
                            generation=gen,
                            rank=int(obj["rank"]), index=int(obj["index"]),
                            node=obj["node"], blob=obj["blob"],
                            size=int(obj["size"]),
                            input_pos=int(obj["input_pos"]),
                            uploaded_ts=float(cp_info.get("completed_ts") or ts))
                    else:
                        job.outputs.restore(
                            generation=gen,
                            output_id=obj["object_id"], blob=obj["blob"],
                            size=int(obj["size"]), rank=obj.get("rank"),
                            ts=float(obj.get("output_ts") or ts))
            cp_adopted = job.cpm.get(gen)
            job.state = RELEASED
            job.current_generation = gen
            job.current_epoch = cp_adopted.epoch
            job.released_generation = gen
            job.released_ts = ts
        elif etype == SECURITY_DENIED:
            self._security.append(evt)
        # 其余事件 (preempt_duplicate / shard_upload_duplicate /
        # commit_duplicate / output_duplicate) 仅审计, 无状态变更

        self._audit_only(job, etype, ts, detail)

    def _apply_gc_event(self, etype: str, detail: dict, ts: float) -> None:
        """重放 GC 生命周期事件, 恢复可继续执行的计划状态。"""
        if etype == GC_PLAN_CREATED:
            plan = dict(detail["plan"])
            plan.setdefault("started_ts", None)
            plan.setdefault("finished_ts", None)
            plan.setdefault("completion_seq", None)
            plan.setdefault("deleted_items", 0)
            plan.setdefault("deleted_bytes", 0)
            plan.setdefault("skipped_items", 0)
            plan.setdefault("skip_reasons", {})
            plan.setdefault("recovery_reasons", {})
            plan.setdefault("error", None)
            plan.setdefault("stale_reason", None)
            self._gc_plans[plan["plan_id"]] = plan
            return

        plan_id = detail["plan_id"]
        plan = self._gc_plans.get(plan_id)
        if plan is None:
            return
        if etype == GC_RUN_STARTED:
            if plan["state"] == GC_PLANNED:
                plan["state"] = GC_RUNNING
            plan["started_ts"] = ts
        elif etype == GC_BLOB_DELETED:
            candidate = self._candidate_index(plan).get(detail["blob"])
            if candidate is None:
                return
            if candidate["status"] != "DELETED":
                candidate["status"] = "DELETED"
                candidate["deleted_ts"] = ts
                plan["deleted_items"] += 1
                plan["deleted_bytes"] += int(detail.get("size") or 0)
                if detail.get("reason"):
                    reason = detail["reason"]
                    plan["recovery_reasons"][reason] = \
                        plan["recovery_reasons"].get(reason, 0) + 1
        elif etype == GC_RUN_FINISHED:
            plan["state"] = detail["state"]
            plan["finished_ts"] = ts
            plan["completion_seq"] = detail.get("completion_seq")
            plan["deleted_items"] = int(
                detail.get("deleted_items", plan["deleted_items"]))
            plan["deleted_bytes"] = int(
                detail.get("deleted_bytes", plan["deleted_bytes"]))
            plan["skipped_items"] = int(
                detail.get("skipped_items", plan["skipped_items"]))
            plan["skip_reasons"] = detail.get(
                "skip_reasons", plan["skip_reasons"])
            finished_plan = detail.get("plan") or {}
            plan["error"] = finished_plan.get("error")
            # 事件中的候选权威状态覆盖累计状态 (含 FAILED/SKIPPED)。
            if finished_plan.get("candidates"):
                plan["candidates"] = finished_plan["candidates"]
        elif etype == GC_PLAN_STALE:
            plan["state"] = GC_STALE
            plan["finished_ts"] = ts
            plan["stale_reason"] = detail.get("reason")

    def _apply_migration_event(self, etype: str, detail: dict,
                               ts: float) -> None:
        """重放迁移生命周期事件, 恢复目标注册、任务进度、保留与发布目录。

        任务终态事件携带完整 task 视图, 直接权威覆盖; 逐项事件只补对象
        状态, 聚合计数在任务结束/重放末尾统一 recount。
        """
        if etype == MIG_DEST_REGISTERED:
            dest_id = detail["destination_id"]
            data_dir = detail.get("data_dir")
            self._dest_dirs[dest_id] = data_dir
            # 磁盘后端就地重建, 保证后续 adopt 作业重放时 store 绑定可用;
            # 内存目标进程重启后无法幸存, 重新 register 即可。
            if dest_id not in self._destinations:
                self._destinations[dest_id] = ContentStore(data_dir)
            return

        mig_id = detail["migration_id"]
        if etype == MIG_TASK_CREATED:
            task_view = detail["task"]
            objects = [
                {
                    "object_id": o["object_id"],
                    "kind": o["kind"],
                    "blob": o["blob"],
                    "size": o["size"],
                    "status": MIG_ITEM_PLANNED,
                    "attempts": 0,
                    "last_error": None,
                    "completed_ts": None,
                }
                for o in task_view["manifest"]["objects"]
            ]
            task = {
                "migration_id": mig_id,
                "source_job_id": detail["source_job_id"],
                "source_name": task_view.get("source_name"),
                "source_generation": detail["source_generation"],
                "destination_id": detail["destination_id"],
                "state": MIG_PLANNED,
                "created_ts": task_view.get("created_ts") or ts,
                "started_ts": None,
                "finished_ts": None,
                "published_ts": None,
                "watermark_seq": detail.get("watermark_seq"),
                "completion_seq": None,
                "manifest_digest": detail["manifest_digest"],
                "manifest": task_view["manifest"],
                "objects": objects,
                "total_items": task_view["total_items"],
                "total_bytes": task_view["total_bytes"],
                "transferred_items": 0,
                "transferred_bytes": 0,
                "reused_items": 0,
                "reused_bytes": 0,
                "failed_items": 0,
                "error": None,
                "reason": None,
                "publication": None,
            }
            self._migrations[mig_id] = task
            return

        task = self._migrations.get(mig_id)
        if task is None:
            return
        if etype == MIG_TASK_STARTED:
            if task["state"] == MIG_PLANNED:
                task["state"] = MIG_TRANSFERRING
            task["started_ts"] = detail.get("started_ts") or ts
        elif etype == MIG_OBJECT_DONE:
            item = next((o for o in task["objects"]
                         if o["object_id"] == detail["object_id"]), None)
            if item is not None and item["status"] not in MIG_ITEM_DONE:
                item["status"] = detail["status"]
                item["attempts"] = int(detail.get("attempts") or 1)
                item["completed_ts"] = ts
            recount(task)
        elif etype in (MIG_TASK_PUBLISHED, MIG_TASK_FAILED,
                       MIG_TASK_CANCELLED):
            view = detail["task"]
            task["state"] = view["state"]
            task["finished_ts"] = view.get("finished_ts") or ts
            task["published_ts"] = view.get("published_ts")
            task["completion_seq"] = view.get("completion_seq")
            task["reason"] = view.get("reason")
            task["error"] = view.get("error")
            # 对象明细以终态视图为权威 (含 FAILED/REUSED/TRANSFERRED)
            by_id = {o["object_id"]: o for o in task["objects"]}
            for vo in view["objects"]:
                cur = by_id.get(vo["object_id"])
                if cur is not None:
                    cur["status"] = vo["status"]
                    cur["attempts"] = vo.get("attempts", cur["attempts"])
                    cur["last_error"] = vo.get("last_error")
                    cur["completed_ts"] = vo.get("completed_ts")
            recount(task)
            if etype == MIG_TASK_PUBLISHED:
                record = {
                    "migration_id": mig_id,
                    "source_job_id": detail["source_job_id"],
                    "source_name": None,
                    "source_generation": detail["source_generation"],
                    "destination_id": detail["destination_id"],
                    "manifest_digest": detail["manifest_digest"],
                    "published_ts": detail.get("published_ts") or ts,
                    "total_items": detail["total_items"],
                    "total_bytes": view["total_bytes"],
                    "transferred_items": detail["transferred_items"],
                    "reused_items": detail["reused_items"],
                    "manifest": detail["publication_manifest"],
                }
                self._published[publication_key(record)] = record
                task["publication"] = detail.get("publication")

    @staticmethod
    def _audit_only(job: Job, etype: str, ts: float, detail: dict) -> None:
        job.audit.append({"ts": ts, "event": etype, "detail": detail})
        if len(job.audit) > AUDIT_LIMIT:
            del job.audit[:-AUDIT_LIMIT]
