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
from .leases import LeaseManager
from .migration import (
    ACTIVE_STATES as MIG_ACTIVE,
    CANCELLED as MIG_CANCELLED,
    FAILED as MIG_FAILED,
    ITEM_PENDING as MIG_ITEM_PENDING,
    PLANNED as MIG_PLANNED,
    PUBLISHED as MIG_PUBLISHED,
    TRANSFERRING as MIG_TRANSFERRING,
    build_manifest_items,
    manifest_digest,
    new_item,
    new_task_id,
    task_public,
)
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

# 迁移任务事件 (归属具体作业; 生命周期事件会改变 GC 保留集合)
MIGRATION_CREATED = "migration_created"
MIGRATION_ITEM_TRANSFERRED = "migration_item_transferred"
MIGRATION_ITEM_FAILED = "migration_item_failed"
MIGRATION_PUBLISHED = "migration_published"
MIGRATION_FAILED = "migration_failed"
MIGRATION_CANCELLED = "migration_cancelled"

_MIGRATION_EVENTS = frozenset({
    MIGRATION_CREATED, MIGRATION_ITEM_TRANSFERRED, MIGRATION_ITEM_FAILED,
    MIGRATION_PUBLISHED, MIGRATION_FAILED, MIGRATION_CANCELLED,
})

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
    # 迁移任务获得/释放临时保留, 直接影响 GC 保留集合与计划水位
    MIGRATION_CREATED,
    MIGRATION_PUBLISHED,
    MIGRATION_FAILED,
    MIGRATION_CANCELLED,
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

    def __init__(self, scheduler: Scheduler, data_dir: str | None = None,
                 destinations: dict[str, ContentStore] | None = None):
        self.scheduler = scheduler
        self._lock = threading.Lock()
        self._jobs: dict[str, Job] = {}
        self.journal = Journal(data_dir)
        self.store = ContentStore(data_dir)
        self._security: list[dict] = []
        self._recording = False

        # 全局引用临界区锁: 计划生成/执行与所有可能新增引用的写操作互斥。
        # 固定顺序为 reference -> GC state -> 各 Job.lock (按 job_id 排序);
        # 迁移操作顺序为 reference -> migration -> Job.lock (与 GC 锁不嵌套)。
        self._reference_lock = threading.RLock()
        self._gc_lock = threading.RLock()
        self._pins: dict[str, set[int]] = {}
        self._gc_plans: dict[str, dict] = {}
        self._reference_seq = 0

        # 迁移任务: 目标内容存储注册表 (配置, 不入事件日志, 重启需重新
        # 注册) + 任务登记表 (事件溯源, 重启后可继续同一任务)。
        # _migration_holds 是"活动任务临时保留"的去锁只读快照, 只在持有
        # reference lock + migration lock 时整体替换, GC 在引用临界区内
        # 直接读取, 因此保留集合与水位检查永远一致。
        self._migration_lock = threading.RLock()
        self._destinations: dict[str, ContentStore] = dict(destinations or {})
        self._migrations: dict[str, dict] = {}
        self._migration_holds: dict[str, list[str]] = {}
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
        """全局 GC 快照/执行临界区: reference -> GC state -> 全部作业锁。

        引用锁在持锁期间也冻结作业注册表 (create_job 同样先取引用锁), 因此
        这里直接读 _jobs, 不再获取注册表锁, 避免与 restore 等
        "Job.lock -> 注册表锁" 的既有顺序相反。
        """
        with self._reference_lock, self._gc_lock:
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
            snapshot = build_snapshot(
                jobs, inventory=self.store.inventory(), pins=pins,
                policy=policy,
                min_retain_generation=min_retain_generation,
                retain_generations=retain,
                migration_holds=self._migration_hold_reasons())
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
            migration_holds=self._migration_hold_reasons(),
        )
        if current["reference_fingerprint"] != plan["reference_fingerprint"]:
            return "reference_fingerprint_changed"
        current_retained = {
            ref["blob"]
            for ref in current.get("references", [])
            if str(ref["generation"]) in current.get(
                "retained_generations", {}).get(ref["job_id"], {})
        } | set(current.get("migration_holds", {}))
        planned = {c["blob"]: c for c in plan["candidates"]}
        physical = {digest for digest, _ in self.store.inventory()}
        extra = physical - current_retained - set(planned)
        if extra:
            return "physical_blob_set_changed"
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

    # ========================================================= migration
    # 迁移任务: 把一个 COMPLETE 检查点及其同代际输出迁移到另一套内容存储。
    # 冻结清单 + 临时保留 -> 逐项断点续传 (按摘要复用) -> 全量校验后原子
    # 发布可恢复代际; 失败/取消释放保留并尽力清理目标端新写入的 blob。
    def register_destination(self, name: str, store: ContentStore) -> dict:
        """注册一个迁移目标内容存储 (配置项, 不入事件日志, 重启需重新注册)。"""
        if not isinstance(name, str) or not name:
            raise ValidationError("destination name must be a non-empty string")
        if not isinstance(store, ContentStore):
            raise ValidationError("destination store must be a ContentStore")
        with self._migration_lock:
            self._destinations[name] = store
        return {"destination": name, "registered": True}

    def list_destinations(self) -> list[str]:
        with self._migration_lock:
            return sorted(self._destinations)

    def _migration_hold_reasons(self) -> dict[str, list[str]]:
        """活动迁移任务的临时保留视图 (blob -> 原因), 供 GC 快照使用。

        只读 ``_migration_holds`` 快照; 快照仅在持有引用锁的迁移临界区内
        被整体替换, 因此 GC 在引用临界区内读到的集合与水位检查一致。
        """
        return {
            blob: [f"migration_hold:{tid}" for tid in task_ids]
            for blob, task_ids in self._migration_holds.items()
        }

    def _refresh_migration_holds(self) -> None:
        """重建临时保留快照。调用方持有 reference lock + migration lock。"""
        holds: dict[str, list[str]] = {}
        for task in self._migrations.values():
            if task["state"] not in MIG_ACTIVE:
                continue
            for item in task["items"]:
                holds.setdefault(item["blob"], []).append(task["task_id"])
        self._migration_holds = {
            blob: sorted(ids) for blob, ids in sorted(holds.items())
        }

    def create_migration(
        self,
        job_id: str,
        *,
        generation: int,
        destination: str,
        task_id: str | None = None,
    ) -> dict:
        """创建迁移任务: 冻结清单并登记临时保留。

        幂等: 同一 (作业, 代际, 目标存储) 已存在活动任务, 或显式 task_id
        已存在时, 直接返回原任务 (重复请求继续同一任务, 不新建)。
        """
        job = self.get(job_id)
        with self._reference_lock, self._migration_lock, job.lock:
            if destination not in self._destinations:
                raise ValidationError(
                    f"unknown destination {destination!r}; registered: "
                    f"{sorted(self._destinations)}")
            for task in self._migrations.values():
                if (task["job_id"] == job_id
                        and task["generation"] == generation
                        and task["destination"] == destination
                        and task["state"] in MIG_ACTIVE):
                    return {**task_public(task), "idempotent_replay": True}
            if task_id is not None and task_id in self._migrations:
                task = self._migrations[task_id]
                if (task["job_id"], task["generation"], task["destination"]) \
                        != (job_id, generation, destination):
                    raise Conflict(
                        f"migration {task_id} is bound to a different "
                        "job/generation/destination")
                return {**task_public(task), "idempotent_replay": True}

            cp = job.cpm.get(generation)
            if cp.state != COMPLETE:
                raise Conflict(
                    f"checkpoint gen{generation} is {cp.state}; only "
                    "COMPLETE checkpoints can be migrated")
            items = build_manifest_items(job, generation)
            digest = manifest_digest(job_id, generation, items)
            ts = time.time()
            task = {
                "task_id": task_id or new_task_id(),
                "job_id": job_id,
                "generation": generation,
                "destination": destination,
                "state": MIG_PLANNED,
                "created_ts": ts,
                "started_ts": None,
                "finished_ts": None,
                "manifest_digest": digest,
                "planned_items": len(items),
                "planned_bytes": sum(it["size"] for it in items),
                "transferred_items": 0,
                "transferred_bytes": 0,
                "reused_items": 0,
                "reused_bytes": 0,
                "failed_items": 0,
                "state_reason": None,
                "error": None,
                "cancel_reason": None,
                "cleanup": {},
                "published": None,
                "items": [new_item(it) for it in items],
            }
            self._migrations[task["task_id"]] = task
            self._refresh_migration_holds()
            self._record(job_id, MIGRATION_CREATED,
                         {"task": task_public(task)}, ts=ts)
            return {**task_public(task), "idempotent_replay": False}

    def get_migration(self, task_id: str) -> dict:
        with self._migration_lock:
            task = self._migrations.get(task_id)
            if task is None:
                raise NotFound(f"unknown migration {task_id}")
            return task_public(task)

    def list_migrations(self, job_id: str | None = None) -> list[dict]:
        with self._migration_lock:
            tasks = [self._migrations[tid] for tid in sorted(self._migrations)]
        if job_id is not None:
            self.get(job_id)
            tasks = [t for t in tasks if t["job_id"] == job_id]
        return [task_public(t) for t in tasks]

    def published_generations(self, job_id: str | None = None) -> list[dict]:
        """已发布的可恢复代际记录 (仅 PUBLISHED 任务产生)。"""
        with self._migration_lock:
            records = [
                dict(t["published"])
                for t in self._migrations.values()
                if t["state"] == MIG_PUBLISHED and t.get("published")
                and (job_id is None or t["job_id"] == job_id)
            ]
        records.sort(key=lambda r: (r["job_id"], r["generation"]))
        return records

    def run_migration(self, task_id: str, *,
                      max_items: int | None = None) -> dict:
        """继续/执行迁移任务: 逐项传输, 全部到位后校验并原子发布。

        每个 item 独立追加事件, 因此传输中断、重复调用或进程重启后用同一
        task_id 继续即可; 已完成的项跳过, 目标端已有内容按复用记录。
        ``max_items`` 限制单次调用处理的项数 (分块传输/测试中断点)。
        """
        if max_items is not None and max_items <= 0:
            raise ValidationError("max_items must be positive")
        with self._reference_lock, self._migration_lock:
            task = self._migrations.get(task_id)
            if task is None:
                raise NotFound(f"unknown migration {task_id}")
            if task["state"] == MIG_PUBLISHED:
                return task_public(task)  # 已发布: 幂等返回
            if task["state"] in (MIG_FAILED, MIG_CANCELLED):
                raise Conflict(
                    f"migration {task_id} is {task['state']} "
                    f"({task.get('state_reason') or task.get('cancel_reason')}"
                    "); create a new task to retry")
            dest = self._destinations.get(task["destination"])
            if dest is None:
                raise NotFound(
                    f"destination {task['destination']!r} is not registered")
            job = self.get(task["job_id"])
            if task["state"] == MIG_PLANNED:
                task["state"] = MIG_TRANSFERRING
                task["started_ts"] = time.time()

            processed = 0
            for item in task["items"]:
                if item["status"] != MIG_ITEM_PENDING:
                    continue
                if max_items is not None and processed >= max_items:
                    break
                processed += 1
                failure = self._transfer_item(task, item, dest)
                if failure is not None:
                    return self._fail_migration(
                        task, failure["reason"], failure["error"], dest)

            if any(it["status"] == MIG_ITEM_PENDING for it in task["items"]):
                return task_public(task)  # 分块模式: 后续调用继续

            # 发布临界区: 清单复核 + 目标端全量重校验, 全部通过才发布。
            with job.lock:
                current = build_manifest_items(job, task["generation"])
                current_digest = manifest_digest(
                    task["job_id"], task["generation"], current)
                if current_digest != task["manifest_digest"]:
                    return self._fail_migration(
                        task, "manifest_changed",
                        "manifest changed while migrating; refusing to "
                        "publish a recoverable generation", dest)
                bad = [it["blob"][:12] for it in task["items"]
                       if not dest.verify(it["blob"])]
                if bad:
                    return self._fail_migration(
                        task, "destination_verification_failed",
                        f"destination verification failed for {bad}", dest)
                ts = time.time()
                record = {
                    "job_id": task["job_id"],
                    "generation": task["generation"],
                    "destination": task["destination"],
                    "task_id": task["task_id"],
                    "manifest_digest": task["manifest_digest"],
                    "items": [
                        {"blob": it["blob"], "size": it["size"],
                         "references": it["references"]}
                        for it in task["items"]
                    ],
                    "planned_items": task["planned_items"],
                    "transferred_items": task["transferred_items"],
                    "reused_items": task["reused_items"],
                    "total_bytes": task["planned_bytes"],
                    "published_ts": ts,
                }
                task["state"] = MIG_PUBLISHED
                task["finished_ts"] = ts
                task["published"] = record
                self._refresh_migration_holds()
                self._record(task["job_id"], MIGRATION_PUBLISHED, {
                    "task_id": task["task_id"],
                    "generation": task["generation"],
                    "destination": task["destination"],
                    "record": record,
                    "task": task_public(task),
                }, ts=ts)
                return task_public(task)

    def cancel_migration(self, task_id: str, *,
                         reason: str = "operator_cancel") -> dict:
        """取消活动任务: 释放临时保留, 尽力清理目标端新写入的 blob。"""
        with self._reference_lock, self._migration_lock:
            task = self._migrations.get(task_id)
            if task is None:
                raise NotFound(f"unknown migration {task_id}")
            if task["state"] in (MIG_FAILED, MIG_CANCELLED):
                return task_public(task)  # 重复取消: 幂等
            if task["state"] == MIG_PUBLISHED:
                raise Conflict(
                    f"migration {task_id} is already PUBLISHED; "
                    "published generations are not rolled back by cancel")
            dest = self._destinations.get(task["destination"])
            ts = time.time()
            task["state"] = MIG_CANCELLED
            task["finished_ts"] = ts
            task["cancel_reason"] = reason
            task["cleanup"] = self._cleanup_destination(task, dest)
            self._refresh_migration_holds()
            self._record(task["job_id"], MIGRATION_CANCELLED, {
                "task_id": task_id, "reason": reason,
                "cleanup": task["cleanup"],
                "task": task_public(task),
            }, ts=ts)
            return task_public(task)

    # -------------------------------------------------- migration internals
    def _transfer_item(self, task: dict, item: dict,
                       dest: ContentStore) -> dict | None:
        """传输一个清单项; 失败时返回 {reason, error} (任务随之失败)。"""
        blob = item["blob"]
        ts = time.time()
        reason = error = None
        try:
            data = self.store.get(blob)
            if sha256_hex(data) != blob:
                reason, error = "source_digest_mismatch", (
                    f"source blob {blob[:12]} failed digest check")
        except NotFound:
            reason, error = "source_missing", (
                f"source blob {blob[:12]} is missing")
        if reason is None:
            try:
                digest, _size, is_new = dest.put(data)
            except OSError as exc:
                reason, error = "destination_write_failed", str(exc)
            else:
                if digest != blob or not dest.verify(blob):
                    reason, error = "destination_digest_mismatch", (
                        f"destination blob {blob[:12]} failed digest check")
        if reason is not None:
            item["status"] = "FAILED"
            item["error"] = error
            item["ts"] = ts
            task["failed_items"] += 1
            self._record(task["job_id"], MIGRATION_ITEM_FAILED, {
                "task_id": task["task_id"], "blob": blob,
                "reason": reason, "error": error,
            }, ts=ts)
            return {"reason": reason, "error": error}

        item["status"] = "REUSED" if not is_new else "TRANSFERRED"
        item["ts"] = ts
        if is_new:
            task["transferred_items"] += 1
            task["transferred_bytes"] += item["size"]
        else:
            task["reused_items"] += 1
            task["reused_bytes"] += item["size"]
        self._record(task["job_id"], MIGRATION_ITEM_TRANSFERRED, {
            "task_id": task["task_id"], "blob": blob,
            "size": item["size"], "status": item["status"],
        }, ts=ts)
        return None

    def _fail_migration(self, task: dict, reason: str, error: str,
                        dest: ContentStore | None) -> dict:
        """任务进入 FAILED: 释放临时保留, 清理目标端, 不产生可恢复代际。"""
        ts = time.time()
        task["state"] = MIG_FAILED
        task["finished_ts"] = ts
        task["state_reason"] = reason
        task["error"] = error
        task["cleanup"] = self._cleanup_destination(task, dest)
        self._refresh_migration_holds()
        self._record(task["job_id"], MIGRATION_FAILED, {
            "task_id": task["task_id"], "reason": reason, "error": error,
            "cleanup": task["cleanup"],
            "task": task_public(task),
        }, ts=ts)
        return task_public(task)

    def _cleanup_destination(self, task: dict,
                             dest: ContentStore | None) -> dict:
        """尽力清理目标端由本任务新写入的 blob。

        只删除状态为 TRANSFERRED (本任务写入) 的项; REUSED 项原本就存在,
        绝不删除。被其他活动任务或已发布代际引用的 blob 保留。目标存储
        未注册 (重启后未配置) 时跳过清理, 不影响保留的释放。
        """
        result = {"deleted_items": 0, "deleted_bytes": 0,
                  "kept_items": 0, "skipped": dest is None}
        if dest is None:
            return result
        protected: set[str] = set()
        for other in self._migrations.values():
            if other["task_id"] == task["task_id"]:
                continue
            if other["state"] in MIG_ACTIVE or other["state"] == MIG_PUBLISHED:
                protected.update(it["blob"] for it in other["items"])
        for item in task["items"]:
            if item["status"] != "TRANSFERRED":
                continue
            blob = item["blob"]
            if blob in protected:
                result["kept_items"] += 1
                continue
            try:
                existed, freed = dest.delete(blob)
            except OSError:
                result["kept_items"] += 1
                continue
            if existed:
                result["deleted_items"] += 1
                result["deleted_bytes"] += freed
            else:
                result["kept_items"] += 1
        return result

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

    # ------------------------------------------------------------ progress
    def progress_page_data(self, job_id: str) -> dict:
        """节点进度页面/进度 API 的聚合数据。"""
        job = self.get(job_id)
        # 固定加锁顺序: GC state -> Job.lock; 先在 Job 锁外读取 GC 视图。
        gc_plans = self.list_gc_plans(job_id)
        collected = self._collected_blob_plans()
        migrations = self.list_migrations(job_id)
        published = self.published_generations(job_id)
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
                "published_migrations": published,
                "server_ts": time.time(),
            }
        return result

    # ============================================================ replay
    def _replay(self) -> None:
        """按 seq 顺序重放全部持久化事件, 重建内存状态。"""
        for evt in self.journal.replay():
            self._apply(evt)

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

        if etype in _MIGRATION_EVENTS:
            self._apply_migration_event(etype, detail, ts)
            mig_job = self._jobs.get(job_id)
            if mig_job is not None:
                self._audit_only(mig_job, etype, ts, detail)
            return

        if etype == "job_created":
            rank_nodes = {int(r): n for r, n in
                          detail["rank_nodes"].items()}
            job = Job(
                job_id=job_id, name=detail["name"],
                world_size=detail["world_size"],
                shards_per_rank=detail["shards_per_rank"],
                rank_nodes=rank_nodes, store=self.store,
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
        """重放迁移任务事件, 恢复可继续执行的任务状态。"""
        if etype == MIGRATION_CREATED:
            task = dict(detail["task"])
            task["items"] = [dict(it, references=[dict(r) for r in
                                                  it["references"]])
                             for it in task.get("items", [])]
            self._migrations[task["task_id"]] = task
            self._refresh_migration_holds()
            return

        task = self._migrations.get(detail["task_id"])
        if task is None:
            return
        if etype == MIGRATION_ITEM_TRANSFERRED:
            item = next((it for it in task["items"]
                         if it["blob"] == detail["blob"]), None)
            if item is None or item["status"] != MIG_ITEM_PENDING:
                return
            item["status"] = detail["status"]
            item["ts"] = ts
            if detail["status"] == "TRANSFERRED":
                task["transferred_items"] += 1
                task["transferred_bytes"] += int(detail.get("size") or 0)
            else:
                task["reused_items"] += 1
                task["reused_bytes"] += int(detail.get("size") or 0)
            if task["state"] == MIG_PLANNED:
                task["state"] = MIG_TRANSFERRING
                task["started_ts"] = ts
        elif etype == MIGRATION_ITEM_FAILED:
            item = next((it for it in task["items"]
                         if it["blob"] == detail["blob"]), None)
            if item is not None and item["status"] == MIG_ITEM_PENDING:
                item["status"] = "FAILED"
                item["error"] = detail.get("error")
                item["ts"] = ts
                task["failed_items"] += 1
        elif etype in (MIGRATION_PUBLISHED, MIGRATION_FAILED,
                       MIGRATION_CANCELLED):
            # 事件内含权威任务视图 (计数/清理/发布记录以落盘为准)
            saved = detail.get("task")
            if saved:
                task.update(saved)
                task["items"] = [dict(it, references=[dict(r) for r in
                                                      it["references"]])
                                 for it in saved.get("items", [])]
            task["finished_ts"] = ts
            if etype == MIGRATION_PUBLISHED:
                task["state"] = MIG_PUBLISHED
                task["published"] = detail.get("record")
            elif etype == MIGRATION_FAILED:
                task["state"] = MIG_FAILED
                task["state_reason"] = detail.get("reason")
                task["error"] = detail.get("error")
            else:
                task["state"] = MIG_CANCELLED
                task["cancel_reason"] = detail.get("reason")
            task["cleanup"] = detail.get("cleanup") or {}
            self._refresh_migration_holds()

    @staticmethod
    def _audit_only(job: Job, etype: str, ts: float, detail: dict) -> None:
        job.audit.append({"ts": ts, "event": etype, "detail": detail})
        if len(job.audit) > AUDIT_LIMIT:
            del job.audit[:-AUDIT_LIMIT]
