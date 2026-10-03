"""可审计的内容寻址存储空间回收 (GC) 计划。

回收分三个显式阶段:

1. ``build_plan`` 在全局引用读锁保护下扫描所有作业, 记录 journal 水位
   (``watermark_seq``)、保留代际、逻辑引用关系指纹和候选 blob;
2. 调用方可持久化锁定需要保留的代际 (``gc_generation_pin``), 已生成计划
   随后若水位/引用指纹变化, 执行必须失败;
3. ``execute_plan`` 只删除仍与计划候选完全一致的 blob, 每个删除动作单独
   记录事件, 崩溃后可按同一计划幂等继续。

这里的函数不持有 JobManager 的锁; 调用方负责固定加锁顺序:
reference lock -> JobManager._gc_lock -> 各 Job.lock (按 job_id 排序)。
"""

from __future__ import annotations

import hashlib
import uuid
from dataclasses import dataclass
from typing import Any, Iterable

from .checkpoints import COMPLETE, PENDING
from .security import canonical_json

PLANNED = "PLANNED"
RUNNING = "RUNNING"
COMPLETED = "COMPLETED"
PARTIALLY_COMPLETED = "PARTIALLY_COMPLETED"
STALE = "STALE"

# 默认只回收既不是可恢复检查点、也不是活动输出代际、也未被锁定的内容。
POLICY_RECOVERABLE = "recoverable"
POLICY_ALL_COMPLETE = "all_complete"
POLICY_WATERMARK = "watermark"


@dataclass
class Reference:
    blob: str
    kind: str          # checkpoint_shard | output
    job_id: str
    generation: int
    ref_id: str        # r0:s1 或 output_id

    def to_dict(self) -> dict:
        return {
            "blob": self.blob,
            "kind": self.kind,
            "job_id": self.job_id,
            "generation": self.generation,
            "ref_id": self.ref_id,
        }


def _job_references(job: Job) -> list[Reference]:
    refs: list[Reference] = []
    for cp in job.cpm.list_all():
        for sid, shard in cp.shards.items():
            refs.append(Reference(
                blob=shard.blob, kind="checkpoint_shard",
                job_id=job.job_id, generation=cp.generation, ref_id=sid))
    for out in job.outputs.list():
        refs.append(Reference(
            blob=out.blob, kind="output", job_id=job.job_id,
            generation=out.generation, ref_id=out.output_id))
    return refs


def _automatic_generations(job: Job, policy: str) -> dict[int, list[str]]:
    """计算策略隐式保留的代际及原因。"""
    retain: dict[int, list[str]] = {}

    def add(gen: int, reason: str) -> None:
        if gen is not None:
            reasons = retain.setdefault(int(gen), [])
            if reason not in reasons:
                reasons.append(reason)

    # 可访问性由作业状态/输出代际决定; 租约是否 ACTIVE 只影响新写入, 不改变
    # 已提交 OutputLog 中历史记录的引用关系。计划生成后的新提交由水位拒绝。

    if policy == POLICY_ALL_COMPLETE:
        for cp in job.cpm.list_all():
            if cp.state == COMPLETE:
                add(cp.generation, "complete_checkpoint")
    else:
        # recoverable / watermark 的基础安全集合:
        # 正在抢占的当前检查点、已释放等待恢复的检查点、当前输出代际。
        if job.state == "PREEMPTING" and job.current_generation is not None:
            add(job.current_generation, "current_preempting_generation")
        if job.state == "RELEASED" and job.released_generation is not None:
            add(job.released_generation, "released_for_restore")
        if job.state == "RUNNING" and job.output_generation is not None:
            add(job.output_generation, "active_output_generation")
        for cp in job.cpm.list_all():
            if cp.state == COMPLETE and cp.generation == job.current_generation:
                add(cp.generation, "current_complete_checkpoint")

    # PENDING 检查点仍可能接收补齐分片; 计划生成后只要有新引用就会过期。
    # 保守地把当前 PENDING 也放入自动保留, 避免正常等待宽限期的数据被回收。
    if job.state == "PREEMPTING":
        for cp in job.cpm.list_all():
            if cp.state == PENDING and cp.generation == job.current_generation:
                add(cp.generation, "pending_checkpoint")

    if job.state == "ABORTED":
        retain = {}
    return retain


def compute_retention(
    jobs: Iterable[Job],
    *,
    pins: dict[str, list[int]],
    policy: str,
    min_retain_generation: int | None,
    explicit_generations: dict[str, list[int]] | None,
) -> tuple[dict[str, dict[int, list[str]]], dict[str, list[dict]]]:
    """返回每作业 ``generation -> 保留原因`` 和供计划展示的代际说明。"""
    explicit_generations = explicit_generations or {}
    retained: dict[str, dict[int, list[str]]] = {}
    explanations: dict[str, list[dict]] = {}

    for job in jobs:
        reasons = _automatic_generations(job, policy)
        for gen in pins.get(job.job_id, []):
            reasons.setdefault(int(gen), []).append("pinned_generation")
        for gen in explicit_generations.get(job.job_id, []):
            reasons.setdefault(int(gen), []).append("explicit_retain")
        if min_retain_generation is not None:
            for cp in job.cpm.list_all():
                if cp.generation >= min_retain_generation:
                    reasons.setdefault(cp.generation, []).append(
                        "generation_watermark")
            # 输出代际可能尚无检查点 (gen0), 同样应用水位。
            if job.output_generation >= min_retain_generation:
                reasons.setdefault(int(job.output_generation), []).append(
                    "generation_watermark")

        gen_notes: list[dict] = []
        all_gens = {cp.generation for cp in job.cpm.list_all()}
        all_gens.update(out.generation for out in job.outputs.list())
        all_gens.update(reasons)
        for gen in sorted(all_gens):
            cp = next((c for c in job.cpm.list_all()
                       if c.generation == gen), None)
            gen_notes.append({
                "generation": gen,
                "checkpoint_state": cp.state if cp else None,
                "retained": gen in reasons,
                "reasons": sorted(reasons.get(gen, [])),
            })
        retained[job.job_id] = reasons
        explanations[job.job_id] = gen_notes
    return retained, explanations


def _fingerprint(snapshot: dict[str, Any]) -> str:
    """逻辑引用关系指纹; 与候选物理集合分离, 便于崩溃后识别缺失/新增。"""
    material = {
        "policy": snapshot["policy"],
        "pins": snapshot["pins"],
        "min_retain_generation": snapshot["min_retain_generation"],
        "retain_generations": snapshot["retain_generations"],
        "retained_generations": snapshot["retained_generations"],
        "migration_holds": snapshot["migration_holds"],
        "references": sorted(
            snapshot["references"],
            key=lambda r: (r["job_id"], r["kind"], r["generation"],
                           r["ref_id"], r["blob"]),
        ),
    }
    return hashlib.sha256(canonical_json(material)).hexdigest()


def build_snapshot(
    jobs: list[Job],
    *,
    inventory: list[tuple[str, int]],
    pins: dict[str, list[int]],
    policy: str = POLICY_RECOVERABLE,
    min_retain_generation: int | None = None,
    retain_generations: dict[str, list[int]] | None = None,
    migration_holds: dict[str, list[str]] | None = None,
) -> dict[str, Any]:
    """构造不可变计划快照 (不分配 plan_id/水位, 由 JobManager 完成)。

    ``migration_holds`` 是迁移任务的临时保留 (blob -> 保留原因列表):
    持有期间这些 blob 一律视为 retained, 且进入引用指纹 —— 计划生成后
    保留集合的任何变化都会使计划过期。
    """
    if policy not in (POLICY_RECOVERABLE, POLICY_ALL_COMPLETE,
                      POLICY_WATERMARK):
        raise ValueError(f"unknown gc policy: {policy}")
    pins_norm = {
        job_id: sorted({int(g) for g in gens})
        for job_id, gens in sorted(pins.items())
    }
    pins_norm = {k: v for k, v in pins_norm.items() if v}
    retain_generations = retain_generations or {}
    holds_norm = {
        blob: sorted(reasons)
        for blob, reasons in sorted((migration_holds or {}).items())
        if reasons
    }
    retained, explanations = compute_retention(
        jobs, pins=pins_norm, policy=policy,
        min_retain_generation=min_retain_generation,
        explicit_generations=retain_generations)

    all_refs: list[Reference] = []
    for job in jobs:
        all_refs.extend(_job_references(job))
    ref_dicts = [r.to_dict() for r in all_refs]

    retained_blob_reasons: dict[str, list[dict]] = {}
    historical_refs: dict[str, list[dict]] = {}
    for ref in ref_dicts:
        is_retained = ref["generation"] in retained.get(ref["job_id"], {})
        target = retained_blob_reasons if is_retained else historical_refs
        target.setdefault(ref["blob"], []).append(ref)
    # 迁移临时保留: 无论引用关系如何, 持有期间一律 retained。
    for blob, reasons in holds_norm.items():
        retained_blob_reasons.setdefault(blob, [])
        for reason in reasons:
            retained_blob_reasons[blob].append({
                "blob": blob, "kind": "migration_hold",
                "job_id": None, "generation": None, "ref_id": reason,
            })

    candidates: list[dict[str, Any]] = []
    physical = dict(inventory)
    for digest, size in sorted(physical.items()):
        if digest in retained_blob_reasons:
            continue
        old_refs = historical_refs.get(digest, [])
        if old_refs:
            kinds = {r["kind"] for r in old_refs}
            if "checkpoint_shard" in kinds:
                kind = "checkpoint_shard"
            else:
                kind = "output"
            eligible = "unreachable_generation"
        else:
            kind = "orphan"
            eligible = "no_reference"
        candidates.append({
            "blob": digest,
            "size": size,
            "kind": kind,
            "eligible_reason": eligible,
            "references": sorted(
                old_refs,
                key=lambda r: (r["job_id"], r["kind"], r["generation"],
                               r["ref_id"])),
            "status": "PLANNED",
            "skip_reason": None,
            "deleted_ts": None,
        })

    retain_payload = {
        job_id: {str(gen): reasons for gen, reasons in gen_map.items()}
        for job_id, gen_map in retained.items()
    }
    snapshot = {
        "policy": policy,
        "pins": pins_norm,
        "min_retain_generation": min_retain_generation,
        "retain_generations": retain_generations,
        "retained_generations": retain_payload,
        "migration_holds": holds_norm,
        "generation_explanations": explanations,
        "references": ref_dicts,
        "retained_blob_count": len(retained_blob_reasons),
        "physical_blob_count": len(physical),
        "candidates": candidates,
        "estimated_reclaim_bytes": sum(c["size"] for c in candidates),
        "estimated_reclaim_items": len(candidates),
    }
    snapshot["reference_fingerprint"] = _fingerprint(snapshot)
    return snapshot


def new_plan_id() -> str:
    return f"gc-{uuid.uuid4().hex[:12]}"


def plan_public(plan: dict[str, Any]) -> dict[str, Any]:
    """返回稳定 JSON 视图。"""
    return {
        "plan_id": plan["plan_id"],
        "state": plan["state"],
        "created_ts": plan.get("created_ts"),
        "started_ts": plan.get("started_ts"),
        "finished_ts": plan.get("finished_ts"),
        "watermark_seq": plan["watermark_seq"],
        "completion_seq": plan.get("completion_seq"),
        "reference_fingerprint": plan["reference_fingerprint"],
        "policy": plan["policy"],
        "pins": plan.get("pins", {}),
        "min_retain_generation": plan.get("min_retain_generation"),
        "retain_generations": plan.get("retain_generations"),
        "retained_generations": plan.get("retained_generations", {}),
        "migration_holds": plan.get("migration_holds", {}),
        "generation_explanations": plan.get("generation_explanations", {}),
        "estimated_reclaim_items": plan.get("estimated_reclaim_items", 0),
        "estimated_reclaim_bytes": plan.get("estimated_reclaim_bytes", 0),
        "deleted_items": plan.get("deleted_items", 0),
        "deleted_bytes": plan.get("deleted_bytes", 0),
        "skipped_items": plan.get("skipped_items", 0),
        "skip_reasons": plan.get("skip_reasons", {}),
        "recovery_reasons": plan.get("recovery_reasons", {}),
        "error": plan.get("error"),
        "stale_reason": plan.get("stale_reason"),
        "candidates": plan.get("candidates", []),
    }
