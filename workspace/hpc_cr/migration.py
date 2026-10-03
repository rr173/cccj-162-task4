"""可恢复的检查点迁移任务 (跨内容存储)。

集群下线/搬迁时, 把一个已完成 (COMPLETE) 检查点及其同代际输出迁移到
另一套内容存储 (destination)。生命周期::

    PLANNED ──run──▶ TRANSFERRING ──全部项到位+清单复核+目标端全量校验──▶ PUBLISHED
       │                 │  (任一项摘要错误/缺失, 或发布前清单变化)
       │                 ▼
       │              FAILED            ── 终端态: 不产生可恢复代际
       └──cancel──▶ CANCELLED          ── 终端态: 不产生可恢复代际

关键设计:

* **冻结清单 + 临时保留**: 创建时在全局引用临界区内固定对象清单
  (检查点分片 + 同代际输出, 按 blob 去重) 并计算 ``manifest_digest``;
  清单内全部载荷登记为 *migration hold*, 保留期间 GC 计划把这些 blob
  视为 retained, 而此前生成的 GC 计划会因水位/指纹变化被拒绝执行。
* **按摘要复用**: 目标端是内容寻址存储, 已存在的 blob 写入时自然去重,
  记为 REUSED (复用项), 新写入记为 TRANSFERRED (已传输项)。
* **逐项事件, 断点续传**: 每个 item 的传输结果独立追加事件; 传输中断、
  重复 run、进程重启后用同一 task_id 继续, 已完成项跳过, 目标端已有
  内容按复用处理 (崩溃恢复与天然去重共用同一条路径)。
* **原子发布**: 全部项到位后进入发布临界区 —— 重新计算清单摘要与冻结值
  比对 (清单变化即失败), 目标端对每个 blob 重算哈希, 全部通过才追加
  **一条** ``migration_published`` 事件发布可恢复代际记录; 失败/取消
  绝不会产生该记录。
* **终端释放**: 进入 PUBLISHED/FAILED/CANCELLED 即释放临时保留; 失败/
  取消还会尽力清理目标端由本任务新写入、且不被其他活动/已发布任务引用
  的 blob, 不妨碍正常空间回收。

本模块不持有锁; 调用方 (JobManager) 负责固定加锁顺序:
reference lock -> migration lock -> Job.lock。
"""

from __future__ import annotations

import hashlib
import uuid
from typing import Any

from .security import canonical_json

# 任务状态
PLANNED = "PLANNED"
TRANSFERRING = "TRANSFERRING"
PUBLISHED = "PUBLISHED"
FAILED = "FAILED"
CANCELLED = "CANCELLED"

#: 活动状态: 临时保留 (migration hold) 生效, 任务可继续 run
ACTIVE_STATES = (PLANNED, TRANSFERRING)
#: 终端状态: 不再变化, 保留已释放
TERMINAL_STATES = (PUBLISHED, FAILED, CANCELLED)

# 单项状态
ITEM_PENDING = "PENDING"
ITEM_TRANSFERRED = "TRANSFERRED"
ITEM_REUSED = "REUSED"
ITEM_FAILED = "FAILED"

# 任务失败原因码 (稳定, 供 API/页面分支)
REASON_SOURCE_MISSING = "source_missing"
REASON_SOURCE_DIGEST = "source_digest_mismatch"
REASON_DEST_WRITE = "destination_write_failed"
REASON_DEST_DIGEST = "destination_digest_mismatch"
REASON_DEST_VERIFY = "destination_verification_failed"
REASON_MANIFEST_CHANGED = "manifest_changed"


def new_task_id() -> str:
    return f"mig-{uuid.uuid4().hex[:12]}"


def build_manifest_items(job, generation: int) -> list[dict]:
    """从作业的当前状态构造迁移清单项 (按 blob 去重, 引用合并)。

    调用方必须持有 ``job.lock``。清单项是传输单元; 同一 blob 被多个
    分片/输出引用时只传一次, 全部引用并入 ``references`` 供审计。
    """
    cp = job.cpm.get(generation)
    by_blob: dict[str, dict] = {}

    def add(blob: str, size: int, kind: str, ref_id: str) -> None:
        item = by_blob.get(blob)
        if item is None:
            item = {"blob": blob, "size": size, "references": []}
            by_blob[blob] = item
        ref = {"kind": kind, "ref_id": ref_id}
        if ref not in item["references"]:
            item["references"].append(ref)

    for sid, shard in cp.shards.items():
        add(shard.blob, shard.size, "checkpoint_shard", sid)
    for out in job.outputs.list(generation):
        add(out.blob, out.size, "output", out.output_id)

    items = []
    for blob in sorted(by_blob):
        item = by_blob[blob]
        item["references"].sort(key=lambda r: (r["kind"], r["ref_id"]))
        items.append(item)
    return items


def manifest_digest(job_id: str, generation: int, items: list[dict]) -> str:
    """冻结清单摘要: 同一 (作业, 代际, 清单内容) 永远得到同一摘要。"""
    material = {
        "job_id": job_id,
        "generation": generation,
        "items": [
            {"blob": it["blob"], "size": it["size"],
             "references": it["references"]}
            for it in items
        ],
    }
    return hashlib.sha256(canonical_json(material)).hexdigest()


def new_item(item: dict) -> dict:
    """清单项 -> 任务内的可传输项 (带状态)。"""
    return {
        "blob": item["blob"],
        "size": item["size"],
        "references": [dict(r) for r in item["references"]],
        "status": ITEM_PENDING,
        "error": None,
        "ts": None,
    }


def task_public(task: dict[str, Any]) -> dict[str, Any]:
    """任务的稳定 JSON 视图 (进度页/API 共用)。"""
    transferred = int(task.get("transferred_items", 0))
    reused = int(task.get("reused_items", 0))
    failed = int(task.get("failed_items", 0))
    planned = int(task.get("planned_items", 0))
    return {
        "task_id": task["task_id"],
        "job_id": task["job_id"],
        "generation": task["generation"],
        "destination": task["destination"],
        "state": task["state"],
        "created_ts": task.get("created_ts"),
        "started_ts": task.get("started_ts"),
        "finished_ts": task.get("finished_ts"),
        "manifest_digest": task["manifest_digest"],
        "planned_items": planned,
        "planned_bytes": int(task.get("planned_bytes", 0)),
        "transferred_items": transferred,
        "transferred_bytes": int(task.get("transferred_bytes", 0)),
        "reused_items": reused,
        "reused_bytes": int(task.get("reused_bytes", 0)),
        "failed_items": failed,
        "pending_items": max(0, planned - transferred - reused - failed),
        "state_reason": task.get("state_reason"),
        "error": task.get("error"),
        "cancel_reason": task.get("cancel_reason"),
        "cleanup": dict(task.get("cleanup") or {}),
        "published": task.get("published"),
        "items": [
            {
                "blob": it["blob"],
                "size": it["size"],
                "references": [dict(r) for r in it["references"]],
                "status": it["status"],
                "error": it.get("error"),
                "ts": it.get("ts"),
            }
            for it in task.get("items", [])
        ],
    }
