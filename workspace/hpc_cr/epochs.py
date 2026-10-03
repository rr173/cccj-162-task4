"""调度器与抢占纪元 (preemption epoch)。

调度器在回收节点前为作业签发一个新的抢占纪元。纪元单调递增并携带:
  - epoch 序号与 generation 代际 (本实现中二者一一对应, generation 用于
    检查点/输出租约的代际栅栏, epoch 用于抢占流程);
  - deadline: 优雅退出窗口, 超时仍未确认释放则调度器可强杀 (abandon);
  - reason/kind: 抢占原因与回收范围 ("all" 或节点子集);
  - 签名令牌: 作业进程组在该纪元内写分片/提交/释放的唯一凭证。

重复收到同一 epoch 的抢占通知必须幂等: 不递增纪元、不轮换令牌, 直接
回传已签发的纪元。
"""

from __future__ import annotations

import threading
import time
from dataclasses import dataclass, field

from .errors import Conflict, FenceViolation, InvalidToken
from .security import Signer

# 纪元令牌授权失败的稳定原因码
REASON_EPOCH_CROSS = "epoch_cross_job_or_epoch"
REASON_EPOCH_STALE_GENERATION = "stale_generation"


class InvalidTokenType(InvalidToken):
    """令牌类型与操作不匹配 (epoch 令牌/lease 令牌混用)。"""

    def __init__(self, message: str = "credential type mismatch", **kw):
        super().__init__(message, reason=kw.pop("reason", "wrong_token_type"),
                         **kw)


@dataclass
class PreemptionEpoch:
    job_id: str
    epoch: int
    generation: int
    reason: str
    kind: str  # "all" | "nodes"
    nodes: tuple[str, ...]
    deadline_ts: float
    grace_seconds: float
    issued_ts: float = field(default_factory=time.time)
    token: str = ""

    def is_expired(self, now: float | None = None) -> bool:
        return (now or time.time()) > self.deadline_ts

    def to_dict(self) -> dict:
        return {
            "job_id": self.job_id,
            "epoch": self.epoch,
            "generation": self.generation,
            "reason": self.reason,
            "kind": self.kind,
            "nodes": list(self.nodes),
            "issued_ts": self.issued_ts,
            "deadline_ts": self.deadline_ts,
            "grace_seconds": self.grace_seconds,
        }


class Scheduler:
    """集群调度器侧: 持有签名密钥, 单调签发抢占纪元。"""

    def __init__(self, signer: Signer):
        self._signer = signer
        self._lock = threading.Lock()
        # job_id -> {"epoch": int, "generation": int, "epoch_obj": PreemptionEpoch}
        self._state: dict[str, dict] = {}

    def issue_preemption(
        self,
        job_id: str,
        *,
        reason: str = "reclaim",
        kind: str = "all",
        nodes: list[str] | tuple[str, ...] | None = None,
        grace_seconds: float = 30.0,
        now: float | None = None,
    ) -> PreemptionEpoch:
        """为作业签发新的抢占纪元; 对同一作业重复调用产生新的纪元。

        抢占通知的幂等重投由 JobManager.preempt 处理 (同一 pending 纪元
        重投直接复用), 调度器本身只负责单调签发。
        """
        if kind not in ("all", "nodes"):
            raise Conflict("preemption kind must be 'all' or 'nodes'")
        nodes = tuple(nodes or ())
        if kind == "nodes" and not nodes:
            raise Conflict("partial preemption requires node list")
        with self._lock:
            st = self._state.setdefault(job_id, {"epoch": 0, "generation": 0})
            st["epoch"] += 1
            st["generation"] += 1
            cur = time.time() if now is None else now
            ep = PreemptionEpoch(
                job_id=job_id,
                epoch=st["epoch"],
                generation=st["generation"],
                reason=reason,
                kind=kind,
                nodes=nodes,
                grace_seconds=grace_seconds,
                deadline_ts=cur + grace_seconds,
                issued_ts=cur,
            )
            ep.token = self._signer.issue(
                {
                    "typ": "epoch",
                    "job": job_id,
                    "epoch": ep.epoch,
                    "generation": ep.generation,
                    "deadline_ts": ep.deadline_ts,
                }
            )
            st["epoch_obj"] = ep
            return ep

    def current_epoch(self, job_id: str) -> PreemptionEpoch | None:
        with self._lock:
            st = self._state.get(job_id)
            return st["epoch_obj"] if st else None

    def verify_epoch_token(
        self, token: str, *, job_id: str, epoch: int
    ) -> dict:
        """验证进程组出示的纪元令牌与当前操作的 job/epoch 绑定一致。

        失败均带稳定 reason 与固定消息, 不回显令牌内容。
        """
        payload = self._signer.verify(token)
        Signer.require_fields(payload, ("typ", "job", "epoch", "generation"))
        if payload["typ"] != "epoch":
            raise InvalidTokenType()
        if payload["job"] != job_id or payload["epoch"] != epoch:
            deny = FenceViolation(
                "epoch credential is not valid for this job epoch",
                reason=REASON_EPOCH_CROSS,
            )
            deny.claimed = {
                "job": payload.get("job"), "epoch": payload.get("epoch"),
                "expected_job": job_id, "expected_epoch": epoch,
            }
            raise deny
        return payload

    def issue_lease_token(
        self,
        *,
        job_id: str,
        generation: int,
        lease_id: str,
        until_ts: float,
    ) -> str:
        return self._signer.issue(
            {
                "typ": "lease",
                "job": job_id,
                "generation": generation,
                "lease": lease_id,
                "until_ts": until_ts,
            }
        )

    def verify_lease_token(self, token: str) -> dict:
        payload = self._signer.verify(token)
        Signer.require_fields(payload, ("typ", "job", "generation"))
        # 类型不符 (epoch 令牌冒充租约令牌) 优先稳定归类为 wrong_token_type,
        # 而不是让后续缺字段检查报出语义不清的 missing_fields。
        if payload["typ"] != "lease":
            raise InvalidTokenType()
        Signer.require_fields(payload, ("lease", "until_ts"))
        return payload

    @property
    def signer(self) -> Signer:
        return self._signer

    # -------------------------------------------------------- replay support
    def restore_epoch(self, ep_dict: dict) -> PreemptionEpoch:
        """事件重放: 按持久化事实重建调度器侧单调水位与当前纪元。

        令牌不入库, 这里用同一密钥按绑定字段重新签发等价令牌 (payload
        完全一致, 签名也一致), 因此重启后旧令牌可继续被验证/栅栏。
        """
        with self._lock:
            st = self._state.setdefault(
                ep_dict["job_id"], {"epoch": 0, "generation": 0})
            st["epoch"] = max(st["epoch"], ep_dict["epoch"])
            st["generation"] = max(st["generation"], ep_dict["generation"])
            ep = PreemptionEpoch(
                job_id=ep_dict["job_id"],
                epoch=ep_dict["epoch"],
                generation=ep_dict["generation"],
                reason=ep_dict["reason"],
                kind=ep_dict["kind"],
                nodes=tuple(ep_dict.get("nodes", ())),
                deadline_ts=ep_dict["deadline_ts"],
                grace_seconds=ep_dict["grace_seconds"],
                issued_ts=ep_dict["issued_ts"],
            )
            ep.token = self._signer.issue(
                {
                    "typ": "epoch",
                    "job": ep.job_id,
                    "epoch": ep.epoch,
                    "generation": ep.generation,
                    "deadline_ts": ep.deadline_ts,
                }
            )
            st["epoch_obj"] = ep
            return ep
