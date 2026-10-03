"""输出租约 (output lease) 与代际栅栏。

每个运行代际至多一份 ACTIVE 输出租约。作业被抢占时租约立即 REVOKED
(原子切换 job epoch), 旧进程即使本地未及时收到信号, 其迟到写入携带的旧
租约令牌也会被 authorize() 拒绝 (FenceViolation)。恢复后签发新代际
租约, 输出从头具备写入资格。

租约令牌由 Scheduler 的 HMAC 签发, 绑定 job/generation/lease/until。

授权失败统一在此归类: 每一种拒绝对应稳定 ``reason`` 码与固定消息,
消息绝不回显令牌/签名; 调用方 (JobManager) 依据 reason 写入顺序化
安全事件。
"""

from __future__ import annotations

import threading
import time
import uuid
from dataclasses import dataclass, field

from .epochs import Scheduler
from .errors import FenceViolation, InvalidToken, NotFound

ACTIVE = "ACTIVE"
REVOKED = "REVOKED"
EXPIRED = "EXPIRED"

# 稳定授权失败原因码 (同时用于错误响应与安全事件归类)
REASON_MISSING_TOKEN = "missing_token"
REASON_BAD_TOKEN = "bad_token"
REASON_WRONG_TOKEN_TYPE = "wrong_token_type"
REASON_CROSS_JOB = "cross_job_token"
REASON_STALE_GENERATION = "stale_generation"
REASON_LEASE_REVOKED = "lease_revoked"
REASON_LEASE_EXPIRED = "lease_expired"
REASON_UNKNOWN_LEASE = "unknown_lease"

# 固定对外消息: 不随具体令牌/绑定值变化, 不泄露任何凭证原文
_STABLE_MSG = {
    REASON_MISSING_TOKEN: "missing or invalid output credential",
    REASON_BAD_TOKEN: "invalid output credential",
    REASON_WRONG_TOKEN_TYPE: "invalid output credential",
    REASON_CROSS_JOB: "output credential is not authorized for this job",
    REASON_STALE_GENERATION: "output credential is not valid for the current "
                             "generation",
    REASON_LEASE_REVOKED: "output lease has been revoked",
    REASON_LEASE_EXPIRED: "output lease has expired",
    REASON_UNKNOWN_LEASE: "output lease is not recognized",
}


def stable_denial(reason: str, *, credential: str = "output_lease") -> str:
    if credential == "epoch_token":
        return "missing or invalid epoch credential"
    return _STABLE_MSG[reason]


@dataclass
class Lease:
    lease_id: str
    job_id: str
    generation: int
    issued_ts: float
    expires_ts: float
    state: str = ACTIVE
    revoked_ts: float | None = None
    token: str = ""

    def to_dict(self) -> dict:
        return {
            "lease_id": self.lease_id,
            "generation": self.generation,
            "state": self.state,
            "issued_ts": self.issued_ts,
            "expires_ts": self.expires_ts,
            "revoked_ts": self.revoked_ts,
        }


class LeaseManager:
    def __init__(self, job_id: str, scheduler: Scheduler):
        self.job_id = job_id
        self._sched = scheduler
        self._lock = threading.RLock()
        self._leases: dict[int, Lease] = {}  # generation -> current lease
        self._by_id: dict[str, Lease] = {}

    def grant(self, generation: int, ttl_seconds: float = 300.0) -> Lease:
        """为指定代际签发租约; 同代际重复获取幂等返回现有 ACTIVE 租约。"""
        now = time.time()
        with self._lock:
            existing = self._leases.get(generation)
            if existing is not None:
                if existing.state == ACTIVE and existing.expires_ts > now:
                    return existing
                # 过期则续签新 lease_id, 旧的保持 EXPIRED 仍可被识别
            lease = Lease(
                lease_id=uuid.uuid4().hex,
                job_id=self.job_id,
                generation=generation,
                issued_ts=now,
                expires_ts=now + ttl_seconds,
            )
            lease.token = self._sched.issue_lease_token(
                job_id=self.job_id,
                generation=generation,
                lease_id=lease.lease_id,
                until_ts=lease.expires_ts,
            )
            self._leases[generation] = lease
            self._by_id[lease.lease_id] = lease
            return lease

    def revoke_generation(self, generation: int) -> Lease | None:
        """抢占时吊销某代际的输出租约。"""
        with self._lock:
            lease = self._leases.get(generation)
            if lease is not None and lease.state == ACTIVE:
                lease.state = REVOKED
                lease.revoked_ts = time.time()
            return lease

    def current(self) -> Lease | None:
        with self._lock:
            for g in sorted(self._leases, reverse=True):
                return self._leases[g]
        return None

    def authorize(
        self, token: str | None, *, expected_generation: int,
        now: float | None = None,
    ) -> Lease:
        """校验写入令牌并返回有效租约, 任何失败都抛出带稳定 reason 的异常。

        分类:
          * 凭证不可验证 (缺失/伪造/类型错) -> InvalidToken 401;
          * 凭证可验证但无授权 (跨作业/旧代际/已吊销/已超期/未知租约)
            -> FenceViolation 412。

        抛出的异常携带:
          * ``reason``: 稳定机器码;
          * ``claimed``: 令牌中可安全公开的绑定字段 (job/generation/lease),
            供安全事件取证; 伪造令牌在签名校验前拿不到 payload, 为 None。
        异常消息固定, 不含令牌原文。
        """
        cur = now or time.time()
        if not token:
            raise InvalidToken(_STABLE_MSG[REASON_MISSING_TOKEN],
                               reason=REASON_MISSING_TOKEN)

        def _claim(payload: dict | None) -> dict:
            if not payload:
                return {}
            return {k: payload.get(k) for k in ("typ", "job", "generation",
                                                "lease") if k in payload}

        try:
            payload = self._sched.verify_lease_token(token)
        except InvalidToken as exc:
            reason = getattr(exc, "reason", None) or REASON_BAD_TOKEN
            deny = InvalidToken(_STABLE_MSG.get(reason,
                                                _STABLE_MSG[REASON_BAD_TOKEN]),
                                reason=reason)
            # 伪造/损坏令牌无 payload, claimed 为空 — 绝不记录令牌原文。
            # 类型不符 (epoch 冒充 lease) 时签名有效, 可安全记录绑定字段。
            try:
                maybe = self._sched.signer.verify(token)
                if isinstance(maybe, dict):
                    deny.claimed = _claim(maybe)
            except InvalidToken:
                pass
            raise deny from None
        claimed = _claim(payload)

        if payload["job"] != self.job_id:
            deny = FenceViolation(_STABLE_MSG[REASON_CROSS_JOB],
                                  reason=REASON_CROSS_JOB)
            deny.claimed = claimed
            raise deny
        if payload["generation"] != expected_generation:
            deny = FenceViolation(_STABLE_MSG[REASON_STALE_GENERATION],
                                  reason=REASON_STALE_GENERATION)
            deny.claimed = {**claimed, "expected_generation":
                            expected_generation}
            raise deny
        with self._lock:
            lease = self._by_id.get(payload["lease"])
            if lease is None:
                deny = InvalidToken(_STABLE_MSG[REASON_UNKNOWN_LEASE],
                                    reason=REASON_UNKNOWN_LEASE)
                deny.claimed = claimed
                raise deny
            if lease.generation != expected_generation:
                deny = FenceViolation(_STABLE_MSG[REASON_STALE_GENERATION],
                                      reason=REASON_STALE_GENERATION)
                deny.claimed = claimed
                raise deny
            if lease.state == REVOKED:
                deny = FenceViolation(_STABLE_MSG[REASON_LEASE_REVOKED],
                                      reason=REASON_LEASE_REVOKED)
                deny.claimed = {**claimed, "revoked_ts": lease.revoked_ts}
                raise deny
            if lease.state == EXPIRED or lease.expires_ts <= cur:
                lease.state = EXPIRED
                deny = FenceViolation(_STABLE_MSG[REASON_LEASE_EXPIRED],
                                      reason=REASON_LEASE_EXPIRED)
                deny.claimed = claimed
                raise deny
            if cur > payload["until_ts"]:
                deny = FenceViolation(_STABLE_MSG[REASON_LEASE_EXPIRED],
                                      reason=REASON_LEASE_EXPIRED)
                deny.claimed = claimed
                raise deny
            return lease

    def get(self, generation: int) -> Lease:
        with self._lock:
            lease = self._leases.get(generation)
            if lease is None:
                raise NotFound(f"no lease for generation {generation}")
            return lease

    def all(self) -> list[Lease]:
        """按代际返回全部租约 (GC 快照用于识别仍可访问的输出代际)。"""
        with self._lock:
            return [self._leases[g] for g in sorted(self._leases)]

    # -------------------------------------------------------- replay support
    def restore_grant(
        self, *, generation: int, lease_id: str, issued_ts: float,
        expires_ts: float, state: str = ACTIVE,
        revoked_ts: float | None = None,
    ) -> Lease:
        """事件重放: 按已持久化事实重建租约, 不重新签发/不改变其生命周期。

        令牌由同一调度器密钥按绑定字段重新派生, 因此重放后旧令牌仍能
        被验证或被栅栏拒绝。
        """
        lease = Lease(
            lease_id=lease_id,
            job_id=self.job_id,
            generation=generation,
            issued_ts=issued_ts,
            expires_ts=expires_ts,
            state=state,
            revoked_ts=revoked_ts,
        )
        lease.token = self._sched.issue_lease_token(
            job_id=self.job_id, generation=generation, lease_id=lease_id,
            until_ts=expires_ts,
        )
        with self._lock:
            prev = self._leases.get(generation)
            if prev is not None and prev.lease_id != lease_id:
                # 重放时构造函数可能预签过一个占位租约, 以持久化事实为准
                self._by_id.pop(prev.lease_id, None)
            self._leases[generation] = lease
            self._by_id[lease_id] = lease
        return lease

    def restore_revoke(self, generation: int, revoked_ts: float) -> None:
        """事件重放: 按 lease_revoked 事实标记租约吊销。"""
        with self._lock:
            lease = self._leases.get(generation)
            if lease is not None and lease.state == ACTIVE:
                lease.state = REVOKED
                lease.revoked_ts = revoked_ts
