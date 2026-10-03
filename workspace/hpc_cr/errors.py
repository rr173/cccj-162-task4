"""服务端错误类型。

每个领域错误携带 HTTP 状态码, 供 API 层统一映射为 JSON 错误响应。

授权失败类错误额外携带稳定的机器可读 ``reason`` 代码: 同一类拒绝永远得到
同一 HTTP 状态/error/reason 三元组, 错误消息保持固定文本, 不回显令牌、
签名或任何凭证原文 (可追踪信息只写入服务端安全事件流)。
"""


class CpError(Exception):
    """所有领域错误的基类。"""

    http_status = 500
    error_code = "internal_error"

    def __init__(self, message: str = "", *, reason: str | None = None):
        super().__init__(message)
        # 稳定细分原因码 (如 lease_revoked / bad_signature / cross_job),
        # 供客户端分支与安全事件归类; 绝不包含凭证内容。
        self.reason = reason
        # 校验已通过的令牌中可安全公开的绑定字段 (job/generation/lease...),
        # 仅用于服务端取证; 伪造令牌在签名校验前拿不到 payload, 留空。
        self.claimed: dict | None = None

    def to_dict(self):
        body = {"error": self.error_code, "message": str(self)}
        if self.reason:
            body["reason"] = self.reason
        return body


class NotFound(CpError):
    http_status = 404
    error_code = "not_found"


class Conflict(CpError):
    """状态冲突: 重复检查点 / 重复分片 / 状态机不允许的迁移。"""

    http_status = 409
    error_code = "conflict"


class FenceViolation(CpError):
    """栅栏拒绝: 凭证本身可识别, 但不具备本次操作的授权。

    旧纪元/旧代际令牌的迟到写入、跨作业凭证、已吊销/超期租约均属此类。
    """

    http_status = 412
    error_code = "fence_violation"


class InvalidToken(CpError):
    """凭证无效: 缺失、伪造、格式损坏或类型不符。"""

    http_status = 401
    error_code = "invalid_token"


class ValidationError(CpError):
    http_status = 400
    error_code = "validation_error"
