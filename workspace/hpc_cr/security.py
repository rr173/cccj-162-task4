"""密码学与序列化原语。

- canonical_json: 稳定字节序列, 供签名/校验与去重。
- sha256_hex: 内容寻址哈希 (CAS 分片标识)。
- Signer: 调度器 HMAC-SHA256 签名, 抢占纪元令牌与输出租约令牌共用。

令牌为紧凑自描述字符串 ``v1.<payload_b64>.<sig_hex>``: 服务端无需在
数据通道上记住"已签发过哪些令牌", 只需验证签名 + 解码后的绑定字段
(job/epoch/generation), 因此迟到节点出示旧令牌也能被立即识别并栅栏。
"""

from __future__ import annotations

import base64
import hashlib
import hmac
import json
from typing import Any

from .errors import InvalidToken

TOKEN_VERSION = "v1"


def sha256_hex(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def canonical_json(obj: Any) -> bytes:
    """带排序键、无空白的 UTF-8 JSON, 同一逻辑文档永远得到同一字节串。"""
    return json.dumps(
        obj, sort_keys=True, separators=(",", ":"), ensure_ascii=False
    ).encode("utf-8")


class Signer:
    """调度器 HMAC 签名器。生产环境密钥应来自 KMS/密钥文件。"""

    def __init__(self, secret: bytes | str):
        self._key = secret.encode("utf-8") if isinstance(secret, str) else secret

    def _sign(self, payload_b64: bytes) -> str:
        return hmac.new(self._key, payload_b64, hashlib.sha256).hexdigest()

    def issue(self, payload: dict) -> str:
        """对规范化 payload 签名, 返回自包含令牌。"""
        body = base64.urlsafe_b64encode(canonical_json(payload))
        return f"{TOKEN_VERSION}.{body.decode()}." + self._sign(body)

    def verify(self, token: str) -> dict:
        """验证令牌签名并返回 payload; 任何篡改/版本不符均拒绝。

        所有拒绝都带稳定 reason 码, 消息固定且不含令牌原文。
        """
        try:
            version, body_b64, sig = token.split(".")
        except (AttributeError, ValueError, TypeError) as exc:
            raise InvalidToken("malformed credential",
                               reason="malformed_token") from exc
        if version != TOKEN_VERSION:
            raise InvalidToken("unsupported credential version",
                               reason="bad_token_version")
        expected = self._sign(body_b64.encode())
        if not hmac.compare_digest(expected, sig):
            raise InvalidToken("invalid credential signature",
                               reason="bad_signature")
        try:
            payload = json.loads(base64.urlsafe_b64decode(body_b64))
        except Exception as exc:  # noqa: BLE001 - 任何解码失败都是伪造令牌
            raise InvalidToken("undecodable credential payload",
                               reason="undecodable_payload") from exc
        if not isinstance(payload, dict):
            raise InvalidToken("credential payload must be an object",
                               reason="bad_payload")
        return payload

    @staticmethod
    def require_fields(payload: dict, fields: tuple[str, ...]) -> None:
        missing = [f for f in fields if f not in payload]
        if missing:
            # 只暴露字段名, 不暴露 payload 内容
            raise InvalidToken("credential is missing required fields",
                               reason="missing_fields")
