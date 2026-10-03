"""持久化原语: 顺序化事件日志 (journal) 与持久化内容寻址存储。

服务采用事件溯源: 每一次状态变更 (作业创建、抢占、分片上传、输出提交、
授权拒绝……) 在持锁区内以 **单调递增 seq** 追加为一条 JSON 事件。事件:

  * **顺序化**: seq 为集群生命周期内单调递增整数, 同序保持写入顺序,
    服务重启后严格按 seq 重放重建全部内存状态;
  * **只追加**: 旧行永不修改, 文件按行追加 + flush/fsync, 崩溃最多丢失
    尚未落盘的最后一条 (该条对外也不可见);
  * **不含凭证原文**: 任何 epoch/lease 令牌、HMAC 签名都不进入事件,
    令牌是自描述可重验的, 重放时由调度器用同一密钥重新派生。

``data_dir=None`` 时退化为纯内存实现 (测试与临时演示使用), 行为一致。
"""

from __future__ import annotations

import json
import os
import threading
from typing import Any, Iterator

JOURNAL_NAME = "journal.jsonl"
BLOBS_DIR = "blobs"


class Journal:
    """全局只追加事件日志 (所有作业共享一条顺序流)。"""

    def __init__(self, data_dir: str | None = None):
        self._data_dir = data_dir
        self._lock = threading.Lock()
        self._seq = 0
        self._events: list[dict] = []          # data_dir=None 时的内存事件
        self._fp = None
        self._replayed = False
        if data_dir is not None:
            os.makedirs(data_dir, exist_ok=True)
            self._path = os.path.join(data_dir, JOURNAL_NAME)
            # 启动重放 (若有), 以恢复 seq 水位
            if os.path.exists(self._path):
                with open(self._path, "r", encoding="utf-8") as fp:
                    for line in fp:
                        line = line.strip()
                        if not line:
                            continue
                        try:
                            evt = json.loads(line)
                        except json.JSONDecodeError:
                            # 崩溃可能留下半行; 半行之前的全部有效
                            break
                        self._events.append(evt)
                        if isinstance(evt.get("seq"), int):
                            self._seq = max(self._seq, evt["seq"])
            # 以追加模式打开, 后续 append 在追加锁内进行
            self._fp = open(self._path, "a", encoding="utf-8")
            self._replayed = True

    # ------------------------------------------------------------- append
    def append(self, event_type: str, detail: dict[str, Any]) -> dict:
        """分配下一个 seq 并持久化事件, 返回带 seq/ts 的完整事件。

        调用方必须保证语义变更与事件追加在同一把领域锁临界区内: 事件
        一旦落盘, 重放必然重现该变更, 因此重复并发请求最多只追加一次
        去重后的结果事件。
        """
        with self._lock:
            self._seq += 1
            evt = {"seq": self._seq, "ts": detail.get("ts") or _now(),
                   "type": event_type, **{k: v for k, v in detail.items()
                                          if k != "ts"}}
            line = json.dumps(evt, ensure_ascii=False, sort_keys=True)
            if self._fp is not None:
                self._fp.write(line + "\n")
                self._fp.flush()
                os.fsync(self._fp.fileno())
            self._events.append(evt)
            return evt

    # -------------------------------------------------------------- reads
    def replay(self) -> Iterator[dict]:
        """按 seq 顺序产生全部事件 (重启后据此重建内存状态)。"""
        with self._lock:
            events = sorted(self._events, key=lambda e: e.get("seq", 0))
        yield from events

    def all(self) -> list[dict]:
        with self._lock:
            return sorted(self._events, key=lambda e: e.get("seq", 0))

    @property
    def last_seq(self) -> int:
        """当前已持久化 (或内存模式已追加) 的最高水位。"""
        with self._lock:
            return self._seq

    @property
    def persistent(self) -> bool:
        return self._data_dir is not None

    def close(self) -> None:
        with self._lock:
            if self._fp is not None:
                self._fp.flush()
                self._fp.close()
                self._fp = None


def _now() -> float:
    import time
    return time.time()


def load_or_create_secret(data_dir: str, *, env_var: str = "HPC_CR_SECRET") -> str:
    """加载/初始化 HMAC 签名密钥: 优先环境变量, 其次 data_dir 内密钥文件。

    令牌是自描述的 (payload+签名), 服务重启后必须使用同一密钥才能验证
    已签发凭证; 密钥文件权限收敛为 0600。
    """
    env = os.environ.get(env_var)
    if env:
        return env
    path = os.path.join(data_dir, "signer.key")
    if os.path.exists(path):
        with open(path, "r", encoding="utf-8") as fp:
            return fp.read().strip()
    import secrets as _secrets
    secret = _secrets.token_hex(32)
    fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    try:
        os.write(fd, secret.encode("utf-8"))
    finally:
        os.close(fd)
    return secret
