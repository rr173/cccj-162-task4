"""内容寻址存储 (CAS) 与输出提交日志。

ContentStore:
  put(data)  -> 若 sha256 已存在则直接复用 (重复检查点的天然去重),
                否则原子写入; 支持测试期注入损坏以验证提交期重校验。
  get(sha256) -> 重新从存储读出字节 (提交校验绝不信任客户端自报哈希)。
  verify(sha256) -> 重算哈希, 任何 bit-rot/损坏立即暴露。

PersistentContentStore: 叠加磁盘后端 (data_dir/blobs/<sha256>), 写入
fsync 落盘; 重启后 blob 按需惰性读回, 校验/恢复在重启后依然成立。

OutputLog:
  输出条目以 (generation, output_id) 唯一。同代际同 idempotency key 的
  重复提交: 内容相同 -> 幂等返回已有条目; 内容不同 -> Conflict。
  代际不同的条目彼此隔离; 能否写入由 LeaseManager 的栅栏决定, OutputLog
  只负责"一旦写入, 永不重复落盘/交付"。
"""

from __future__ import annotations

import os
import tempfile
import threading
from dataclasses import dataclass, field

from .errors import Conflict, NotFound, ValidationError
from .security import sha256_hex


class ContentStore:
    def __init__(self, data_dir: str | None = None):
        self._blobs: dict[str, bytes] = {}
        self._lock = threading.Lock()
        self._data_dir = data_dir
        self._disk = data_dir is not None
        if data_dir is not None:
            self._blob_dir = os.path.join(data_dir, "blobs")
            os.makedirs(self._blob_dir, exist_ok=True)
            # 重启后内存为空, 磁盘上已有哪些 blob 直接列目录
            for name in os.listdir(self._blob_dir):
                self._blobs.setdefault(name, None)  # 惰性加载
        else:
            self._blob_dir = None

    def put(self, data: bytes) -> tuple[str, int, bool]:
        """存储数据, 返回 (sha256, 长度, 是否新写入)。"""
        if not isinstance(data, (bytes, bytearray)):
            raise ValidationError("blob payload must be bytes")
        digest = sha256_hex(data)
        with self._lock:
            existed = digest in self._blobs
            if not existed:
                self._blobs[digest] = bytes(data)
                if self._disk:
                    self._write_durable(digest, bytes(data))
            return digest, len(data), not existed

    def _write_durable(self, digest: str, data: bytes) -> None:
        """同目录临时文件 + fsync + 原子 rename。"""
        path = os.path.join(self._blob_dir, digest)
        if os.path.exists(path):
            return
        self._write_durable_at(path, data)

    def _write_durable_at(self, path: str, data: bytes) -> None:
        fd, tmp = tempfile.mkstemp(dir=self._blob_dir, prefix=".tmp-")
        try:
            with os.fdopen(fd, "wb") as fp:
                fp.write(data)
                fp.flush()
                os.fsync(fp.fileno())
            os.replace(tmp, path)
        except BaseException:
            try:
                os.unlink(tmp)
            except OSError:
                pass
            raise

    def rewrite(self, digest: str, data: bytes) -> None:
        """强制以 ``data`` 重写 blob (修复 bit-rot/损坏副本)。

        正常写入走 put() 的内容寻址去重; 损坏的"同键"对象需要物理替换,
        原子 rename 保证校验路径绝不会读到半个文件。
        """
        if not isinstance(data, (bytes, bytearray)):
            raise ValidationError("blob payload must be bytes")
        if sha256_hex(data) != digest:
            raise ValidationError(
                "rewrite refused: payload does not match content digest")
        with self._lock:
            self._blobs[digest] = bytes(data)
            if self._disk:
                self._write_durable_at(
                    os.path.join(self._blob_dir, digest), bytes(data))

    def _load(self, digest: str) -> bytes | None:
        cached = self._blobs.get(digest)
        if cached is not None:
            return cached
        if self._disk:
            path = os.path.join(self._blob_dir, digest)
            if os.path.exists(path):
                with open(path, "rb") as fp:
                    data = fp.read()
                self._blobs[digest] = data
                return data
        return None

    def get(self, digest: str) -> bytes:
        with self._lock:
            data = self._load(digest)
            if data is None:
                raise NotFound(f"blob not found: {digest[:12]}")
            return data

    def has(self, digest: str) -> bool:
        with self._lock:
            return self._load(digest) is not None

    def verify(self, digest: str) -> bool:
        """重算哈希, 损坏/缺失返回 False (调用方决定如何标记检查点)。"""
        with self._lock:
            data = self._load(digest)
        if data is None:
            return False
        return sha256_hex(data) == digest

    # -- 测试支持: 模拟存储 bit-rot / 节点磁盘损坏 --
    def corrupt(self, digest: str) -> None:
        with self._lock:
            data = self._load(digest)
            if data is None:
                raise NotFound(f"blob not found: {digest[:12]}")
            corrupted = data + b"\x00CORRUPT"
            self._blobs[digest] = corrupted
            if self._disk:
                # 内存损坏同时覆盖磁盘, 模拟真实 bit-rot 持久化
                with open(os.path.join(self._blob_dir, digest), "wb") as fp:
                    fp.write(corrupted)
                    fp.flush()
                    os.fsync(fp.fileno())

    def blob_count(self) -> int:
        with self._lock:
            return sum(1 for d in self._blobs if self._load(d) is not None)

    def inventory(self) -> list[tuple[str, int]]:
        """返回当前物理存在的 ``(blob, size)``; 磁盘缺失的惰性键被忽略。"""
        with self._lock:
            found: list[tuple[str, int]] = []
            for digest in list(self._blobs):
                data = self._load(digest)
                if data is not None:
                    found.append((digest, len(data)))
                else:
                    # 磁盘文件可能由外部运维/测试清理; 不能让缺失键继续伪装
                    # 成"存在"的 blob 并污染 GC 物理集合。
                    self._blobs.pop(digest, None)
            return found

    def delete(self, digest: str) -> tuple[bool, int]:
        """删除一个 blob, 返回 ``(此前是否存在, 实际释放字节数)``。

        删除是幂等的: 重试/崩溃恢复时再次调用得到 ``(False, 0)``。调用方
        必须已经通过引用快照确认该内容没有任何可达引用。
        """
        with self._lock:
            data = self._load(digest)
            if self._disk:
                path = os.path.join(self._blob_dir, digest)
                if data is None or not os.path.exists(path):
                    self._blobs.pop(digest, None)
                    return False, 0
                try:
                    size = os.path.getsize(path)
                except OSError:
                    size = len(data)
                os.unlink(path)
                self._blobs.pop(digest, None)
                return True, size
            if digest not in self._blobs or data is None:
                return False, 0
            self._blobs.pop(digest, None)
            return True, len(data)


@dataclass
class OutputRecord:
    generation: int
    output_id: str
    blob: str
    size: int
    rank: int | None
    ts: float

    def to_dict(self) -> dict:
        return {
            "generation": self.generation,
            "output_id": self.output_id,
            "blob": self.blob,
            "size": self.size,
            "rank": self.rank,
            "ts": self.ts,
        }


class OutputLog:
    """每作业一份的输出去重日志 (JobManager 内按 generation 隔离)。"""

    def __init__(self):
        self._lock = threading.Lock()
        # (generation, output_id) -> OutputRecord
        self._records: dict[tuple[int, str], OutputRecord] = {}

    def append(
        self,
        *,
        generation: int,
        output_id: str,
        blob: str,
        size: int,
        rank: int | None,
        ts: float,
    ) -> tuple[OutputRecord, bool]:
        """返回 (记录, 是否新写入)。重复但内容相同 -> 幂等; 冲突 -> 409。"""
        key = (generation, output_id)
        with self._lock:
            existing = self._records.get(key)
            if existing is not None:
                if existing.blob != blob:
                    raise Conflict(
                        f"output_id {output_id!r} already committed in "
                        f"generation {generation} with different content"
                    )
                return existing, False
            rec = OutputRecord(
                generation=generation,
                output_id=output_id,
                blob=blob,
                size=size,
                rank=rank,
                ts=ts,
            )
            self._records[key] = rec
            return rec, True

    def restore(
        self,
        *,
        generation: int,
        output_id: str,
        blob: str,
        size: int,
        rank: int | None,
        ts: float,
    ) -> None:
        """事件重放入口: 已提交事实按 (generation, output_id) 唯一插入。"""
        self._records[(generation, output_id)] = OutputRecord(
            generation=generation, output_id=output_id, blob=blob,
            size=size, rank=rank, ts=ts)

    def list(self, generation: int | None = None) -> list[OutputRecord]:
        with self._lock:
            recs = list(self._records.values())
        if generation is not None:
            recs = [r for r in recs if r.generation == generation]
        recs.sort(key=lambda r: r.ts)
        return recs
