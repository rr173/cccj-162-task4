"""高性能计算作业的抢占与检查点恢复服务 (Preemption & Checkpoint-Restore)。

核心不变量:
  1. 调度器回收节点前签发 *抢占纪元* (epoch), 作业必须持有该纪元的签名令牌
     才能提交检查点 / 确认释放。
  2. 检查点为内容寻址 (CAS): 每个数据分片的标识 = sha256(载荷), 提交时逐片
     重新读取校验; 只有 *全部分片* (进程集合 × 每进程分片) 校验完成才置为
     COMPLETE (可用), 否则永远停留在 PENDING/INCOMPLETE。
  3. 节点失联会阻塞对应分片, 只有分区恢复后补齐并整体校验, 或调度器放弃
     (abandon) 该纪元, 绝不把缺失分片的检查点标记为可用。
  4. 恢复必须引用同一检查点代际 (generation, 与 epoch 一一对应), 并重新
     取得新代际的 *输出租约*; 旧纪元/旧代际令牌的迟到写入一律被栅栏
     (fence) 拒绝。输出按 (代际, 幂等键) 去重, 杜绝双重输出。
"""

from .errors import (
    CpError,
    NotFound,
    Conflict,
    FenceViolation,
    ValidationError,
    InvalidToken,
)
from .jobs import JobManager, RestoreReport

__all__ = [
    "JobManager",
    "RestoreReport",
    "CpError",
    "NotFound",
    "Conflict",
    "FenceViolation",
    "ValidationError",
    "InvalidToken",
]
