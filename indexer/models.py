"""区块与交易的规范化数据模型。"""

from __future__ import annotations

import enum
from dataclasses import dataclass, field


class BlockStatus(str, enum.Enum):
    """区块在索引中的三类可区分状态。"""

    CANONICAL = "canonical"   # 规范链上的区块
    REORGED = "reorged"       # 曾被纳入规范链、后被重组替换
    STAGED = "staged"         # 父块缺失、未连通，暂存待连接


@dataclass(frozen=True)
class Transaction:
    tx_hash: str
    block_hash: str
    sender: str
    recipient: str
    amount: int
    nonce: int


@dataclass(frozen=True)
class Block:
    block_hash: str
    parent_hash: str
    height: int
    timestamp: int
    transactions: tuple[Transaction, ...] = field(default_factory=tuple)


@dataclass(frozen=True)
class IngestResult:
    """单个区块摄取的结果。"""

    block_hash: str
    status: BlockStatus
    applied: bool          # 是否实际写入了新数据（重复投递时为 False）
    reorg_depth: int = 0   # 本次摄取触发的重组深度


@dataclass(frozen=True)
class ReorgEvent:
    """一次规范链切换的审计记录，与状态切换在同一事务内同生共死。"""

    id: int
    occurred_at: str        # ISO-8601 UTC 时间戳，便于按时间倒序翻阅
    depth: int              # 切换深度（分叉点到旧链尖的高度差）
    old_tip_hash: str       # 切换前的规范链尖
    old_tip_height: int
    new_tip_hash: str       # 切换后的规范链尖
    new_tip_height: int
    fork_height: int        # 分叉点高度
    replaced_hashes: tuple[str, ...]   # 本次被换下去的原规范块
    adopted_hashes: tuple[str, ...]    # 本次进入规范链的块
    readopted_hashes: tuple[str, ...]  # 曾被换下、本次重新回到规范链的块


@dataclass(frozen=True)
class CheckResult:
    """一个自检分项的结论；issues 为空表示该项通过。"""

    name: str
    ok: bool
    issues: tuple[str, ...]


@dataclass(frozen=True)
class HealthReport:
    """只读自检总报告。"""

    ok: bool
    checks: tuple[CheckResult, ...]


@dataclass(frozen=True)
class BatchIngestResult:
    """整批原子摄取的结果：整批落库或整批不落。"""

    accepted: int                          # 本批实际新写入的块数
    duplicates: int                        # 本批中幂等重复投递的块数
    results: tuple[IngestResult, ...]      # 逐块结果，与输入顺序对齐
