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
