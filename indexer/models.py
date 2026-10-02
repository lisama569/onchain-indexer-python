"""区块与交易的规范化数据模型。"""

from __future__ import annotations

import enum
from dataclasses import dataclass, field

class BlockStatus(str, enum.Enum):
    """区块在索引中的三类状态（顺序无关的结构判定，见 indexer.topology）。"""

    CANONICAL = "canonical"   # 当前规范链上的区块
    REORGED = "reorged"       # 已连通到创世块但输掉竞争的分叉块
    #   历史细分（当前三态相同，靠 blocks.ever_canonical 与审计区分）：
    #   ever_canonical=1：曾进入规范链、后被换下（审计 replaced 可查）；
    #   ever_canonical=0：连通时即落败、从未进入规范链（不在任何审计清单）。
    STAGED = "staged"         # 祖先链无法连通到创世块（缺父块/高度不连续）


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
class ReorgAuditRecord:
    """一次规范链切换的审计记录，与状态切换在同一事务内同生共死。"""

    id: int
    created_at_ns: int          # 记录产生时间（纳秒时间戳，单调来源 time.time_ns）
    trigger_block_hash: str     # 触发本次切换的区块
    fork_height: int            # 分叉点高度（共同祖先高度）
    depth: int                  # 切换深度（旧链被替换的区块数）
    old_tip_hash: str           # 切换前规范链尖
    old_tip_height: int
    new_tip_hash: str           # 切换后规范链尖
    new_tip_height: int
    replaced_blocks: tuple[str, ...]   # 本次被换下去的区块（高度升序）
    readopted_blocks: tuple[str, ...]  # 原在分叉上、本次重新回到规范链的区块（高度升序）


@dataclass(frozen=True)
class BatchIngestResult:
    """整批原子摄取的结果：要么全部生效，要么整块回滚。"""

    applied: int                       # 本批实际新写入的区块数
    duplicates: int                    # 本批中幂等重复的区块数
    results: tuple[IngestResult, ...]  # 逐块结果（保持入参顺序）


@dataclass(frozen=True)
class BlockSnapshot:
    """自检快照中的区块视图（不带交易体，状态单独给出）。"""

    block_hash: str
    parent_hash: str
    height: int
    status: BlockStatus
    ever_canonical: bool = False   # 是否曾经进入过规范链（历史标记，不可逆）


@dataclass(frozen=True)
class StoreSnapshot:
    """某一时刻的一致性只读快照：自检全程在此快照上计算。"""

    blocks: tuple[BlockSnapshot, ...]
    txs: tuple[Transaction, ...]
    progress: int
    audits: tuple[ReorgAuditRecord, ...] = ()
