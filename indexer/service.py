"""摄取与规范链维护的核心服务。

规则概述：
- 幂等：同一区块重复投递不产生重复记录，返回 applied=False。
- 三态判定顺序无关：每次写入后基于「全部已落库区块集合」做一次确定性的
  拓扑分类（indexer.topology.classify_blocks，service/health/查询同一口径），
  因此同一份数据无论以什么顺序、什么并发度投递，最终三类归类完全一致：
  - CANONICAL：从创世块连通的链中链尖最高（同高取链尖哈希最小）的唯一链；
  - REORGED：已连通到创世块、但不在规范链上的分叉块（含历史上曾被换下去
    的块，以及连通时即输掉竞争、从未进入过规范链的块——二者的区分可从
    重组审计的 replaced/readopted 清单复查）；
  - STAGED：祖先链无法连通到创世块的块（缺父块、或与父块高度不连续）。
- 审计：只有「本次重算导致规范链集合发生变化」时才在同一事务内写一条
  reorg_audits，记录切换前后链尖、深度、被换下去的块(replaced：
  CANONICAL→非规范)与重新接纳的块(readopted：非规范→CANONICAL，触发块
  本身不计入)；初始建链、规范链正常延伸、落败分叉连通、重复投递都不记账。
- 重组深度超过 max_reorg_depth 时整体拒绝（事务回滚，不产生部分写入）。
- 进度只随规范链尖端单调前进。
"""

from __future__ import annotations

import sqlite3
import time

from .config import IndexerConfig
from .errors import (MalformedPayloadError, ReorgTooDeepError,
                     StateConflictError, StorageError)
from .models import (BatchIngestResult, Block, BlockStatus, IngestResult,
                     ReorgAuditRecord)
from .store import Store
from .topology import classify_blocks


class IngestService:
    """负责区块摄取、暂存连接、重组与进度推进。"""

    def __init__(self, store: Store, config: IndexerConfig) -> None:
        self._store = store
        self._config = config

    # ---- 校验 ----
    def _validate(self, block: Block) -> None:
        problems: list[str] = []
        if not block.block_hash or not isinstance(block.block_hash, str):
            problems.append("block_hash 缺失或非法")
        if not isinstance(block.parent_hash, str):
            problems.append("parent_hash 非法")
        if block.height < 0:
            problems.append("height 为负")
        if block.timestamp < 0:
            problems.append("timestamp 为负")
        seen: set[str] = set()
        for tx in block.transactions:
            if tx.block_hash != block.block_hash:
                problems.append(f"tx {tx.tx_hash} 的 block_hash 与区块不一致")
            if not tx.tx_hash:
                problems.append("存在 tx_hash 为空的交易")
            elif tx.tx_hash in seen:
                problems.append(f"区块内交易重复: {tx.tx_hash}")
            seen.add(tx.tx_hash)
            if tx.amount < 0:
                problems.append(f"tx {tx.tx_hash} amount 为负")
            if tx.nonce < 0:
                problems.append(f"tx {tx.tx_hash} nonce 为负")
        if problems:
            raise MalformedPayloadError(
                "区块负载畸形: " + "; ".join(problems),
                detail={"block_hash": block.block_hash, "problems": problems},
            )

    # ---- 规范链（顺序无关的整体重算） ----
    def _reclassify(self, trigger_hash: str | None) -> int:
        """基于全量已落库区块做确定性三态重算并落地状态翻转与审计。

        返回本次规范链切换的重组深度（0 表示无切换）。
        调用方必须已持有 store 的写事务。
        """
        all_entries = self._store.list_blocks()
        all_blocks = [b for b, _ in all_entries]
        old_canonical = {b.block_hash for b, st in all_entries
                         if st == BlockStatus.CANONICAL}

        view = classify_blocks(all_blocks,
                               genesis_hash=self._config.genesis_hash)
        new_canonical = set(view.canonical_hashes)
        replaced_set = old_canonical - new_canonical
        adopted_set = new_canonical - old_canonical

        # 规范链集合变化但没有旧规范块被换下：初始建链或正常延伸，
        # 不构成「切换」，只落地状态/进度，不翻转、不记账。
        # 只标记「本次新进入规范链」的块（adopted），不能把整个规范集合
        # 都打上标记——否则乱序下最终落败、仅在投递瞬间临时胜出的块会被
        # 误记成「曾进入规范链」，导致标记与审计清单无法对账。
        if not replaced_set:
            self._apply_statuses(view.statuses)
            self._store.mark_ever_canonical(adopted_set)
            if view.tip_height >= 0:
                self._store.advance_progress(view.tip_height)
            return 0

        # 真正的切换：旧规范块被换下去、分叉块被接纳。
        fork_height = self._fork_height(old_canonical, new_canonical, view)
        depth = len(replaced_set)
        old_tip_height = max((h for blk_hash, h in view.heights.items()
                              if blk_hash in old_canonical), default=-1)
        old_tip_hash = next(
            (blk_hash for blk_hash in old_canonical
             if view.heights.get(blk_hash) == old_tip_height), "")

        if depth > self._config.max_reorg_depth:
            raise ReorgTooDeepError(
                f"重组深度 {depth} 超过上限 {self._config.max_reorg_depth}，拒绝摄取",
                detail={"block_hash": trigger_hash, "depth": depth,
                        "max_reorg_depth": self._config.max_reorg_depth},
            )

        # replaced：本次真正 CANONICAL -> REORGED 的块（高度升序）。
        replaced = sorted(replaced_set,
                          key=lambda h: view.heights.get(h, -1))

        # readopted：本次回到规范链的块中，除触发块外的部分。
        # 触发块是审计记录的新链尖（全局重算下真正让新链胜出的可能是先
        # 暂存、后因别的块补齐父链而连通的块），而非本次新投递的块。
        # 是否属于「重新接纳」由持久化的 ever_canonical 历史标记判定，
        # 而不是当前状态——全新分叉首次胜出时，落败段在新口径下落库即为
        # REORGED，但它们从未上过规范链（ever_canonical=0），不算重新接纳。
        # 获胜段最低处的块 ever_canonical=1 即说明本段建在曾被换下去的
        # 回归链上，此时回归旧块与先于触发块到达的暂存延伸块都计入。
        readopted: list[str] = []
        assert view.tip_hash is not None
        adopted_others = adopted_set - {view.tip_hash}
        if adopted_others:
            heights = view.heights
            first_adopted = min(adopted_others, key=lambda h: heights[h])
            if self._store.ever_canonical(first_adopted):
                readopted = sorted(adopted_others,
                                   key=lambda h: heights[h])

        self._apply_statuses(view.statuses)
        # 只标记本次新进入规范链的块；被换下的旧块标记保留（不可逆），
        # 这正是「曾被换下去」与「从未进入规范链」的权威区分。
        self._store.mark_ever_canonical(adopted_set)
        self._store.advance_progress(view.tip_height)

        assert view.tip_hash is not None
        # 触发块如实记录为本次切换后的规范链尖：全局重算下真正让新链连通
        # 胜出的可能是先暂存、后因别的块补齐父链而连通的块（见审计测试）。
        self._store.insert_reorg_audit(ReorgAuditRecord(
            id=0,
            created_at_ns=time.time_ns(),
            trigger_block_hash=view.tip_hash,
            fork_height=fork_height,
            depth=depth,
            old_tip_hash=old_tip_hash,
            old_tip_height=old_tip_height,
            new_tip_hash=view.tip_hash,
            new_tip_height=view.tip_height,
            replaced_blocks=tuple(replaced),
            readopted_blocks=tuple(readopted),
        ))
        return depth

    @staticmethod
    def _fork_height(old_canonical: set[str], new_canonical: set[str],
                     view) -> int:
        """两条规范链共同祖先的最高高度（无共同块时为 -1）。"""
        common = old_canonical & new_canonical
        return max((view.heights[h] for h in common), default=-1)

    def _apply_statuses(self, statuses: dict[str, BlockStatus]) -> None:
        for block_hash, status in statuses.items():
            current = self._store.get_block(block_hash)
            assert current is not None
            if current[1] != status:
                self._store.set_block_status(block_hash, status)

    # ---- 对外入口 ----
    def ingest_block(self, block: Block) -> IngestResult:
        """摄取单个区块：幂等、可暂存、可触发重组；失败整体回滚。"""
        self._validate(block)
        try:
            with self._store.transaction():
                return self._ingest_block_inner(block)
        except (MalformedPayloadError, StateConflictError, ReorgTooDeepError):
            raise
        except sqlite3.Error as exc:
            raise StorageError(
                f"底层存储故障，摄取未生效: {exc.__class__.__name__}",
                detail={"block_hash": block.block_hash},
            ) from exc

    def ingest_blocks_atomic(self, blocks: list[Block]) -> BatchIngestResult:
        """整批原子摄取：一个批次要么全部生效，要么整体回滚不留任何数据。

        - 逐块的幂等规则不变：批次内/与已存数据重复投递按幂等处理（计入
          duplicates、applied=False），不算失败。
        - 任一块畸形、状态冲突或重组过深 -> 整批拒绝，已暂存/已切换的部分
          随外层事务一起回滚，绝不出现前半批入库、后半批缺失。
        - 等待写锁超过 config.lock_timeout 抛 LockTimeoutError（可重试临时故障）。
        - 底层数据库异常统一包装为 StorageError，不把原生异常甩给调用方。
        """
        # 先做纯内存校验，畸形块在触碰锁/数据前即拒绝，整批自然不落库
        for block in blocks:
            self._validate(block)
        results: list[IngestResult] = []
        try:
            with self._store.transaction(self._config.lock_timeout):
                for block in blocks:
                    results.append(self._ingest_block_inner(block))
        except (MalformedPayloadError, StateConflictError, ReorgTooDeepError):
            raise
        except sqlite3.Error as exc:
            raise StorageError(
                f"底层存储故障，整批未生效: {exc.__class__.__name__}",
                detail={"batch_size": len(blocks)},
            ) from exc
        applied = sum(1 for r in results if r.applied)
        duplicates = len(results) - applied
        return BatchIngestResult(applied=applied, duplicates=duplicates,
                                 results=tuple(results))

    def _ingest_block_inner(self, block: Block) -> IngestResult:
        """事务体：单块摄取逻辑（不含自身开事务），供原子批次复用。"""
        existing = self._store.get_block(block.block_hash)
        if existing is not None:
            if existing[0] != block:
                raise StateConflictError(
                    f"block {block.block_hash} 已存在但内容不一致",
                    detail={"block_hash": block.block_hash},
                )
            # 重复投递：幂等返回当前状态，不写新数据
            return IngestResult(block.block_hash, existing[1], applied=False)

        if block.height == 0:
            if (self._config.genesis_hash
                    and block.block_hash != self._config.genesis_hash):
                raise MalformedPayloadError(
                    "创世块哈希与配置不符",
                    detail={"block_hash": block.block_hash,
                            "expected": self._config.genesis_hash},
                )
        else:
            parent = self._store.get_block(block.parent_hash)
            if parent is None:
                # 父块缺失：先落库为暂存块，再做一次全量重算（它带来的
                # 其它子树状态不变；它自身因不连通仍为 STAGED）。
                self._store.upsert_block(block, BlockStatus.STAGED)
                depth = self._reclassify(block.block_hash)
                final_status = self._store.get_block(
                    block.block_hash)[1]  # type: ignore[index]
                return IngestResult(block.block_hash, final_status,
                                    applied=True, reorg_depth=depth)
            if parent[0].height + 1 != block.height:
                raise MalformedPayloadError(
                    "区块高度与父块不连续",
                    detail={"block_hash": block.block_hash,
                            "height": block.height,
                            "parent_height": parent[0].height},
                )

        # 新块以 STAGED 落库后，在全量拓扑上统一重算三态与规范链切换；
        # 结论只取决于最终区块集合，与投递顺序/并发度无关。
        self._store.upsert_block(block, BlockStatus.STAGED)
        depth = self._reclassify(block.block_hash)
        final_status = self._store.get_block(block.block_hash)[1]  # type: ignore[index]
        return IngestResult(block.block_hash, final_status,
                            applied=True, reorg_depth=depth)

    def canonical_chain(self) -> list[Block]:
        """按高度升序返回当前规范链。"""
        return [b for b, _ in sorted(
            self._store.list_blocks(BlockStatus.CANONICAL),
            key=lambda item: item[0].height,
        )]
