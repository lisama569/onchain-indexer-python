"""摄取与规范链维护的核心服务。

规则概述：
- 幂等：同一区块重复投递不产生重复记录，返回 applied=False。
- 暂存：父块缺失的区块以 STAGED 保留；已连通但暂未胜出的分叉块同样为 STAGED。
- 规范链：始终选择已连通链中高度最高者；触发切换时同步更新被替换
  区块及其交易的归属（区块状态 CANONICAL -> REORGED，交易随区块派生）。
- 审计：每次真正的规范链切换在状态切换的同一事务内写一条 reorg_audits，
  记录切换前后链尖、切换深度、被换下去的块(replaced)与原本在分叉上、
  本次重新回到规范链的块(readopted)；切换失败随事务整体回滚不留记录，
  重复投递不触发切换、不凭空记账。
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

    # ---- 规范链 ----
    def _canonical_tip(self) -> Block | None:
        canonical = self._store.list_blocks(BlockStatus.CANONICAL)
        if not canonical:
            return None
        return max((b for b, _ in canonical), key=lambda b: b.height)

    def _connect(self, block_hash: str) -> int:
        """已连通区块的规范链判定；返回本次触发的重组深度（0 表示无切换）。"""
        entry = self._store.get_block(block_hash)
        assert entry is not None
        blk, _ = entry
        tip = self._canonical_tip()
        if tip is not None and blk.height <= tip.height:
            return 0  # 已连通但未胜出，保持 STAGED（未来可能重新参与竞争）

        # 回溯到最近的规范祖先，确定分叉点；沿途经过的非规范块
        # （STAGED 的延伸块或 REORGED 的回归块）都属于本次要提升的新链；
        # 若链未连通到创世块则保持暂存。
        chain: list[Block] = []
        cur = blk
        fork_height = -1
        while True:
            chain.append(cur)
            parent = self._store.get_block(cur.parent_hash)
            if parent is None:
                if cur.height == 0:
                    break  # 创世块自身为根
                return 0  # 祖先链未连通到规范链，保持 STAGED，不提升
            if parent[1] == BlockStatus.CANONICAL:
                fork_height = parent[0].height
                break
            cur = parent[0]

        depth = (tip.height - fork_height) if tip is not None else 0
        if tip is not None and depth == 0:
            # 新块只是当前规范链的正常延伸（分叉点即链尖），无切换、无翻转、
            # 不记账。递归连接子块时也可能走到这里，必须短路，避免把规范链
            # 当成旧链「重放」一遍并误写审计。
            for b in chain:
                self._store.set_block_status(b.block_hash, BlockStatus.CANONICAL)
            self._store.advance_progress(blk.height)
            return 0
        if tip is not None and depth > self._config.max_reorg_depth:
            raise ReorgTooDeepError(
                f"重组深度 {depth} 超过上限 {self._config.max_reorg_depth}，拒绝摄取",
                detail={"block_hash": blk.block_hash, "depth": depth,
                        "max_reorg_depth": self._config.max_reorg_depth},
            )

        # 旧规范链分叉点以上 -> REORGED（这些正是被「换下去」的块）
        replaced: list[str] = []
        if tip is not None:
            for h in range(fork_height + 1, tip.height + 1):
                for old in self._store.blocks_at_height(h, BlockStatus.CANONICAL):
                    self._store.set_block_status(old.block_hash, BlockStatus.REORGED)
                    replaced.append(old.block_hash)
        # 新链 -> CANONICAL。readopted 只包含此前确实上过规范链、已被换为
        # REORGED 的块：
        #   - 直接状态为 REORGED 的块（如回归的 a1..a3）；
        #   - 搭在 REORGED 块之上的 STAGED 延伸块（如 a4）——其根是回归链；
        # 全新分叉首次胜出时其上的 STAGED 块（b1..b3）不属于重新接纳。
        readopted: list[str] = []
        for b in chain:
            if b.block_hash == blk.block_hash:
                self._store.set_block_status(b.block_hash, BlockStatus.CANONICAL)
                continue
            old_entry = self._store.get_block(b.block_hash)
            assert old_entry is not None
            if old_entry[1] == BlockStatus.REORGED:
                readopted.append(b.block_hash)
            elif old_entry[1] == BlockStatus.STAGED:
                base_entry = self._store.get_block(b.parent_hash)
                if base_entry is not None \
                        and base_entry[1] == BlockStatus.REORGED:
                    readopted.append(b.block_hash)
            self._store.set_block_status(b.block_hash, BlockStatus.CANONICAL)

        self._store.advance_progress(blk.height)

        # 仅在确实发生了规范链切换时落审计（初始建链无切换、不记账）
        if tip is not None:
            self._store.insert_reorg_audit(ReorgAuditRecord(
                id=0,
                created_at_ns=time.time_ns(),
                trigger_block_hash=blk.block_hash,
                fork_height=fork_height,
                depth=depth,
                old_tip_hash=tip.block_hash,
                old_tip_height=tip.height,
                new_tip_hash=blk.block_hash,
                new_tip_height=blk.height,
                replaced_blocks=tuple(sorted(replaced)),
                readopted_blocks=tuple(sorted(readopted)),
            ))
        return depth

    def _try_connect_children(self, parent_hash: str) -> int:
        """父块到位后，尝试连接挂在其下的暂存子树；返回累计最大重组深度。

        先按高度降序收集整棵暂存子树，再从最深块依次判定，使规范链切换由
        该分叉上真正最深的链尖触发（例如 b4 到位时由 b4 而非 b1 触发重组），
        避免只提升到中间块、把更新的块遗留在 STAGED。
        """
        staged: list[Block] = []

        def collect(hash_: str) -> None:
            for child_hash in self._store.children_of(hash_):
                entry = self._store.get_block(child_hash)
                assert entry is not None
                child, status = entry
                if status != BlockStatus.STAGED:
                    continue
                parent_entry = self._store.get_block(child.parent_hash)
                if parent_entry is None \
                        or parent_entry[0].height + 1 != child.height:
                    continue  # 高度与父块不自洽，保持暂存，不静默入库
                staged.append(child)
                collect(child_hash)

        collect(parent_hash)
        max_depth = 0
        for child in sorted(staged, key=lambda b: b.height, reverse=True):
            refreshed = self._store.get_block(child.block_hash)
            if refreshed is not None and refreshed[1] == BlockStatus.STAGED:
                max_depth = max(max_depth, self._connect(child.block_hash))
        return max_depth

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

        parent = self._store.get_block(block.parent_hash)
        if block.height == 0:
            if (self._config.genesis_hash
                    and block.block_hash != self._config.genesis_hash):
                raise MalformedPayloadError(
                    "创世块哈希与配置不符",
                    detail={"block_hash": block.block_hash,
                            "expected": self._config.genesis_hash},
                )
        elif parent is None:
            self._store.upsert_block(block, BlockStatus.STAGED)
            return IngestResult(block.block_hash, BlockStatus.STAGED, applied=True)
        else:
            if parent[0].height + 1 != block.height:
                raise MalformedPayloadError(
                    "区块高度与父块不连续",
                    detail={"block_hash": block.block_hash,
                            "height": block.height,
                            "parent_height": parent[0].height},
                )

        self._store.upsert_block(block, BlockStatus.STAGED)
        depth = self._connect(block.block_hash)
        depth = max(depth, self._try_connect_children(block.block_hash))
        final_status = self._store.get_block(block.block_hash)[1]  # type: ignore[index]
        return IngestResult(block.block_hash, final_status,
                            applied=True, reorg_depth=depth)

    def canonical_chain(self) -> list[Block]:
        """按高度升序返回当前规范链。"""
        return [b for b, _ in sorted(
            self._store.list_blocks(BlockStatus.CANONICAL),
            key=lambda item: item[0].height,
        )]
