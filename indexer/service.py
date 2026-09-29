"""摄取与规范链维护的核心服务。

规则概述：
- 幂等：同一区块重复投递不产生重复记录，返回 applied=False。
- 暂存：父块缺失的区块以 STAGED 保留；已连通但暂未胜出的分叉块同样为 STAGED。
- 规范链：始终选择已连通链中高度最高者；触发切换时同步更新被替换
  区块及其交易的归属（区块状态 CANONICAL -> REORGED，交易随区块派生）。
- 重组深度超过 max_reorg_depth 时整体拒绝（事务回滚，不产生部分写入）。
- 进度只随规范链尖端单调前进。
"""

from __future__ import annotations

from datetime import datetime, timezone

from .config import IndexerConfig
from .errors import (MalformedPayloadError, ReorgTooDeepError,
                     StateConflictError)
from .models import (BatchIngestResult, Block, BlockStatus, IngestResult,
                     ReorgEvent)
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
        """已连通区块的规范链判定；返回本次触发的重组深度。"""
        entry = self._store.get_block(block_hash)
        assert entry is not None
        blk, _ = entry
        tip = self._canonical_tip()
        if tip is not None and blk.height <= tip.height:
            return 0  # 未胜出，保持 STAGED

        # 回溯到最近的规范祖先，确定分叉点；若链未连通到创世块则保持暂存
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
        if tip is not None and depth > self._config.max_reorg_depth:
            raise ReorgTooDeepError(
                f"重组深度 {depth} 超过上限 {self._config.max_reorg_depth}，拒绝摄取",
                detail={"block_hash": blk.block_hash, "depth": depth,
                        "max_reorg_depth": self._config.max_reorg_depth},
            )

        # 切换前先快照新链各块的旧状态，用于区分“首次接纳”与“重新接纳”
        adopted: list[str] = []
        readopted: list[str] = []
        for b in chain:
            old_entry = self._store.get_block(b.block_hash)
            assert old_entry is not None
            if old_entry[1] == BlockStatus.REORGED:
                readopted.append(b.block_hash)
            adopted.append(b.block_hash)

        # 旧规范链分叉点以上 -> REORGED（其交易随区块一并失去规范归属）
        replaced: list[str] = []
        if tip is not None:
            for h in range(fork_height + 1, tip.height + 1):
                for old in self._store.blocks_at_height(h, BlockStatus.CANONICAL):
                    self._store.set_block_status(old.block_hash, BlockStatus.REORGED)
                    replaced.append(old.block_hash)
        # 新链 -> CANONICAL
        for b in chain:
            self._store.set_block_status(b.block_hash, BlockStatus.CANONICAL)
        self._store.advance_progress(blk.height)

        # 只有真实“切换”（有原规范块被换下）才记审计；纯延长链不算切换。
        # 与上面的状态翻转处在同一事务内：任一步失败本记录随状态一起回滚。
        if tip is not None and replaced:
            self._store.insert_reorg_event(ReorgEvent(
                id=0,
                occurred_at=datetime.now(timezone.utc).isoformat(),
                depth=depth,
                old_tip_hash=tip.block_hash,
                old_tip_height=tip.height,
                new_tip_hash=blk.block_hash,
                new_tip_height=blk.height,
                fork_height=fork_height,
                replaced_hashes=tuple(replaced),
                adopted_hashes=tuple(adopted),
                readopted_hashes=tuple(readopted),
            ))
        return depth

    def _try_connect_children(self, parent_hash: str) -> int:
        """父块到位后，递归尝试连接暂存子块；返回累计最大重组深度。"""
        max_depth = 0
        for child_hash in self._store.children_of(parent_hash):
            entry = self._store.get_block(child_hash)
            assert entry is not None
            child, status = entry
            if status != BlockStatus.STAGED:
                continue
            parent_entry = self._store.get_block(child.parent_hash)
            assert parent_entry is not None
            if parent_entry[0].height + 1 != child.height:
                continue  # 高度与父块不自洽，保持暂存，不静默入库
            max_depth = max(max_depth, self._connect(child_hash))
            max_depth = max(max_depth, self._try_connect_children(child_hash))
        return max_depth

    # ---- 对外入口 ----
    def ingest_block(self, block: Block) -> IngestResult:
        """摄取单个区块：幂等、可暂存、可触发重组；失败整体回滚。"""
        self._validate(block)
        with self._store.transaction(timeout=self._config.lock_timeout):
            return self._ingest_block_unlocked(block)

    def _ingest_block_unlocked(self, block: Block) -> IngestResult:
        """调用方须已持有写事务（顶层事务或批次内的 SAVEPOINT）。"""
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
            # 父块缺失：暂存，不丢数据也不入库为规范数据
            self._store.upsert_block(block, BlockStatus.STAGED)
            return IngestResult(block.block_hash, BlockStatus.STAGED,
                                applied=True)
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

    def ingest_blocks_atomic(self, blocks: list[Block]) -> BatchIngestResult:
        """整批原子摄取。

        - 整批在一个顶层事务内逐块处理（逐块复用幂等/暂存/重组逻辑）；
        - 任一块因畸形/内容冲突/重组过深被拒，异常传出顶层事务，整批回滚，
          不会出现前半批已入库、后半批缺失的部分应用；
        - 批内重复投递与单块语义一致：幂等，applied=False，不算失败；
        - 等写锁超过 ``config.lock_timeout`` 秒抛 LockBusyError（可重试）。
        """
        # 先做不涉及存储的廉价校验，避免无意义地占用写锁
        for block in blocks:
            self._validate(block)

        results: list[IngestResult] = []
        accepted = 0
        duplicates = 0
        with self._store.transaction(timeout=self._config.lock_timeout):
            for block in blocks:
                result = self._ingest_block_unlocked(block)
                results.append(result)
                if result.applied:
                    accepted += 1
                else:
                    duplicates += 1
        return BatchIngestResult(accepted=accepted, duplicates=duplicates,
                                 results=tuple(results))
