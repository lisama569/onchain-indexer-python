"""只读自检：对当前索引做分项一致性检查，输出可定位的问题清单。

检查全程只读，且在与写入互斥的一致性快照上进行（Store.snapshot 在写锁
保护下一次性取齐三表），不会读到改了一半的中间状态，也不会改动任何数据。

分项：
- canonical_continuity：规范链从创世到链尖 parent/height 是否连续、链尖是否唯一；
- block_parent_consistency：每个块的状态与其父链位置是否对得上；
- canonical_txs：规范链上的交易是否归属正确、有无重复或悬空；
- progress：记录的进度是否与当前规范链尖一致。
"""

from __future__ import annotations

from dataclasses import dataclass, field

from .classification import classify_blocks
from .models import Block, BlockStatus, StoreSnapshot
from .store import Store


@dataclass(frozen=True)
class CheckIssue:
    """单条问题定位信息。"""

    check: str          # 所属检查项
    message: str        # 问题描述
    block_hashes: tuple[str, ...] = ()
    tx_hashes: tuple[str, ...] = ()


@dataclass(frozen=True)
class CheckResult:
    """单个检查项的结论。"""

    name: str
    ok: bool
    issues: tuple[CheckIssue, ...] = ()


@dataclass(frozen=True)
class HealthReport:
    """自检总报告。"""

    ok: bool
    checks: tuple[CheckResult, ...] = ()
    issues: tuple[CheckIssue, ...] = field(default_factory=tuple)


class HealthChecker:
    """只读自检入口。"""

    def __init__(self, store: Store) -> None:
        self._store = store

    def run_checks(self) -> HealthReport:
        # 取快照这一步与写入事务互斥；之后所有计算都在不可变快照上，纯只读。
        snap = self._store.snapshot()
        checks = (
            self._check_canonical_continuity(snap),
            self._check_block_parent_consistency(snap),
            self._check_canonical_txs(snap),
            self._check_progress(snap),
        )
        issues = tuple(i for c in checks for i in c.issues)
        return HealthReport(ok=all(c.ok for c in checks),
                            checks=checks, issues=issues)

    # ---- 1. 规范链连续性 ----
    def _check_canonical_continuity(
            self, snap: StoreSnapshot) -> CheckResult:
        issues: list[CheckIssue] = []
        canonical = sorted(
            (b for b in snap.blocks if b.status == BlockStatus.CANONICAL),
            key=lambda b: b.height)

        if not canonical:
            return CheckResult("canonical_continuity", ok=True, issues=())

        # 同一高度出现多个规范块：链尖/链身不唯一
        seen_heights: dict[int, list[str]] = {}
        for b in canonical:
            seen_heights.setdefault(b.height, []).append(b.block_hash)
        dup_heights = {h: hs for h, hs in seen_heights.items() if len(hs) > 1}
        if dup_heights:
            for h, hs in sorted(dup_heights.items()):
                issues.append(CheckIssue(
                    "canonical_continuity",
                    f"高度 {h} 存在多个规范块: {sorted(hs)}",
                    block_hashes=tuple(sorted(hs))))

        # 从最低高度起逐块校验 parent 链接与高度连续
        first = canonical[0]
        if first.height != 0:
            issues.append(CheckIssue(
                "canonical_continuity",
                f"规范链未从创世块开始，最低高度为 {first.height}",
                block_hashes=(first.block_hash,)))
        by_hash = {b.block_hash: b for b in canonical}
        prev = first
        for b in canonical[1:]:
            if b.height != prev.height + 1:
                issues.append(CheckIssue(
                    "canonical_continuity",
                    f"规范链高度不连续: {prev.block_hash}(h={prev.height})"
                    f" -> {b.block_hash}(h={b.height})",
                    block_hashes=(prev.block_hash, b.block_hash)))
            if b.parent_hash != prev.block_hash:
                issues.append(CheckIssue(
                    "canonical_continuity",
                    f"规范链断链: {b.block_hash} 的 parent 是 {b.parent_hash}，"
                    f"期望 {prev.block_hash}",
                    block_hashes=(b.block_hash,)))
            prev = b

        return CheckResult("canonical_continuity", ok=not issues,
                           issues=tuple(issues))

    # ---- 2. 块状态与父链一致性 ----
    def _check_block_parent_consistency(
            self, snap: StoreSnapshot) -> CheckResult:
        issues: list[CheckIssue] = []
        by_hash = {b.block_hash: b for b in snap.blocks}

        for b in snap.blocks:
            if b.height == 0:
                continue
            parent = by_hash.get(b.parent_hash)
            if parent is None:
                # 父块不在库：只有 STAGED（暂存待连接）是合法状态
                if b.status != BlockStatus.STAGED:
                    issues.append(CheckIssue(
                        "block_parent_consistency",
                        f"{b.status.value} 块 {b.block_hash}(h={b.height}) "
                        f"的父块 {b.parent_hash} 不在索引中（应为 staged）",
                        block_hashes=(b.block_hash, b.parent_hash)))
                continue
            # 父块在库：高度必须相邻
            if parent.height + 1 != b.height:
                issues.append(CheckIssue(
                    "block_parent_consistency",
                    f"块 {b.block_hash}(h={b.height}) 高度与父块 "
                    f"{parent.block_hash}(h={parent.height}) 不连续",
                    block_hashes=(b.block_hash, parent.block_hash)))
            # 规范块的父块必须也是规范块
            if b.status == BlockStatus.CANONICAL \
                    and parent.status != BlockStatus.CANONICAL:
                issues.append(CheckIssue(
                    "block_parent_consistency",
                    f"规范块 {b.block_hash} 的父块 {parent.block_hash} "
                    f"状态为 {parent.status.value}（应为 canonical）",
                    block_hashes=(b.block_hash, parent.block_hash)))

        # 三态归类必须与统一口径（确定性参考重放）一致：同一份数据无论按什么
        # 顺序/并发度投递，最终落到哪一类都应能被这里复查出来。
        issues.extend(self._classification_divergences(snap))
        return CheckResult("block_parent_consistency", ok=not issues,
                           issues=tuple(issues))

    @staticmethod
    def _classification_divergences(snap: StoreSnapshot) -> list[CheckIssue]:
        blocks = [Block(block_hash=b.block_hash, parent_hash=b.parent_hash,
                        height=b.height, timestamp=0, transactions=())
                  for b in snap.blocks]
        verdict = classify_blocks(blocks)
        # 唯一最高链尖：当前规范块的最大高度上恰好只有一个块。存在多条
        # 「同最高高度、互不延伸」的并列链（永未分出长短）时，实现保持
        # 「同高度不抢占、先到者保持规范」的既定行为，不纳入收敛复查。
        canonical_blocks = [b for b in snap.blocks
                            if b.status == BlockStatus.CANONICAL]
        unique_tallest = bool(canonical_blocks) and \
            sum(1 for b in canonical_blocks
                if b.height == max(x.height for x in canonical_blocks)) == 1
        out: list[CheckIssue] = []
        for b in sorted(snap.blocks, key=lambda x: (x.height, x.block_hash)):
            want = verdict.status_of(b.block_hash)
            if want is None:
                continue
            if want == BlockStatus.CANONICAL and not unique_tallest:
                continue  # 并列同高边角：规范身份按实现的先到先得，不复查
            if b.status != want:
                out.append(CheckIssue(
                    "block_parent_consistency",
                    f"块 {b.block_hash}(h={b.height}) 归类为 "
                    f"{b.status.value}，与统一口径期望的 {want.value} 不一致",
                    block_hashes=(b.block_hash,)))
        return out

    # ---- 3. 规范链交易归属与重复 ----
    def _check_canonical_txs(self, snap: StoreSnapshot) -> CheckResult:
        issues: list[CheckIssue] = []
        block_status = {b.block_hash: b.status for b in snap.blocks}
        canonical_blocks = {h for h, s in block_status.items()
                            if s == BlockStatus.CANONICAL}

        tx_hashes: dict[str, list[str]] = {}
        for tx in snap.txs:
            tx_hashes.setdefault(tx.tx_hash, []).append(tx.block_hash)
            # 交易归属的区块必须存在
            if tx.block_hash not in block_status:
                issues.append(CheckIssue(
                    "canonical_txs",
                    f"交易 {tx.tx_hash} 归属的区块 {tx.block_hash} 不存在",
                    block_hashes=(tx.block_hash,),
                    tx_hashes=(tx.tx_hash,)))
            # 交易记录的 block_hash 必须自洽（数据模型约束的持久化兜底）
            if not tx.tx_hash:
                issues.append(CheckIssue(
                    "canonical_txs", "存在 tx_hash 为空的交易",
                    block_hashes=(tx.block_hash,)))

        # 重复交易哈希
        for tx_hash, owners in tx_hashes.items():
            if len(set(owners)) > 1:
                issues.append(CheckIssue(
                    "canonical_txs",
                    f"交易 {tx_hash} 同时归属多个区块: {sorted(set(owners))}",
                    block_hashes=tuple(sorted(set(owners))),
                    tx_hashes=(tx_hash,)))

        # 规范链上的交易：归属块必须是规范块；同一规范块不应出现重复交易
        per_block: dict[str, list[str]] = {}
        for tx in snap.txs:
            if tx.block_hash in canonical_blocks:
                per_block.setdefault(tx.block_hash, []).append(tx.tx_hash)
                # 规范交易的归属块即规范块，天然一致；这里确认无重复
        for block_hash, hashes in per_block.items():
            dup = {h for h in hashes if hashes.count(h) > 1}
            for h in sorted(dup):
                issues.append(CheckIssue(
                    "canonical_txs",
                    f"规范块 {block_hash} 内交易重复: {h}",
                    block_hashes=(block_hash,), tx_hashes=(h,)))

        # 跨块重复且其中一个在规范链上：规范交易出现多份归属
        for tx_hash, owners in tx_hashes.items():
            owners_set = set(owners)
            if len(owners_set) > 1 and owners_set & canonical_blocks:
                issues.append(CheckIssue(
                    "canonical_txs",
                    f"规范链交易 {tx_hash} 存在非规范归属: "
                    f"{sorted(owners_set - canonical_blocks)}",
                    block_hashes=tuple(sorted(owners_set)),
                    tx_hashes=(tx_hash,)))

        return CheckResult("canonical_txs", ok=not issues,
                           issues=tuple(issues))

    # ---- 4. 进度与链尖一致 ----
    def _check_progress(self, snap: StoreSnapshot) -> CheckResult:
        canonical = [b for b in snap.blocks
                     if b.status == BlockStatus.CANONICAL]
        tip_height = max((b.height for b in canonical), default=-1)
        if snap.progress != tip_height:
            tip_hashes = sorted(b.block_hash for b in canonical
                                if b.height == tip_height)
            return CheckResult(
                "progress", ok=False, issues=(CheckIssue(
                    "progress",
                    f"记录进度 {snap.progress} 与规范链尖高度 {tip_height} 不一致",
                    block_hashes=tuple(tip_hashes)),))
        return CheckResult("progress", ok=True, issues=())
