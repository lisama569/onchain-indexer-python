"""只读自检：分项核对索引当前是否自洽，不改动任何数据。

检查项：
1. canonical_continuity  规范链从头到尾连得上、高度连续、根合法；
2. parent_status         每个块的状态与其父链对得上（父存在、高度连续、
                         canonical 的父必 canonical、reorged 的父不能是 staged）；
3. canonical_txs         交易无重复、归属与所在区块一致、无孤儿交易；
4. progress_tip          记录的进度等于当前规范链最高高度。

所有读取都在 store.read_snapshot() 内完成，与并发写互斥，读不到改到一半的状态。
"""

from __future__ import annotations

from .models import BlockStatus, CheckResult, HealthReport
from .store import Store


class HealthChecker:
    """对持久化状态做只读一致性检查，返回可定位问题的分项报告。"""

    def __init__(self, store: Store) -> None:
        self._store = store

    def run(self) -> HealthReport:
        """执行全部自检分项；只读，任何时候都不得写入或修改数据。"""
        with self._store.read_snapshot():
            all_entries = self._store.list_blocks(None)
            by_hash = {b.block_hash: (b, s) for b, s in all_entries}
            canonical = sorted(
                ((b, s) for b, s in all_entries if s == BlockStatus.CANONICAL),
                key=lambda item: item[0].height,
            )
            progress = self._store.get_progress()
            checks = (
                self._check_canonical_continuity(canonical),
                self._check_parent_status(all_entries, by_hash),
                self._check_transactions(by_hash),
                self._check_progress_tip(canonical, progress),
            )
        return HealthReport(ok=all(c.ok for c in checks), checks=checks)

    # ---- 分项 1：规范链连续性 ----
    def _check_canonical_continuity(self, canonical) -> CheckResult:
        issues: list[str] = []
        prev = None
        for block, _ in canonical:
            if prev is None:
                if block.height != 0:
                    issues.append(
                        f"规范链缺少高度 0 的根，第一个块是 "
                        f"{block.block_hash}(height={block.height})")
                elif block.parent_hash not in ("", block.block_hash):
                    issues.append(
                        f"规范链根块 {block.block_hash} 的 parent_hash="
                        f"{block.parent_hash!r} 非空且非自指")
            else:
                if block.parent_hash != prev.block_hash:
                    issues.append(
                        f"规范链断裂: 块 {block.block_hash}(height={block.height}) "
                        f"的 parent_hash={block.parent_hash}，前一块应为 "
                        f"{prev.block_hash}")
                if block.height != prev.height + 1:
                    issues.append(
                        f"规范链高度不连续: {prev.block_hash}"
                        f"(height={prev.height}) -> {block.block_hash}"
                        f"(height={block.height})")
            prev = block
        return CheckResult("canonical_continuity", not issues, tuple(issues))

    # ---- 分项 2：父链与状态一致性 ----
    def _check_parent_status(self, entries, by_hash) -> CheckResult:
        issues: list[str] = []
        for block, status in entries:
            if block.height == 0:
                if block.parent_hash not in ("", block.block_hash):
                    issues.append(
                        f"块 {block.block_hash} 高度为 0 但 parent_hash="
                        f"{block.parent_hash!r}")
                continue
            parent_entry = by_hash.get(block.parent_hash)
            if parent_entry is None:
                if status != BlockStatus.STAGED:
                    issues.append(
                        f"块 {block.block_hash}(status={status.value}) 父块 "
                        f"{block.parent_hash} 缺失，但状态不是 staged")
                continue
            parent, parent_status = parent_entry
            if block.height != parent.height + 1:
                issues.append(
                    f"块 {block.block_hash}(height={block.height}) 与父块 "
                    f"{parent.block_hash}(height={parent.height}) 高度不连续")
            if status == BlockStatus.CANONICAL and \
                    parent_status != BlockStatus.CANONICAL:
                issues.append(
                    f"块 {block.block_hash} 是 canonical，但其父块 "
                    f"{parent.block_hash} 状态为 {parent_status.value}")
            if status == BlockStatus.REORGED and \
                    parent_status == BlockStatus.STAGED:
                issues.append(
                    f"块 {block.block_hash} 是 reorged，但其父块 "
                    f"{parent.block_hash} 仍为 staged")
        return CheckResult("parent_status", not issues, tuple(issues))

    # ---- 分项 3：交易归属与重复（以 txs 表为唯一事实来源）----
    def _check_transactions(self, by_hash) -> CheckResult:
        issues: list[str] = []
        canonical_hashes = {
            h for h, (_, status) in by_hash.items()
            if status == BlockStatus.CANONICAL}
        ownership: dict[str, list[str]] = {}
        for tx in self._store.all_transactions():
            if tx.block_hash not in by_hash:
                issues.append(
                    f"交易 {tx.tx_hash} 归属的块 {tx.block_hash} 不存在"
                    "（孤儿交易）")
                continue
            ownership.setdefault(tx.tx_hash, []).append(tx.block_hash)

        for tx_hash, owners in ownership.items():
            canonical_owners = [o for o in owners if o in canonical_hashes]
            if len(canonical_owners) > 1:
                issues.append(
                    f"规范链上交易 {tx_hash} 同时出现在多个规范块: "
                    f"{canonical_owners}")
            elif len(owners) > 1:
                issues.append(
                    f"交易 {tx_hash} 重复归属到多个块: {owners}")
        return CheckResult("canonical_txs", not issues, tuple(issues))

    # ---- 分项 4：进度与链尖一致 ----
    def _check_progress_tip(self, canonical, progress: int) -> CheckResult:
        issues: list[str] = []
        tip_height = canonical[-1][0].height if canonical else -1
        if progress != tip_height:
            suffix = (f"（链尖 {canonical[-1][0].block_hash}）" if canonical
                      else "（规范链为空，期望 -1）")
            issues.append(
                f"记录的进度 height={progress} 与当前规范链最高高度 "
                f"{tip_height} 不一致{suffix}")
        return CheckResult("progress_tip", not issues, tuple(issues))
