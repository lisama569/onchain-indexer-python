"""顺序无关的三态拓扑判定（service 与 health 的同一套口径）。

判定只依赖「最终已落库的区块集合」，与区块的投递顺序、并发度无关：
- CANONICAL：从创世块连通的链中，按「链尖高度最高、同高取链尖哈希
  字典序最小」确定性选出的唯一一条链；
- REORGED：祖先链能逐块连通到创世块、但不在规范链上的块
  （输掉竞争的分叉块：无论历史上曾被换下去，还是连通时即落败）；
- STAGED：祖先链无法连通到创世块的块（缺父块、或与父块高度不连续）。
"""

from __future__ import annotations

from dataclasses import dataclass

from .models import BlockStatus


@dataclass(frozen=True)
class TopologyView:
    """某一区块集合上的确定性拓扑结论。"""

    canonical_hashes: frozenset[str]
    connected_hashes: frozenset[str]
    statuses: dict[str, BlockStatus]
    heights: dict[str, int]
    tip_hash: str | None
    tip_height: int

    def canonical_chain(self) -> tuple[str, ...]:
        """规范链哈希，按高度升序。"""
        return tuple(sorted(self.canonical_hashes,
                            key=lambda h: self.heights[h]))


def classify_blocks(
    blocks: list,
    *,
    genesis_hash: str = "",
) -> TopologyView:
    """对给定区块集合做顺序无关的三态分类。

    blocks 中每项只需暴露 block_hash / parent_hash / height 三个属性
    （Block 与自检用的 BlockSnapshot 均可）。结论只取决于这个集合本身。
    """
    by_hash: dict[str, object] = {b.block_hash: b for b in blocks}

    # 有效亲子关系：父块在集合内，且高度恰好相邻。
    children: dict[str, list[str]] = {}
    for b in blocks:
        if b.height == 0:
            continue
        parent = by_hash.get(b.parent_hash)
        if parent is not None and parent.height + 1 == b.height:
            children.setdefault(parent.block_hash, []).append(b.block_hash)

    # 从创世根出发，沿有效亲子关系可达的块即「已连通到创世块」。
    roots = [b.block_hash for b in blocks
             if b.height == 0
             and (not genesis_hash or b.block_hash == genesis_hash)]
    connected: set[str] = set()
    stack = list(roots)
    while stack:
        h = stack.pop()
        if h in connected:
            continue
        connected.add(h)
        stack.extend(children.get(h, ()))

    # 候选规范链由其链尖唯一确定（每个块的父链唯一）。确定性规则：
    # 链尖高度最高者胜；同高时取链尖哈希字典序最小者，保证无并列、可复现。
    best_tip: str | None = None
    for h in connected:
        if best_tip is None:
            best_tip = h
            continue
        cur = by_hash[h]
        win = by_hash[best_tip]
        if cur.height > win.height or (cur.height == win.height
                                       and cur.block_hash < best_tip):
            best_tip = h

    canonical: set[str] = set()
    tip_height = -1
    if best_tip is not None:
        cur = best_tip
        while True:
            canonical.add(cur)
            blk = by_hash[cur]
            if blk.height == 0:
                break
            cur = blk.parent_hash  # 连通块的父链必在集合内且连续
        tip_height = by_hash[best_tip].height

    statuses: dict[str, BlockStatus] = {}
    for b in blocks:
        if b.block_hash in canonical:
            statuses[b.block_hash] = BlockStatus.CANONICAL
        elif b.block_hash in connected:
            statuses[b.block_hash] = BlockStatus.REORGED
        else:
            statuses[b.block_hash] = BlockStatus.STAGED

    return TopologyView(
        canonical_hashes=frozenset(canonical),
        connected_hashes=frozenset(connected),
        statuses=statuses,
        heights={b.block_hash: b.height for b in blocks},
        tip_hash=best_tip,
        tip_height=tip_height,
    )
