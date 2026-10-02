"""三态分类的唯一口径：无论投递顺序/并发度，对同一份区块图给出同一套结论。

判定口径（对「当前已知区块图」做确定性参考重放，参考顺序与实际投递顺序无关）：
- 只考虑沿 parent 链连通到高度 0 根块、且高度逐块连续的区块（未连通的块
  与高度不连续的块不参与竞争）；
- 按 (height, block_hash) 升序依次模拟规范链竞争，沿用实现里的规则：严格
  更高的链尖胜出并翻转；同高度并列时，参考重放里让**哈希较大者**先领先
  （这样它若随后被另一条严格延伸的链超过，也会如实落到 REORGED，与按
  到达顺序真实跑出来的「先规范后被换下」结论一致）；
- canonical：参考重放结束时位于规范链上的块；
- reorged：在参考重放过程中「曾经位于规范链上」、但最终规范链上没有它
  （即曾在某个高度上领先、后被严格更长的链反超）；
- staged：从未进入规范链（父块缺失未连通，或虽已连通但从未领先的分叉块）。

口径特性：随着新区块到达，某块的「曾经领先」判定只会从 False 变 True，
不会反向（严格更长的链反超只会让旧领先者加入 REORGED；同高度并列时
哈希较大者在参考重放中先领先、若对方随后严格延伸则它同样转为 REORGED），
因此 STAGED -> REORGED 的收敛是单调的，对乱序/并发投递顺序不敏感。

边角：最终图上若存在多条「同最高高度、互不延伸」的并列链（永未分出长短），
没有唯一胜出者：实现保持「同高度不抢占、先到者保持规范」的既有行为（后到
的并列块保持 STAGED），参考重放虽会按哈希较大者给出一个参考领先者，但这
种无唯一最高链的情形不做补判收敛、自检也不复查其规范身份，亦不产生审计。
"""

from __future__ import annotations

from dataclasses import dataclass

from .models import Block, BlockStatus


@dataclass(frozen=True)
class Classification:
    """对当前区块图做参考重放后，每个块哈希的三态归类。"""

    canonical: frozenset[str]
    reorged: frozenset[str]      # 曾在参考重放中领先（上过规范链）、最终落败
    staged: frozenset[str]       # 从未连通或从未领先

    def status_of(self, block_hash: str) -> BlockStatus | None:
        if block_hash in self.canonical:
            return BlockStatus.CANONICAL
        if block_hash in self.reorged:
            return BlockStatus.REORGED
        if block_hash in self.staged:
            return BlockStatus.STAGED
        return None


def _connected(blocks_by_hash: dict[str, Block], block: Block) -> bool:
    """沿 parent 链是否连通到高度 0 的根块，且高度逐块连续。"""
    cur = block
    while cur.height > 0:
        parent = blocks_by_hash.get(cur.parent_hash)
        if parent is None or parent.height + 1 != cur.height:
            return False
        cur = parent
    return True


def classify_blocks(blocks: list[Block]) -> Classification:
    """对给定区块集合做确定性参考重放，返回与投递顺序无关的三态归类。"""
    by_hash = {b.block_hash: b for b in blocks}
    connected = [b for b in blocks if _connected(by_hash, b)]
    # 参考投递顺序：先到先得只在「同高度并列」时起作用，因此按
    # (height, block_hash) 升序即完整确定参考重放，与真实投递顺序无关。
    order = sorted(connected, key=lambda b: (b.height, b.block_hash))

    status: dict[str, BlockStatus] = {}
    ever_canonical: set[str] = set()
    tip: Block | None = None

    def mark(blks: list[Block], new_status: BlockStatus) -> None:
        for b in blks:
            status[b.block_hash] = new_status
            if new_status == BlockStatus.CANONICAL:
                ever_canonical.add(b.block_hash)

    for blk in order:
        # 回溯到最近的规范祖先，确定分叉点与本次要提升的整条链
        chain: list[Block] = []
        cur = blk
        fork_height = -1
        while True:
            chain.append(cur)
            parent = by_hash.get(cur.parent_hash) if cur.height > 0 else None
            if parent is None:
                break  # 高度 0 根块（与实现里 _connect 的终止条件一致）
            if status[parent.block_hash] == BlockStatus.CANONICAL:
                fork_height = parent.height
                break
            cur = parent

        if tip is None:
            # 初始建链
            mark(chain, BlockStatus.CANONICAL)
            tip = blk
            continue

        if blk.height > tip.height:
            # 严格更长的链尖：正常延伸或深度反超
            depth = tip.height - fork_height
            if depth == 0:
                mark(chain, BlockStatus.CANONICAL)
            else:
                replaced = [h for h, s in status.items()
                            if s == BlockStatus.CANONICAL
                            and by_hash[h].height > fork_height]
                for h in replaced:
                    status[h] = BlockStatus.REORGED
                mark(chain, BlockStatus.CANONICAL)
            tip = blk
        elif blk.height == tip.height:
            # 同高度并列：参考口径让哈希较大者先领先（fork_height 以上的
            # 当前规范块换下去）；哈希较小者暂存。若哈希较小的那条之后
            # 严格延伸，它会在后续高度上把领先者再换为 REORGED。
            if blk.block_hash > tip.block_hash:
                replaced = [h for h, s in status.items()
                            if s == BlockStatus.CANONICAL
                            and by_hash[h].height > fork_height]
                for h in replaced:
                    status[h] = BlockStatus.REORGED
                mark(chain, BlockStatus.CANONICAL)
                tip = blk
            else:
                status[blk.block_hash] = BlockStatus.STAGED
        else:
            # 低于当前链尖：保持暂存
            status[blk.block_hash] = BlockStatus.STAGED

    canonical = frozenset(h for h, s in status.items()
                          if s == BlockStatus.CANONICAL)
    reorged = frozenset(ever_canonical - canonical)
    staged = frozenset(by_hash.keys() - ever_canonical)
    return Classification(canonical=canonical, reorged=reorged, staged=staged)


def settle_to_reference(
    blocks: list[Block],
    current_status: dict[str, BlockStatus],
    set_status,
) -> tuple[str, ...]:
    """把实际存储状态向参考口径单调收敛，返回本次被补判的块哈希。

    只做单调的 STAGED -> REORGED 补判（某分叉在参考重放中曾领先，但它在
    实际投递顺序里从未登上规范链——例如更长胜出链先到、落败分叉随后才连
    通）。不做任何「把规范块换下来」的动作：真实发生过的切换由状态机即
    时处理并写审计，规范身份因此保持为实现里「同高度不抢占」的既定语义。
    """
    verdict = classify_blocks(blocks)
    settled: list[str] = []
    for h in sorted(verdict.reorged):
        if current_status.get(h) == BlockStatus.STAGED:
            set_status(h, BlockStatus.REORGED)
            settled.append(h)
    return tuple(settled)
