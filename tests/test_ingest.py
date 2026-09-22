"""摄取行为测试：乱序、重复、重组与状态区分。"""

from __future__ import annotations

import logging

from indexer.models import BlockStatus

from .conftest import fork_b, main_chain, make_block

log = logging.getLogger("test")


def _hashes(chain):
    return [b.block_hash for b in chain]


def test_out_of_order_delivery_connects_staged(service):
    """乱序到达：子块先到应暂存，父块补齐后自动接入规范链，不丢数据。"""
    chain = main_chain()
    order = [chain[2], chain[0], chain[3], chain[1]]  # a2, g0, a3, a1
    log.info("输入: 乱序投递 %s", [b.block_hash for b in order])

    r0 = service.ingest_block(order[0])
    assert r0.status == BlockStatus.STAGED and r0.applied
    log.info("判定依据: 父块缺失 -> a2 暂存 (%s)", r0.status)

    service.ingest_block(order[1])  # g0
    service.ingest_block(order[2])  # a3 仍缺 a1 -> 暂存
    assert service._store.get_block("a3")[1] == BlockStatus.STAGED

    service.ingest_block(order[3])  # a1 到位，触发 a2/a3 连接
    canonical = _hashes(service.canonical_chain())
    log.info("判定依据: 父块补齐后规范链应为 g0,a1,a2,a3，实际 %s", canonical)
    assert canonical == ["g0", "a1", "a2", "a3"]
    assert service._store.get_progress() == 3


def test_duplicate_delivery_is_idempotent(service):
    """重复投递：不产生重复记录，applied=False，状态不变。"""
    blk = main_chain()[0]
    r1 = service.ingest_block(blk)
    r2 = service.ingest_block(blk)
    log.info("输入: 同一区块 %s 投递两次", blk.block_hash)
    log.info("判定依据: 第一次 applied=True，第二次 applied=False")
    assert r1.applied and not r2.applied
    assert r2.status == BlockStatus.CANONICAL

    all_blocks = service._store.list_blocks()
    assert len(all_blocks) == 1
    assert len(service._store.transactions_of(blk.block_hash)) == 1
    log.info("判定依据: 存储中区块数=1、交易数=1，无重复记录")


def test_reorg_switches_canonical_and_marks_replaced(service):
    """重组：更长分叉胜出后，被替换区块标记 REORGED，交易归属同步变化。"""
    for b in main_chain():
        service.ingest_block(b)
    fork = fork_b()
    for b in fork[:3]:
        r = service.ingest_block(b)
        assert r.status == BlockStatus.STAGED  # 未超过主链高度，保持暂存
    log.info("输入: 主链 g0..a3（高3），分叉 b1..b4（高4），max_reorg_depth=3")

    r = service.ingest_block(fork[3])  # b4 触发重组，深度 = 3-0 = 3
    log.info("判定依据: b4 摄取触发重组深度 %d（<= 上限 3，允许）", r.reorg_depth)
    assert r.reorg_depth == 3

    canonical = _hashes(service.canonical_chain())
    log.info("判定依据: 规范链应切换为 g0,b1..b4，实际 %s", canonical)
    assert canonical == ["g0", "b1", "b2", "b3", "b4"]

    for h in ("a1", "a2", "a3"):
        blk, status = service._store.get_block(h)
        assert status == BlockStatus.REORGED
        # 被替换区块的交易随区块一并失去规范归属
        txs = service._store.transactions_of(h)
        assert txs and all(t.block_hash == h for t in txs)
    log.info("判定依据: a1..a3 状态=REORGED，其交易仍归属原区块可审计")
    assert service._store.get_progress() == 4


def test_staged_block_kept_when_parent_missing(service):
    """父块缺失的区块保留为 STAGED，可查询区分，不进入规范数据。"""
    orphan = make_block("x9", "missing-parent", 9)
    r = service.ingest_block(orphan)
    log.info("输入: 父块不存在的区块 x9")
    log.info("判定依据: 状态应为 staged 且 applied=True（暂存而非丢弃）")
    assert r.status == BlockStatus.STAGED and r.applied
    assert service.canonical_chain() == []
    staged = service._store.list_blocks(BlockStatus.STAGED)
    assert [b.block_hash for b, _ in staged] == ["x9"]
