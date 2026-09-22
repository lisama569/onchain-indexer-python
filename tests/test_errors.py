"""失败分类测试：畸形数据、状态冲突、重组过深、进度回退。"""

from __future__ import annotations

import logging

import pytest

from indexer.errors import (MalformedPayloadError, ReorgTooDeepError,
                            StateConflictError)
from indexer.models import Block, BlockStatus, Transaction

from .conftest import fork_c, main_chain, make_block

log = logging.getLogger("test")


def test_malformed_payload_rejected(service):
    """畸形负载：负金额、空哈希、区块内交易重复、高度不连续均被拒绝且可区分。"""
    cases = {
        "负金额": make_block("m1", "g0", 1).__class__(
            "m1", "g0", 1, 100,
            (Transaction("t", "m1", "a", "b", -1, 0),)),
        "空区块哈希": make_block("m2", "g0", 1).__class__(
            "", "g0", 1, 100, ()),
        "交易重复": Block(
            "m3", "g0", 1, 100,
            (Transaction("t", "m3", "a", "b", 1, 0),
             Transaction("t", "m3", "a", "b", 2, 1))),
        "交易归属不符": Block(
            "m4", "g0", 1, 100,
            (Transaction("t", "other-block", "a", "b", 1, 0),)),
    }
    service.ingest_block(main_chain()[0])  # g0，供高度检查
    cases["高度不连续"] = make_block("m5", "g0", 5)

    for name, blk in cases.items():
        with pytest.raises(MalformedPayloadError) as exc_info:
            service.ingest_block(blk)
        log.info("输入: %s -> 拒绝原因分类=%s, 详情=%s",
                 name, exc_info.value.category, exc_info.value.detail)
        assert exc_info.value.category == "malformed_payload"
    # 畸形数据不得留下任何记录
    assert service._store.get_block("m1") is None
    assert service._store.get_block("m3") is None
    log.info("判定依据: 畸形块未写入存储，无部分应用")


def test_conflicting_block_content_rejected(service):
    """状态冲突：同哈希不同内容的区块被拒绝，原有记录不被覆盖。"""
    blk = main_chain()[0]
    service.ingest_block(blk)
    tampered = Block(blk.block_hash, blk.parent_hash, blk.height,
                     blk.timestamp + 1, blk.transactions)
    log.info("输入: 已存在的 %s 以不同 timestamp 再次投递", blk.block_hash)
    with pytest.raises(StateConflictError) as exc_info:
        service.ingest_block(tampered)
    log.info("判定依据: 分类=%s，原记录内容保持不变", exc_info.value.category)
    assert exc_info.value.category == "state_conflict"
    stored, _ = service._store.get_block(blk.block_hash)
    assert stored == blk


def test_deep_reorg_rejected_atomically(service):
    """重组过深：明确拒绝，已暂存分叉保持不变，规范链与进度不受影响。"""
    for b in main_chain():  # g0..a3，规范链高 3
        service.ingest_block(b)
    service.ingest_block(make_block("a4", "a3", 4))  # 规范链推进到高 4
    fork = fork_c()  # c1..c6，从 g0 分叉
    for b in fork[:4]:
        service.ingest_block(b)  # c1..c4 高度 <= 4，全部暂存
    log.info("输入: 主链高 4，分叉 c 链推进到 c5，max_reorg_depth=3")

    with pytest.raises(ReorgTooDeepError) as exc_info:
        service.ingest_block(fork[4])  # c5：胜出需深度 4 > 上限 3
    log.info("判定依据: 分类=%s, depth=%s", exc_info.value.category,
             exc_info.value.detail.get("depth"))
    assert exc_info.value.category == "reorg_too_deep"

    assert [b.block_hash for b in service.canonical_chain()] == \
        ["g0", "a1", "a2", "a3", "a4"]
    assert service._store.get_block("c5") is None  # 整体回滚，无部分应用
    assert service._store.get_progress() == 4
    log.info("判定依据: 规范链不变、被拒块未落库、进度未动")


def test_progress_regression_rejected(service):
    """进度回退：写回退进度被拒绝，进度只能单调前进。"""
    store = service._store
    for b in main_chain():
        service.ingest_block(b)
    assert store.get_progress() == 3
    log.info("输入: 当前进度 3，尝试写回 1")
    with pytest.raises(StateConflictError) as exc_info:
        store.advance_progress(1)
    log.info("判定依据: 分类=%s, current=%s requested=%s",
             exc_info.value.category, exc_info.value.detail["current"],
             exc_info.value.detail["requested"])
    assert exc_info.value.category == "state_conflict"
    assert store.get_progress() == 3
    store.advance_progress(3)  # 同值重放允许（幂等）
    assert store.get_progress() == 3


def test_statuses_are_distinguishable(service):
    """三类状态可区分查询：canonical / reorged / staged。"""
    for b in main_chain():
        service.ingest_block(b)
    for b in fork_c()[:4]:
        service.ingest_block(b)  # 触发一次合法重组（深度3），c 链胜出
    orphan = make_block("z9", "nowhere", 9)
    service.ingest_block(orphan)

    store = service._store
    canonical = {b.block_hash for b, _ in store.list_blocks(BlockStatus.CANONICAL)}
    reorged = {b.block_hash for b, _ in store.list_blocks(BlockStatus.REORGED)}
    staged = {b.block_hash for b, _ in store.list_blocks(BlockStatus.STAGED)}
    log.info("判定依据: canonical=%s reorged=%s staged=%s",
             sorted(canonical), sorted(reorged), sorted(staged))
    assert canonical == {"g0", "c1", "c2", "c3", "c4"}
    assert reorged == {"a1", "a2", "a3"}
    assert staged == {"z9"}
    assert not (canonical & reorged) and not (canonical & staged) \
        and not (reorged & staged)
