"""规范链切换审计测试：同生共死、重复不记账、重新接纳可追溯。"""

from __future__ import annotations

import logging

from indexer.models import BlockStatus

from .conftest import fork_b, fork_c, main_chain, make_block

log = logging.getLogger("test")


def test_no_audit_on_plain_extension(service):
    """纯延长链（无原规范块被换下）不算切换，不留审计记录。"""
    for b in main_chain():
        service.ingest_block(b)
    events = service._store.list_reorg_events()
    log.info("输入: 顺序摄取 g0..a3（仅延长，无分叉）")
    log.info("判定依据: 审计记录数=%d（期望 0）", len(events))
    assert events == []


def test_audit_record_written_on_switch_and_ordered_desc(service):
    """发生真实切换时留一条记录，字段完整，接口按时间倒序返回。"""
    for b in main_chain():
        service.ingest_block(b)
    for b in fork_b()[:3]:
        service.ingest_block(b)
    r = service.ingest_block(fork_b()[3])  # b4 触发深度 3 切换
    log.info("输入: 主链 a3 被分叉 b4 替换，重组深度=%d", r.reorg_depth)

    events = service._store.list_reorg_events()
    assert len(events) == 1
    e = events[0]
    log.info("判定依据: 记录=%s", e)
    assert e.old_tip_hash == "a3" and e.old_tip_height == 3
    assert e.new_tip_hash == "b4" and e.new_tip_height == 4
    assert e.fork_height == 0 and e.depth == 3
    assert e.replaced_hashes == ("a1", "a2", "a3")
    assert e.adopted_hashes == ("b4", "b3", "b2", "b1")
    assert e.readopted_hashes == ()
    assert e.occurred_at and e.id > 0


def test_failed_switch_leaves_no_audit_and_no_state(service):
    """切换失败（重组过深）时审计记录与状态变更同生共死，整体回滚。"""
    for b in main_chain():
        service.ingest_block(b)
    service.ingest_block(make_block("a4", "a3", 4))  # 规范链推进到高 4
    for b in fork_c()[:4]:
        service.ingest_block(b)  # c1..c4 暂存
    log.info("输入: c5 触发深度 4 重组，超过上限 3，应整体失败")

    from indexer.errors import ReorgTooDeepError
    import pytest
    with pytest.raises(ReorgTooDeepError):
        service.ingest_block(fork_c()[4])

    assert service._store.list_reorg_events() == []
    assert service._store.get_block("c5") is None
    assert [b.block_hash for b in service.canonical_chain()] == \
        ["g0", "a1", "a2", "a3", "a4"]
    assert service._store.get_progress() == 4
    log.info("判定依据: 审计记录=0、c5 未落库、规范链与进度不变")


def test_duplicate_delivery_does_not_double_count_audit(service):
    """同一份数据重复投递：切换只记一次，重复投递不凭空增加记录。"""
    for b in main_chain():
        service.ingest_block(b)
    for b in fork_b():
        service.ingest_block(b)
    first = service._store.list_reorg_events()
    assert len(first) == 1

    # 触发块与整批数据再次投递
    r = service.ingest_block(fork_b()[3])
    assert r.applied is False
    for b in fork_b():
        service.ingest_block(b)
    second = service._store.list_reorg_events()
    log.info("输入: 切换后重复投递 b1..b4，审计记录数 %d -> %d",
             len(first), len(second))
    assert len(second) == 1
    assert second[0].id == first[0].id


def test_readopted_blocks_reflected_in_audit():
    """被换下去的块之后又回到规范链，记录里如实反映 replaced/readopted。"""
    from indexer.config import IndexerConfig
    from indexer.store import Store
    from indexer.service import IngestService

    svc = IngestService(Store(":memory:"),
                        IndexerConfig(max_reorg_depth=4, genesis_hash="g0"))
    for b in main_chain():
        svc.ingest_block(b)          # g0..a3
    for b in fork_b():
        svc.ingest_block(b)          # b1..b4 胜出，a1..a3 被换下
    svc.ingest_block(make_block("a4", "a3", 4))
    svc.ingest_block(make_block("a5", "a4", 5))  # a 链切回，深度 4

    events = svc._store.list_reorg_events()
    log.info("输入: a 链胜出 -> b 链胜出 -> a 链再次胜出，共 %d 条记录",
             len(events))
    assert len(events) == 2  # 倒序：最近一次在前
    latest, previous = events
    assert previous.replaced_hashes == ("a1", "a2", "a3")
    assert previous.readopted_hashes == ()
    assert latest.replaced_hashes == ("b1", "b2", "b3", "b4")
    # a1..a3 曾被换下，本次重新接纳；a4/a5 是首次进入规范链
    assert latest.readopted_hashes == ("a3", "a2", "a1")
    assert set(latest.adopted_hashes) == {"a1", "a2", "a3", "a4", "a5"}
    assert [b.block_hash for b in svc.canonical_chain()] == \
        ["g0", "a1", "a2", "a3", "a4", "a5"]
    for h in ("b1", "b2", "b3", "b4"):
        assert svc._store.get_block(h)[1] == BlockStatus.REORGED
    log.info("判定依据: 最近一次 readopted=%s replaced=%s",
             latest.readopted_hashes, latest.replaced_hashes)
