"""规范链切换审计测试：与状态切换同事务、失败回滚、幂等不记账、readopted。"""

from __future__ import annotations

import logging

import pytest

from indexer.errors import ReorgTooDeepError
from indexer.models import BlockStatus

from .conftest import fork_b, fork_c, main_chain, make_block

log = logging.getLogger("test")


def _audits(service):
    return service._store.list_reorg_audits(limit=500)


def test_audit_written_on_canonical_switch(service):
    """发生真正的规范链切换时，落一条可倒序查询的审计，字段如实。"""
    for b in main_chain():
        service.ingest_block(b)
    log.info("输入: 主链 g0..a3，随后投递分叉 b1..b4（更深，深度3）")
    for b in fork_b():
        service.ingest_block(b)

    audits = _audits(service)
    assert len(audits) == 1
    rec = audits[0]
    log.info("判定依据: 审计=%s", rec)
    assert rec.trigger_block_hash == "b4"
    assert rec.fork_height == 0 and rec.depth == 3
    assert (rec.old_tip_hash, rec.old_tip_height) == ("a3", 3)
    assert (rec.new_tip_hash, rec.new_tip_height) == ("b4", 4)
    assert list(rec.replaced_blocks) == ["a1", "a2", "a3"]
    assert rec.readopted_blocks == ()


def test_no_audit_on_plain_extension_or_initial_build(service):
    """初始建链与规范链正常延伸都不构成「切换」，不得凭空记账。"""
    for b in main_chain():
        service.ingest_block(b)
    service.ingest_block(make_block("a4", "a3", 4))
    log.info("输入: 顺序建链 g0..a3 并延伸 a4，期间无分叉胜出")
    assert _audits(service) == []
    log.info("判定依据: 审计条数=0（没有发生规范链切换）")


def test_audit_rolled_back_when_switch_fails(service):
    """切换没成功（重组过深）整体回滚：状态、进度、审计记录都不得留下。"""
    for b in main_chain():
        service.ingest_block(b)
    service.ingest_block(make_block("a4", "a3", 4))  # 规范链高 4
    for b in fork_c()[:4]:
        service.ingest_block(b)                    # c1..c4 暂存
    assert _audits(service) == []
    log.info("输入: 主链高4，投递 c5 触发深度4 > 上限3 的切换，应失败")

    with pytest.raises(ReorgTooDeepError):
        service.ingest_block(fork_c()[4])

    # 状态切换与审计同生共死：切换回滚后不得有任何审计
    assert _audits(service) == []
    # 被拒块不落库，规范链与进度不变
    assert service._store.get_block("c5") is None
    assert [b.block_hash for b in service.canonical_chain()] == \
        ["g0", "a1", "a2", "a3", "a4"]
    assert service._store.get_progress() == 4
    log.info("判定依据: 审计=0、c5 未落库、规范链与进度保持切换前")


def test_audit_rolled_back_when_commit_side_fails(service, monkeypatch):
    """状态已翻转、审计写入这一步失败：审计与状态一起回滚，不留记录。"""
    for b in main_chain():
        service.ingest_block(b)
    for b in fork_b()[:3]:
        service.ingest_block(b)  # b1..b3 暂存，b4 到位才触发深度3切换

    store = service._store
    calls = {"n": 0}
    real_insert = store.insert_reorg_audit

    def fail_insert(record):
        calls["n"] += 1
        raise RuntimeError("模拟审计写入失败")

    # 在 b4 触发切换、状态已翻转后，让审计落库这一步失败
    monkeypatch.setattr(store, "insert_reorg_audit", fail_insert)
    log.info("输入: b1..b3 暂存，b4 触发切换，但审计写入步骤被注入失败")
    with pytest.raises(RuntimeError):
        service.ingest_block(fork_b()[3])
    monkeypatch.setattr(store, "insert_reorg_audit", real_insert)

    assert _audits(service) == []
    # 规范链也回到切换前，没有改了一半的中间状态
    assert [b.block_hash for b in service.canonical_chain()] == \
        ["g0", "a1", "a2", "a3"]
    assert service._store.get_block("a3")[1] == BlockStatus.CANONICAL
    log.info("判定依据: 注入失败后审计=0、规范链仍为 a 链（事务整体回滚）")
    assert calls["n"] == 1


def test_duplicate_delivery_does_not_add_audit(service):
    """同一份数据重复投递不触发新切换、不凭空多记审计。"""
    for b in main_chain():
        service.ingest_block(b)
    for b in fork_b():
        service.ingest_block(b)
    assert len(_audits(service)) == 1

    # 触发块 b4 与整批数据重复投递
    for _ in range(3):
        r = service.ingest_block(fork_b()[3])
        assert r.applied is False
    for b in main_chain() + fork_b():
        service.ingest_block(b)
    log.info("输入: 切换后把同一数据集重复投递多轮")
    assert len(_audits(service)) == 1
    log.info("判定依据: 审计始终只有 1 条，重复投递幂等不记账")


def test_readopted_blocks_recorded_when_fork_returns(service):
    """被换下去的块之后随分叉反超重新回到规范链，readopted 如实反映。

    构造：a 链先规范，b 链反超把 a1..a3 换下去；此时把触发块 a5 作为
    「父块缺失」的孤立块先暂存，再投 a4（连通即落败 REORGED，随后因其
    让 a5 连通且高度反超而立即触发 a 链回归）。这样 readopted 同时覆盖：
    曾被换下去的旧块 a1..a3、连通即落败又随链回归的 a4。
    """
    # 放宽上限以便演示 a 链回归需要的深度
    service._config = type(service._config)(
        max_reorg_depth=10, genesis_hash="g0")

    for b in main_chain():  # a 链先规范
        service.ingest_block(b)
    for b in fork_b():     # b 链更高，a1..a3 被换下去
        service.ingest_block(b)
    # 触发块 a5 先到：父 a4 尚不在库 -> STAGED
    service.ingest_block(make_block("a5", "a4", 5))
    assert service._store.get_block("a5")[1] == BlockStatus.STAGED
    log.info("输入: b 链胜出后，a5 因父缺失暂存；再投 a4 连通并使 a5 反超")

    # a4 先以 REORGED 落库（连通但高度不超 b4），随后重算发现 a5 已连通、
    # 高 5 > 4，由链尖 a5 触发 a 链回归（深度4）
    service.ingest_block(make_block("a4", "a3", 4))

    audits = _audits(service)
    # 两次切换：a->b，b->a
    latest = audits[0]  # 倒序：最新在前
    log.info("判定依据: 最近一次审计 trigger=%s replaced=%s readopted=%s",
             latest.trigger_block_hash, latest.replaced_blocks,
             latest.readopted_blocks)
    assert latest.trigger_block_hash == "a5"
    assert latest.old_tip_hash == "b4" and latest.new_tip_hash == "a5"
    assert set(latest.replaced_blocks) == {"b1", "b2", "b3", "b4"}
    # a1..a3 是曾被换下去的回归旧块；a4 先连通即落败、随即随链回归：
    # 本次都重新回到规范链
    assert set(latest.readopted_blocks) == {"a1", "a2", "a3", "a4"}
    assert "a5" not in latest.readopted_blocks  # 触发块本身不计入
    assert [b.block_hash for b in service.canonical_chain()] == \
        ["g0", "a1", "a2", "a3", "a4", "a5"]


def test_audits_returned_newest_first(service):
    """审计列表按时间倒序：新切换在前，可用 offset 翻页。"""
    service._config = type(service._config)(
        max_reorg_depth=10, genesis_hash="g0")
    for b in main_chain():
        service.ingest_block(b)
    for b in fork_b():  # 切换 1：a->b
        service.ingest_block(b)
    # a5 先作为父缺失块暂存，再投 a4：a4 连通使 a5 反超，切换 2 的链尖为 a5
    # （若直接先投 a4，同高平局按链尖哈希 a4<b4 会在 a4 处先切换，
    #  这本身也是确定结论；这里采用暂存顺序以演示链尖 a5 的记录形态）
    service.ingest_block(make_block("a5", "a4", 5))
    service.ingest_block(make_block("a4", "a3", 4))

    audits = _audits(service)
    assert [a.new_tip_hash for a in audits] == ["a5", "b4"]
    assert audits[0].created_at_ns >= audits[1].created_at_ns
    page = service._store.list_reorg_audits(limit=1, offset=1)
    assert [a.new_tip_hash for a in page] == ["b4"]
    log.info("判定依据: 倒序 [a5,b4]，offset=1 可取到较早的 b4")
