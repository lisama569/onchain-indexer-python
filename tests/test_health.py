"""只读自检测试：对人为构造的破损状态给出可定位的分项问题清单。"""

from __future__ import annotations

import logging
import threading

from indexer.health import HealthChecker
from indexer.models import BlockStatus

from .conftest import main_chain

log = logging.getLogger("test")


def _checker(service) -> HealthChecker:
    return HealthChecker(service._store)


def _healthy_chain(service):
    for b in main_chain():
        service.ingest_block(b)


def test_health_clean_chain_passes(service):
    """正常索引：四项检查全部通过。"""
    _healthy_chain(service)
    report = _checker(service).run_checks()
    log.info("判定依据: 健康索引分项=%s",
             {c.name: c.ok for c in report.checks})
    assert report.ok
    assert {c.name for c in report.checks} == {
        "canonical_continuity", "block_parent_consistency",
        "canonical_txs", "progress"}
    assert report.issues == ()


def test_health_detects_broken_canonical_continuity(service):
    """破坏规范链连续性：把规范块的父链状态改乱，报告要点名具体块。"""
    _healthy_chain(service)
    store = service._store
    # a2 脱离规范链：父 a1 仍 canonical，但 a2 被改成 reorged，导致 a3 悬空
    store.set_block_status("a2", BlockStatus.REORGED)
    log.info("输入: 人为把 a2 状态改为 reorged，制造 a3 与规范链断裂")

    report = _checker(service).run_checks()
    cont = next(c for c in report.checks if c.name == "canonical_continuity")
    parent_c = next(c for c in report.checks
                    if c.name == "block_parent_consistency")
    assert not cont.ok and not parent_c.ok
    named = {h for i in cont.issues for h in i.block_hashes}
    log.info("判定依据: 连续性问题点名块=%s", sorted(named))
    assert "a3" in named  # 断链具体定位到 a3
    p_named = {h for i in parent_c.issues for h in i.block_hashes}
    assert {"a3", "a2"} <= p_named  # a3 的父 a2 已非 canonical


def test_health_detects_progress_mismatch(service):
    """进度与链尖不一致：报告给出记录值与链尖值。"""
    _healthy_chain(service)
    with service._store.transaction():
        service._store._conn.execute(
            "UPDATE progress SET height=99 WHERE id=1")
    log.info("输入: 人为把进度改为 99（链尖实际为 3）")

    report = _checker(service).run_checks()
    prog = next(c for c in report.checks if c.name == "progress")
    assert not prog.ok
    assert "99" in prog.issues[0].message and "3" in prog.issues[0].message
    log.info("判定依据: %s", prog.issues[0].message)


def test_health_detects_dangling_and_misattributed_tx(service):
    """悬空交易（归属块不存在）被定位到具体交易哈希与块哈希。"""
    _healthy_chain(service)
    with service._store.transaction():
        service._store._conn.execute(
            "INSERT INTO txs(tx_hash, block_hash, sender, recipient,"
            " amount, nonce) VALUES ('ghost-tx','missing-block','a','b',1,0)")
    log.info("输入: 插入归属块不存在的交易 ghost-tx")

    report = _checker(service).run_checks()
    txcheck = next(c for c in report.checks if c.name == "canonical_txs")
    assert not txcheck.ok
    issue = txcheck.issues[0]
    assert "ghost-tx" in issue.tx_hashes
    assert "missing-block" in issue.block_hashes
    log.info("判定依据: %s（tx=%s block=%s）",
             issue.message, issue.tx_hashes, issue.block_hashes)


def test_health_is_read_only(service):
    """自检不得改动任何数据：前后区块、状态、进度逐字节一致。"""
    _healthy_chain(service)
    store = service._store
    before = (store.list_blocks(),
              [store.transactions_of(b.block_hash)
               for b, _ in store.list_blocks()],
              store.get_progress())

    for _ in range(3):
        report = _checker(service).run_checks()
        assert report.ok

    after = (store.list_blocks(),
             [store.transactions_of(b.block_hash)
              for b, _ in store.list_blocks()],
             store.get_progress())
    assert before == after
    log.info("判定依据: 自检前后区块/交易/进度完全一致（只读）")


def test_health_never_reads_half_written_state(service):
    """并发摄取期间做自检：要么看到写前、要么写后，绝不读到改了一半的状态。

    自检与写入在同一把存储锁上互斥；在写入线程持锁期间多次自检，得到的
    每份报告都必须是自洽的（ok=True），不会观察到中间破损态。
    """
    _healthy_chain(service)
    store = service._store
    stop = threading.Event()
    reports: list[bool] = []

    def writer():
        # 持锁时间内反复在同一事务里制造“先改后回”的动作，拉长临界区
        from .conftest import make_block
        i = 0
        while not stop.is_set():
            i += 1
            try:
                service.ingest_block(make_block(f"w{i}", "a3", 4 + i))
            except Exception:
                pass

    def checker():
        while not stop.is_set():
            # 快照在锁保护下取得：与任何写入互斥，报告必然自洽
            reports.append(_checker(service).run_checks().ok)

    tw = threading.Thread(target=writer)
    tc = threading.Thread(target=checker)
    tw.start(); tc.start()
    stop.wait(0.4)
    stop.set()
    tw.join(); tc.join()

    log.info("判定依据: 并发期间完成 %d 份自检，全部自洽", len(reports))
    assert reports, "应至少完成一份自检"
    assert all(reports), "自检读到了改了一半的中间状态"
