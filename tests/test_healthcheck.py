"""只读自检测试：健康状态全绿，人为破损给出可定位的问题清单且不改数据。"""

from __future__ import annotations

import logging

from indexer.healthcheck import HealthChecker
from indexer.models import BlockStatus

from .conftest import fork_b, main_chain

log = logging.getLogger("test")


def _healthy_service(service):
    for b in main_chain():
        service.ingest_block(b)
    for b in fork_b():
        service.ingest_block(b)  # 留一组 reorged 块一起检查
    return service


def test_healthy_index_passes_all_checks(service):
    """正常摄取（含一次重组）后，所有分项都应通过。"""
    _healthy_service(service)
    report = HealthChecker(service._store).run()
    log.info("输入: 健康索引（canonical + reorged + 交易），结论 ok=%s", report.ok)
    assert report.ok
    assert {c.name for c in report.checks} == {
        "canonical_continuity", "parent_status",
        "canonical_txs", "progress_tip"}
    assert all(c.ok and c.issues == () for c in report.checks)


def test_check_detects_broken_canonical_continuity(service):
    """人为把规范块的 parent_hash 改断，连续性分项要点名具体块。"""
    _healthy_service(service)
    store = service._store
    with store.transaction():
        store._conn.execute(
            "UPDATE blocks SET parent_hash='ghost' WHERE block_hash='b3'")
    report = HealthChecker(store).run()
    by_name = {c.name: c for c in report.checks}
    log.info("输入: 篡改 b3 的 parent_hash=ghost，issues=%s",
             by_name["canonical_continuity"].issues)
    assert not report.ok
    assert not by_name["canonical_continuity"].ok
    joined = " ".join(by_name["canonical_continuity"].issues)
    assert "b3" in joined and "ghost" in joined
    # 数据未被自检改动
    assert store.get_block("b3")[0].parent_hash == "ghost"


def test_check_detects_parent_status_mismatch(service):
    """canonical 块的父块被改成 staged，父链分项要定位到父子两块。"""
    _healthy_service(service)
    store = service._store
    with store.transaction():
        store.set_block_status("b3", BlockStatus.STAGED)
    report = HealthChecker(store).run()
    by_name = {c.name: c for c in report.checks}
    log.info("输入: b3 强制 staged（其父 b4 仍 canonical），issues=%s",
             by_name["parent_status"].issues)
    assert not by_name["parent_status"].ok
    joined = " ".join(by_name["parent_status"].issues)
    assert "b4" in joined and "b3" in joined
    assert not by_name["canonical_continuity"].ok  # 连续性也应同时报出


def test_check_detects_orphan_transaction(service):
    """交易归属被改到不存在的块，canonical_txs 分项要点名该交易。"""
    _healthy_service(service)
    store = service._store
    with store.transaction():
        store._conn.execute(
            "UPDATE txs SET block_hash='ghost-block' WHERE tx_hash='tx-b2-0'")
    report = HealthChecker(store).run()
    by_name = {c.name: c for c in report.checks}
    log.info("输入: 交易 tx-b2-0 归属改到 ghost-block，issues=%s",
             by_name["canonical_txs"].issues)
    assert not by_name["canonical_txs"].ok
    joined = " ".join(by_name["canonical_txs"].issues)
    assert "tx-b2-0" in joined and "ghost-block" in joined


def test_check_detects_duplicate_canonical_transaction(service):
    """同笔交易哈希被塞进两个规范块，自检要列出冲突的两笔。

    正常 schema 下 tx_hash 主键会阻止这种破损；测试中临时把 txs 表重建为
    无主键版本，模拟存储层被外部破坏后自检能否定位。
    """
    _healthy_service(service)
    store = service._store
    with store.transaction():
        # 先把原交易复制到临时表，再把 txs 重建为无主键版本并回填
        store._conn.execute(
            "CREATE TEMP TABLE txs_backup AS SELECT * FROM txs")
        store._conn.execute("DROP TABLE txs")
        store._conn.execute(
            "CREATE TABLE txs (tx_hash TEXT, block_hash TEXT NOT NULL,"
            " sender TEXT NOT NULL, recipient TEXT NOT NULL,"
            " amount INTEGER NOT NULL, nonce INTEGER NOT NULL)")
        store._conn.execute("INSERT INTO txs SELECT * FROM txs_backup")
        # 同一个 tx_hash 插到两个规范块下，制造归属重复的破损状态
        store._conn.execute(
            "INSERT INTO txs VALUES('dup-tx', 'b2', 'x', 'y', 1, 0)")
        store._conn.execute(
            "INSERT INTO txs VALUES('dup-tx', 'b4', 'x', 'y', 1, 0)")
    report = HealthChecker(store).run()
    by_name = {c.name: c for c in report.checks}
    log.info("输入: dup-tx 同时挂在规范块 b2 与 b4，issues=%s",
             by_name["canonical_txs"].issues)
    dup_issues = [i for i in by_name["canonical_txs"].issues
                  if "dup-tx" in i]
    assert len(dup_issues) == 1
    assert "b2" in dup_issues[0] and "b4" in dup_issues[0]
    # 原有交易未被误报
    assert not [i for i in by_name["canonical_txs"].issues if "孤儿" in i]


def test_check_detects_progress_tip_mismatch(service):
    """进度与规范链最高高度不一致，progress_tip 分项要给出两个数字。"""
    _healthy_service(service)
    store = service._store
    with store.transaction():
        store._conn.execute("UPDATE progress SET height=42")
    report = HealthChecker(store).run()
    by_name = {c.name: c for c in report.checks}
    log.info("输入: 进度被改成 42（链尖高 4），issues=%s",
             by_name["progress_tip"].issues)
    assert not by_name["progress_tip"].ok
    joined = " ".join(by_name["progress_tip"].issues)
    assert "42" in joined and "4" in joined


def test_health_check_is_read_only(service):
    """自检前后存储内容逐字节一致（区块、交易、进度、审计）。"""
    _healthy_service(service)
    store = service._store
    def dump():
        blocks = sorted(
            (r["block_hash"], r["status"], r["parent_hash"], r["height"])
            for r in store._conn.execute("SELECT * FROM blocks"))
        txs = sorted(r["tx_hash"] for r in store._conn.execute("SELECT tx_hash FROM txs"))
        progress = store.get_progress()
        return blocks, txs, progress

    before = dump()
    for _ in range(3):
        report = HealthChecker(store).run()
        assert isinstance(report.ok, bool)
    after = dump()
    log.info("输入: 连续 3 次只读自检")
    log.info("判定依据: 前后区块/交易/进度快照完全一致")
    assert before == after


def test_health_check_never_sees_half_applied_state(service):
    """并发摄取期间反复自检：每次看到的都是已提交的自洽状态，从不报错。"""
    import threading
    from concurrent.futures import ThreadPoolExecutor

    chain = main_chain() + fork_b()
    deliveries = chain * 3
    rng = __import__("random").Random(7)
    rng.shuffle(deliveries)

    stop = threading.Event()
    violations: list[list[str]] = []

    def checker_loop():
        hc = HealthChecker(service._store)
        while not stop.is_set():
            report = hc.run()
            if not report.ok:
                violations.append([
                    f"{c.name}: {list(c.issues)}"
                    for c in report.checks if not c.ok])

    with ThreadPoolExecutor(max_workers=3) as pool:
        checker_future = pool.submit(checker_loop)
        list(pool.map(service.ingest_block, deliveries))
        stop.set()
        checker_future.result()

    log.info("输入: 摄取线程 + 并发自检线程同时运行")
    log.info("判定依据: 自检发现不自洽状态的次数=%d", len(violations))
    assert violations == []
    # 最终状态确定：b 链胜出
    assert [b.block_hash for b in service.canonical_chain()] == \
        ["g0", "b1", "b2", "b3", "b4"]
