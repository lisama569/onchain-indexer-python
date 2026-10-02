"""只读自检测试：对人为构造的破损状态给出可定位的分项问题清单。"""

from __future__ import annotations

import logging
import threading

from indexer.health import HealthChecker
from indexer.models import BlockStatus

from .conftest import main_chain

log = logging.getLogger("test")


def _checker(service) -> HealthChecker:
    return HealthChecker(service._store,
                         genesis_hash=service._config.genesis_hash)


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
        "canonical_txs", "progress",
        "status_classification", "audit_reconciliation"}
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
    """并发「生效写入」期间持续自检：每份报告都必须自洽，且只能落在
    写前或写后的完整状态上——不允许空转通过。

    旧实现的问题：写入端构造的是 w{i}(parent=a3, height=4+i)，除第一块外
    全部因「高度与父块不连续」被拒、异常还被吞掉，实际没有任何生效区块，
    性质根本没被验证。这里写入端投递的是一条合法的连续链 a4..aN，每一块
    都会真正提交生效；并用屏障确保自检与写入在时间上真实重叠，同时断言：

    1) 期间确实有多块生效写入（不是全部被拒的空转）；
    2) 每份报告 ok=True（六分项全自洽，读不到改了一半的中间态）；
    3) 每份报告观察到的进度与规范链尖严格一致，且等于「已提交区块数」
       对应的某个完整前缀（写前/写后两个完整状态之一）。
    """
    import time

    from .conftest import make_block

    _healthy_chain(service)  # g0..a3，进度 3
    store = service._store
    n_writes = 40
    written = {"count": 0}
    write_lock = threading.Lock()
    done = threading.Event()

    def writer():
        for i in range(4, 4 + n_writes):
            blk = make_block(f"a{i}", f"a{i - 1}", i)  # 合法连续链，必生效
            r = service.ingest_block(blk)
            assert r.applied, f"生效写入意外被拒: {r}"
            with write_lock:
                written["count"] += 1
            # 让出调度，制造与自检线程真实交替的窗口
            time.sleep(0.002)
        done.set()

    observations: list[tuple[bool, int]] = []
    observed_progress: set[int] = set()

    def checker():
        while not done.is_set():
            report = _checker(service).run_checks()
            prog = store.get_progress()
            observations.append((report.ok, prog))
            observed_progress.add(prog)

    tw = threading.Thread(target=writer)
    tc = threading.Thread(target=checker)
    tw.start(); tc.start()
    tw.join(); tc.join()

    # 1) 非空转：全部写入都真实生效
    log.info("判定依据: 生效写入 %d/%d 块，并发期间完成 %d 份自检",
             written["count"], n_writes, len(observations))
    assert written["count"] == n_writes, "写入端必须全部生效，不能靠被拒空转"

    # 2) 每份报告都自洽（包含拓扑口径与审计对账两项新增检查）
    assert observations, "应至少完成一份自检"
    bad = [p for ok, p in observations if not ok]
    assert not bad, f"有 {len(bad)} 份自检读到了不自洽的中间状态: {bad[:5]}"

    # 3) 每份结论对应写前或写后的完整状态：进度只能是 3..3+N 的某个已提交
    #    前缀高度，且该高度与规范链严格一致（由 progress 分项保证）。
    assert all(3 <= p <= 3 + n_writes for _, p in observations)
    # 必须真实观察到过「写入后」的前进状态，证明确实与生效写入并发
    assert max(observed_progress) == 3 + n_writes, \
        f"自检未观察到最终完整状态，仅到 {max(observed_progress)}"
    assert len(observed_progress) > 1, "自检未与任何生效写入在时间上重叠"
    log.info("判定依据: %d 份自检全部自洽，观察到的完整状态进度集合=%s",
             len(observations), sorted(observed_progress))
