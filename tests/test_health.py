"""只读自检测试：对人为构造的破损状态给出可定位的分项问题清单。"""

from __future__ import annotations

import logging
import threading
import time

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
    """并发摄取「真正生效」的区块期间自检：每份快照只能是某个已提交状态。

    旧版用例的写入端拼的是 w1(h=5, 父 a3 在 h=3，高度跳变)、w2(h=6,...)
    等区块，全部被判畸形拒绝且异常被吞，实际上没有任何生效写入，性质从未
    被验证。这里写入端从 a3 开始**逐块连续延伸**（h=4,5,...），每块都真正
    提交并推进规范链与进度，并断言：

    - 写入端确实提交了多块（applied 计数显著增长，杜绝空转通过）；
    - 自检确实在写入进行中拿到多份快照；
    - 每份快照自检自洽（ok=True），且其进度要么是「写前」、要么是某次提交
      「写后」的完整状态：观测到的进度序列必须单调非降、相邻快照之间要么
      不变（读到同一已提交状态）、要么跳到已真实提交的高度，绝不出现进度
      与规范链对不上的半成品态。
    """
    _healthy_chain(service)  # g0..a3，规范链高 3
    from .conftest import make_block

    stop = threading.Event()
    applied = {"n": 0}
    lock = threading.Lock()
    observed: list[int] = []   # 每份快照看到的进度（规范链尖高度）
    oks: list[bool] = []

    def writer():
        # 逐块连续延伸：父块必在上一已提交块上，高度相邻 => 每块都生效
        i = 0
        while not stop.is_set() and i < 60:
            h = 4 + i
            parent = "a3" if i == 0 else f"w{i - 1}"
            try:
                r = service.ingest_block(make_block(f"w{i}", parent, h))
            except Exception as exc:  # pragma: no cover - 不应有失败
                log.error("写入端意外失败: %s", exc)
                break
            with lock:
                if r.applied:
                    applied["n"] += 1
            i += 1
            time.sleep(0.003)  # 留出窗口，让自检在两次提交之间并发执行

    def checker():
        while not stop.is_set():
            snap = service._store.snapshot()
            report = HealthChecker(service._store).run_checks()
            with lock:
                oks.append(report.ok)
                observed.append(snap.progress)

    tw = threading.Thread(target=writer)
    tc = threading.Thread(target=checker)
    tw.start(); tc.start()
    tw.join(timeout=5.0)   # 写完最多 60 块就结束（正常远早于 5s）
    stop.set()
    tc.join(timeout=5.0)

    log.info("输入: 写入端连续投递生效块 w0..，实际 applied=%d；"
             "并发期间取得 %d 份自检快照，进度观测序列=%s",
             applied["n"], len(observed), observed)

    # 1) 写入端真的写进了多块生效区块（这是旧用例缺失的关键前提）
    assert applied["n"] >= 10, f"写入端只生效了 {applied['n']} 块，疑似空转"
    # 2) 自检确实在写入进行中执行过，且每份都自洽
    assert len(observed) >= 10, "并发期间取得的自检快照过少"
    assert all(oks), "自检读到了改了一半的中间状态"
    # 3) 每份快照只能是「写前」或某次「写后」的完整状态：进度序列单调非降，
    #    且每步增量 >= 0（不存在回退或对不上的中间值）；起点 3、终点追平写入
    assert all(b >= a for a, b in zip(observed, observed[1:])), \
        f"观测进度非单调: {observed}"
    assert observed[0] >= 3
    assert observed[-1] == service._store.get_progress() == 3 + applied["n"]
    log.info("判定依据: 进度从 %d 单调推进到 %d，全部快照 ok，无半成品态",
             observed[0], observed[-1])
