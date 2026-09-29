"""整批原子摄取测试：全成全败、批内幂等、锁超时可区分、底层异常不外泄。"""

from __future__ import annotations

import logging
import threading
from concurrent.futures import ThreadPoolExecutor

import pytest

from indexer.config import IndexerConfig
from indexer.errors import (IngestError, LockBusyError,
                            MalformedPayloadError, ReorgTooDeepError,
                            StateConflictError)
from indexer.models import BlockStatus
from indexer.service import IngestService
from indexer.store import Store

from .conftest import fork_c, main_chain, make_block

log = logging.getLogger("test")


def test_batch_commits_whole_order_independent(service):
    """乱序整批：父块缺失也能暂存，补齐在同批内时最终连通规范链。"""
    chain = main_chain()
    batch = [chain[2], chain[0], chain[3], chain[1]]  # a2, g0, a3, a1
    result = service.ingest_blocks_atomic(batch)
    log.info("输入: 乱序整批 %s -> accepted=%d duplicates=%d",
             [b.block_hash for b in batch], result.accepted, result.duplicates)
    assert result.accepted == 4 and result.duplicates == 0
    assert [b.block_hash for b in service.canonical_chain()] == \
        ["g0", "a1", "a2", "a3"]
    assert service._store.get_progress() == 3


def test_batch_duplicates_are_idempotent_not_failure(service):
    """同批内/跨批重复投递：幂等成功，整批 accepted=0 duplicates=N。"""
    chain = main_chain()
    service.ingest_blocks_atomic(chain)
    result = service.ingest_blocks_atomic(chain)  # 全部已存在
    log.info("输入: 整批重复投递 4 个已存在块 -> accepted=%d duplicates=%d",
             result.accepted, result.duplicates)
    assert result.accepted == 0 and result.duplicates == 4
    assert all(not r.applied for r in result.results)
    assert len(service._store.list_blocks()) == 4


def test_batch_rolls_back_when_one_block_malformed(service):
    """批次中一个畸形块 -> 整批不落库，前面已处理的块也回滚。"""
    chain = main_chain()
    bad = make_block("bad", "a3", -1)  # 负高度
    batch = [chain[0], chain[1], chain[2], bad]
    with pytest.raises(MalformedPayloadError):
        service.ingest_blocks_atomic(batch)
    log.info("输入: 合法块 g0,a1,a2 + 畸形块 bad（负高度），整批应失败")
    assert service._store.list_blocks() == []
    assert service._store.get_progress() == -1
    assert service._store.get_block("g0") is None
    assert service._store.get_block("bad") is None
    log.info("判定依据: 批次回滚后区块表为空、进度仍为 -1")


def test_batch_rolls_back_on_content_conflict(service):
    """批次中同哈希不同内容 -> state_conflict，整批不落库。"""
    service.ingest_blocks_atomic(main_chain())
    from indexer.models import Block
    g0 = main_chain()[0]
    tampered = Block(g0.block_hash, g0.parent_hash, g0.height,
                     g0.timestamp + 1, g0.transactions)
    extra = make_block("a9", "a3", 4)
    with pytest.raises(StateConflictError):
        service.ingest_blocks_atomic(
            [extra, tampered, make_block("a10", "a9", 5)])
    log.info("输入: 批次内含同哈希不同内容块 -> state_conflict，整批回滚")
    assert service._store.get_block("a9") is None
    assert service._store.get_block("a10") is None
    # 原有数据不受影响
    assert len(service._store.list_blocks()) == 4


def test_batch_rolls_back_on_too_deep_reorg(service):
    """批次内最后一块触发过深重组 -> 整批（含暂存块）全部回滚。"""
    service.ingest_blocks_atomic(main_chain())
    service.ingest_block(make_block("a4", "a3", 4))  # 链尖高 4
    fork = fork_c()
    with pytest.raises(ReorgTooDeepError):
        service.ingest_blocks_atomic(fork)  # c1..c6，c5 即触发深度 4
    log.info("输入: c1..c6 整批，c5 触发深度 4 超上限 -> 整批回滚")
    for b in fork:
        assert service._store.get_block(b.block_hash) is None
    assert [x.block_hash for x in service.canonical_chain()] == \
        ["g0", "a1", "a2", "a3", "a4"]
    assert service._store.get_progress() == 4


def test_lock_timeout_returns_distinct_retryable_error():
    """并发批量提交撞锁：等待超时时按 lock_busy（可重试）明确返回。"""
    cfg = IndexerConfig(max_reorg_depth=3, genesis_hash="g0",
                        lock_timeout=0.2)
    store = Store(":memory:")
    svc = IngestService(store, cfg)

    entered = threading.Event()
    release = threading.Event()

    def hold_lock():
        with store.transaction():
            entered.set()
            release.wait(timeout=2.0)

    t = threading.Thread(target=hold_lock)
    t.start()
    entered.wait(timeout=2.0)
    try:
        with pytest.raises(LockBusyError) as exc_info:
            svc.ingest_blocks_atomic(main_chain())
        log.info("输入: 锁被占用时提交整批（timeout=0.2s）-> %s",
                 exc_info.value.category)
        assert exc_info.value.category == "lock_busy"
        assert exc_info.value.detail.get("timeout") == 0.2
    finally:
        release.set()
        t.join()
    # 超时方没有写入任何数据
    assert store.list_blocks() == []


def test_concurrent_batches_never_partially_apply():
    """多线程并发投递（含重复），不卡死、不报锁忙以外的错，且无半批残留。"""
    cfg = IndexerConfig(max_reorg_depth=3, genesis_hash="g0",
                        lock_timeout=2.0)
    store = Store(":memory:")
    svc = IngestService(store, cfg)
    batches = [main_chain(), main_chain(),
               [make_block("d1", "g0", 1)], main_chain()]

    errors: list[IngestError] = []

    def submit(batch):
        try:
            return svc.ingest_blocks_atomic(batch)
        except LockBusyError as exc:
            errors.append(exc)  # 锁忙是可区分、可重试的唯一允许错误
            return None

    with ThreadPoolExecutor(max_workers=4) as pool:
        results = list(pool.map(submit, batches))
    log.info("输入: 4 个并发整批（3 个相同主链、1 个分叉），lock_busy=%d",
             len(errors))
    assert all(e.category == "lock_busy" for e in errors)

    # 不变量 1：主链四件套要么整批都在，要么一个都不在（无半批）
    present = {b.block_hash for b, _ in store.list_blocks()}
    main_hashes = {"g0", "a1", "a2", "a3"}
    assert main_hashes <= present or not (main_hashes & present)

    # 不变量 2：d1 即使落库也只能是 staged（等高不胜出），规范链仍是主链
    if main_hashes <= present:
        assert [b.block_hash for b in svc.canonical_chain()] == \
            ["g0", "a1", "a2", "a3"]
        if "d1" in present:
            assert store.get_block("d1")[1] == BlockStatus.STAGED
        assert store.get_progress() == 3
    else:
        assert store.get_progress() == -1


def test_nested_transaction_savepoint_partial_rollback(service):
    """同线程嵌套事务用 SAVEPOINT 隔离：内层失败不拖垮外层已做的写入。"""
    store = service._store
    with store.transaction():
        service._store.upsert_block(main_chain()[0], BlockStatus.CANONICAL)
        # 内层事务（SAVEPOINT）抛错时仅回滚到保存点，外层继续提交
        with pytest.raises(MalformedPayloadError):
            with store.transaction():
                service._store.upsert_block(main_chain()[1], BlockStatus.STAGED)
                raise MalformedPayloadError("内层模拟失败")
        # 外层事务仍有效：再写一块，最终 g0 与 a2 提交、a1 未落库
        service._store.upsert_block(main_chain()[2], BlockStatus.STAGED)
    log.info("输入: 外层事务内嵌一个失败的内层事务")
    assert service._store.get_block("g0") is not None
    assert service._store.get_block("a1") is None
    assert service._store.get_block("a2") is not None


def test_raw_database_error_is_not_leaked(service, monkeypatch):
    """底层 sqlite3 异常须包装成分类错误，不把原始异常直接甩给调用方。"""
    import sqlite3

    real_execute = sqlite3.Connection.execute
    write_calls = {"n": 0}

    class ExplodingConn:
        def __init__(self, inner):
            self._inner = inner

        def execute(self, sql, params=()):
            # 在事务体的第一次写入上抛出原始底层异常
            if isinstance(sql, str) and sql.strip().upper().startswith("INSERT") \
                    and write_calls["n"] == 0:
                write_calls["n"] += 1
                raise sqlite3.OperationalError("disk I/O error")
            return real_execute(self._inner, sql, params)

    # Store 方法均通过 self._conn 访问连接，用代理替换连接对象的 execute
    proxy = ExplodingConn(service._store._conn)
    monkeypatch.setattr(service._store, "_conn", proxy, raising=False)

    with pytest.raises(IngestError) as exc_info:
        service.ingest_blocks_atomic(main_chain())
    log.info("输入: 模拟底层 disk I/O error -> 包装分类=%s",
             exc_info.value.category)
    assert exc_info.value.category == "storage_error"
    assert isinstance(exc_info.value.__cause__, sqlite3.OperationalError)
    # 整批回滚，无残留
    assert service._store.list_blocks() == []
