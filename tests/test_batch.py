"""整批原子摄取测试：全有或全无、批内重复幂等、锁超时可区分可重试。"""

from __future__ import annotations

import logging
import threading
import time

import pytest

from indexer.config import IndexerConfig
from indexer.errors import (LockTimeoutError, MalformedPayloadError,
                            ReorgTooDeepError)
from indexer.models import Block, Transaction
from indexer.service import IngestService
from indexer.store import Store

from .conftest import fork_c, main_chain, make_block

log = logging.getLogger("test")


def test_atomic_batch_all_or_nothing_on_bad_block(service):
    """批次内任一块畸形：整批拒绝，前半批暂存的块也不得落库。"""
    good = main_chain()
    orphan = make_block("orphan", "missing-parent", 9)
    bad = Block("bad", "g0", 1, 100,
                (Transaction("t", "bad", "a", "b", -1, 0),))
    batch = [orphan, good[0], bad]
    log.info("输入: 原子批次 [孤立块orphan, 创世块g0, 畸形块bad]")

    with pytest.raises(MalformedPayloadError) as exc_info:
        service.ingest_blocks_atomic(batch)
    log.info("判定依据: 整批拒绝分类=%s", exc_info.value.category)
    assert exc_info.value.category == "malformed_payload"

    # 全有或全无：连先处理的 orphan / g0 都不应留下
    assert service._store.get_block("orphan") is None
    assert service._store.get_block("g0") is None
    assert service._store.get_block("bad") is None
    assert service.canonical_chain() == []
    assert service._store.get_progress() == -1
    log.info("判定依据: orphan/g0/bad 均未落库，规范链为空，进度=-1")


def test_atomic_batch_all_or_nothing_on_deep_reorg(service):
    """批次内重组过深：整批回滚，已入库的规范数据不受影响、新块不落库。"""
    for b in main_chain():  # 既有规范链 g0..a3
        service.ingest_block(b)
    # 一个批次同时带来 a4 延伸与触发深度4的 c5，整体应被拒
    batch = [make_block("a4", "a3", 4)] + [fork_c()[i] for i in range(5)]
    log.info("输入: 原子批次含 a4 与 c1..c5（c5 触发深度4 > 上限3）")
    with pytest.raises(ReorgTooDeepError) as exc_info:
        service.ingest_blocks_atomic(batch)
    assert exc_info.value.category == "reorg_too_deep"
    for h in ("a4", "c1", "c2", "c3", "c4", "c5"):
        assert service._store.get_block(h) is None, h
    assert [b.block_hash for b in service.canonical_chain()] == \
        ["g0", "a1", "a2", "a3"]
    assert service._store.get_progress() == 3
    log.info("判定依据: a4/c1..c5 全部未落库，规范链与进度不变")


def test_atomic_batch_applies_when_all_valid(service):
    """整批合法（含乱序）：一次生效，applied 计数正确。"""
    chain = main_chain()
    order = [chain[2], chain[0], chain[3], chain[1]]  # a2,g0,a3,a1
    log.info("输入: 合法原子批次乱序 %s", [b.block_hash for b in order])
    result = service.ingest_blocks_atomic(order)
    assert result.applied == 4 and result.duplicates == 0
    assert [b.block_hash for b in service.canonical_chain()] == \
        ["g0", "a1", "a2", "a3"]
    assert service._store.get_progress() == 3
    log.info("判定依据: applied=4，规范链连通为 g0..a3，进度=3")


def test_atomic_batch_duplicates_are_idempotent_not_failure(service):
    """同批内 / 与已存数据重复：按幂等处理计入 duplicates，不算失败。"""
    chain = main_chain()
    service.ingest_blocks_atomic(chain)
    # 批次内：两个已存在块 + 一个批次内自身重复
    batch = [chain[0], chain[1], chain[0]]
    log.info("输入: 第二批全部为重复块（含批次内重复）%s",
             [b.block_hash for b in batch])
    result = service.ingest_blocks_atomic(batch)
    assert result.applied == 0 and result.duplicates == 3
    assert all(not r.applied for r in result.results)
    assert len(service._store.list_blocks()) == 4  # 没有多余记录
    log.info("判定依据: applied=0 duplicates=3，区块总数仍为4")


def test_atomic_batch_lock_timeout_is_distinguished_retryable():
    """并发批量提交撞锁：等待有上限，超时返回可区分的可重试临时故障。"""
    store = Store(":memory:", lock_timeout=0.2)
    svc = IngestService(store, IndexerConfig(max_reorg_depth=3,
                                            genesis_hash="g0",
                                            lock_timeout=0.2))
    held = threading.Event()

    def holder():
        with store.transaction(timeout=5.0):
            held.set()
            time.sleep(1.0)

    t = threading.Thread(target=holder)
    t.start()
    held.wait()
    t0 = time.monotonic()
    log.info("输入: 写锁被占用，提交一个原子批次（上限0.2s）")
    with pytest.raises(LockTimeoutError) as exc_info:
        svc.ingest_blocks_atomic(main_chain())
    waited = time.monotonic() - t0
    t.join()
    log.info("判定依据: 等待 %.2fs 后分类=%s（可重试），detail=%s",
             waited, exc_info.value.category, exc_info.value.detail)
    assert exc_info.value.category == "lock_timeout"
    assert 0.15 <= waited < 0.8  # 有上限，不会一直卡死
    # 超时未写入任何数据
    assert store.list_blocks() == []
    store.close()
