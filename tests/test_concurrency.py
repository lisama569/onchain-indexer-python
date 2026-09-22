"""并发摄取测试：多线程同时投递同一批区块，结果须不重不漏、无部分应用。"""

from __future__ import annotations

import logging
import random
from concurrent.futures import ThreadPoolExecutor

from indexer.errors import IngestError
from indexer.models import BlockStatus

from .conftest import fork_b, main_chain

log = logging.getLogger("test")


def test_concurrent_ingest_is_consistent(service):
    """16 线程乱序重复投递同一固定数据集，最终状态必须与顺序摄取一致。"""
    chain = main_chain()          # g0..a3
    fork = fork_b()               # b1..b4（更长，最终应胜出）
    dataset = chain + fork
    # 每个区块投递 3 次并打乱，模拟重复 + 乱序 + 并发
    deliveries = dataset * 3
    rng = random.Random(42)
    rng.shuffle(deliveries)
    log.info("输入: %d 个区块 x3 次重复投递，共 %d 次摄取，16 线程并发",
             len(dataset), len(deliveries))

    outcomes = {"ok": 0, "dup": 0, "err": 0}

    def ingest(blk):
        try:
            r = service.ingest_block(blk)
            return "ok" if r.applied else "dup"
        except IngestError as exc:
            log.info("摄取失败(可接受): %s %s", blk.block_hash, exc.category)
            return "err"

    with ThreadPoolExecutor(max_workers=16) as pool:
        for result in pool.map(ingest, deliveries):
            outcomes[result] += 1
    log.info("判定依据: 结果分布 %s", outcomes)

    # 每个区块恰好应用一次，其余为幂等重放；不允许出现错误
    assert outcomes["ok"] == len(dataset)
    assert outcomes["err"] == 0
    assert outcomes["ok"] + outcomes["dup"] == len(deliveries)

    # 最终状态：b 链更长，规范链为 g0,b1..b4；a1..a3 被重组替换
    canonical = [b.block_hash for b in service.canonical_chain()]
    log.info("判定依据: 规范链=%s", canonical)
    assert canonical == ["g0", "b1", "b2", "b3", "b4"]
    for h in ("a1", "a2", "a3"):
        assert service._store.get_block(h)[1] == BlockStatus.REORGED

    # 不重不漏：区块总数 = 数据集大小，每块交易数与输入一致
    all_blocks = service._store.list_blocks()
    assert len(all_blocks) == len(dataset)
    for blk, _ in all_blocks:
        assert len(service._store.transactions_of(blk.block_hash)) == 1
    assert service._store.get_progress() == 4
    log.info("判定依据: 区块数=%d 无重复，进度=%d 单调推进",
             len(all_blocks), service._store.get_progress())


def test_concurrent_progress_never_regresses(service):
    """并发推进进度：任何时刻不出现回退，最终为最大值。"""
    store = service._store
    errors = []

    def advance(h):
        try:
            store.advance_progress(h)
        except IngestError:
            errors.append(h)  # 回退写被拒绝是预期行为

    with ThreadPoolExecutor(max_workers=8) as pool:
        list(pool.map(advance, [5, 3, 9, 1, 7, 9, 2, 9]))
    log.info("输入: 并发写入进度 [5,3,9,1,7,9,2,9]，被拒=%s", sorted(errors))
    assert store.get_progress() == 9
    # 不变量：最大值 9 从未被拒（进度不曾被推高到 9 以上），
    # 所有被拒写入都小于最终进度（确属回退写，被分类拒绝而非静默生效）
    assert 9 not in errors
    assert errors and all(h < 9 for h in errors)
    log.info("判定依据: 最终进度=9（单调最大值），回退写 %s 全部被拒",
             sorted(errors))
