"""三态顺序无关性与可解释性测试。

覆盖验收点：
- 同一份区块数据，无论投递顺序、并发度如何，最终三态归类完全一致且可解释；
- 「曾被换下去」(REORGED + ever_canonical) 与「从未进入规范链」
  (REORGED + 无 ever_canonical) 前后口径一致，且与审计清单对得上；
- 自检的 status_classification / audit_reconciliation 在各种历史下通过；
- 上述结论在多轮复跑（不同随机种子/线程数）下稳定。
"""

from __future__ import annotations

import itertools
import logging
import random
from concurrent.futures import ThreadPoolExecutor

import pytest

from indexer.config import IndexerConfig
from indexer.health import HealthChecker
from indexer.models import BlockStatus
from indexer.service import IngestService
from indexer.store import Store
from indexer.topology import classify_blocks

from .conftest import fork_b, main_chain, make_block

log = logging.getLogger("test")


def _new_service():
    cfg = IndexerConfig(max_reorg_depth=10, genesis_hash="g0")
    return IngestService(Store(":memory:"), cfg), cfg


def test_final_classification_is_order_independent():
    """同一份数据在多种投递顺序下，最终三态归类完全一致且可解释。

    三态（canonical/reorged/staged）是纯结构性结论，顺序无关；
    reorged 内部「曾被换下去 / 从未进入规范链」是历史细分，由
    ever_canonical 标记与审计清单承担，二者在每种历史下都必须逐块
    自洽对账（由自检 audit_reconciliation 与本用例断言保证）。
    """
    dataset = main_chain() + fork_b()  # g0,a1..a3,b1..b4，b 链更高
    expected = classify_blocks(list(dataset), genesis_hash="g0").statuses

    orders = [
        list(dataset),                       # 主链后分叉
        list(reversed(dataset)),             # 完全逆序
        [dataset[i] for i in (3, 7, 0, 4, 1, 5, 2, 6)],  # 交错
        sorted(dataset, key=lambda b: b.block_hash),      # 按哈希
    ]
    for order in orders:
        svc, cfg = _new_service()
        for b in order:
            svc.ingest_block(b)
        got = {h: svc._store.get_block(h)[1] for h in expected}
        assert got == expected, \
            f"顺序 {[b.block_hash for b in order]} 改变了最终归类"

        # 每种历史下标记与审计都必须能逐块对账（同一套口径）
        report = HealthChecker(svc._store,
                               genesis_hash=cfg.genesis_hash).run_checks()
        assert report.ok, [i.message for i in report.issues]

        # 且标记集合必须与审计清单严格一致：出现在清单里的块都曾进入规范链，
        # 没出现过的 REORGED 块标记必须为 0（无论历史如何走）
        audits = svc._store.list_reorg_audits(limit=500)
        mentioned = {h for a in audits
                     for h in a.replaced_blocks + a.readopted_blocks}
        for h, status in got.items():
            if status == BlockStatus.REORGED:
                assert svc._store.ever_canonical(h) == (h in mentioned), h

    log.info("输入: 同一 8 块数据集按 %d 种顺序投递", len(orders))
    log.info("判定依据: 各顺序最终 canonical=%s reorged=%s staged=%s，"
             "且 ever_canonical 与审计清单逐块对账一致",
             sorted(h for h, s in expected.items() if s == BlockStatus.CANONICAL),
             sorted(h for h, s in expected.items() if s == BlockStatus.REORGED),
             sorted(h for h, s in expected.items() if s == BlockStatus.STAGED))


@pytest.mark.parametrize("seed,workers",
                         [(s, w) for s, w in itertools.product(
                             range(10), (1, 4, 16))])
def test_concurrent_shuffled_delivery_is_stable(seed, workers):
    """并发 + 乱序 + 重复投递（多随机种子/多并发度）：结论稳定可复现。"""
    dataset = main_chain() + fork_b()
    deliveries = dataset * 2
    rng = random.Random(seed)
    rng.shuffle(deliveries)

    svc, cfg = _new_service()

    def ingest(blk):
        svc.ingest_block(blk)

    with ThreadPoolExecutor(max_workers=workers) as pool:
        list(pool.map(ingest, deliveries))

    canonical = [b.block_hash for b in svc.canonical_chain()]
    assert canonical == ["g0", "b1", "b2", "b3", "b4"]
    # 关键收敛点：输掉的 a 链确定是 REORGED，不再随调度在 staged 间摇摆
    for h in ("a1", "a2", "a3"):
        assert svc._store.get_block(h)[1] == BlockStatus.REORGED
    assert svc._store.get_progress() == 4
    assert len(svc._store.list_blocks()) == len(dataset)

    # 同一口径下自检必然自洽（含拓扑归类与审计对账）
    report = HealthChecker(svc._store, genesis_hash=cfg.genesis_hash).run_checks()
    assert report.ok, [i.message for i in report.issues]
    log.info("判定依据[seed=%s workers=%s]: canonical=%s, a链=REORGED, 自检ok",
             seed, workers, canonical)


def test_two_kinds_of_reorged_are_consistent_with_audit():
    """「曾被换下去」与「从未进入规范链」在状态、历史标记、审计三处口径一致。

    a 链先规范后被更长的 b 链换下 -> a1..a3 是「曾被换下去」：
      REORGED + ever_canonical=1 + 出现在某条审计的 replaced 清单。
    在 b 已胜出后再投递一条连通但更短的分叉 d1（高1）-> d1 连通即落败：
      REORGED + ever_canonical=0 + 从不出现在任何审计清单。
    """
    svc, cfg = _new_service()
    for b in main_chain():
        svc.ingest_block(b)
    for b in fork_b():
        svc.ingest_block(b)
    d1 = make_block("d1", "g0", 1)
    svc.ingest_block(d1)  # b 链已在高4，d1 连通即落败

    store = svc._store
    audits = store.list_reorg_audits(limit=500)
    replaced_flat = {h for a in audits for h in a.replaced_blocks}
    readopted_flat = {h for a in audits for h in a.readopted_blocks}
    mentioned = replaced_flat | readopted_flat

    # 曾被换下去
    for h in ("a1", "a2", "a3"):
        assert store.get_block(h)[1] == BlockStatus.REORGED
        assert store.ever_canonical(h) is True
        assert h in replaced_flat
    # 从未进入规范链
    assert store.get_block("d1")[1] == BlockStatus.REORGED
    assert store.ever_canonical("d1") is False
    assert "d1" not in mentioned

    report = HealthChecker(store, genesis_hash=cfg.genesis_hash).run_checks()
    assert report.ok, [i.message for i in report.issues]
    log.info("判定依据: a1..a3=曾被换下去(在replaced清单)，d1=从未进入规范链"
             "(不在任何清单)，自检对账通过")


def test_health_flags_contradiction_between_audit_and_state(service):
    """人为制造「审计说被换下、状态却是 canonical」的矛盾时，对账检查须点名。"""
    for b in main_chain():
        service.ingest_block(b)
    for b in fork_b():
        service.ingest_block(b)  # 触发 a->b，a1 在 replaced 清单

    # 手工把 a1 改回 canonical，制造审计与最终状态互相矛盾
    service._store.set_block_status("a1", BlockStatus.CANONICAL)
    checker = HealthChecker(service._store,
                            genesis_hash=service._config.genesis_hash)
    report = checker.run_checks()
    assert not report.ok
    recon = next(c for c in report.checks
                 if c.name == "audit_reconciliation")
    assert not recon.ok
    named = {h for i in recon.issues for h in i.block_hashes}
    log.info("判定依据: 制造矛盾后对账检查点名块=%s", sorted(named))
    assert "a1" in named
