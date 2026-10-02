"""稳定性与口径一致性测试：

- 并发 + 乱序 + 重复投递下，三态最终归类与投递顺序/并发度无关、稳定可解释；
- 审计「被换下去 / 重新接纳」两份名单与最终状态严格对账，无矛盾块；
- 自检口径与实现归类同一套结论。

这些用例是对既有重组/乱序/并发/进度回退用例的补充，多轮复跑用于确认
不再时红时绿（见文件内 rounds 与文末复现说明）。
"""

from __future__ import annotations

import logging
import random
from concurrent.futures import ThreadPoolExecutor

import pytest

from indexer.classification import classify_blocks
from indexer.config import IndexerConfig
from indexer.health import HealthChecker
from indexer.models import BlockStatus
from indexer.service import IngestService
from indexer.store import Store

from .conftest import fork_b, main_chain, make_block

log = logging.getLogger("test")


def _service(max_depth: int = 10) -> IngestService:
    return IngestService(Store(":memory:"),
                         IndexerConfig(max_reorg_depth=max_depth,
                                       genesis_hash="g0"))


def _final_labels(svc: IngestService) -> dict[str, BlockStatus]:
    return {b.block_hash: s for b, s in svc._store.list_blocks()}


def _reference_labels(blocks) -> dict[str, BlockStatus]:
    verdict = classify_blocks(blocks)
    return {b.block_hash: verdict.status_of(b.block_hash) for b in blocks}


def _reconcile_audits_with_final(svc: IngestService):
    """审计两份名单与最终状态对账，返回可读的矛盾清单（空=一致）。

    口径（审计记录只追加、与真实切换同事务）：
    1. replaced 与同一条 readopted 不相交；同一条内两份名单各自不重复。
    2. 「最终还在规范链上」的块，若在审计里出现，最后一次出场必须是
       readopted（被换下去又回来）；绝不能最后停在 replaced。
    3. 最终 REORGED 的块：要么其最后一次审计出场是 replaced（真实切换换下
       去），要么它从未出现在任何审计里——后者是「参考口径下曾领先、实际
       顺序里从未登上规范链」的补判块（reorged 分类的第二种成因）。
    4. 最终 STAGED / 未连通的块永远不出现在任何审计名单里。
    """
    audits = svc._store.list_reorg_audits(limit=10_000)
    statuses = _final_labels(svc)
    problems: list[str] = []

    last_role: dict[str, str] = {}
    appears: set[str] = set()
    for rec in reversed(audits):  # 时间正序，后写覆盖 -> 得到最后一次角色
        replaced = set(rec.replaced_blocks)
        readopted = set(rec.readopted_blocks)
        if replaced & readopted:
            problems.append(
                f"审计#{rec.id} replaced/readopted 相交: {sorted(replaced & readopted)}")
        if len(rec.replaced_blocks) != len(replaced):
            problems.append(f"审计#{rec.id} replaced_blocks 有重复")
        if len(rec.readopted_blocks) != len(readopted):
            problems.append(f"审计#{rec.id} readopted_blocks 有重复")
        for h in replaced:
            last_role[h] = "replaced"
            appears.add(h)
        for h in readopted:
            last_role[h] = "readopted"
            appears.add(h)

    for h, role in last_role.items():
        final = statuses.get(h)
        if final == BlockStatus.CANONICAL and role != "readopted":
            problems.append(
                f"{h} 最终 canonical，但审计最后角色是 {role}（说法矛盾）")
        if final == BlockStatus.STAGED:
            problems.append(f"{h} 最终 staged，却出现在审计名单里")
        if final == BlockStatus.REORGED and role == "readopted":
            problems.append(
                f"{h} 最终 reorged，但审计最后角色是 readopted（说法矛盾）")
    for h, final in statuses.items():
        if final == BlockStatus.STAGED and h in appears:
            problems.append(f"{h} 最终 staged，却曾出现在审计名单里")
    return problems


# ---- 数据集：覆盖 主链胜出 / 更长分叉胜出 / 反超后再回归 / 孤立块 ----

DATASETS = {
    "fork_b_wins": main_chain() + fork_b(),
    "fork_returns": (main_chain() + fork_b()
                     + [make_block("a4", "a3", 4),
                        make_block("a5", "a4", 5)]),
    "orphan_kept_staged": main_chain() + [make_block("z9", "nowhere", 9)],
    "plain_extension": main_chain() + [make_block("a4", "a3", 4)],
}


@pytest.mark.parametrize("dataset_name", sorted(DATASETS))
def test_final_classification_is_order_independent(dataset_name):
    """同一份数据按多种乱序（含重复）投递，最终三态归类必须逐块一致。"""
    blocks = DATASETS[dataset_name]
    ref = _reference_labels(blocks)
    rng = random.Random(1234)
    finals = []
    for round_ in range(12):
        order = list(blocks)
        if round_ % 2 == 0:
            order = order * 2  # 一半轮次带重复投递
        rng.shuffle(order)
        svc = _service()
        for b in order:
            svc.ingest_block(b)
        labels = _final_labels(svc)
        assert set(labels) == set(ref), "区块集合不应随顺序变化"
        diff = {h: (labels[h], ref[h]) for h in ref if labels[h] != ref[h]}
        assert not diff, f"第 {round_} 轮归类与统一口径不符: {diff}"
        finals.append((labels, svc))
        log.info("数据集=%s 轮次=%2d canonical=%s reorged=%s staged=%s",
                 dataset_name, round_,
                 sorted(h for h, s in labels.items() if s == BlockStatus.CANONICAL),
                 sorted(h for h, s in labels.items() if s == BlockStatus.REORGED),
                 sorted(h for h, s in labels.items() if s == BlockStatus.STAGED))
    # 所有轮次之间也必须完全一致（不只是和参考口径一致）
    first = finals[0][0]
    for labels, _ in finals[1:]:
        assert labels == first
    log.info("判定依据: 数据集 %s 12 轮乱序投递最终归类逐块一致", dataset_name)


@pytest.mark.parametrize("dataset_name", sorted(DATASETS))
def test_final_classification_stable_under_concurrency(dataset_name):
    """同一份数据在多线程乱序+重复投递下，最终归类稳定且与统一口径一致。"""
    blocks = DATASETS[dataset_name]
    ref = _reference_labels(blocks)
    for round_ in range(8):
        svc = _service()
        deliveries = blocks * 3
        random.Random(900 + round_).shuffle(deliveries)
        with ThreadPoolExecutor(max_workers=12) as pool:
            list(pool.map(svc.ingest_block, deliveries))
        labels = _final_labels(svc)
        diff = {h: (labels[h], ref[h]) for h in ref if labels[h] != ref[h]}
        assert not diff, f"并发第 {round_} 轮归类不稳: {diff}"
        heights = {b.block_hash: b.height for b in blocks}
        assert svc._store.get_progress() == max(
            (heights[h] for h, s in labels.items()
             if s == BlockStatus.CANONICAL), default=-1)
    log.info("判定依据: 数据集 %s 8 轮并发乱序，最终归类与统一口径一致、进度匹配",
             dataset_name)


def test_concurrent_dataset_matches_pinned_expectation():
    """原并发用例数据集：a1..a3 无论投递时序如何都稳定归 REORGED。"""
    blocks = main_chain() + fork_b()
    ref = _reference_labels(blocks)
    assert ref["a1"] == ref["a2"] == ref["a3"] == BlockStatus.REORGED
    assert {h for h, s in ref.items() if s == BlockStatus.CANONICAL} == \
        {"g0", "b1", "b2", "b3", "b4"}
    for seed in range(15):
        svc = _service(max_depth=3)
        order = blocks * 3
        random.Random(seed).shuffle(order)
        with ThreadPoolExecutor(max_workers=16) as pool:
            list(pool.map(svc.ingest_block, order))
        for h in ("a1", "a2", "a3"):
            assert svc._store.get_block(h)[1] == BlockStatus.REORGED, \
                f"seed={seed} {h} 未稳定归为 REORGED"
        assert [b.block_hash for b in svc.canonical_chain()] == \
            ["g0", "b1", "b2", "b3", "b4"]
    log.info("判定依据: 15 个不同并发调度下 a1..a3 恒为 REORGED，规范链恒为 b 链")


@pytest.mark.parametrize("dataset_name", sorted(DATASETS))
def test_audits_reconcile_with_final_state_across_orders(dataset_name):
    """多种投递顺序下，审计名单与最终状态对账均无矛盾块。"""
    blocks = DATASETS[dataset_name]
    rng = random.Random(55)
    for round_ in range(10):
        svc = _service()
        order = list(blocks)
        rng.shuffle(order)
        if round_ % 2:
            order = order * 2
        for b in order:
            svc.ingest_block(b)
        problems = _reconcile_audits_with_final(svc)
        assert not problems, f"数据集 {dataset_name} 第 {round_} 轮: {problems}"
        # 自检同一口径：健康状态下不应报归类分歧
        report = HealthChecker(svc._store).run_checks()
        assert report.ok, report.issues
    log.info("判定依据: 数据集 %s 10 轮乱序后审计与最终状态全部对得上，自检 ok",
             dataset_name)


def test_settled_reorged_blocks_absent_from_audits_are_explained():
    """参考补判的 REORGED 块不出现在审计里时，分类仍能被统一口径解释。

    模拟实际并发中「胜出分叉先到位、落败分叉随后才连通」的投递：a 链在
    实际顺序里从未登上规范链（无切换审计），但最终仍应是 REORGED，因为
    按统一参考口径它在 h1..h3 曾领先。
    """
    svc = _service(max_depth=3)
    # b 链先连通成为规范链，a 链各块之后才到（无严格更高链尖，不会真实切换）
    for b in [main_chain()[0]] + fork_b():
        svc.ingest_block(b)
    for b in main_chain()[1:]:
        svc.ingest_block(b)
    for h in ("a1", "a2", "a3"):
        assert svc._store.get_block(h)[1] == BlockStatus.REORGED
    audits = svc._store.list_reorg_audits(limit=100)
    mentioned = {h for rec in audits
                 for h in rec.replaced_blocks + rec.readopted_blocks}
    assert not ({"a1", "a2", "a3"} & mentioned), \
        "实际未发生的切换不应凭空写进审计"
    # 但统一口径（参考重放）同样判 REORGED：可复查、可解释
    ref = _reference_labels(main_chain() + fork_b())
    assert {ref["a1"], ref["a2"], ref["a3"]} == {BlockStatus.REORGED}
    assert _reconcile_audits_with_final(svc) == []
    assert HealthChecker(svc._store).run_checks().ok
    log.info("判定依据: a 链实际未规范过但按统一口径补判 REORGED；审计无凭空"
             "记录，对账与自检均自洽")
