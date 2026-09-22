"""Chain reorganization: replacement, depth limit, three-state queries."""

import logging

from app.config import Settings
from app.errors import ReorgTooDeepError
from app.models import BlockStatus
from conftest import block, fork_ap, ingest, seed_chain_a

logger = logging.getLogger("indexer")


def test_reorg_replaces_chain_and_marks_three_states(service):
    seed_chain_a(service)  # G A1 A2 A3
    # INPUT: longer competing fork A2p A3p A4p (depth 2: replaces A2,A3)
    result = service.ingest(ingest(4, fork_ap()))
    logger.info("INPUT: longer fork A2p..A4p, accepted=%s counts=%s",
                result.accepted,
                (result.canonical_blocks, result.staged_blocks, result.reorged_blocks))

    statuses = {b.block_hash: b.status.value for b in service.list_blocks()}
    # ASSERT: replaced blocks reorged; common prefix + fork canonical
    assert statuses == {
        "G": "canonical", "A1": "canonical",
        "A2": "reorged", "A3": "reorged",
        "A2p": "canonical", "A3p": "canonical", "A4p": "canonical",
    }
    tx_statuses = {t.tx_hash: t.status.value for t in service.list_transactions()}
    # tx ownership follows the block status
    assert tx_statuses["tx-A2"] == "reorged"
    assert tx_statuses["tx-A3"] == "reorged"
    assert tx_statuses["tx-A4p"] == "canonical"

    # queries can distinguish all three states
    by_status = {s: {b.block_hash for b in service.list_blocks(s)}
                 for s in BlockStatus}
    assert by_status[BlockStatus.CANONICAL] == {"G", "A1", "A2p", "A3p", "A4p"}
    assert by_status[BlockStatus.REORGED] == {"A2", "A3"}
    assert by_status[BlockStatus.STAGED] == set()
    assert service.get_progress().canonical_tip_hash == "A4p"
    logger.info("JUDGE: canonical/reorged partition correct -> PASS")


def test_reorg_too_deep_is_rejected_and_atomic(service, settings):
    assert settings.max_reorg_depth == 2
    seed_chain_a(service)
    # INPUT: fork diverges at G => replacing A1,A2,A3 = depth 3 > limit 2
    fork = [
        block("B1p", "G", 1),
        block("B2p", "B1p", 2),
        block("B3p", "B2p", 3),
        block("B4p", "B3p", 4),
    ]
    logger.info("INPUT: depth-3 reorg while limit=%s", settings.max_reorg_depth)
    try:
        service.ingest(ingest(4, fork))
        raise AssertionError("expected ReorgTooDeepError")
    except ReorgTooDeepError as e:
        logger.info("JUDGE: rejected category=%s detail=%s", e.category, e)
        assert e.category == "reorg_too_deep"

    # ASSERT atomic: original chain intact, fork rows absent, progress not moved
    statuses = {b.block_hash: b.status.value for b in service.list_blocks()}
    assert statuses == {"G": "canonical", "A1": "canonical",
                        "A2": "canonical", "A3": "canonical"}
    assert service.get_progress().progress == 3
    assert service.get_progress().canonical_tip_hash == "A3"
    logger.info("JUDGE: state unchanged after rejection -> PASS")


def test_equal_height_fork_does_not_replace_incumbent(service):
    seed_chain_a(service)
    # INPUT: equal-height competitor at tip (tie rule keeps incumbent)
    fork = [block("A2p", "A1", 2), block("A3p", "A2p", 3)]
    service.ingest(ingest(3, fork))
    logger.info("INPUT: equal-height fork against tip A3")
    statuses = {b.block_hash: b.status.value for b in service.list_blocks()}
    assert statuses["A3"] == "canonical"
    assert statuses["A2p"] == "staged"
    assert statuses["A3p"] == "staged"
    assert service.get_progress().canonical_tip_hash == "A3"
    logger.info("JUDGE: incumbent retained, competitor staged -> PASS")


def test_reorged_block_promoted_again_when_its_chain_wins(settings):
    # needs to tolerate a depth-3 flip-flop; use a dedicated service
    storage = __import__("app.storage", fromlist=["Storage"]).Storage(
        "sqlite:///:memory:", settings.lock_timeout_seconds)
    service = __import__("app.service", fromlist=["IndexerService"]).IndexerService(
        storage, Settings(max_reorg_depth=10))
    from conftest import seed_chain_a
    # A2/A3 get reorged, then a longer extension on the old fork
    # makes it win again: reorged blocks return to canonical.
    seed_chain_a(service)
    service.ingest(ingest(4, fork_ap()))
    # INPUT: extend old chain A3 -> A4 -> A5 (height 5 beats A4p height 4)
    service.ingest(ingest(5, [block("A4", "A3", 4), block("A5", "A4", 5)]))
    statuses = {b.block_hash: b.status.value for b in service.list_blocks()}
    assert statuses["A2"] == "canonical"
    assert statuses["A3"] == "canonical"
    assert statuses["A4"] == "canonical"
    assert statuses["A5"] == "canonical"
    assert statuses["A2p"] == "reorged"
    assert statuses["A3p"] == "reorged"
    assert statuses["A4p"] == "reorged"
    logger.info("JUDGE: previously reorged blocks restored to canonical -> PASS")
