"""Duplicate delivery and out-of-order arrival."""

from app.models import BlockStatus

import logging

from conftest import block, ingest, seed_chain_a

logger = logging.getLogger("indexer")


def test_duplicate_delivery_is_idempotent(service):
    seed_chain_a(service)
    payload = [block("G", None, 0), block("A1", "G", 1)]
    # INPUT: redeliver already-canonical blocks, progress unchanged
    result = service.ingest(ingest(3, payload))
    logger.info("INPUT: redeliver G,A1 at progress=3 -> accepted=%s", result.accepted)

    blocks = service.list_blocks()
    txns = service.list_transactions()
    # ASSERT: 不重不漏 — row counts unchanged
    assert len(blocks) == 4
    assert len(txns) == 4
    assert all(b.status.value == "canonical" for b in blocks)
    logger.info("JUDGE: no extra rows after redelivery -> PASS")


def test_duplicate_block_inside_one_payload_rejected(service):
    from app.errors import MalformedDataError
    # INPUT: same block hash twice in a single request
    req = ingest(1, [block("G", None, 0), block("G", None, 0)])
    logger.info("INPUT: duplicated block in one payload")
    try:
        service.ingest(req)
        raise AssertionError("should have raised")
    except MalformedDataError as e:
        logger.info("JUDGE: category=%s detail=%s", e.category, e)
        assert e.category == "malformed_data"
    # nothing persisted
    assert service.list_blocks() == []
    assert service.get_progress().progress == 0


def test_out_of_order_block_stays_staged_then_promoted(service):
    # INPUT: A2 arrives before A1 (its parent is missing)
    r1 = service.ingest(ingest(0, [block("G", None, 0)]))
    r2 = service.ingest(ingest(2, [block("A2", "A1", 2)]))
    logger.info("INPUT: G then orphan A2")
    assert r2.staged_blocks == 1

    staged = service.list_blocks(BlockStatus.STAGED)
    assert [b.block_hash for b in staged] == ["A2"]
    # orphan must NOT appear as canonical data
    assert service.get_progress().canonical_tip_hash == "G"

    # parent arrives (乱序补齐)
    r3 = service.ingest(ingest(2, [block("A1", "G", 1)]))
    logger.info("INPUT: parent A1 delivered, promotion expected canonical=%s",
                r3.canonical_blocks)
    statuses = {b.block_hash: b.status.value for b in service.list_blocks()}
    # ASSERT: every distinct block exactly once; G,A1,A2 canonical
    assert statuses == {"G": "canonical", "A1": "canonical", "A2": "canonical"}
    tx_statuses = {t.tx_hash: t.status.value for t in service.list_transactions()}
    assert tx_statuses == {"tx-G": "canonical", "tx-A1": "canonical", "tx-A2": "canonical"}
    logger.info("JUDGE: orphan staged then promoted, no duplicates -> PASS")
