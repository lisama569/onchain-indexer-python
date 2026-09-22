"""Ordered ingestion produces exactly the canonical chain."""

import logging

from app.models import BlockStatus
from conftest import chain_a, ingest, seed_chain_a

logger = logging.getLogger("indexer")


def test_ordered_ingest_builds_canonical_chain(service):
    # INPUT: G..A3 delivered in order
    for i, b in enumerate(chain_a()):
        result = service.ingest(ingest(i, [b]))
        logger.info("JUDGE: ingest height=%s accepted=%s", i, result.accepted)
        assert result.accepted
        assert result.progress == i

    blocks = service.list_blocks()
    # ASSERT: 不重不漏 — exactly 4 distinct blocks, all canonical
    assert len(blocks) == 4
    assert {b.block_hash for b in blocks} == {"G", "A1", "A2", "A3"}
    assert all(b.status == BlockStatus.CANONICAL for b in blocks)

    txns = service.list_transactions()
    # one tx per block, no duplicates
    assert len(txns) == 4
    assert {t.tx_hash for t in txns} == {"tx-G", "tx-A1", "tx-A2", "tx-A3"}

    progress = service.get_progress()
    assert progress.progress == 3
    assert progress.canonical_tip_hash == "A3"
    assert progress.canonical_tip_height == 3
    logger.info("JUDGE: tip=A3 height=3, all canonical -> PASS")


def test_filter_query_by_status(service):
    seed_chain_a(service)
    logger.info("INPUT: filter queries over seeded chain G..A3")
    assert len(service.list_blocks(BlockStatus.CANONICAL)) == 4
    assert service.list_blocks(BlockStatus.STAGED) == []
    assert service.list_blocks(BlockStatus.REORGED) == []
