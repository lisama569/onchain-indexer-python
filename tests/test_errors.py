"""Malformed payloads and progress regression — categories must differ."""

import logging

from app.errors import MalformedDataError, StateConflictError
from app.service import IndexerService
from conftest import block, ingest, seed_chain_a, tx

logger = logging.getLogger("indexer")


def test_progress_regression_rejected(service):
    seed_chain_a(service)
    # INPUT: progress 3 stored, client claims 2
    logger.info("INPUT: progress regression 3 -> 2")
    try:
        service.ingest(ingest(2, []))
        raise AssertionError("expected StateConflictError")
    except StateConflictError as e:
        logger.info("JUDGE: category=%s detail=%s", e.category, e)
        assert e.category == "state_conflict"
    assert service.get_progress().progress == 3


def test_equal_progress_allowed(service):
    seed_chain_a(service)
    # INPUT: same progress with empty payload is a harmless no-op
    result = service.ingest(ingest(3, []))
    assert result.accepted and result.progress == 3
    logger.info("JUDGE: equal progress accepted as no-op -> PASS")


def test_genesis_with_parent_is_malformed(service):
    logger.info("INPUT: height-0 block with non-null parent")
    try:
        service.ingest(ingest(0, [block("G", "X", 0)]))
        raise AssertionError("expected MalformedDataError")
    except MalformedDataError as e:
        logger.info("JUDGE: category=%s detail=%s", e.category, e)
        assert e.category == "malformed_data"
    assert service.list_blocks() == []


def test_non_genesis_without_parent_is_malformed(service):
    service.ingest(ingest(0, [block("G", None, 0)]))
    logger.info("INPUT: height-2 block with null parent")
    try:
        service.ingest(ingest(2, [block("X", None, 2)]))
        raise AssertionError("expected MalformedDataError")
    except MalformedDataError as e:
        assert e.category == "malformed_data"
    assert {b.block_hash for b in service.list_blocks()} == {"G"}


def test_block_above_progress_is_malformed(service):
    logger.info("INPUT: block height 5 under progress 3")
    try:
        service.ingest(ingest(3, [block("X", None, 5)]))
        raise AssertionError("expected MalformedDataError")
    except MalformedDataError as e:
        assert e.category == "malformed_data"


def test_conflicting_block_content_is_malformed(service):
    service.ingest(ingest(0, [block("G", None, 0)]))
    logger.info("INPUT: same hash G with different parent/height")
    try:
        service.ingest(ingest(1, [block("G", "Z", 1)]))
        raise AssertionError("expected MalformedDataError")
    except MalformedDataError as e:
        assert e.category == "malformed_data"
    # original content untouched
    g = service.list_blocks()[0]
    assert g.parent_hash is None and g.height == 0


def test_parent_height_mismatch_is_malformed(service):
    service.ingest(ingest(0, [block("G", None, 0)]))
    logger.info("INPUT: block claims parent G but height 5")
    try:
        service.ingest(ingest(5, [block("X", "G", 5)]))
        raise AssertionError("expected MalformedDataError")
    except MalformedDataError as e:
        assert e.category == "malformed_data"


def test_tx_misassigned_to_other_block_is_malformed(service):
    bad = block("A1", "G", 1, txs=[tx("t1", "OTHER", 0)])
    logger.info("INPUT: tx block_hash disagrees with carrier block")
    try:
        service.ingest(ingest(1, [bad]))
        raise AssertionError("expected MalformedDataError")
    except MalformedDataError as e:
        assert e.category == "malformed_data"


def test_error_categories_are_distinguishable(service):
    from app.errors import (
        ReorgTooDeepError, TemporaryUnavailableError, IngestError,
    )
    cats = {
        MalformedDataError.category, StateConflictError.category,
        ReorgTooDeepError.category, TemporaryUnavailableError.category,
    }
    assert len(cats) == 4
    logger.info("JUDGE: four distinct error categories=%s", sorted(cats))


def test_lock_contention_is_temporary_unavailable(settings):
    import threading
    import time
    from app.storage import Storage
    from app.errors import TemporaryUnavailableError

    busy = Storage("sqlite:///:memory:", lock_timeout_seconds=0.2)
    slow = IndexerService(busy, settings)
    released = threading.Event()

    def hold():
        with busy.write_tx():
            released.wait(timeout=5)

    t = threading.Thread(target=hold)
    t.start()
    time.sleep(0.05)
    logger.info("INPUT: ingest while write lock is held by another thread")
    try:
        slow.ingest(ingest(0, [block("G", None, 0)]))
        raise AssertionError("expected TemporaryUnavailableError")
    except TemporaryUnavailableError as e:
        logger.info("JUDGE: category=%s detail=%s", e.category, e)
        assert e.category == "temporary_unavailable"
    finally:
        released.set()
        t.join()


def test_http_layer_maps_categories(client):
    # malformed JSON-shape => 422 from pydantic; regression => 409
    r = client.post("/ingest", json={"progress": -1, "blocks": []})
    assert r.status_code == 422

    client.post("/ingest", json={"progress": 1, "blocks": []})
    r = client.post("/ingest", json={"progress": 0, "blocks": []})
    assert r.status_code == 409
    assert r.json()["reason"] == "state_conflict"
    logger.info("JUDGE: HTTP status and reason category correct -> PASS")
