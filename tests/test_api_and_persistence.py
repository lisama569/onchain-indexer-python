"""HTTP surface and durability across service restart."""

import logging
import tempfile

from app.config import Settings
from app.service import IndexerService
from app.storage import Storage
from fastapi.testclient import TestClient

from conftest import block, fork_ap, ingest, seed_chain_a

logger = logging.getLogger("indexer")


def test_http_happy_path_and_status_filters(client):
    seed_chain_a(client.app.state.service)
    logger.info("INPUT: GET /blocks, /transactions, /progress over seeded chain")

    r = client.get("/progress")
    assert r.status_code == 200 and r.json()["progress"] == 3

    r = client.get("/blocks")
    assert len(r.json()) == 4
    r = client.get("/blocks", params={"status": "canonical"})
    assert len(r.json()) == 4
    r = client.get("/blocks", params={"status": "staged"})
    assert r.json() == []
    r = client.get("/transactions", params={"status": "canonical"})
    assert len(r.json()) == 4
    logger.info("JUDGE: endpoints and status filters -> PASS")


def test_http_ingest_duplicate_is_idempotent(client):
    body = ingest(0, [block("G", None, 0)]).model_dump()
    r1 = client.post("/ingest", json=body)
    r2 = client.post("/ingest", json=body)  # equal progress, same block
    assert r1.status_code == r2.status_code == 200
    assert len(client.get("/blocks").json()) == 1
    logger.info("JUDGE: duplicate POST idempotent -> PASS")


def test_http_malformed_payload_422(client):
    # missing required fields
    r = client.post("/ingest", json={"blocks": [{"block_hash": "X"}]})
    assert r.status_code == 422
    # semantically malformed: genesis with parent (passes pydantic)
    r = client.post("/ingest", json={
        "progress": 0,
        "blocks": [{"block_hash": "X", "parent_hash": "Y", "height": 0,
                    "transactions": []}],
    })
    assert r.status_code == 422
    body = r.json()
    assert body["accepted"] is False
    assert body["reason"] == "malformed_data"
    logger.info("INPUT/JUDGE: malformed -> 422 reason=%s", body["reason"])


def test_http_reorg_too_deep_422_distinct_reason(client, settings):
    seed_chain_a(client.app.state.service)
    fork = [block("B1p", "G", 1), block("B2p", "B1p", 2),
            block("B3p", "B2p", 3), block("B4p", "B3p", 4)]
    r = client.post("/ingest", json=ingest(4, fork).model_dump())
    assert r.status_code == 422
    assert r.json()["reason"] == "reorg_too_deep"
    logger.info("JUDGE: reorg-too-deep distinguishable from malformed 422 -> PASS")


def test_state_survives_restart_with_file_db(settings):
    with tempfile.TemporaryDirectory() as tmp:
        db = f"sqlite:///{tmp}/idx.db"

        first = IndexerService(Storage(db, settings.lock_timeout_seconds), settings)
        seed_chain_a(first)
        first.ingest(ingest(4, fork_ap()))  # A2/A3 reorged, fork canonical
        before = {b.block_hash: b.status.value for b in first.list_blocks()}
        logger.info("INPUT: built state before restart: %s", before)

        # simulate process restart: brand new Storage over the same file
        second = IndexerService(Storage(db, settings.lock_timeout_seconds), settings)
        after = {b.block_hash: b.status.value for b in second.list_blocks()}
        assert before == after
        prog = second.get_progress()
        assert prog.progress == 4 and prog.canonical_tip_hash == "A4p"
        txs = {t.tx_hash: t.status.value for t in second.list_transactions()}
        assert txs["tx-A3"] == "reorged" and txs["tx-A4p"] == "canonical"
        # progress regression is still rejected after restart
        from app.errors import StateConflictError
        try:
            second.ingest(ingest(0, []))
            raise AssertionError("expected conflict")
        except StateConflictError as e:
            assert e.category == "state_conflict"
        logger.info("JUDGE: progress + three states durable across restart -> PASS")
