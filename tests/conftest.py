"""Shared fixtures: fixed local block data + in-memory indexer service."""

from __future__ import annotations

import logging

import pytest
from fastapi.testclient import TestClient

from app.api import create_app
from app.config import Settings
from app.service import IndexerService
from app.storage import Storage
from app.models import InBlock, InTransaction, IngestRequest


@pytest.fixture
def settings() -> Settings:
    return Settings(max_reorg_depth=2, lock_timeout_seconds=5.0)


@pytest.fixture
def service(settings: Settings) -> IndexerService:
    storage = Storage("sqlite:///:memory:", settings.lock_timeout_seconds)
    return IndexerService(storage, settings)


@pytest.fixture
def client(service: IndexerService) -> TestClient:
    return TestClient(create_app(service=service))


# ----------------------------------------------------------------------
# fixed local data builders
# ----------------------------------------------------------------------
def tx(tx_hash: str, block_hash: str, index: int = 0,
       amount: int = 100) -> InTransaction:
    return InTransaction(
        tx_hash=tx_hash, block_hash=block_hash, tx_index=index,
        sender=f"sender-{tx_hash}", recipient=f"recipient-{tx_hash}",
        amount=amount,
    )


def block(h: str, parent: str | None, height: int,
          txs: list[InTransaction] | None = None) -> InBlock:
    return InBlock(
        block_hash=h, parent_hash=parent, height=height,
        transactions=txs if txs is not None else [tx(f"tx-{h}", h)],
    )


def ingest(progress: int, blocks: list[InBlock]) -> IngestRequest:
    return IngestRequest(progress=progress, blocks=blocks)


# canonical fixture chain: G -> A1 -> A2 -> A3
def chain_a() -> list[InBlock]:
    return [
        block("G", None, 0),
        block("A1", "G", 1),
        block("A2", "A1", 2),
        block("A3", "A2", 3),
    ]


# competing fork from A1: A2p -> A3p
def fork_ap() -> list[InBlock]:
    return [
        block("A2p", "A1", 2),
        block("A3p", "A2p", 3),
        block("A4p", "A3p", 4),
    ]


def seed_chain_a(service: IndexerService) -> None:
    """Deliver G..A3 in four progress-monotone ingests."""
    for i, b in enumerate(chain_a()):
        service.ingest(ingest(i, [b]))


@pytest.fixture(autouse=True)
def _log_test_inputs(caplog: pytest.LogCaptureFixture):
    caplog.set_level(logging.INFO, logger="indexer")
    yield
