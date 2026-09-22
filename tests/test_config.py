"""Configuration overrides through environment variables."""

import logging

from app import config

logger = logging.getLogger("indexer")


def test_env_overrides(monkeypatch):
    monkeypatch.setenv("INDEXER_MAX_REORG_DEPTH", "128")
    monkeypatch.setenv("INDEXER_LOCK_TIMEOUT_SECONDS", "0.5")
    monkeypatch.setenv("INDEXER_DATABASE_URL", "sqlite:///./custom.db")
    s = config.get_settings()
    logger.info("INPUT: env overrides -> JUDGE %s", s)
    assert s.max_reorg_depth == 128
    assert s.lock_timeout_seconds == 0.5
    assert s.database_url == "sqlite:///./custom.db"


def test_defaults(monkeypatch):
    for k in ("INDEXER_MAX_REORG_DEPTH", "INDEXER_LOCK_TIMEOUT_SECONDS",
              "INDEXER_DATABASE_URL"):
        monkeypatch.delenv(k, raising=False)
    s = config.get_settings()
    assert (s.max_reorg_depth, s.lock_timeout_seconds) == (64, 5.0)
