"""Indexer configuration.

Values can be overridden through environment variables (prefix
``INDEXER_``), e.g. ``INDEXER_MAX_REORG_DEPTH=100``.
"""

from __future__ import annotations

import os
from dataclasses import dataclass


@dataclass(frozen=True)
class Settings:
    # Maximum reorganization depth (in blocks) that will be accepted.
    max_reorg_depth: int = 64
    # Seconds to wait for the storage write lock / busy database before
    # failing as temporarily unavailable.
    lock_timeout_seconds: float = 5.0
    # SQLite database URL used when the service is built directly.
    database_url: str = "sqlite:///./indexer.db"


def _env(name: str, default):
    raw = os.environ.get(name)
    if raw is None or raw == "":
        return default
    return type(default)(raw)


def get_settings() -> Settings:
    return Settings(
        max_reorg_depth=_env("INDEXER_MAX_REORG_DEPTH", 64),
        lock_timeout_seconds=_env("INDEXER_LOCK_TIMEOUT_SECONDS", 5.0),
        database_url=os.environ.get("INDEXER_DATABASE_URL",
                                    "sqlite:///./indexer.db"),
    )
