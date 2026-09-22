"""SQLite-backed storage.

A single in-process :class:`threading.RLock` plus ``BEGIN IMMEDIATE``
transactions serializes every ingest, so concurrent ingest requests
can never observe a partially applied state or overwrite each other.
"""

from __future__ import annotations

import sqlite3
import threading
from collections.abc import Callable
from contextlib import contextmanager
from typing import TypeVar

T = TypeVar("T")


class Storage:
    def __init__(self, database_url: str, lock_timeout_seconds: float = 5.0) -> None:
        prefix = "sqlite:///"
        if not database_url.startswith(prefix):
            raise ValueError(f"unsupported database url: {database_url}")
        path = database_url[len(prefix):]
        self._path = ":memory:" if path in ("", ":memory:") else path
        self._timeout = lock_timeout_seconds
        self._write_lock = threading.RLock()
        # One persistent connection: required so that ``:memory:``
        # databases survive, and it also removes connect-per-call
        # overhead. All access is serialized by ``_write_lock``.
        self._conn = self._connect()
        self._init_schema()

    # ------------------------------------------------------------------
    # connection / schema
    # ------------------------------------------------------------------
    def _connect(self) -> sqlite3.Connection:
        # check_same_thread=False is safe because every call is
        # serialized through ``_write_lock``.
        conn = sqlite3.connect(
            self._path, timeout=self._timeout, check_same_thread=False
        )
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA foreign_keys = ON")
        return conn

    def _init_schema(self) -> None:
        with self._write_lock:
            self._conn.executescript(
                """
                CREATE TABLE IF NOT EXISTS blocks (
                    block_hash  TEXT PRIMARY KEY,
                    parent_hash TEXT,
                    height      INTEGER NOT NULL CHECK (height >= 0),
                    status      TEXT NOT NULL
                                CHECK (status IN ('canonical', 'staged', 'reorged'))
                );
                CREATE TABLE IF NOT EXISTS transactions (
                    tx_hash    TEXT PRIMARY KEY,
                    block_hash TEXT NOT NULL REFERENCES blocks(block_hash),
                    tx_index   INTEGER NOT NULL CHECK (tx_index >= 0),
                    sender     TEXT NOT NULL,
                    recipient  TEXT NOT NULL,
                    amount     INTEGER NOT NULL CHECK (amount >= 0),
                    status     TEXT NOT NULL
                               CHECK (status IN ('canonical', 'staged', 'reorged')),
                    UNIQUE (block_hash, tx_index)
                );
                CREATE TABLE IF NOT EXISTS meta (
                    key   TEXT PRIMARY KEY,
                    value TEXT NOT NULL
                );
                """
            )

    # ------------------------------------------------------------------
    # transaction primitives
    # ------------------------------------------------------------------
    @contextmanager
    def write_tx(self):
        """Acquire the process-wide write lock for the duration of a block."""
        if not self._write_lock.acquire(timeout=self._timeout):
            raise TimeoutError("write lock acquisition timed out")
        try:
            yield
        finally:
            self._write_lock.release()

    def transactional(self, fn: Callable[[sqlite3.Connection], T]) -> T:
        """Run ``fn`` while holding the write lock and one DB transaction.

        Commits on success, rolls back on any exception. The whole ingest
        runs inside one ``BEGIN IMMEDIATE`` transaction so a crash or
        failure can never leave a partially applied state behind.
        """
        if not self._write_lock.acquire(timeout=self._timeout):
            raise TimeoutError("write lock acquisition timed out")
        try:
            conn = self._conn
            try:
                conn.execute("BEGIN IMMEDIATE")
                result = fn(conn)
                conn.commit()
                return result
            except BaseException:
                conn.rollback()
                raise
        finally:
            self._write_lock.release()

    # ------------------------------------------------------------------
    # reads
    # ------------------------------------------------------------------
    def query(self, sql: str, params: tuple = ()) -> list[sqlite3.Row]:
        with self._write_lock:
            return list(self._conn.execute(sql, params))

    def query_one(self, sql: str, params: tuple = ()) -> sqlite3.Row | None:
        with self._write_lock:
            return self._conn.execute(sql, params).fetchone()
