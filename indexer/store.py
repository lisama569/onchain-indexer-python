"""SQLite 持久化存储：区块、交易、规范链标记与摄取进度。"""

from __future__ import annotations

import sqlite3
import threading
from contextlib import contextmanager

from .errors import StateConflictError
from .models import Block, BlockStatus, Transaction

_SCHEMA = """
CREATE TABLE IF NOT EXISTS blocks (
    block_hash  TEXT PRIMARY KEY,
    parent_hash TEXT NOT NULL,
    height      INTEGER NOT NULL,
    timestamp   INTEGER NOT NULL,
    status      TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_blocks_height ON blocks(height);
CREATE INDEX IF NOT EXISTS idx_blocks_parent ON blocks(parent_hash);

CREATE TABLE IF NOT EXISTS txs (
    tx_hash    TEXT PRIMARY KEY,
    block_hash TEXT NOT NULL,
    sender     TEXT NOT NULL,
    recipient  TEXT NOT NULL,
    amount     INTEGER NOT NULL,
    nonce      INTEGER NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_txs_block ON txs(block_hash);

CREATE TABLE IF NOT EXISTS progress (
    id     INTEGER PRIMARY KEY CHECK (id = 1),
    height INTEGER NOT NULL
);
"""


def _row_to_block(row: sqlite3.Row, txs: tuple[Transaction, ...]) -> Block:
    return Block(
        block_hash=row["block_hash"],
        parent_hash=row["parent_hash"],
        height=row["height"],
        timestamp=row["timestamp"],
        transactions=txs,
    )


def _row_to_tx(row: sqlite3.Row) -> Transaction:
    return Transaction(
        tx_hash=row["tx_hash"],
        block_hash=row["block_hash"],
        sender=row["sender"],
        recipient=row["recipient"],
        amount=row["amount"],
        nonce=row["nonce"],
    )


class Store:
    """线程安全的 SQLite 存储封装，多步写入在单事务内完成。"""

    def __init__(self, db_path: str = ":memory:") -> None:
        # isolation_level=None -> 自动提交，由 transaction() 显式控制事务边界
        self._conn = sqlite3.connect(db_path, check_same_thread=False,
                                     isolation_level=None)
        self._conn.row_factory = sqlite3.Row
        self._lock = threading.RLock()
        self._init_schema()

    def _init_schema(self) -> None:
        with self._lock:
            self._conn.executescript(_SCHEMA)

    # ---- 区块与交易 ----
    def get_block(self, block_hash: str) -> tuple[Block, BlockStatus] | None:
        with self._lock:
            row = self._conn.execute(
                "SELECT * FROM blocks WHERE block_hash = ?", (block_hash,)
            ).fetchone()
            if row is None:
                return None
            txs = tuple(self.transactions_of(block_hash))
            return _row_to_block(row, txs), BlockStatus(row["status"])

    def upsert_block(self, block: Block, status: BlockStatus) -> bool:
        """插入区块及其交易。

        已存在且内容完全一致 -> 返回 False（幂等重放，不产生重复记录）；
        已存在但内容矛盾 -> 抛 StateConflictError。
        """
        with self._lock:
            existing = self.get_block(block.block_hash)
            if existing is not None:
                old_block, _ = existing
                if old_block == block:
                    return False
                raise StateConflictError(
                    f"block {block.block_hash} 已存在但内容不一致",
                    detail={"block_hash": block.block_hash},
                )
            self._conn.execute(
                "INSERT INTO blocks(block_hash, parent_hash, height, timestamp, status)"
                " VALUES (?, ?, ?, ?, ?)",
                (block.block_hash, block.parent_hash, block.height,
                 block.timestamp, status.value),
            )
            for tx in block.transactions:
                self._conn.execute(
                    "INSERT INTO txs(tx_hash, block_hash, sender, recipient, amount, nonce)"
                    " VALUES (?, ?, ?, ?, ?, ?)",
                    (tx.tx_hash, tx.block_hash, tx.sender, tx.recipient,
                     tx.amount, tx.nonce),
                )
            return True

    def set_block_status(self, block_hash: str, status: BlockStatus) -> None:
        with self._lock:
            self._conn.execute(
                "UPDATE blocks SET status = ? WHERE block_hash = ?",
                (status.value, block_hash),
            )

    def children_of(self, parent_hash: str) -> list[str]:
        with self._lock:
            rows = self._conn.execute(
                "SELECT block_hash FROM blocks WHERE parent_hash = ?",
                (parent_hash,),
            ).fetchall()
            return [r["block_hash"] for r in rows]

    def blocks_at_height(self, height: int,
                         status: BlockStatus | None = None) -> list[Block]:
        with self._lock:
            if status is None:
                rows = self._conn.execute(
                    "SELECT * FROM blocks WHERE height = ?", (height,)
                ).fetchall()
            else:
                rows = self._conn.execute(
                    "SELECT * FROM blocks WHERE height = ? AND status = ?",
                    (height, status.value),
                ).fetchall()
            return [_row_to_block(r, tuple(self.transactions_of(r["block_hash"])))
                    for r in rows]

    def list_blocks(self,
                    status: BlockStatus | None = None) -> list[tuple[Block, BlockStatus]]:
        with self._lock:
            if status is None:
                rows = self._conn.execute(
                    "SELECT * FROM blocks ORDER BY height, block_hash").fetchall()
            else:
                rows = self._conn.execute(
                    "SELECT * FROM blocks WHERE status = ? ORDER BY height, block_hash",
                    (status.value,),
                ).fetchall()
            return [(_row_to_block(r, tuple(self.transactions_of(r["block_hash"]))),
                     BlockStatus(r["status"])) for r in rows]

    def transactions_of(self, block_hash: str) -> list[Transaction]:
        with self._lock:
            rows = self._conn.execute(
                "SELECT * FROM txs WHERE block_hash = ? ORDER BY rowid",
                (block_hash,),
            ).fetchall()
            return [_row_to_tx(r) for r in rows]

    # ---- 进度 ----
    def get_progress(self) -> int:
        """已确认的规范链最高高度，无记录时为 -1。"""
        with self._lock:
            row = self._conn.execute(
                "SELECT height FROM progress WHERE id = 1").fetchone()
            return row["height"] if row is not None else -1

    def advance_progress(self, height: int) -> None:
        """进度只允许单调前进，回退抛 StateConflictError。"""
        with self._lock:
            current = self.get_progress()
            if height < current:
                raise StateConflictError(
                    f"进度回退被拒绝: {current} -> {height}",
                    detail={"current": current, "requested": height},
                )
            if height == current:
                return
            self._conn.execute(
                "INSERT INTO progress(id, height) VALUES (1, ?)"
                " ON CONFLICT(id) DO UPDATE SET height = excluded.height",
                (height,),
            )

    # ---- 事务 ----
    @contextmanager
    def transaction(self):
        """多步写入要么全部提交要么全部回滚；并发写由锁串行化。"""
        with self._lock:
            self._conn.execute("BEGIN IMMEDIATE")
            try:
                yield
            except Exception:
                self._conn.execute("ROLLBACK")
                raise
            else:
                self._conn.execute("COMMIT")

    def close(self) -> None:
        self._conn.close()
