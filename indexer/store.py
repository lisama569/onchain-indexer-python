"""SQLite 持久化存储：区块、交易、规范链标记、摄取进度与重组审计。"""

from __future__ import annotations

import json
import sqlite3
import threading
from contextlib import contextmanager

from .errors import LockTimeoutError, StateConflictError
from .models import (Block, BlockSnapshot, BlockStatus, ReorgAuditRecord,
                     StoreSnapshot, Transaction)

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

CREATE TABLE IF NOT EXISTS reorg_audits (
    id                 INTEGER PRIMARY KEY AUTOINCREMENT,
    created_at_ns      INTEGER NOT NULL,
    trigger_block_hash TEXT NOT NULL,
    fork_height        INTEGER NOT NULL,
    depth              INTEGER NOT NULL,
    old_tip_hash       TEXT NOT NULL,
    old_tip_height     INTEGER NOT NULL,
    new_tip_hash       TEXT NOT NULL,
    new_tip_height     INTEGER NOT NULL,
    replaced_blocks    TEXT NOT NULL,
    readopted_blocks   TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_audits_created
    ON reorg_audits(created_at_ns DESC, id DESC);
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


def _row_to_audit(row: sqlite3.Row) -> ReorgAuditRecord:
    return ReorgAuditRecord(
        id=row["id"],
        created_at_ns=row["created_at_ns"],
        trigger_block_hash=row["trigger_block_hash"],
        fork_height=row["fork_height"],
        depth=row["depth"],
        old_tip_hash=row["old_tip_hash"],
        old_tip_height=row["old_tip_height"],
        new_tip_hash=row["new_tip_hash"],
        new_tip_height=row["new_tip_height"],
        replaced_blocks=tuple(json.loads(row["replaced_blocks"])),
        readopted_blocks=tuple(json.loads(row["readopted_blocks"])),
    )


class Store:
    """线程安全的 SQLite 存储封装，多步写入在单事务内完成。"""

    def __init__(self, db_path: str = ":memory:",
                 lock_timeout: float = 5.0) -> None:
        # isolation_level=None -> 自动提交，由 transaction() 显式控制事务边界
        self._conn = sqlite3.connect(db_path, check_same_thread=False,
                                     isolation_level=None)
        self._conn.row_factory = sqlite3.Row
        self._lock = threading.RLock()
        self._tx_depth = 0          # 仅由持锁线程访问：保存点嵌套层级
        self._lock_timeout = lock_timeout
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
    def transaction(self, timeout: float | None = None):
        """多步写入要么全部提交要么全部回滚；并发写由锁串行化。

        - 等待锁超过 timeout（默认取构造期 lock_timeout）抛 LockTimeoutError，
          归类为可重试的临时故障，不会无限等待。
        - 可重入：同一线程在外层事务内再次进入时使用 SAVEPOINT，内层回滚
          到保存点并向外抛出，由外层决定整体回滚，保证「整批失败即全不落库」。
        """
        timeout = self._lock_timeout if timeout is None else timeout
        if not self._lock.acquire(timeout=timeout):
            raise LockTimeoutError(
                f"等待写锁超时（{timeout}s），本次写入未执行，可重试",
                detail={"timeout_seconds": timeout},
            )
        try:
            if self._tx_depth == 0:
                self._conn.execute("BEGIN IMMEDIATE")
                self._tx_depth = 1
                try:
                    yield
                except BaseException:
                    self._conn.execute("ROLLBACK")
                    raise
                else:
                    self._conn.execute("COMMIT")
                finally:
                    self._tx_depth = 0
            else:
                sp = f"sp_{self._tx_depth}"
                self._conn.execute(f"SAVEPOINT {sp}")
                self._tx_depth += 1
                try:
                    yield
                except BaseException:
                    self._conn.execute(f"ROLLBACK TO SAVEPOINT {sp}")
                    self._conn.execute(f"RELEASE SAVEPOINT {sp}")
                    raise
                else:
                    self._conn.execute(f"RELEASE SAVEPOINT {sp}")
                finally:
                    self._tx_depth -= 1
        finally:
            self._lock.release()

    # ---- 重组审计 ----
    def insert_reorg_audit(self, record: ReorgAuditRecord) -> None:
        """写入一条切换审计；必须在调用方事务内调用，随事务同生共死。"""
        self._conn.execute(
            "INSERT INTO reorg_audits(created_at_ns, trigger_block_hash,"
            " fork_height, depth, old_tip_hash, old_tip_height,"
            " new_tip_hash, new_tip_height, replaced_blocks, readopted_blocks)"
            " VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (record.created_at_ns, record.trigger_block_hash,
             record.fork_height, record.depth,
             record.old_tip_hash, record.old_tip_height,
             record.new_tip_hash, record.new_tip_height,
             json.dumps(list(record.replaced_blocks)),
             json.dumps(list(record.readopted_blocks))),
        )

    def list_reorg_audits(self, limit: int = 50,
                          offset: int = 0) -> list[ReorgAuditRecord]:
        """按产生时间倒序（新在前）翻查审计记录。"""
        with self._lock:
            rows = self._conn.execute(
                "SELECT * FROM reorg_audits"
                " ORDER BY created_at_ns DESC, id DESC"
                " LIMIT ? OFFSET ?",
                (limit, offset),
            ).fetchall()
            return [_row_to_audit(r) for r in rows]

    def count_reorg_audits(self) -> int:
        with self._lock:
            return self._conn.execute(
                "SELECT COUNT(*) AS n FROM reorg_audits").fetchone()["n"]

    def truly_was_canonical(self, block_hash: str) -> bool:
        """该块是否在**实际发生过的**规范链切换历史中真正上过规范链。

        用来与「仅按参考口径补判为 REORGED、实际投递顺序中从未登上规范链」
        的块做区分，避免补判块在后续真实切换里被误算进 readopted 名单。

        判定依据是只追加、与状态翻转同事务的真实审计链：块只要出现在某条
        审计的 replaced_blocks（那次切换前它确实在旧规范链上）或
        readopted_blocks（它曾随某次切换真正回到规范链）中，就真正规范过。
        参考补判不写审计，因此补判块永远不会出现在这两份名单里。

        调用方须已持有存储锁/处于事务内（不再加锁，调用均在临界区深处）。
        """
        rows = self._conn.execute(
            "SELECT replaced_blocks, readopted_blocks FROM reorg_audits"
            " ORDER BY id ASC").fetchall()
        for r in rows:
            if block_hash in set(json.loads(r["replaced_blocks"])):
                return True
            if block_hash in set(json.loads(r["readopted_blocks"])):
                return True
        return False

    # ---- 只读一致性快照 ----
    def snapshot(self) -> StoreSnapshot:
        """在与写入互斥的临界区内一次性读出全量数据，供只读自检使用。

        快照在单连接、读锁保护下取得；与写入事务互斥，因此自检不会读到
        改了一半的中间状态，且自身不做任何写入。
        """
        with self._lock:
            if self._tx_depth > 0:
                # 同线程已在事务内：直接读到的就是本事务的一致视图
                return self._take_snapshot()
            # 显式只读事务，保证三张表的读取落在同一快照上
            self._conn.execute("BEGIN")
            try:
                snap = self._take_snapshot()
            finally:
                self._conn.execute("ROLLBACK")  # 未做任何写入，仅释放快照
            return snap

    def _take_snapshot(self) -> StoreSnapshot:
        block_rows = self._conn.execute(
            "SELECT * FROM blocks ORDER BY height, block_hash").fetchall()
        blocks = tuple(
            BlockSnapshot(block_hash=r["block_hash"],
                          parent_hash=r["parent_hash"],
                          height=r["height"],
                          status=BlockStatus(r["status"]))
            for r in block_rows
        )
        tx_rows = self._conn.execute(
            "SELECT * FROM txs ORDER BY rowid").fetchall()
        txs = tuple(_row_to_tx(r) for r in tx_rows)
        prow = self._conn.execute(
            "SELECT height FROM progress WHERE id = 1").fetchone()
        return StoreSnapshot(blocks=blocks, txs=txs,
                             progress=prow["height"] if prow else -1)

    def close(self) -> None:
        self._conn.close()
