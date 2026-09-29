"""SQLite 持久化存储：区块、交易、规范链标记、进度与重组审计。"""

from __future__ import annotations

import json
import sqlite3
import threading
from contextlib import contextmanager
from dataclasses import replace

from .errors import LockBusyError, StateConflictError, StorageError
from .models import Block, BlockStatus, ReorgEvent, Transaction

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

CREATE TABLE IF NOT EXISTS reorg_events (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    occurred_at     TEXT NOT NULL,
    depth           INTEGER NOT NULL,
    old_tip_hash    TEXT NOT NULL,
    old_tip_height  INTEGER NOT NULL,
    new_tip_hash    TEXT NOT NULL,
    new_tip_height  INTEGER NOT NULL,
    fork_height     INTEGER NOT NULL,
    replaced_hashes TEXT NOT NULL,
    adopted_hashes  TEXT NOT NULL,
    readopted_hashes TEXT NOT NULL
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
        # RLock：同一线程内“整批事务 + 逐块事务”可重入（内层用 SAVEPOINT）；
        # 持有该锁期间其它线程的写入被阻塞，读操作也不会看到改到一半的状态。
        self._lock = threading.RLock()
        # 事务嵌套状态是“线程私有”的：锁全局串行化，深度/保存点计数按线程独立
        self._local = threading.local()
        self._init_schema()

    @property
    def _depth(self) -> int:
        return getattr(self._local, "depth", 0)

    @_depth.setter
    def _depth(self, value: int) -> None:
        self._local.depth = value

    @property
    def _sp_counter(self) -> int:
        return getattr(self._local, "sp_counter", 0)

    @_sp_counter.setter
    def _sp_counter(self, value: int) -> None:
        self._local.sp_counter = value

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
            self._exec(
                "INSERT INTO blocks(block_hash, parent_hash, height, timestamp, status)"
                " VALUES (?, ?, ?, ?, ?)",
                (block.block_hash, block.parent_hash, block.height,
                 block.timestamp, status.value),
            )
            for tx in block.transactions:
                self._exec(
                    "INSERT INTO txs(tx_hash, block_hash, sender, recipient, amount, nonce)"
                    " VALUES (?, ?, ?, ?, ?, ?)",
                    (tx.tx_hash, tx.block_hash, tx.sender, tx.recipient,
                     tx.amount, tx.nonce),
                )
            return True

    def set_block_status(self, block_hash: str, status: BlockStatus) -> None:
        with self._lock:
            self._exec(
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

    def all_transactions(self) -> list[Transaction]:
        """全量交易行（含 block_hash 指向缺失块的孤儿行），供只读自检使用。"""
        with self._lock:
            rows = self._conn.execute(
                "SELECT * FROM txs ORDER BY rowid").fetchall()
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
            self._exec(
                "INSERT INTO progress(id, height) VALUES (1, ?)"
                " ON CONFLICT(id) DO UPDATE SET height = excluded.height",
                (height,),
            )

    # ---- 审计 ----
    def insert_reorg_event(self, event: ReorgEvent) -> ReorgEvent:
        """在当前事务内写入一条规范链切换审计记录。

        必须由调用方在 transaction() 内调用：切换事务回滚时本记录随之回滚，
        保证审计与状态切换同生共死。
        """
        cur = self._exec(
            "INSERT INTO reorg_events(occurred_at, depth, old_tip_hash,"
            " old_tip_height, new_tip_hash, new_tip_height, fork_height,"
            " replaced_hashes, adopted_hashes, readopted_hashes)"
            " VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (event.occurred_at, event.depth, event.old_tip_hash,
             event.old_tip_height, event.new_tip_hash, event.new_tip_height,
             event.fork_height, json.dumps(list(event.replaced_hashes)),
             json.dumps(list(event.adopted_hashes)),
             json.dumps(list(event.readopted_hashes))),
        )
        return replace(event, id=cur.lastrowid)

    def list_reorg_events(self, limit: int = 50) -> list[ReorgEvent]:
        """按时间（写入顺序 id）倒序返回审计记录，最新一条在前。"""
        with self._lock:
            rows = self._conn.execute(
                "SELECT * FROM reorg_events ORDER BY id DESC LIMIT ?",
                (max(1, limit),),
            ).fetchall()
            return [ReorgEvent(
                id=row["id"], occurred_at=row["occurred_at"],
                depth=row["depth"], old_tip_hash=row["old_tip_hash"],
                old_tip_height=row["old_tip_height"],
                new_tip_hash=row["new_tip_hash"],
                new_tip_height=row["new_tip_height"],
                fork_height=row["fork_height"],
                replaced_hashes=tuple(json.loads(row["replaced_hashes"])),
                adopted_hashes=tuple(json.loads(row["adopted_hashes"])),
                readopted_hashes=tuple(json.loads(row["readopted_hashes"])),
            ) for row in rows]

    # ---- 只读快照 ----
    @contextmanager
    def read_snapshot(self):
        """只读一致性视图。

        持有写锁直到快照内所有读取结束：写事务在锁外排队，自检因此不可能
        读到改了一半的中间状态；快照内不执行任何写入。
        """
        with self._lock:
            yield

    # ---- 事务 ----
    @contextmanager
    def transaction(self, timeout: float | None = None):
        """多步写入要么全部提交要么整体回滚。

        - 最外层：以 BEGIN IMMEDIATE 开启真正的数据库事务，锁等待超过
          ``timeout`` 秒抛 LockBusyError（可重试的临时故障）；
        - 同线程嵌套（整批事务里的逐块摄取）：用 SAVEPOINT 表达内层边界；
        - 底层 sqlite3 异常统一包装为 StorageError，不把原始数据库错误外泄。
        """
        if self._depth > 0:
            # 同线程重入：仅建立保存点，不重复抢锁、不重复 BEGIN
            self._sp_counter += 1
            sp = f"sp_{self._sp_counter}"
            self._exec(f"SAVEPOINT {sp}")
            self._depth += 1
            try:
                yield
            except BaseException:
                self._exec(f"ROLLBACK TO SAVEPOINT {sp}")
                self._exec(f"RELEASE SAVEPOINT {sp}")
                self._depth -= 1
                raise
            else:
                self._exec(f"RELEASE SAVEPOINT {sp}")
                self._depth -= 1
            return

        if timeout is None:
            acquired = self._lock.acquire()
        else:
            acquired = self._lock.acquire(timeout=timeout)
        if not acquired:
            raise LockBusyError(
                f"等待写锁超时（{timeout}s）：有其它摄取正在进行，可重试",
                detail={"timeout": timeout},
            )
        try:
            self._exec("BEGIN IMMEDIATE")
        except BaseException:
            self._lock.release()
            raise
        self._depth = 1
        try:
            yield
        except BaseException:
            self._safe_exec("ROLLBACK")
            self._depth = 0
            self._lock.release()
            raise
        else:
            try:
                self._exec("COMMIT")
            except BaseException:
                self._safe_exec("ROLLBACK")
                raise
            finally:
                self._depth = 0
                self._lock.release()

    def _safe_exec(self, sql: str) -> None:
        """异常路径下的尽力执行（如 ROLLBACK），不再向外抛二次异常。"""
        try:
            self._conn.execute(sql)
        except sqlite3.Error:
            pass

    def _exec(self, sql: str, params: tuple = ()):
        """执行 SQL 并把底层数据库异常包装成分类错误。"""
        try:
            return self._conn.execute(sql, params)
        except sqlite3.OperationalError as exc:
            if "locked" in str(exc).lower():
                raise LockBusyError(
                    f"数据库暂时被占用: {exc}",
                    detail={"sqlite_error": str(exc)},
                ) from exc
            raise StorageError(
                f"存储操作失败: {exc}",
                detail={"sqlite_error": str(exc)},
            ) from exc
        except sqlite3.Error as exc:
            raise StorageError(
                f"存储操作失败: {exc}",
                detail={"sqlite_error": str(exc)},
            ) from exc

    def close(self) -> None:
        self._conn.close()
