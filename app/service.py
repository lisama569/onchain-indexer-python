"""Core indexing service.

Ingest pipeline (all of it runs inside ONE ``BEGIN IMMEDIATE``
transaction, guarded by the storage write lock, so concurrent callers
are strictly serialized and a failure never leaves a partial result):

1. Normalize/validate the payload (malformed -> ``MalformedDataError``).
2. Reject progress regression (``StateConflictError``).
3. Upsert blocks/transactions; blocks not connected to the canonical
   chain stay ``staged`` and are promoted when their parent arrives.
4. Re-select the best (highest-tip; incumbent wins ties) canonical
   chain rooted at genesis. If the incumbent tip is replaced, measure
   reorg depth and reject ingests deeper than ``max_reorg_depth``.
5. Persist statuses and the monotone progress marker.
"""

from __future__ import annotations

import logging
import sqlite3

from app.config import Settings
from app.errors import (
    MalformedDataError,
    ReorgTooDeepError,
    StateConflictError,
    TemporaryUnavailableError,
)
from app.models import (
    BlockStatus,
    BlockView,
    IngestRequest,
    IngestResult,
    InTransaction,
    ProgressView,
    TransactionView,
)
from app.storage import Storage

logger = logging.getLogger("indexer")

CANONICAL = "canonical"
STAGED = "staged"
REORGED = "reorged"


class IndexerService:
    def __init__(self, storage: Storage, settings: Settings | None = None) -> None:
        self.storage = storage
        self.settings = settings or Settings()

    # ------------------------------------------------------------------
    # public API
    # ------------------------------------------------------------------
    def ingest(self, request: IngestRequest) -> IngestResult:
        logger.info("ingest request: progress=%s blocks=%s",
                    request.progress, [b.block_hash for b in request.blocks])
        try:
            result = self.storage.transactional(
                lambda conn: self._ingest(conn, request)
            )
        except (MalformedDataError, StateConflictError, ReorgTooDeepError):
            raise
        except (sqlite3.OperationalError, TimeoutError) as exc:
            # lock timeout / busy database — caller may safely retry
            logger.warning("ingest temporary unavailable: %s", exc)
            raise TemporaryUnavailableError(str(exc)) from exc
        logger.info(
            "ingest accepted: progress=%s canonical=%s staged=%s reorged=%s",
            result.progress, result.canonical_blocks,
            result.staged_blocks, result.reorged_blocks,
        )
        return result

    def list_blocks(self, status: BlockStatus | None = None) -> list[BlockView]:
        rows = self.storage.query(
            "SELECT block_hash, parent_hash, height, status FROM blocks "
            "ORDER BY height, block_hash"
        )
        views = [
            BlockView(
                block_hash=r["block_hash"], parent_hash=r["parent_hash"],
                height=r["height"], status=BlockStatus(r["status"]),
            )
            for r in rows
        ]
        return [v for v in views if status is None or v.status == status]

    def list_transactions(self, status: BlockStatus | None = None) -> list[TransactionView]:
        rows = self.storage.query(
            "SELECT tx_hash, block_hash, tx_index, sender, recipient, amount, status "
            "FROM transactions ORDER BY block_hash, tx_index"
        )
        views = [
            TransactionView(
                tx_hash=r["tx_hash"], block_hash=r["block_hash"],
                tx_index=r["tx_index"], sender=r["sender"],
                recipient=r["recipient"], amount=r["amount"],
                status=BlockStatus(r["status"]),
            )
            for r in rows
        ]
        return [v for v in views if status is None or v.status == status]

    def get_progress(self) -> ProgressView:
        row = self.storage.query_one("SELECT value FROM meta WHERE key='progress'")
        progress = int(row["value"]) if row else 0
        tip = self.storage.query_one(
            "SELECT block_hash, height FROM blocks WHERE status=? ORDER BY height DESC LIMIT 1",
            (CANONICAL,),
        )
        return ProgressView(
            progress=progress,
            canonical_tip_hash=tip["block_hash"] if tip else None,
            canonical_tip_height=tip["height"] if tip else None,
        )

    # ------------------------------------------------------------------
    # ingest internals
    # ------------------------------------------------------------------
    def _ingest(self, conn: sqlite3.Connection, request: IngestRequest) -> IngestResult:
        progress = self._get_meta(conn, "progress")
        if progress is not None and request.progress < int(progress):
            logger.info("reject: progress regression stored=%s got=%s",
                        progress, request.progress)
            raise StateConflictError(
                f"progress regression: stored={progress} got={request.progress}"
            )

        blocks = self._load_blocks(conn)
        txns = self._load_txns(conn)
        old_tip = self._get_meta(conn, "canonical_tip_hash") or None

        self._apply_payload(blocks, txns, request)
        canonical_hashes, new_tip = self._select_canonical(blocks, old_tip)
        self._check_reorg_depth(blocks, canonical_hashes, old_tip, new_tip)
        counts = self._persist(conn, blocks, txns, canonical_hashes,
                               request.progress, new_tip)

        return IngestResult(accepted=True, progress=request.progress, **counts)

    # -- normalization ------------------------------------------------
    def _apply_payload(
        self,
        blocks: dict[str, dict],
        txns: dict[str, dict],
        request: IngestRequest,
    ) -> None:
        """Validate and merge the payload into the in-memory snapshots.

        Duplicate delivery of identical data is a no-op; any identity
        conflict or structural inconsistency raises MalformedDataError.
        """
        seen_blocks: set[str] = set()
        seen_txns: set[str] = set()

        for block in request.blocks:
            logger.info("normalize block hash=%s height=%s parent=%s txs=%d",
                        block.block_hash, block.height,
                        block.parent_hash, len(block.transactions))
            if block.block_hash in seen_blocks:
                raise MalformedDataError(
                    f"duplicate block in payload: {block.block_hash}")
            seen_blocks.add(block.block_hash)

            if block.height > request.progress:
                raise MalformedDataError(
                    f"block {block.block_hash} height {block.height} "
                    f"exceeds progress {request.progress}"
                )
            # parent linkage sanity (parent may arrive later -> staged)
            if block.height == 0 and block.parent_hash is not None:
                raise MalformedDataError(
                    f"genesis block {block.block_hash} must have null parent")
            if block.height > 0 and block.parent_hash is None:
                raise MalformedDataError(
                    f"non-genesis block {block.block_hash} requires a parent")

            existing = blocks.get(block.block_hash)
            if existing is not None:
                if existing["height"] != block.height or \
                        existing["parent_hash"] != block.parent_hash:
                    raise MalformedDataError(
                        f"conflicting block content for {block.block_hash}: "
                        f"stored height={existing['height']} "
                        f"parent={existing['parent_hash']}"
                    )
                logger.info("block %s already known, idempotent merge",
                            block.block_hash)
            else:
                blocks[block.block_hash] = {
                    "parent_hash": block.parent_hash,
                    "height": block.height,
                    "status": STAGED,  # finalized during canonical selection
                }

            # parent height check only when parent is currently known
            if block.parent_hash is not None and block.parent_hash in blocks:
                if blocks[block.parent_hash]["height"] != block.height - 1:
                    raise MalformedDataError(
                        f"block {block.block_hash} height {block.height} does "
                        f"not extend parent {block.parent_hash} height "
                        f"{blocks[block.parent_hash]['height']}"
                    )

            seen_index: set[int] = set()
            for tx in block.transactions:
                if tx.block_hash != block.block_hash:
                    raise MalformedDataError(
                        f"tx {tx.tx_hash} declares block {tx.block_hash} but "
                        f"was carried by {block.block_hash}")
                if tx.tx_index in seen_index:
                    raise MalformedDataError(
                        f"duplicate tx_index {tx.tx_index} in block "
                        f"{block.block_hash}")
                seen_index.add(tx.tx_index)
                if tx.tx_hash in seen_txns:
                    raise MalformedDataError(
                        f"duplicate tx in payload: {tx.tx_hash}")
                seen_txns.add(tx.tx_hash)

                old = txns.get(tx.tx_hash)
                if old is not None:
                    if old["block_hash"] != tx.block_hash:
                        raise MalformedDataError(
                            f"tx {tx.tx_hash} reassigned from block "
                            f"{old['block_hash']} to {tx.block_hash}")
                    self._check_tx_unchanged(old, tx)
                    logger.info("tx %s already known, idempotent merge",
                                tx.tx_hash)
                else:
                    txns[tx.tx_hash] = {
                        "block_hash": tx.block_hash,
                        "tx_index": tx.tx_index,
                        "sender": tx.sender,
                        "recipient": tx.recipient,
                        "amount": tx.amount,
                        "status": STAGED,
                    }

    @staticmethod
    def _check_tx_unchanged(old: dict, tx: InTransaction) -> None:
        for field in ("tx_index", "sender", "recipient", "amount"):
            if old[field] != getattr(tx, field):
                raise MalformedDataError(
                    f"conflicting tx content for {tx.tx_hash}: "
                    f"stored {field}={old[field]} got {getattr(tx, field)}"
                )

    # -- canonical chain selection ------------------------------------
    def _select_canonical(
        self, blocks: dict[str, dict], old_tip: str | None
    ) -> tuple[set[str], str | None]:
        """Pick the canonical chain among all fully connected chains.

        A chain is a parent-linked sequence starting at a height-0
        genesis (null parent). Orphaned/fork blocks whose ancestry is
        incomplete are excluded and stay ``staged``.

        Selection rule: highest tip wins; on equal height the incumbent
        tip's chain wins; otherwise the lexicographically smallest tip
        hash wins (deterministic and explainable).
        """
        # Build only well-formed chains: walk back from each block; a
        # chain is valid iff it terminates at a null-parent genesis and
        # every hop has the expected parent height.
        chains: list[tuple[str, list[str]]] = []  # (tip_hash, hash path)
        for h, b in blocks.items():
            path: list[str] = []
            cur: str | None = h
            guard = 0
            while cur is not None:
                guard += 1
                if guard > len(blocks) + 1:
                    path = []  # cycle / broken ancestry
                    break
                node = blocks.get(cur)
                if node is None:
                    path = []  # ancestor not delivered yet -> orphan fork
                    break
                path.append(cur)
                expected_h = blocks[h]["height"] - (len(path) - 1)
                if node["height"] != expected_h:
                    path = []
                    break
                cur = node["parent_hash"]
            if path and blocks[path[-1]]["height"] == 0 and \
                    blocks[path[-1]]["parent_hash"] is None:
                chains.append((h, list(reversed(path))))

        if not chains:
            logger.info("canonical selection: no fully connected chain")
            return set(), None

        def sort_key(entry: tuple[str, list[str]]):
            tip_hash, path = entry
            incumbent = 0 if tip_hash == old_tip else 1
            # prefer height desc, then incumbent, then smallest hash
            return (-len(path), incumbent, tip_hash)

        chains.sort(key=sort_key)
        tip_hash, path = chains[0]
        logger.info(
            "canonical selection: tip=%s height=%d (%d candidates)",
            tip_hash, len(path) - 1, len(chains),
        )
        return set(path), tip_hash

    def _check_reorg_depth(
        self,
        blocks: dict[str, dict],
        canonical_hashes: set[str],
        old_tip: str | None,
        new_tip: str | None,
    ) -> None:
        """Reject ingests that switch canonical chain too far back.

        Reorg depth = number of old-canonical blocks that get replaced,
        i.e. the height delta between the old tip and the highest common
        ancestor. Simple extension (same tip) is not a reorg and has
        depth 0; the very first canonical chain has depth 0 as well.
        """
        if old_tip is None or new_tip is None or old_tip == new_tip:
            logger.info("reorg check: depth=0 (old_tip=%s new_tip=%s)",
                        old_tip, new_tip)
            return

        old_block = blocks.get(old_tip)
        if old_block is None or old_tip in canonical_hashes:
            # old tip stays canonical: extension, nothing was replaced
            logger.info("reorg check: depth=0 (old tip retained)")
            return

        # walk the old chain back until we hit a block still canonical:
        # that is the highest common ancestor.
        depth = 1
        cur = old_block["parent_hash"]
        while cur is not None:
            if cur in canonical_hashes:
                break
            depth += 1
            node = blocks.get(cur)
            cur = node["parent_hash"] if node else None
        else:
            # old chain shares no ancestor with the new one at all
            depth = old_block["height"] + 1

        logger.info("reorg check: depth=%d limit=%d (old_tip=%s new_tip=%s)",
                    depth, self.settings.max_reorg_depth, old_tip, new_tip)
        if depth > self.settings.max_reorg_depth:
            raise ReorgTooDeepError(
                f"reorg depth {depth} exceeds limit "
                f"{self.settings.max_reorg_depth}"
            )

    # -- persistence ---------------------------------------------------
    def _persist(
        self,
        conn: sqlite3.Connection,
        blocks: dict[str, dict],
        txns: dict[str, dict],
        canonical_hashes: set[str],
        progress: int,
        new_tip: str | None,
    ) -> dict[str, int]:
        """Assign final statuses and upsert everything atomically.

        Status rule (three distinguishable states):
        * ``canonical`` — block is on the canonical chain right now;
        * ``reorged``   — not canonical anymore, but it once was;
        * ``staged``    — everything else (new orphans, detached forks
                          and blocks whose parent has not arrived yet).
        Transactions inherit the status of the block that owns them.
        """
        counts = {"canonical_blocks": 0, "staged_blocks": 0, "reorged_blocks": 0}

        for h, b in blocks.items():
            ever_canonical = b["status"] in (CANONICAL, REORGED)
            if h in canonical_hashes:
                b["status"] = CANONICAL
            elif ever_canonical:
                b["status"] = REORGED
            else:
                b["status"] = STAGED
            counts[f"{b['status']}_blocks"] += 1

            conn.execute(
                "INSERT INTO blocks(block_hash, parent_hash, height, status) "
                "VALUES(?, ?, ?, ?) ON CONFLICT(block_hash) DO UPDATE SET "
                "parent_hash=excluded.parent_hash, height=excluded.height, "
                "status=excluded.status",
                (h, b["parent_hash"], b["height"], b["status"]),
            )

        for h, t in txns.items():
            t["status"] = blocks[t["block_hash"]]["status"]
            conn.execute(
                "INSERT INTO transactions(tx_hash, block_hash, tx_index, "
                "sender, recipient, amount, status) VALUES(?, ?, ?, ?, ?, ?, ?) "
                "ON CONFLICT(tx_hash) DO UPDATE SET block_hash=excluded.block_hash, "
                "tx_index=excluded.tx_index, sender=excluded.sender, "
                "recipient=excluded.recipient, amount=excluded.amount, "
                "status=excluded.status",
                (h, t["block_hash"], t["tx_index"], t["sender"],
                 t["recipient"], t["amount"], t["status"]),
            )

        self._set_meta(conn, "progress", str(progress))
        self._set_meta(conn, "canonical_tip_hash", new_tip or "")
        return counts

    # ------------------------------------------------------------------
    # helpers
    # ------------------------------------------------------------------
    @staticmethod
    def _load_blocks(conn: sqlite3.Connection) -> dict[str, dict]:
        return {
            r["block_hash"]: {
                "parent_hash": r["parent_hash"],
                "height": r["height"],
                "status": r["status"],
            }
            for r in conn.execute("SELECT block_hash, parent_hash, height, status FROM blocks")
        }

    @staticmethod
    def _load_txns(conn: sqlite3.Connection) -> dict[str, dict]:
        return {
            r["tx_hash"]: {
                "block_hash": r["block_hash"],
                "tx_index": r["tx_index"],
                "sender": r["sender"],
                "recipient": r["recipient"],
                "amount": r["amount"],
                "status": r["status"],
            }
            for r in conn.execute(
                "SELECT tx_hash, block_hash, tx_index, sender, recipient, amount, status "
                "FROM transactions"
            )
        }

    @staticmethod
    def _get_meta(conn: sqlite3.Connection, key: str) -> str | None:
        row = conn.execute("SELECT value FROM meta WHERE key=?", (key,)).fetchone()
        return row["value"] if row else None

    @staticmethod
    def _set_meta(conn: sqlite3.Connection, key: str, value: str) -> None:
        conn.execute(
            "INSERT INTO meta(key, value) VALUES(?, ?) "
            "ON CONFLICT(key) DO UPDATE SET value=excluded.value",
            (key, value),
        )
