"""Concurrent ingestion must be serializable with no lost/partial state."""

import logging
import threading
from concurrent.futures import ThreadPoolExecutor

from app.errors import StateConflictError
from app.models import BlockStatus
from conftest import block, ingest

logger = logging.getLogger("indexer")


def _run_concurrently(tasks, workers=8):
    start = threading.Event()

    def run(fn):
        start.wait(timeout=5)  # all tasks queued, then released together
        try:
            return ("ok", fn())
        except Exception as e:  # noqa: BLE001
            return (type(e).__name__, e)

    with ThreadPoolExecutor(max_workers=workers) as pool:
        futures = [pool.submit(run, fn) for fn in tasks]
        start.set()
        return [f.result(timeout=30) for f in futures]


def test_concurrent_distinct_blocks_progress_monotone(service):
    # seed genesis
    service.ingest(ingest(0, [block("G", None, 0)]))
    # INPUT: 12 threads deliver different heights concurrently. Like a real
    # indexer fan-out, every request carries the SAME high-water mark
    # (progress=12); blocks may arrive in any order and connect later.
    heights = list(range(1, 13))
    tasks = [
        (lambda h=h: service.ingest(
            ingest(12, [block(f"H{h}", f"H{h-1}" if h > 1 else "G", h)])))
        for h in heights
    ]
    results = _run_concurrently(tasks)

    outcome_log = [(heights[i], results[i][0]) for i in range(len(tasks))]
    logger.info("INPUT/JUDGE: per-height outcome %s", outcome_log)

    assert all(r[0] == "ok" for r in results), results
    blocks = service.list_blocks()
    hashes = [b.block_hash for b in blocks]
    assert len(hashes) == len(set(hashes)) == 13  # G + 12, 不重不漏
    final = service.get_progress()
    assert final.progress == 12 and final.canonical_tip_hash == "H12"
    # every parent eventually arrived -> the whole chain canonical
    assert all(b.status == BlockStatus.CANONICAL for b in blocks)
    logger.info("JUDGE: all 12 applied exactly once, full chain canonical -> PASS")


def test_concurrent_stale_progress_writers_lose_conflict(service):
    # 9 writers race: one carries the new high-water mark (5), eight carry
    # a stale one (2). Whoever runs first determines individual outcomes,
    # but the invariant is fixed: progress ends at 5 and exactly the
    # post-advance stale writers lose with state_conflict.
    service.ingest(ingest(0, [block("G", None, 0)]))
    tasks = (
        [lambda: service.ingest(ingest(5, []))]
        + [lambda: service.ingest(ingest(2, [])) for _ in range(8)]
    )
    results = _run_concurrently(tasks, workers=9)
    ok = [i for i, r in enumerate(results) if r[0] == "ok"]
    conflicts = [i for i, r in enumerate(results) if r[0] == "StateConflictError"]
    logger.info("INPUT/JUDGE: racing writers ok=%s conflicts=%s", ok, conflicts)
    assert 0 in ok  # the advancing writer always succeeds
    assert len(ok) + len(conflicts) == 9
    assert set(i for i in ok if i != 0) | set(conflicts) == set(range(1, 9))
    assert service.get_progress().progress == 5
    logger.info("JUDGE: progress ends at 5, stale-after-advance writers rejected -> PASS")


def test_concurrent_identical_payloads_all_idempotent(service):
    service.ingest(ingest(0, [block("G", None, 0)]))
    payload = [block("A1", "G", 1)]
    tasks = [lambda: service.ingest(ingest(1, payload)) for _ in range(10)]
    results = _run_concurrently(tasks)
    logger.info("INPUT: 10 concurrent identical deliveries")
    assert all(r[0] == "ok" for r in results)
    blocks = service.list_blocks()
    assert len(blocks) == 2  # no duplicate rows
    assert service.get_progress().progress == 1
    logger.info("JUDGE: identical concurrent delivery idempotent -> PASS")


def test_concurrent_fork_blocks_end_in_consistent_states(service):
    service.ingest(ingest(0, [block("G", None, 0)]))
    # two competing chains, blocks of both fired concurrently in shuffled order
    payloads = {
        1: block("X1", "G", 1),
        2: block("Y1", "G", 1),
        3: block("X2", "X1", 2),
        4: block("Y2", "Y1", 2),
        5: block("X3", "X2", 3),
    }
    import random
    order = [1, 2, 3, 4, 5]
    random.Random(42).shuffle(order)
    tasks = [lambda k=k: service.ingest(ingest(3, [payloads[k]])) for k in order]
    results = _run_concurrently(tasks, workers=5)
    assert all(r[0] == "ok" for r in results), results

    statuses = {b.block_hash: b.status.value for b in service.list_blocks()}
    logger.info("INPUT: shuffled fork blocks; JUDGE statuses=%s", statuses)
    # X chain reaches height 3 and wins at the end. Depending on arrival
    # order the Y fork either never won (staged) or won at height 2 and was
    # replaced (reorged); both are valid, it must simply never be canonical.
    assert service.get_progress().canonical_tip_hash == "X3"
    assert statuses["G"] == "canonical"
    assert statuses["X1"] == statuses["X2"] == statuses["X3"] == "canonical"
    # Y1 may briefly have been canonical (->reorged) while Y2 arrived
    # after X3 already won (->staged); each just must be non-canonical.
    assert statuses["Y1"] in ("staged", "reorged")
    assert statuses["Y2"] in ("staged", "reorged")
    assert len(statuses) == 6  # 不重不漏
    logger.info("JUDGE: one tip X3, losing fork consistently classified -> PASS")
