"""Records that this node just served a part from local flash, so eviction can order on use.

The drain-agent's evictor used to order on `resident_at` — when a part JOINED the cache, which
says nothing about whether anyone is still reading it. That is FIFO replacement, and FIFO is a
poor fit for the workload this tier exists for: a training set re-read every epoch is maximally
skewed, so evicting by arrival order tends to take exactly the parts about to be read again.
Each such eviction costs a peer-or-pool read plus a local write to put the same part back.

Migration 0017 added `cephor_ssd_residency.last_read_at` and the evictor now orders on
`COALESCE(last_read_at, resident_at)`. This is what fills that column.

**Sampled, because it sits on the read path.** A DB write per chunk read would be far worse than
the eviction it is preventing. A part is re-stamped at most once per `_SAMPLE_WINDOW_SECONDS`,
which bounds the write rate by the sampling window rather than by read throughput — and costs
nothing in fidelity, since the evictor only cares about ordering at hour scale.

Losing a sample is harmless: the part keeps its previous recency and is evicted slightly earlier
than it deserves, which is the same outcome the tier had before this existed.
"""

from __future__ import annotations

import logging
from typing import Optional

import asyncpg

from hippius_s3.cache.part_memo import PartMemo
from hippius_s3.monitoring import ReadRecencyOutcome


logger = logging.getLogger(__name__)


def _record_write(outcome: ReadRecencyOutcome) -> None:
    """Count a recency stamp attempt. Never let observability break a read."""
    try:
        from hippius_s3.monitoring import get_metrics_collector

        collector = get_metrics_collector()
        if collector is not None:
            collector.record_read_recency_write(outcome)
    except Exception:  # noqa: BLE001 - a metrics failure must not fail a read
        pass


# How long a part's recency stamp is considered fresh enough to skip re-writing. The evictor
# orders at hour scale, so a few minutes of staleness cannot change which part it picks — while
# the difference in write rate between "once per read" and "once per window" is total.
_SAMPLE_WINDOW_SECONDS = 300.0
# Bound on the sampling map so a long-lived pod cannot grow it without limit. Over-evicting an
# entry only costs one redundant UPDATE.
_SAMPLE_ENTRIES = 100_000
# Bound on one stamp's wait for a pool connection and on the UPDATE itself. The stamp is awaited
# inline on the read path — ahead of the first chunk in `read_response`, and inside the peer-serve
# in-flight slot on `read_local_chunk` — so a saturated pool must cost a lost sample, not a stalled
# read or a serve slot held for as long as the pool stays saturated.
_STAMP_TIMEOUT_SECONDS = 2.0


class ReadRecencyRecorder:
    """Stamps `last_read_at` on parts this node served locally, at most once per part per window."""

    def __init__(self, pool: asyncpg.Pool, node_id: str) -> None:
        self._pool = pool
        self._node_id = node_id
        self._recent: PartMemo[tuple[str, int, int], bool] = PartMemo(_SAMPLE_WINDOW_SECONDS, _SAMPLE_ENTRIES)

    async def __call__(self, object_id: str, object_version: int, part_number: int) -> None:
        key = (str(object_id), int(object_version), int(part_number))
        if self._recent.get(key) is not None:
            return
        self._recent.put(key, True)
        # Node-scoped: recency is per (node, part). Stamping a peer's row would protect a
        # copy on a disk this node cannot see while leaving its own unprotected — and the
        # evictor that reads this column is scoped to its own node_id.
        #
        # A part with no row here updates nothing, which is correct: the row is what says
        # "this node holds it and this node's evictor owns it".
        await self._stamp(
            """
            UPDATE cephor_ssd_residency SET last_read_at = now()
            WHERE node_id = $1 AND object_id = $2 AND version = $3 AND part_number = $4
            """,
            key,
        )

    async def touch_parts(self, object_id: str, object_version: int, part_numbers: list[int]) -> None:
        """Stamp every un-sampled part of one object in a single node-scoped UPDATE.

        For a multi-part read the per-chunk stamp in `get_chunk` only reaches a part when its
        first chunk streams, which for a long stream can be minutes after the request started —
        long enough for the evictor to take the tail parts of the very object being read. One
        statement up front costs one round trip regardless of part count, and shares the memo so
        the parts the per-chunk path just stamped are not written twice.
        """
        oid = str(object_id)
        version = int(object_version)
        pending: list[int] = []
        for part_number in part_numbers:
            key = (oid, version, int(part_number))
            if self._recent.get(key) is not None:
                continue
            self._recent.put(key, True)
            pending.append(key[2])
        if not pending:
            return
        await self._stamp(
            """
            UPDATE cephor_ssd_residency SET last_read_at = now()
            WHERE node_id = $1 AND object_id = $2 AND version = $3 AND part_number = ANY($4::bigint[])
            """,
            (oid, version, pending),
        )

    async def _stamp(self, sql: str, args: tuple[object, ...]) -> None:
        try:
            async with self._pool.acquire(timeout=_STAMP_TIMEOUT_SECONDS) as conn:
                await conn.execute(sql, self._node_id, *args, timeout=_STAMP_TIMEOUT_SECONDS)
            _record_write("written")
        except Exception as exc:  # noqa: BLE001 - a bookkeeping stamp must never fail its caller
            _record_write("failed")
            # Best-effort, like every other bookkeeping write on this path. The chunk is already
            # served; losing the stamp means the part is evicted somewhat earlier than it
            # deserves, which is exactly the FIFO behaviour this replaces. Deliberately broader
            # than (PostgresError, OSError): asyncpg.InterfaceError — a closing/uninitialised
            # pool — is NEITHER, and since write_meta awaits this recorder bare, letting it
            # escape would fail the client PUT and skip the landed announcement below it.
            logger.debug("recording read recency failed for %s: %s", args, exc)


def create_read_recency_recorder(pool: Optional[asyncpg.Pool], node_id: str) -> Optional[ReadRecencyRecorder]:
    """A recorder, or `None` when this process cannot attribute a read to a node.

    Without a node identity there is no way to say WHOSE copy was read, and the evictor is
    node-scoped — so a stamp would either miss or, worse, land on another node's row.
    """
    if pool is None or not node_id:
        return None
    return ReadRecencyRecorder(pool, node_id)


_recorder: Optional[ReadRecencyRecorder] = None


def initialize_read_recency_recorder(pool: Optional[asyncpg.Pool], node_id: str) -> Optional[ReadRecencyRecorder]:
    """Installs the process-wide recorder (the module-singleton pattern `landed.py` uses).

    The singleton exists for the WRITE path: `WriteThroughPartsWriter.write_meta` stamps recency
    on a just-(re)written part so a rewrite of an already-replicated part is LRU-hottest — not
    coldest — while its announcement is still in flight to the drain agent (the evict-vs-reland
    race; see `crates/hippius-drain-core/src/redrive.rs`). The read path keeps taking the same
    instance by reference, so both paths share one sampling memo.
    """
    global _recorder
    _recorder = create_read_recency_recorder(pool, node_id)
    return _recorder


def get_read_recency_recorder() -> Optional[ReadRecencyRecorder]:
    return _recorder
