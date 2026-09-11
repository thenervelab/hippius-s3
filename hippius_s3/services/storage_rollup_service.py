"""Compaction and reconciliation for the per-bucket storage rollup.

The triggers in 20260910120000_storage_usage_rollup.sql only ever INSERT into
`storage_delta_ledger`. Everything that turns that stream into `bucket_storage_usage` lives here,
so the worker loop, the backfill script and the tests all drive the same code.

Three operations, in increasing cost:

  compact_once      claim a batch of ledger rows and add them to the rollup. Continuous.
  recompute_bucket  full scan of one bucket, SET the rollup, report drift. Backfill + reconciler.
  ledger_stats      queue depth, lag, and any counter that has gone negative. Metrics.
"""

from __future__ import annotations

import logging
import uuid
from dataclasses import dataclass
from typing import Any

from hippius_s3.utils import get_query


logger = logging.getLogger(__name__)

# Guards against a wedged cycle: without a bound, a ledger that is filling faster than it drains
# keeps compact_until_drained in its inner loop forever and the reconciler never runs.
MAX_BATCHES_PER_CYCLE = 200


@dataclass(frozen=True)
class CompactionResult:
    rows_claimed: int
    buckets_folded: int
    buckets_applied: int


@dataclass(frozen=True)
class LedgerStats:
    depth: int
    oldest_age_seconds: int
    negative_buckets: int


@dataclass(frozen=True)
class RecomputeResult:
    bucket_id: uuid.UUID
    bytes_before: int
    bytes_after: int

    @property
    def drift_bytes(self) -> int:
        """How wrong the counter was. Expected ZERO -- anything else is an unaccounted write path."""
        return self.bytes_after - self.bytes_before


async def compact_once(conn: Any, batch_size: int) -> CompactionResult:
    """Fold up to `batch_size` ledger rows into the rollup, atomically.

    Returns rows_claimed=0 both when the ledger is empty and when a recompute holds the advisory
    lock; the caller cannot tell those apart and does not need to -- both mean "nothing to do right
    now, come back".

    The transaction wraps the lock and the fold together because pg_try_advisory_xact_lock is
    released at COMMIT: taken outside a transaction it would be dropped before the fold ran.
    """
    async with conn.transaction():
        got_lock = await conn.fetchval("SELECT pg_try_advisory_xact_lock(storage_usage_rollup_lock_key(), 0)")
        if not got_lock:
            return CompactionResult(0, 0, 0)

        row = await conn.fetchrow(get_query("compact_storage_delta_ledger"), batch_size)

    return CompactionResult(
        rows_claimed=int(row["rows_claimed"]),
        buckets_folded=int(row["buckets_folded"]),
        buckets_applied=int(row["buckets_applied"]),
    )


async def compact_until_drained(conn: Any, batch_size: int) -> CompactionResult:
    """Compact repeatedly until a batch comes back short (or the batch bound is hit)."""
    totals = CompactionResult(0, 0, 0)

    for _ in range(MAX_BATCHES_PER_CYCLE):
        result = await compact_once(conn, batch_size)
        totals = CompactionResult(
            rows_claimed=totals.rows_claimed + result.rows_claimed,
            buckets_folded=totals.buckets_folded + result.buckets_folded,
            buckets_applied=totals.buckets_applied + result.buckets_applied,
        )
        if result.rows_claimed < batch_size:
            return totals

    logger.warning(
        f"STORAGE_ROLLUP_COMPACTION_BOUND claimed {totals.rows_claimed} rows over "
        f"{MAX_BATCHES_PER_CYCLE} batches without draining the ledger; yielding so the reconciler "
        f"can run. Depth is exported as storage_rollup_ledger_depth."
    )
    return totals


async def recompute_bucket(
    conn: Any,
    bucket_id: uuid.UUID,
    timeout: float,  # noqa: ASYNC109
) -> RecomputeResult:
    """Full scan of one bucket; SET the rollup to the truth. Idempotent and convergent.

    SET rather than ADD is what makes running this twice, or concurrently with live writes, safe:
    each run overwrites with the truth as of its own snapshot instead of accumulating.

    `timeout` is asyncpg's own, which cancels client-side and best-effort. Callers ALSO set
    statement_timeout on the connection they hand in, so the backend enforces its own bound: the
    usage-rollup pool and the backfill script both do, pinned by
    tests/unit/test_recompute_statement_timeout.py. Production's own statement_timeout is 0, so
    without that this aggregate is unbounded on the primary -- and whichever of the two limits is
    shorter is the one that fires, so they are set together deliberately. This is the ONE expensive
    statement in the whole mechanism: for the largest prod bucket it is the aggregate the rollup
    exists to stop running every cycle.
    """
    row = await conn.fetchrow(get_query("recompute_bucket_storage_usage"), bucket_id, timeout=timeout)
    return RecomputeResult(
        bucket_id=bucket_id,
        bytes_before=int(row["bytes_before"]),
        bytes_after=int(row["bytes_after"]),
    )


async def ledger_stats(conn: Any) -> LedgerStats:
    row = await conn.fetchrow(get_query("get_storage_delta_ledger_stats"))
    return LedgerStats(
        depth=int(row["depth"]),
        oldest_age_seconds=int(row["oldest_age_seconds"]),
        negative_buckets=int(row["negative_buckets"]),
    )


async def reconcile_buckets(
    conn: Any,
    limit: int,
    timeout: float,  # noqa: ASYNC109
) -> list[RecomputeResult]:
    """Recompute the `limit` live buckets recomputed longest ago, and report drift.

    Serial, not concurrent: these are the heaviest aggregates in the schema and the whole point of
    the rollup is that nothing has to wait for them. A pass that takes a while is fine.

    DRIFT IS ONLY DRIFT ONCE THE ROLLUP IS SEEDED. Before the backfill every counter is 0 while
    ground truth is the bucket's whole contents, so each recompute moves the number by the bucket's
    full size. That is seeding, not a defect: reporting it as drift fires once per bucket across the
    whole estate, blames a write path that is behaving perfectly, and teaches whoever reads the log
    to ignore the one alert this design depends on. Observed on the staging rollout -- 22 of the
    first 25 buckets reported as drift, every one of them correct behaviour.
    """
    ready = bool(await conn.fetchval(get_query("get_storage_usage_rollup_ready")))
    rows = await conn.fetch(get_query("list_buckets_for_usage_reconcile"), limit)

    results: list[RecomputeResult] = []
    for row in rows:
        # PER-BUCKET ISOLATION, and one of the few places in this codebase where catching is
        # correct rather than lazy. The queue is `ORDER BY recomputed_at ASC NULLS FIRST` and
        # recompute_bucket_storage_usage() only stamps recomputed_at on success, so a bucket that
        # raises stays the queue HEAD forever. Letting that raise abandons the rest of the pass and
        # the reconciler then re-attempts the same bucket every cycle and never verifies another
        # one again -- silently losing the only drift detector this design has, which is the single
        # worst outcome available here. One wasted bucket per cycle is the right trade.
        #
        # It is logged at ERROR and fails the cycle metric, so it is loud; nothing is swallowed.
        try:
            result = await recompute_bucket(conn, row["bucket_id"], timeout)
        except Exception as e:
            logger.error(
                f"STORAGE_ROLLUP_RECOMPUTE_FAILED bucket={row['bucket_id']}: {type(e).__name__}: {e}. "
                f"This bucket stays at the head of the least-recently-recomputed queue and will be "
                f"retried next cycle; the rest of this pass continues. If it repeats, that bucket is "
                f"never being verified -- check whether its aggregate exceeds "
                f"HIPPIUS_USAGE_RECONCILE_TIMEOUT_SECONDS.",
                exc_info=True,
            )
            continue

        results.append(result)
        if not result.drift_bytes:
            continue

        if ready:
            logger.error(
                f"STORAGE_ROLLUP_DRIFT bucket={result.bucket_id} "
                f"was={result.bytes_before} truth={result.bytes_after} "
                f"drift={result.drift_bytes}; a write path is moving bytes without emitting a "
                f"delta, or a statement is repointing current_object_version and editing the "
                f"outgoing version at the same time."
            )
        else:
            logger.info(
                f"STORAGE_ROLLUP_SEEDING bucket={result.bucket_id} "
                f"was={result.bytes_before} truth={result.bytes_after}; the rollup is not "
                f"backfilled yet, so this is the reconciler seeding a counter rather than drift."
            )

    return results
