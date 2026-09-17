"""Compaction and reconciliation for the per-bucket storage rollup.

The triggers in 20260910120000_storage_usage_rollup.sql only ever INSERT into
`storage_delta_ledger`. Everything that turns that stream into `bucket_storage_usage` lives here,
so the worker loop, the backfill script and the tests all drive the same code.

Four operations, in increasing cost:

  compact_bucket_once  claim a batch of ONE bucket's ledger rows and add them to its counter.
  ledger_stats         queue depth, lag, and any counter that has gone negative. Metrics.
  recompute_bucket     full scan of one bucket, SET the rollup, report drift. Backfill + reconciler.
  verify_bucket_sliced key-range sweep for a bucket too large to recompute at all. Measures only.

Everything here is scoped to ONE bucket, including the advisory lock. That is not decomposition for
its own sake: with one estate-wide lock, a bucket whose aggregate cannot complete stalled compaction
for every other bucket for the whole reconcile timeout, repeatedly. See
20260916090000_rollup_verify_giant_buckets.sql for the measurements.
"""

from __future__ import annotations

import dataclasses
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
    # Whether this recompute ran BEFORE the backfill, i.e. whether a non-zero delta means "this
    # counter was never seeded" rather than "a write path is unaccounted for". Carried per result so
    # each one is self-describing at the point the metric is recorded; it is a per-PASS fact that
    # reconcile_buckets stamps onto every row it returns.
    seeding: bool = False

    @property
    def drift_bytes(self) -> int:
        """How wrong the counter was. Expected ZERO -- anything else is an unaccounted write path."""
        return self.bytes_after - self.bytes_before

    @property
    def counts_as_drift(self) -> bool:
        """Whether this should reach the drift METRIC, and so the drift alert.

        The seeding-vs-drift distinction has to hold for the metric, not only the log. Before the
        backfill every recompute legitimately moves a counter off zero, so counting those as drift
        makes `increase(storage_rollup_drifted_buckets_total[1h]) > 0` fire from the first reconcile
        pass and keep firing for the whole pre-backfill window -- the exact cry-wolf failure the log
        gating was added to prevent. Gating one half and not the other makes both pointless.
        """
        return bool(self.drift_bytes) and not self.seeding


async def compact_bucket_once(conn: Any, bucket_id: uuid.UUID, batch_size: int) -> CompactionResult:
    """Fold up to `batch_size` of ONE bucket's ledger rows into its counter, atomically.

    Returns rows_claimed=0 both when that bucket's ledger is empty and when a recompute of the SAME
    bucket holds its lock; the caller cannot tell those apart and does not need to -- both mean
    "nothing to do for this bucket right now".

    THE LOCK IS PER-BUCKET, which is the point of doing this a bucket at a time. It used to be one
    estate-wide key with a hardcoded second argument of 0, so a single slow recompute blocked
    compaction for every unrelated bucket: prod 2026-09-15 measured the ledger's oldest row aging to
    412s against a normal 2s while one 136M-object bucket held that lock for its full 300s timeout,
    over and over. Now a recompute of bucket A and a fold of bucket B never meet.

    The transaction wraps the lock and the fold together because pg_try_advisory_xact_lock is
    released at COMMIT: taken outside a transaction it would be dropped before the fold ran.
    """
    async with conn.transaction():
        got_lock = await conn.fetchval(
            "SELECT pg_try_advisory_xact_lock(storage_usage_rollup_lock_key(), storage_usage_bucket_lock_key($1))",
            bucket_id,
        )
        if not got_lock:
            return CompactionResult(0, 0, 0)

        row = await conn.fetchrow(get_query("compact_storage_delta_ledger_bucket"), bucket_id, batch_size)

    rows_claimed = int(row["rows_claimed"])
    return CompactionResult(
        rows_claimed=rows_claimed,
        buckets_folded=1 if rows_claimed else 0,
        buckets_applied=int(row["buckets_applied"]),
    )


async def compact_until_drained(conn: Any, batch_size: int, buckets_per_batch: int = 64) -> CompactionResult:
    """Drain the ledger bucket by bucket, oldest work first, until nothing moves.

    Two nested bounds, both load-bearing. `buckets_per_batch` caps how many buckets one pass even
    looks at, so a fanned-out ledger cannot turn a single cycle into thousands of round trips.
    MAX_BATCHES_PER_CYCLE caps the outer loop, so a ledger filling faster than it drains yields to
    the reconciler instead of spinning here forever.

    "Nothing moves" rather than "a batch came back short" is the termination condition now: with a
    per-bucket lock a zero can mean "that bucket is being recomputed", and the old short-batch test
    would have ended the cycle on the first such bucket while others still had work.
    """
    totals = CompactionResult(0, 0, 0)

    for _ in range(MAX_BATCHES_PER_CYCLE):
        candidates = await conn.fetch(get_query("list_ledger_buckets_by_age"), buckets_per_batch)
        if not candidates:
            return totals

        progressed = False
        for row in candidates:
            result = await compact_bucket_once(conn, row["bucket_id"], batch_size)
            totals = CompactionResult(
                rows_claimed=totals.rows_claimed + result.rows_claimed,
                buckets_folded=totals.buckets_folded + result.buckets_folded,
                buckets_applied=totals.buckets_applied + result.buckets_applied,
            )
            progressed = progressed or bool(result.rows_claimed)

        # Every candidate was empty or locked by a concurrent recompute. Spinning would burn the
        # cycle on lock attempts; the rows are still there and the next cycle is 5s away.
        if not progressed:
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


@dataclass(frozen=True)
class SliceSweepResult:
    """Progress of a sliced verification, or its verdict once the sweep completes."""

    bucket_id: uuid.UUID
    slices_run: int
    objects_scanned: int
    complete: bool
    # Only meaningful when complete.
    swept_bytes: int = 0
    counter_bytes: int = 0
    tolerance_bytes: int = 0

    @property
    def gap_bytes(self) -> int:
        return self.swept_bytes - self.counter_bytes

    @property
    def exceeds_tolerance(self) -> bool:
        """Whether the gap is larger than in-flight writes could possibly explain.

        ONE-SIDED BY CONSTRUCTION. The sweep reads the bucket across many cycles, so its total is a
        smear over a window in which writes landed; the counter is a point reading at the end. The
        two can differ legitimately by at most the absolute byte movement the ledger carried while
        the sweep ran, which is what churn_bytes accumulates. Below that, this says nothing -- it
        never claims drift it cannot prove. Above it, no amount of concurrent traffic explains the
        gap and something is genuinely unaccounted for.
        """
        return self.complete and abs(self.gap_bytes) > self.tolerance_bytes


async def verify_bucket_sliced(
    conn: Any,
    bucket_id: uuid.UUID,
    page_objects: int,
    max_slices: int,
) -> SliceSweepResult:
    """Advance (or start, or finish) a keyset sweep over one bucket, as a stand-in for a recompute.

    For a bucket holding a large fraction of the objects table there is no such thing as a fast
    aggregate -- prod's largest is 80.7% of the table, and the planner rightly seq-scans 167 GB for
    it. Bounded to a key range the same sum is an index scan, ~12,000x cheaper and flat in bucket
    size, so the bucket becomes verifiable in pieces spread over many cycles.

    THIS ONLY EVER MEASURES. It never writes bytes_used: a multi-cycle total is smeared across
    concurrent writes, and only recompute_bucket_storage_usage() may SET the counter because only it
    does so in the one snapshot that also discards the ledger rows its aggregate already covers.
    Repairing an oversized bucket is therefore still a manual backfill; this tells you whether it
    needs one.
    """
    state = await conn.fetchrow(get_query("get_bucket_verify_state"), bucket_id)
    if state is None:
        await conn.fetchval(get_query("start_bucket_verify_sweep"), bucket_id)
        state = await conn.fetchrow(get_query("get_bucket_verify_state"), bucket_id)
        if state is None:
            # The bucket was deleted between the queue read and here.
            return SliceSweepResult(bucket_id=bucket_id, slices_run=0, objects_scanned=0, complete=False)

    cursor = state["cursor_key"]
    slices_run = 0
    scanned_now = 0
    exhausted = False

    for _ in range(max_slices):
        page = await conn.fetchrow(get_query("sum_bucket_object_key_slice"), bucket_id, cursor, page_objects)
        scanned = int(page["objects_scanned"])
        slices_run += 1
        scanned_now += scanned

        if scanned:
            cursor = page["next_cursor"]
            await conn.fetchrow(
                get_query("advance_bucket_verify_sweep"), bucket_id, cursor, int(page["bytes"]), scanned
            )

        # A short page is the end of the bucket. Checking `< page_objects` rather than `== 0` saves a
        # whole extra round trip per sweep, and cannot end early: the page is LIMIT-bounded, so fewer
        # rows than the limit means there were no more to give.
        if scanned < page_objects:
            exhausted = True
            break

    final = await conn.fetchrow(get_query("get_bucket_verify_state"), bucket_id)
    if final is None:
        return SliceSweepResult(bucket_id=bucket_id, slices_run=slices_run, objects_scanned=scanned_now, complete=False)

    if not exhausted:
        return SliceSweepResult(
            bucket_id=bucket_id,
            slices_run=slices_run,
            objects_scanned=int(final["objects_scanned"]),
            complete=False,
        )

    swept = int(final["partial_bytes"])
    counter = int(final["bytes_used"])
    tolerance = max(0, int(final["churn_bytes"]) - int(final["start_churn_bytes"]))
    result = SliceSweepResult(
        bucket_id=bucket_id,
        slices_run=slices_run,
        objects_scanned=int(final["objects_scanned"]),
        complete=True,
        swept_bytes=swept,
        counter_bytes=counter,
        tolerance_bytes=tolerance,
    )

    await conn.execute(get_query("finish_bucket_verify_sweep"), bucket_id)
    await conn.execute(get_query("delete_bucket_verify_state"), bucket_id)
    return result


async def _verify_sliced_and_report(
    conn: Any,
    bucket_id: uuid.UUID,
    ready: bool,
    page_objects: int,
    max_slices: int,
    failures: int,
) -> None:
    """Advance one oversized bucket's sweep and log the verdict when it completes.

    Kept out of reconcile_buckets so the fast path reads as one thing. Returns nothing because a
    sweep produces no RecomputeResult: it never moves the counter, so it has no before/after and
    must not reach the drift metric, whose meaning is "a recompute corrected a counter by N bytes".
    """
    try:
        sweep = await verify_bucket_sliced(conn, bucket_id, page_objects, max_slices)
    except Exception as e:
        logger.error(
            f"STORAGE_ROLLUP_SLICE_FAILED bucket={bucket_id}: {type(e).__name__}: {e}. This bucket "
            f"has failed {failures} recomputes and its sliced verification is now failing too, so it "
            f"is not being verified by any path.",
            exc_info=True,
        )
        return

    if not sweep.complete:
        logger.info(
            f"STORAGE_ROLLUP_SLICE_PROGRESS bucket={bucket_id} slices={sweep.slices_run} "
            f"objects_scanned={sweep.objects_scanned}; oversized bucket, verifying in key-range "
            f"slices across cycles."
        )
        return

    if not ready:
        logger.info(
            f"STORAGE_ROLLUP_SLICE_SEEDING bucket={bucket_id} swept={sweep.swept_bytes} "
            f"counter={sweep.counter_bytes}; the rollup is not backfilled yet, so a gap here is an "
            f"unseeded counter rather than drift."
        )
        return

    if sweep.exceeds_tolerance:
        logger.error(
            f"STORAGE_ROLLUP_DRIFT bucket={bucket_id} was={sweep.counter_bytes} "
            f"truth={sweep.swept_bytes} drift={sweep.gap_bytes} tolerance={sweep.tolerance_bytes} "
            f"(sliced over {sweep.objects_scanned} objects); the gap is larger than the byte "
            f"movement the ledger carried during the sweep, so concurrent writes cannot explain it. "
            f"This bucket is too large to recompute in one statement -- repair needs a targeted "
            f"backfill run."
        )
        from hippius_s3.monitoring import get_metrics_collector

        get_metrics_collector().record_storage_rollup_recompute(sweep.gap_bytes)
        return

    logger.info(
        f"STORAGE_ROLLUP_SLICE_VERIFIED bucket={bucket_id} swept={sweep.swept_bytes} "
        f"counter={sweep.counter_bytes} gap={sweep.gap_bytes} tolerance={sweep.tolerance_bytes} "
        f"objects={sweep.objects_scanned}; within what concurrent writes explain."
    )


async def reconcile_buckets(
    conn: Any,
    limit: int,
    timeout: float,  # noqa: ASYNC109
    slice_after_failures: int = 2,
    slice_page_objects: int = 50_000,
    slice_max_per_cycle: int = 4,
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
        bucket_id = row["bucket_id"]

        # OVERSIZED BUCKETS TAKE THE SLOW PATH. A bucket earns it by failing the fast one
        # `slice_after_failures` times rather than by exceeding an object count, so the threshold
        # never needs retuning as the estate grows, and a bucket that becomes aggregatable again
        # (objects deleted, or the timeout raised) returns to the fast path the moment one recompute
        # succeeds and resets the counter.
        if row["recompute_failures"] >= slice_after_failures:
            await _verify_sliced_and_report(
                conn, bucket_id, ready, slice_page_objects, slice_max_per_cycle, row["recompute_failures"]
            )
            continue

        # PER-BUCKET ISOLATION, and one of the few places in this codebase where catching is
        # correct rather than lazy. Letting this raise would abandon the rest of the pass, so the
        # reconciler would re-attempt the same bucket every cycle and never verify another one --
        # silently losing the only drift detector this design has.
        #
        # The attempt is stamped in its OWN transaction because this one has already rolled back.
        # Without that stamp the bucket keeps a NULL attempted_at, stays at the head of the queue
        # forever and starves every bucket behind it: prod 2026-09-15, 168 failures in 24h on one
        # bucket and 47 of 50 slots used per pass.
        try:
            result = await recompute_bucket(conn, bucket_id, timeout)
        except Exception as e:
            failures = await conn.fetchval(get_query("mark_bucket_reconcile_failed"), bucket_id)
            logger.error(
                f"STORAGE_ROLLUP_RECOMPUTE_FAILED bucket={bucket_id}: {type(e).__name__}: {e}. "
                f"Consecutive failures: {failures}. The attempt is recorded, so this bucket rotates "
                f"out of the queue head instead of pinning it; at {slice_after_failures} it switches "
                f"to sliced verification, which does not need the whole aggregate to fit in "
                f"HIPPIUS_USAGE_RECONCILE_TIMEOUT_SECONDS.",
                exc_info=True,
            )
            continue

        result = dataclasses.replace(result, seeding=not ready)
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
