#!/usr/bin/env python3
"""Folds the storage delta ledger into the per-bucket rollup, and reconciles it.

Postgres triggers INSERT one row into `storage_delta_ledger` for every movement of billable bytes
(see 20260910120000_storage_usage_rollup.sql). This worker is the only thing that turns that stream
into `bucket_storage_usage`, which is what the plans-cacher reads as an account's usage.

Two jobs, one loop:

  COMPACT, every HIPPIUS_USAGE_ROLLUP_LOOP_SLEEP seconds. Claim ledger rows with
  `DELETE ... RETURNING` and add them to the rollup. Exactly-once by construction: the rows leave
  the ledger in the same transaction that applies them.

  RECONCILE, every HIPPIUS_USAGE_RECONCILE_INTERVAL_SECONDS. Fully recompute a small rolling slice
  of live buckets and report the correction as drift. Expected drift is ZERO -- the triggers are
  maintained against get_account_storage_bytes.sql -- so any drift means a write path is moving
  bytes without emitting a delta, and that is the signal this worker exists to produce.

WHY A SEPARATE WORKER, NOT A SECOND LOOP IN THE PLANS-CACHER. The plans-cacher's pool is
DATABASE_READONLY_URL, a read REPLICA, because its counts must not run on the primary. Compaction
WRITES. Bolting it on would mean a second pool with opposite requirements inside one worker, and it
would tie the rollup's freshness -- which wants seconds -- to the scrape interval, which wants ten
minutes. Splitting them also means the ledger keeps draining if the upstream plans API is down.

This worker being down is not an outage and is not silent: nothing on the request path depends on
it, the ledger simply accumulates (it is insert-only, so accumulating is cheap and lossless), and
storage_rollup_ledger_depth / _lag_seconds rise. Usage figures freeze at their last folded value
until it comes back -- which, where plan enforcement is ON, means a customer who deletes data stays
refused. Alert on the lag.

DO NOT RUN THIS BEFORE THE BACKFILL. Compaction is correct from the moment the triggers exist, but
until hippius_s3/scripts/backfill_bucket_storage_usage.py has run, the rollup holds deltas rather
than totals. usage_service refuses to serve it in that state, so the ordering is enforced rather
than merely documented.
"""

import asyncio
import logging
import sys
import time
from pathlib import Path

import asyncpg


sys.path.insert(0, str(Path(__file__).parent.parent))

from hippius_s3.config import get_config
from hippius_s3.logging_config import setup_loki_logging
from hippius_s3.monitoring import get_metrics_collector
from hippius_s3.monitoring import initialize_metrics_collector
from hippius_s3.sentry import init_sentry
from hippius_s3.services import storage_rollup_service
from hippius_s3.workers.shutdown import run_worker


config = get_config()

setup_loki_logging(config, "usage-rollup")
logger = logging.getLogger(__name__)
init_sentry("usage-rollup", is_worker=True)


async def run_cycle(pool: asyncpg.Pool, reconcile: bool) -> bool:
    """One compaction pass, optionally followed by a reconcile pass. Never raises.

    A failed cycle must not stop the loop: the ledger is insert-only, so the cost of skipping a
    cycle is latency on the rollup, and the next cycle picks up everything that was left. Retrying
    from the top is always safe -- the claim is transactional, so a cycle that died mid-fold left
    its rows in the ledger.
    """
    collector = get_metrics_collector()

    try:
        async with pool.acquire() as conn:
            compaction = await storage_rollup_service.compact_until_drained(conn, config.usage_rollup_batch_size)
            collector.record_storage_rollup_compaction(compaction.rows_claimed)

            stats = await storage_rollup_service.ledger_stats(conn)
            collector.record_storage_rollup_ledger(stats.depth, stats.oldest_age_seconds, stats.negative_buckets)

            if compaction.rows_claimed or stats.depth:
                logger.info(
                    f"usage-rollup compacted {compaction.rows_claimed} ledger row(s) over "
                    f"{compaction.buckets_folded} bucket(s); depth={stats.depth} "
                    f"lag={stats.oldest_age_seconds}s"
                )
            if stats.negative_buckets:
                logger.error(
                    f"STORAGE_ROLLUP_NEGATIVE {stats.negative_buckets} bucket(s) hold a negative "
                    f"byte counter. A decrement was recorded without its increment; the reconciler "
                    f"repairs the value but not the cause."
                )

            if reconcile:
                results = await storage_rollup_service.reconcile_buckets(
                    conn,
                    config.usage_reconcile_buckets_per_cycle,
                    config.usage_reconcile_timeout_seconds,
                )
                changed = [r for r in results if r.drift_bytes]
                for result in results:
                    collector.record_storage_rollup_recompute(result.drift_bytes)
                # "changed", not "drift": before the backfill every recompute legitimately moves a
                # counter off zero, and calling that drift here would contradict the per-bucket
                # lines the service now emits. reconcile_buckets owns the interpretation.
                logger.info(
                    f"usage-rollup reconciled {len(results)} bucket(s), {len(changed)} changed"
                    + (f" totalling {sum(r.drift_bytes for r in changed)} bytes" if changed else "")
                )

        collector.record_storage_rollup_cycle(success=True)
        return True
    except Exception as e:
        logger.error(f"usage-rollup cycle failed: {e}; the ledger is unchanged and will be retried", exc_info=True)
        collector.record_storage_rollup_cycle(success=False)
        return False


async def run_usage_rollup_loop() -> None:
    # PRIMARY, not the replica the plans-cacher uses: this worker writes. Two connections is
    # plenty -- the work is serial by design and an oversized pool here is idle backends against
    # max_connections for nothing.
    #
    # statement_timeout is raised on this pool because production sets a 1-minute one for the
    # application role and the largest bucket's recompute takes longer. Whichever limit is SHORTER
    # fires, so asyncpg's own `timeout=` never got a chance -- the reconciler would have failed its
    # cycle every time that bucket came up in the queue, i.e. the one bucket most worth verifying
    # would have been the one bucket it could never verify. It also covers the compactor's
    # DELETE ... RETURNING on this pool, which is a bounded batch and wants the same headroom.
    pool = await asyncpg.create_pool(
        config.database_url,
        min_size=1,
        max_size=2,
        server_settings={"statement_timeout": f"{int(config.usage_reconcile_timeout_seconds * 1000)}"},
    )
    initialize_metrics_collector()

    logger.info(
        f"Starting usage-rollup: compacting every {config.usage_rollup_loop_sleep}s in batches of "
        f"{config.usage_rollup_batch_size}, reconciling {config.usage_reconcile_buckets_per_cycle} "
        f"bucket(s) every {config.usage_reconcile_interval_seconds}s"
    )

    # Reconcile on the first cycle, so a deploy immediately says whether the rollup agrees with the
    # canonical query rather than waiting an hour to find out.
    last_reconcile = 0.0

    try:
        while True:
            now = time.monotonic()
            reconcile = (now - last_reconcile) >= config.usage_reconcile_interval_seconds
            if reconcile:
                last_reconcile = now

            await run_cycle(pool, reconcile=reconcile)
            await asyncio.sleep(config.usage_rollup_loop_sleep)
    finally:
        await pool.close()


if __name__ == "__main__":
    run_worker(run_usage_rollup_loop, "usage-rollup")
