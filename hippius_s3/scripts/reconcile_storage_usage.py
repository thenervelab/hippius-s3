#!/usr/bin/env python3
"""Backfill and reconcile bucket_storage_usage against ground truth.

Two modes, same operation underneath:

    --backfill    every bucket, oldest-unreconciled first. Run once after the migration.
    (default)     the rolling sweep: buckets not reconciled in --interval seconds.

The triggers keep the counter correct; this exists to PROVE they do. Expected drift is exactly
zero, so any non-zero `previous_bytes_used` difference is a trigger defect and not noise -- it is
reported per bucket and exported as usage_counter_drift_bytes.

Safe to run against a live primary and safe to run twice: recompute_bucket_storage_usage SETs each
row from ground truth rather than adding to it, so a bucket written mid-sweep converges on the next
pass instead of double-counting. Batches are separated by --sleep so a large fleet does not turn
into one long scan.

    python -m hippius_s3.scripts.reconcile_storage_usage --backfill
    python -m hippius_s3.scripts.reconcile_storage_usage --interval 86400 --limit 500
"""

import argparse
import asyncio
import logging
import sys

import asyncpg

from hippius_s3.config import get_config
from hippius_s3.monitoring import get_metrics_collector
from hippius_s3.monitoring import initialize_metrics_collector
from hippius_s3.services import usage_service


logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)


async def reconcile(
    db: asyncpg.Connection,
    interval_seconds: int,
    batch_size: int,
    max_batches: int,
    sleep_between: float,
) -> tuple[int, int]:
    """Returns (buckets_reconciled, buckets_with_drift)."""
    collector = get_metrics_collector()
    reconciled = 0
    drifted = 0

    for _ in range(max_batches):
        bucket_ids = await usage_service.find_buckets_to_reconcile(db, interval_seconds, batch_size)
        if not bucket_ids:
            break

        for bucket_id in bucket_ids:
            bytes_used, objects_count, previous = await usage_service.recompute_bucket(db, bucket_id)
            reconciled += 1

            if previous is not None and previous != bytes_used:
                drifted += 1
                drift = bytes_used - previous
                collector.record_usage_counter_drift(drift)
                logger.error(
                    f"USAGE_DRIFT bucket={bucket_id} counter={previous} truth={bytes_used} "
                    f"drift={drift:+d} objects={objects_count}. Expected drift is zero -- this is a "
                    f"trigger defect, not staleness."
                )

        logger.info(f"Reconciled {reconciled} buckets so far ({drifted} drifted)")
        await asyncio.sleep(sleep_between)

    return reconciled, drifted


async def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--backfill",
        action="store_true",
        help="Reconcile every bucket regardless of when it was last verified (interval 0).",
    )
    parser.add_argument("--interval", type=int, default=86400, help="Re-verify buckets older than this (seconds).")
    parser.add_argument("--limit", type=int, default=500, help="Buckets per batch.")
    parser.add_argument("--max-batches", type=int, default=100_000, help="Safety bound on total batches.")
    parser.add_argument("--sleep", type=float, default=1.0, help="Seconds between batches.")
    args = parser.parse_args()

    config = get_config()
    initialize_metrics_collector()

    db = await asyncpg.connect(config.database_url)
    try:
        interval = 0 if args.backfill else args.interval
        logger.info(
            f"Starting {'backfill' if args.backfill else 'reconcile'}: interval={interval}s "
            f"batch={args.limit} sleep={args.sleep}s"
        )
        reconciled, drifted = await reconcile(db, interval, args.limit, args.max_batches, args.sleep)
    finally:
        await db.close()

    logger.info(f"Done. Reconciled {reconciled} buckets, {drifted} had drift.")
    return 1 if drifted else 0


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
