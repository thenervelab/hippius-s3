"""How many bytes an account stores.

Read from a MAINTAINED counter, not computed. `bucket_storage_usage` holds per-bucket bytes,
Postgres triggers record every movement into an insert-only ledger, and a compactor folds the
ledger into the rollup -- see 20260910120000_storage_usage_rollup.sql for the mechanism and
storage_rollup_service.py for the compactor and reconciler.

This used to walk every bucket in keyset pages and sum. That was O(objects) and ran every
plans-cacher cycle, forever, on a bucket holding millions of objects that only grows -- minutes of
replica work per cycle to recompute a number that had moved by a handful of objects. The full scan
now happens once, as a backfill, and never again.

get_account_storage_bytes.sql remains THE canonical definition and is the oracle the rollup is
asserted against, case by case, in tests/integration/test_storage_usage_rollup.py. It has no runtime
caller and that is deliberate; do not delete it.

The only caller is the plans-cacher, in the background. Nothing on the request path runs this --
though at sub-millisecond it could now afford to.
"""

from __future__ import annotations

import logging
from typing import Any

from hippius_s3.utils import get_query


logger = logging.getLogger(__name__)


class StorageRollupNotBackfilled(RuntimeError):
    """The rollup exists but has not been seeded, so its numbers are not totals."""


_NOT_BACKFILLED = (
    "bucket_storage_usage has not been backfilled (storage_usage_rollup_state.backfilled_at is "
    "NULL), so its rows are deltas rather than totals. Run "
    "hippius_s3/scripts/backfill_bucket_storage_usage.py."
)


async def require_rollup_ready(db: Any) -> None:
    """Raise unless the backfill has run, i.e. unless the counters are totals rather than deltas.

    One indexed single-row SELECT. Exists so a caller can ask BEFORE doing expensive work it would
    have to throw away: the plans-cacher's usage read raises at the END of a full paginated upstream
    scrape, so every pre-backfill cycle fetched the whole roll and discarded it.

    Raising rather than returning a bool keeps ONE copy of the message -- the two call sites had
    near-verbatim duplicates of it.
    """
    async with db.acquire() as conn:
        if not await conn.fetchval(get_query("get_storage_usage_rollup_ready")):
            raise StorageRollupNotBackfilled(_NOT_BACKFILLED)


async def get_account_storage_bytes(
    db: Any,
    main_account_id: str,
    timeout: float,  # noqa: ASYNC109
) -> int:
    """Bytes stored by this account: current versions of live objects in live buckets.

    Must stay in step with get_account_storage_bytes.sql, get_admin_account_stats.sql and
    console_list_buckets.sql -- those are the numbers an operator and a customer respectively see,
    and enforcing a third one is how you get a support ticket nobody can resolve.

    RAISES until the backfill has run. Before that the rollup holds only deltas recorded since the
    migration, which would under-report every account that existed beforehand. Raising fails the
    plans-cacher cycle, which leaves the previous roll serving -- the same degradation as any other
    failed cycle, and far better than publishing a small number as a customer's usage.

    `timeout` is asyncpg's own (hence the ASYNC109 waiver), so a cancellation lands server-side
    rather than abandoning the coroutine with the query still running.
    """
    row = await db.fetchrow(get_query("get_account_storage_bytes_rollup"), main_account_id, timeout=timeout)

    if not row["ready"]:
        raise StorageRollupNotBackfilled(
            "bucket_storage_usage has not been backfilled (storage_usage_rollup_state.backfilled_at "
            "is NULL), so its rows are deltas rather than totals. Run "
            "hippius_s3/scripts/backfill_bucket_storage_usage.py."
        )

    if row["negative_buckets"]:
        # Only reachable if a decrement was recorded without its increment, so it is a defect
        # report rather than a condition to handle. Reported and served (clamped) rather than
        # raised: the error direction is under-counting, which is the same fail-open direction as
        # every other degraded path in the quota gate, and the reconciler repairs it within a pass.
        logger.error(
            f"STORAGE_ROLLUP_NEGATIVE account={main_account_id} "
            f"buckets={row['negative_buckets']} bytes_used={row['bytes_used']}; "
            f"serving the clamped total. The delta ledger has drifted -- check the reconciler's "
            f"drift metric and recompute the account's buckets."
        )

    return int(row["bytes_used"])
