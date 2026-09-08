"""Read and repair side of the per-account storage rollup.

The rollup itself is maintained by database triggers -- see
hippius_s3/sql/migrations/20260908120000_bucket_storage_usage.sql for the accounting definition and
the division of labour between the four triggers. Nothing in this module increments or decrements
anything; there is deliberately no application-side write path, because a write path can be
forgotten by a future endpoint and the out-of-band scripts bypass it entirely.

Two reads, and the asymmetry between them is the point:

  * get_account_bytes()               cheap, cached, may be slightly stale -- may only ALLOW.
  * get_account_bytes_authoritative() expensive, exact                     -- may DENY.

A drifted counter can therefore never 402 a paying customer; the worst it can do is let an
over-quota upload through until the reconciler catches up.
"""

from __future__ import annotations

import logging
from typing import Any

from hippius_s3.utils import get_query


logger = logging.getLogger(__name__)

USAGE_CACHE_PREFIX = "hippius_s3_usage:"


def _cache_key(main_account_id: str) -> str:
    return f"{USAGE_CACHE_PREFIX}{main_account_id}"


async def get_account_bytes(
    db: Any,
    redis_client: Any,
    main_account_id: str,
    cache_ttl_seconds: int,
) -> int:
    """Cached total billable bytes for an account. Read-through against the rollup."""
    key = _cache_key(main_account_id)

    cached = await redis_client.get(key)
    if cached is not None:
        return int(cached)

    row = await db.fetchrow(get_query("get_account_storage_usage"), main_account_id)
    total = int(row["bytes_used"]) if row else 0

    await redis_client.setex(key, cache_ttl_seconds, str(total))
    return total


async def invalidate_account_bytes(redis_client: Any, main_account_id: str) -> None:
    """Drop the cached total. Best-effort -- a stale entry only shortens the enforcement lag."""
    await redis_client.delete(_cache_key(main_account_id))


async def get_account_bytes_authoritative(db: Any, main_account_id: str) -> int:
    """Ground-truth total. Expensive; see the query header for who is allowed to call it."""
    row = await db.fetchrow(get_query("get_account_storage_usage_authoritative"), main_account_id)
    return int(row["bytes_used"]) if row else 0


async def recompute_bucket(db: Any, bucket_id: str) -> tuple[int, int, int | None]:
    """Reset one bucket's rollup row from ground truth.

    Returns (bytes_used, objects_count, previous_bytes_used). `previous_bytes_used` is None for a
    bucket that had no row yet; otherwise the difference is the drift the triggers accumulated,
    which the reconciler exports as a metric. Expected drift is exactly zero.
    """
    row = await db.fetchrow(get_query("recompute_bucket_storage_usage"), bucket_id)
    if row is None:
        return 0, 0, None

    previous = row["previous_bytes_used"]
    return (
        int(row["bytes_used"]),
        int(row["objects_count"]),
        int(previous) if previous is not None else None,
    )


async def find_buckets_to_reconcile(db: Any, interval_seconds: int, limit: int) -> list[str]:
    rows = await db.fetch(get_query("find_buckets_for_usage_reconcile"), interval_seconds, limit)
    return [str(row["bucket_id"]) for row in rows]
