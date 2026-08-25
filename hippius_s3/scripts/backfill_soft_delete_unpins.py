"""Backfill unpins for the soft-delete backlog so hard-delete can finally drain it.

Why this exists: millions of objects have `deleted_at` set but their `chunk_backend`
rows are still `deleted=false` — their unpins were lost (never processed / purged,
see the redis-queues unpin-overrun incident, and destructive scripts that hard-deleted
without enqueuing unpins). The janitor hard-delete query (now fast + index-driven)
therefore deletes nothing, because nothing is "ready". This script re-drives the unpins:

  - Live backends with an unpin worker: re-enqueue an unpin via `enqueue_unpin_request`,
    which routes to the configured `delete_backends` (HIPPIUS_DELETE_BACKENDS). The
    unpin workers delete the chunks from their backend AND mark `chunk_backend.deleted=true`,
    so the object becomes ready for hard-delete.
  - Deprecated backends with no worker (default: `ipfs`) — those chunks are gone, so we
    reconcile the rows in-DB (mark deleted). Set with --deprecated-backends.

Then the next janitor Phase-4 cycle hard-deletes the now-ready metadata (cascade clears
object_versions/parts/part_chunks/chunk_backend → DB bloat shrinks).

SAFETY: only touches objects with `deleted_at` set (genuinely deleted — verified no live
same-key object exists). Re-enqueues are idempotent (a worker deleting an already-gone
chunk is a no-op).

THROTTLE: the load-bearing guard against re-flooding the unpin queues — before each batch
we wait until the combined depth of --throttle-queues is under --queue-cap, so we never
enqueue faster than the workers drain. Pass every unpin queue you want paced against here;
a --backends queue missing from --throttle-queues is enqueued uncapped and logs a WARNING
(it is not auto-added — pacing against a subset can be deliberate).

Resumable: keyset-paginates by (deleted_at, object_id); pass the last printed cursor via
--start-after-ts/--start-after-id to resume. --loop re-scans from the start after each full
pass (continuous sweeper mode) to catch newly soft-deleted objects.

ROUTING (--backends): by default unpins fan out to `<backend>_unpin_requests` for each
backend in `config.delete_backends`. --backends instead enqueues DIRECTLY to each named
queue — deliberately bypassing `enqueue_unpin_request`'s allowlist intersection with
config.delete_backends, which would otherwise drop the override (empty intersection =>
"disallowed; not enqueuing", zero work reaches the queue). Reaching an off-allowlist
backend is the whole point: on prod delete_backends is ['arion'], but the objects that
block the hard-delete scan have their LIVE rows on a secondary backend (arion is already
fully unpinned). That backend's unpin queue has its own consumer, which performs the
delete *and* marks `chunk_backend.deleted`.

  python -m hippius_s3.scripts.backfill_soft_delete_unpins --dry-run
  python -m hippius_s3.scripts.backfill_soft_delete_unpins --batch 1000 --queue-cap 50000 \
      --backends ovh --deprecated-backends ipfs \
      --throttle-queues arion_unpin_requests,ovh_unpin_requests
"""

from __future__ import annotations

import argparse
import asyncio
import dataclasses
import datetime
import logging
from typing import Any
from typing import Mapping

import asyncpg
import redis.asyncio as async_redis

from hippius_s3.config import get_config
from hippius_s3.queue import UnpinChainRequest
from hippius_s3.queue import enqueue_unpin_request
from hippius_s3.queue import initialize_queue_client


logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger("backfill_unpins")

# Select a batch of soft-deleted-past-grace objects after a keyset cursor, flagging
# whether each still has live unpinnable chunks (needs an unpin enqueue) and/or live
# deprecated-backend chunks (needs in-DB reconcile). $4 = deprecated backends array.
# Index-driven via idx_objects_deleted.
_SELECT = """
WITH c AS MATERIALIZED (
    SELECT object_id, current_object_version, deleted_at, bucket_id
    FROM objects
    WHERE deleted_at IS NOT NULL
      AND deleted_at < now() - INTERVAL '1 hour'
      AND (deleted_at, object_id) > ($2::timestamptz, $3::uuid)
    ORDER BY deleted_at, object_id
    LIMIT $1
)
SELECT c.object_id, c.current_object_version AS object_version, c.deleted_at, b.main_account_id,
    EXISTS (
        SELECT 1 FROM parts p JOIN part_chunks pc ON pc.part_id = p.part_id
        JOIN chunk_backend cb ON cb.chunk_id = pc.id
        WHERE p.object_id = c.object_id AND NOT cb.deleted AND cb.backend <> ALL($4::text[])
    ) AS needs_unpin,
    EXISTS (
        SELECT 1 FROM parts p JOIN part_chunks pc ON pc.part_id = p.part_id
        JOIN chunk_backend cb ON cb.chunk_id = pc.id
        WHERE p.object_id = c.object_id AND NOT cb.deleted AND cb.backend = ANY($4::text[])
    ) AS has_deprecated
FROM c JOIN buckets b ON b.bucket_id = c.bucket_id
ORDER BY c.deleted_at, c.object_id
"""

# Reconcile deprecated-backend rows for one object (chunks gone; no backend call).
_RECONCILE_DEPRECATED = """
UPDATE chunk_backend cb
SET deleted = true, deleted_at = now()
FROM part_chunks pc
JOIN parts p ON p.part_id = pc.part_id
WHERE cb.chunk_id = pc.id
  AND cb.backend = ANY($2::text[])
  AND NOT cb.deleted
  AND p.object_id = $1
"""

_EPOCH = datetime.datetime(1970, 1, 1, tzinfo=datetime.timezone.utc)
_ZERO_UUID = "00000000-0000-0000-0000-000000000000"


def _parse_csv_flag(raw: str) -> list[str]:
    """Split one comma-separated CLI flag into trimmed, non-empty, de-duplicated values.

    Deduping is load-bearing for --backends: a repeat would LPUSH the same payload twice
    to one queue and count the object twice against --queue-cap. Harmless for the other
    two flags, but they share the helper so one flag can't parse differently from another.
    """
    values: list[str] = []
    for item in raw.split(","):
        value = item.strip()
        if value and value not in values:
            values.append(value)
    return values


def _unpin_queue(backend: str) -> str:
    return f"{backend}_unpin_requests"


@dataclasses.dataclass(frozen=True)
class _RunPlan:
    """The per-run decisions taken once from the CLI flags, so every scanned object is
    handled the same way — and dry-run and real runs differ only in side effects."""

    backends: list[str]
    deprecated: list[str]
    dry_run: bool


async def _enqueue_unpin(payload: UnpinChainRequest, backends: list[str]) -> None:
    """Route one unpin. An explicit --backends override enqueues directly to each named
    queue: the generic path intersects the request with config.delete_backends, which
    would drop exactly the off-allowlist backend the operator is trying to reach."""
    if backends:
        for b in backends:
            await enqueue_unpin_request(payload=payload, queue_name=_unpin_queue(b))
    else:
        await enqueue_unpin_request(payload=payload)


def _warn_unthrottled_backends(backends: list[str], throttle_queues: list[str]) -> None:
    """Warn when --backends routes to a queue that --throttle-queues does not pace.

    Deliberately not auto-included: an operator may be pacing against a subset on
    purpose (say, only the slower worker's queue), and silently widening the throttle
    set would change the sweep rate they asked for. Naming the gap is the whole job.
    """
    unthrottled = [_unpin_queue(b) for b in backends if _unpin_queue(b) not in throttle_queues]
    if unthrottled:
        logger.warning(
            "--backends routes to %s but --throttle-queues only paces %s; "
            "those queues are enqueued without a depth cap",
            unthrottled,
            throttle_queues or "nothing",
        )


async def _process_object(row: Mapping[str, Any], plan: _RunPlan, db: asyncpg.Connection) -> tuple[int, int]:
    """Handle one scanned object; return its (enqueued, reconciled) counter deltas.

    Dry-run takes the same branches and yields the same deltas as a real run — only the
    enqueue and the UPDATE are skipped. Counting in a separate dry-run arm is how a
    summary starts lying about what a real run would do.
    """
    enqueued = reconciled = 0
    oid = row["object_id"]

    if row["needs_unpin"]:
        if not row["main_account_id"]:
            logger.warning(f"skip {oid}: bucket has no main_account_id")
        else:
            if not plan.dry_run:
                await _enqueue_unpin(
                    UnpinChainRequest(
                        address=row["main_account_id"],
                        object_id=str(oid),
                        object_version=int(row["object_version"]),
                        # Provenance only when --backends routes directly; None
                        # => the generic path falls back to config.delete_backends.
                        delete_backends=plan.backends or None,
                    ),
                    plan.backends,
                )
            enqueued = 1

    if row["has_deprecated"]:
        if not plan.dry_run:
            await db.execute(_RECONCILE_DEPRECATED, oid, plan.deprecated)
        reconciled = 1

    return enqueued, reconciled


async def _wait_for_queue_room(redis_q: async_redis.Redis, queues: list[str], cap: int) -> None:
    """Block until the combined depth of `queues` is under cap (throttle)."""
    while True:
        depth = sum([int(await redis_q.llen(q)) for q in queues])  # ty: ignore[invalid-await]
        if depth < cap:
            return
        logger.info(f"unpin queue depth {depth} >= cap {cap}; waiting for workers to drain...")
        await asyncio.sleep(10)


async def main_async(args: argparse.Namespace) -> int:
    config = get_config()
    deprecated = _parse_csv_flag(args.deprecated_backends)
    throttle_queues = _parse_csv_flag(args.throttle_queues)
    backends = _parse_csv_flag(args.backends)
    plan = _RunPlan(backends=backends, deprecated=deprecated, dry_run=args.dry_run)
    logger.info(
        "unpin routing: %s (deprecated, reconciled in-DB: %s)",
        f"--backends {backends} (direct to queues, allowlist bypassed)"
        if backends
        else f"config.delete_backends {config.delete_backends}",
        deprecated or "none",
    )
    _warn_unthrottled_backends(backends, throttle_queues)
    db = await asyncpg.connect(config.database_url)
    redis_q = async_redis.from_url(config.redis_queues_url)
    initialize_queue_client(redis_q)

    start_ts = datetime.datetime.fromisoformat(args.start_after_ts) if args.start_after_ts else _EPOCH
    start_id = args.start_after_id or _ZERO_UUID
    cursor_ts, cursor_id = start_ts, start_id

    enqueued = reconciled = scanned = 0
    try:
        while True:
            if not args.dry_run and throttle_queues:
                await _wait_for_queue_room(redis_q, throttle_queues, args.queue_cap)

            rows = await db.fetch(_SELECT, args.batch, cursor_ts, cursor_id, deprecated)
            if not rows:
                if args.loop and not args.dry_run:
                    logger.info(f"reached end; sweeper sleeping {args.loop_sleep}s then re-scanning")
                    cursor_ts, cursor_id = start_ts, start_id
                    await asyncio.sleep(args.loop_sleep)
                    continue
                logger.info("no more soft-deleted objects past the cursor; done")
                break

            for r in rows:
                scanned += 1
                e, d = await _process_object(dict(r), plan, db)
                enqueued += e
                reconciled += d

            last = rows[-1]
            cursor_ts, cursor_id = last["deleted_at"], last["object_id"]
            logger.info(
                f"batch done: scanned={scanned} enqueued={enqueued} deprecated_reconciled={reconciled} "
                f"cursor=({cursor_ts.isoformat()}, {cursor_id})"
            )

            if args.max_objects and scanned >= args.max_objects:
                logger.info(f"reached --max-objects {args.max_objects}; stopping")
                break
            if args.sleep:
                await asyncio.sleep(args.sleep)

        logger.info(
            f"DONE{' (dry-run)' if args.dry_run else ''}: scanned={scanned} unpins_enqueued={enqueued} "
            f"deprecated_reconciled={reconciled}. Resume after a stop with: "
            f"--start-after-ts '{cursor_ts.isoformat()}' --start-after-id {cursor_id}"
        )
        return 0
    finally:
        await db.close()
        await redis_q.aclose()


def main() -> None:
    ap = argparse.ArgumentParser(description="Backfill unpins for the soft-delete backlog so hard-delete can drain it")
    ap.add_argument("--batch", type=int, default=1000, help="Objects selected per batch")
    ap.add_argument(
        "--queue-cap", type=int, default=50000, help="Pause enqueueing while combined throttle-queue depth >= this"
    )
    ap.add_argument(
        "--throttle-queues",
        default="arion_unpin_requests",
        help="Comma-separated unpin queues to pace against (pass every unpin queue you want throttled)",
    )
    ap.add_argument(
        "--deprecated-backends",
        default="ipfs",
        help="Comma-separated backends with no unpin worker; their rows are reconciled in-DB (no backend call)",
    )
    ap.add_argument(
        "--backends",
        default="",
        help=(
            "Comma-separated backends to route the unpins to. Each is enqueued DIRECTLY to its "
            "`<backend>_unpin_requests` queue, bypassing the config.delete_backends allowlist "
            "(which would intersect an off-allowlist override away). REQUIRED when the blocking "
            "rows are on a backend that delete_backends omits: on prod delete_backends is "
            "['arion'] but the stuck objects' live rows are on a secondary backend, so allowlist "
            "routing could never reach them. That queue has its own consumer, which performs the "
            "delete AND marks chunk_backend. Repeats are collapsed."
        ),
    )
    ap.add_argument("--max-objects", type=int, default=0, help="Stop after scanning this many objects (0 = all)")
    ap.add_argument("--sleep", type=float, default=0.0, help="Seconds to sleep between batches")
    ap.add_argument("--loop", action="store_true", help="Sweeper mode: re-scan from the start after each full pass")
    ap.add_argument("--loop-sleep", type=float, default=300.0, help="Seconds to sleep between sweeps in --loop mode")
    ap.add_argument("--start-after-ts", default="", help="Resume: deleted_at cursor (ISO-8601)")
    ap.add_argument("--start-after-id", default="", help="Resume: object_id cursor (UUID)")
    ap.add_argument("--dry-run", action="store_true", help="Count what would be enqueued/reconciled; change nothing")
    args = ap.parse_args()
    raise SystemExit(asyncio.run(main_async(args)))


if __name__ == "__main__":
    main()
