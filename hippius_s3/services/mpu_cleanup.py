# Terminal cleanup for aborted or abandoned multipart uploads.
#
# An MPU writes each part to a node-local SSD object cache (the drain's CEPHOR_SSD_ROOT)
# and the drain records a cephor_replication_status row per part, but
# object_versions.address is written only at complete. So an abort — or an abandoned
# upload the client never completes — leaves a version whose address stays NULL: the
# drain copies each part to Ceph, then defers the upload enqueue as not-ready, and the
# reconciler keeps re-recording the parts it still sees on local SSD, so the drain
# re-claims + re-copies + re-defers them every cycle.
#
# The churn-stopper has to work from a CENTRAL caller (the api pod handling an abort, or
# a singleton reaper) that cannot reach every ingest node's local SSD. So instead of
# deleting (which each node's reconciler would undo by re-recording the still-present
# local part), we mark the version's replication rows terminal ('failed'): the
# reconciler skips 'failed' rows and claim_part never re-claims them, so the per-node
# drain stops touching the parts on every node. Reclaiming the node-local SSD/Ceph bytes
# is the separate orphan-GC concern (deferred). Keyed on the only reliable "never
# serveable" signal — object_versions.address IS NULL — so a completed or simple upload
# (always address-bearing) is never matched.

from __future__ import annotations

import asyncio
import json
import logging
import time
from dataclasses import dataclass
from typing import Any
from typing import Iterable

from hippius_s3.monitoring import get_metrics_collector
from hippius_s3.utils import get_query


logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class ReapResult:
    """Outcome of one reaper pass: how many uploads were reaped and how far behind we
    were (the age of the oldest one we reaped, or None when nothing was reaped)."""

    count: int
    oldest_reaped_age_seconds: float | None


def _max_optional(a: float | None, b: float | None) -> float | None:
    """Largest of two optional ages, ignoring None (nothing-reaped) operands."""
    if a is None:
        return b
    if b is None:
        return a
    return max(a, b)


async def fail_version_replication(db: Any, *, object_id: Any, object_version: int | None) -> None:
    """Mark one object version's active replication rows terminal ('failed').

    The central, node-agnostic churn-stopper: a 'failed' row is neither re-recorded by
    the reconciler nor re-claimed by the drain, so the per-node re-copy/re-defer loop
    halts on every node. Idempotent — rows already 'failed'/'replicated' are untouched.
    A NULL ``object_version`` (legacy parts carry one) fails every version of the object.
    """
    await db.execute(
        get_query("fail_replication_status_for_version"),
        str(object_id),
        None if object_version is None else int(object_version),
    )


async def wake_version_replication(db: Any, *, object_id: Any, object_version: int) -> None:
    """Clear drain defer backoff for one completed version's still-'pending' rows.

    The counterpart of ``fail_version_replication``: completion (not abort) is the
    signal. In-progress-MPU parts defer with exponential backoff because their upload
    enqueue is not ready until the address lands at complete; clearing the backoff lets
    the drain claim them on its next poll instead of waiting out up to the cap.
    """
    await db.execute(
        get_query("wake_replication_status_for_version"),
        str(object_id),
        int(object_version),
    )


async def reap_abandoned_uploads(db: Any, *, stale_seconds: int, dlq_object_ids: set[str]) -> ReapResult:
    """Auto-abort never-finalized multipart uploads older than ``stale_seconds``.

    For each abandoned ``(upload_id, object_id, object_version)`` (address never written),
    mark its replication rows terminal and delete the multipart_uploads row (the cascade
    drops its ``parts`` rows). DLQ-protected objects are skipped, mirroring the janitor's
    gate — their data may still be needed. Returns the count reaped + the age of the
    oldest reaped upload (the reaper's lag).
    """
    rows = await db.fetch(get_query("list_abandoned_versions"), int(stale_seconds))
    reaped = 0
    oldest_age: float | None = None
    for row in rows:
        object_id = row["object_id"]
        if str(object_id) in dlq_object_ids:
            logger.debug("mpu-reaper: skipping DLQ-protected object_id=%s", object_id)
            continue
        await fail_version_replication(db, object_id=object_id, object_version=row["object_version"])
        await db.execute(get_query("abort_multipart_upload"), row["upload_id"])
        reaped += 1
        age = row.get("age_seconds")
        if age is not None:
            oldest_age = age if oldest_age is None else max(oldest_age, age)
    return ReapResult(count=reaped, oldest_reaped_age_seconds=oldest_age)


async def sweep_orphan_replication_versions(db: Any, *, stale_seconds: int, dlq_object_ids: set[str]) -> ReapResult:
    """Mark leaked drain replication rows terminal, keyed directly on cephor (the A21 backstop).

    The abandoned-MPU reaper only sees uploads that still have a ``multipart_uploads`` row;
    an abort deletes that header before its best-effort terminal-mark, so an abort that dies
    in that window orphans the version's ``cephor_replication_status`` rows where the reaper
    can never find them — and the drain re-claims/re-copies/re-defers them forever. This
    sweep finds those versions from the drain rows alone (``list_orphan_replication_versions``)
    and marks each one 'failed'. DLQ-protected objects are spared, mirroring the reaper.

    Unlike the reaper this only ever MARKS (never deletes), so it is safe to be resilient
    per version: a transient mark failure (e.g. a row lock) is logged and skipped rather than
    aborting the pass, and the version — still active in cephor — is retried next cycle.
    Returns the count marked + the age of the oldest swept version (the backstop's lag).
    """
    rows = await db.fetch(get_query("list_orphan_replication_versions"), int(stale_seconds))
    swept = 0
    oldest_age: float | None = None
    for row in rows:
        object_id = row["object_id"]
        if str(object_id) in dlq_object_ids:
            logger.debug("mpu-sweep: skipping DLQ-protected object_id=%s", object_id)
            continue
        try:
            await fail_version_replication(db, object_id=object_id, object_version=row["version"])
        except Exception:
            logger.exception("mpu-sweep: failed to mark object_id=%s terminal; retrying next cycle", object_id)
            continue
        swept += 1
        age = row.get("age_seconds")
        if age is not None:
            oldest_age = age if oldest_age is None else max(oldest_age, age)
    return ReapResult(count=swept, oldest_reaped_age_seconds=oldest_age)


async def gather_dlq_object_ids(redis_client: Any, upload_backends: Iterable[str]) -> set[str]:
    """Object ids currently parked in any upload/unpin DLQ — never reap their data.

    Mirrors the janitor's DLQ protection (the same per-backend key layout), kept here so
    the reaper shares the guard without importing the janitor entry script. Best-effort:
    a Redis read failure logs and skips that key rather than aborting the cycle.
    """
    object_ids: set[str] = set()
    dlq_keys = [f"{backend}_upload_requests:dlq" for backend in upload_backends]
    dlq_keys.append("unpin_requests:dlq")
    for dlq_key in dlq_keys:
        try:
            entries = await asyncio.wait_for(redis_client.lrange(dlq_key, 0, -1), timeout=5.0)
        except Exception as exc:
            logger.warning("mpu-reaper: failed to read DLQ %s: %s", dlq_key, exc)
            continue
        for entry_json in entries:
            try:
                entry = json.loads(entry_json)
            except (json.JSONDecodeError, TypeError):
                continue
            if obj_id := entry.get("object_id"):
                object_ids.add(str(obj_id))
    return object_ids


async def run_reaper_cycle(
    db_pool: Any,
    redis_client: Any,
    *,
    stale_seconds: int,
    sweep_grace_seconds: int,
    upload_backends: Iterable[str],
) -> None:
    """Run one reaper pass, time it, and record its metrics. Never raises — a failed
    cycle is logged and recorded as ``success=false`` so the loop keeps going.

    ``stale_seconds`` is the abandoned-MPU reaper's window; ``sweep_grace_seconds`` is the
    orphan-replication sweep's window. They are separate knobs (though they default equal) so
    the leak backstop can be tuned without moving the reaper — and, critically, so the sweep
    grace stays in lockstep with the janitor's aged-pending-orphan gauge, which counts exactly
    the population this sweep clears."""
    collector = get_metrics_collector()
    started = time.monotonic()
    try:
        dlq_object_ids = await gather_dlq_object_ids(redis_client, upload_backends)
        async with db_pool.acquire() as db:
            result = await reap_abandoned_uploads(db, stale_seconds=stale_seconds, dlq_object_ids=dlq_object_ids)
            sweep = await sweep_orphan_replication_versions(
                db, stale_seconds=sweep_grace_seconds, dlq_object_ids=dlq_object_ids
            )
        if result.count:
            logger.info("mpu-reaper: reaped %d abandoned multipart upload(s)", result.count)
        if sweep.count:
            logger.info("mpu-sweep: marked %d leaked orphan version(s) terminal", sweep.count)
        # Report the lag as the oldest across BOTH paths so the dashboard reflects the
        # worst backstop delay, not just the abandoned-MPU reaper's.
        oldest = _max_optional(result.oldest_reaped_age_seconds, sweep.oldest_reaped_age_seconds)
        collector.record_mpu_reaper_cycle(
            success=True,
            reaped=result.count,
            swept=sweep.count,
            duration=time.monotonic() - started,
            oldest_reaped_age=oldest,
        )
    except Exception:
        logger.exception("mpu-reaper cycle failed; will retry next interval")
        collector.record_mpu_reaper_cycle(success=False, reaped=0, swept=0, duration=time.monotonic() - started)


async def run_mpu_reaper_loop() -> None:
    """Periodically reap abandoned multipart uploads until the process is stopped.

    Pure DB + Redis work (no per-node FS), so a single replica suffices. Imports its
    heavy deps lazily so the module stays importable by the unit tests.
    """
    import asyncpg
    import redis.asyncio as async_redis

    from hippius_s3.config import get_config
    from hippius_s3.monitoring import initialize_metrics_collector

    config = get_config()
    # command_timeout is load-bearing, not tidiness: without it a bad plan runs unbounded and
    # pins the xmin horizon, which stops VACUUM reclaiming anywhere in the database. See
    # mpu_reaper_statement_timeout_seconds in config.py for the incident this bounds.
    db_pool = await asyncpg.create_pool(
        config.database_url,
        min_size=1,
        max_size=5,
        command_timeout=config.mpu_reaper_statement_timeout_seconds,
        # Belt AND braces, because they fail differently. command_timeout is client-side:
        # asyncpg sends a CancelRequest, which does nothing if the client itself is wedged
        # or the connection is black-holed — exactly when a statement would keep running and
        # keep pinning the horizon. statement_timeout is enforced by the backend and needs no
        # live client. Same value; whichever notices first wins.
        server_settings={"statement_timeout": f"{config.mpu_reaper_statement_timeout_seconds * 1000}"},
    )
    # The upload/unpin DLQs live on redis-queues (not the main cache), matching the
    # janitor's DLQ scan — read protection ids from the same instance.
    redis_client = async_redis.from_url(config.redis_queues_url)
    initialize_metrics_collector(redis_client)
    logger.info(
        "mpu-reaper: started (stale_seconds=%s sweep_grace_seconds=%s interval=%ss)",
        config.mpu_stale_seconds,
        config.mpu_sweep_grace_seconds,
        config.mpu_reaper_interval_seconds,
    )
    try:
        while True:
            await run_reaper_cycle(
                db_pool,
                redis_client,
                stale_seconds=config.mpu_stale_seconds,
                sweep_grace_seconds=config.mpu_sweep_grace_seconds,
                upload_backends=config.upload_backends,
            )
            await asyncio.sleep(config.mpu_reaper_interval_seconds)
    finally:
        await db_pool.close()
