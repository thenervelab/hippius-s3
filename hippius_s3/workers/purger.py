from __future__ import annotations

import asyncio
import json
import logging
import time
from typing import Any

from redis.exceptions import RedisError

from gateway.services.sub_token_scope_cache import scope_cache_key
from hippius_s3.config import Config
from hippius_s3.config import get_config
from hippius_s3.monitoring import get_metrics_collector
from hippius_s3.queue import UnpinChainRequest
from hippius_s3.queue import enqueue_unpin_request
from hippius_s3.utils import get_query


logger = logging.getLogger(__name__)


async def _wait_for_unpin_headroom(redis_queues: Any, db_pool: Any, job_id: Any, config: Config) -> None:
    """Block until every unpin queue is below the high-water mark.

    A mass purge must never be able to flood redis-queues: 1.29M queued unpin requests
    once degraded the instance badly enough to break prod GETs. The heartbeat keeps
    advancing while parked so the job's lease is not reclaimed.
    """
    while True:
        depths = [int(await redis_queues.llen(f"{b}_unpin_requests")) for b in config.delete_backends]
        if max(depths, default=0) < config.purger_unpin_queue_high_water:
            return
        logger.info(
            f"purger: unpin queue depth {max(depths)} >= high water {config.purger_unpin_queue_high_water}, backing off"
        )
        get_metrics_collector().record_purger_backpressure_wait()
        await db_pool.execute("UPDATE purge_jobs SET heartbeat_at = now() WHERE job_id = $1", job_id)
        await asyncio.sleep(config.purger_backpressure_sleep_seconds)


async def _purge_account(
    db_pool: Any,
    redis_queues: Any,
    redis_cache: Any,
    job: Any,
    config: Config,
) -> tuple[int, int]:
    job_id = job["job_id"]
    account_id = job["account_id"]
    # Resume-safe: counters continue from the claimed row (reclaimed jobs keep their tally).
    deleted_objects = int(job["deleted_objects"])
    deleted_bytes = int(job["deleted_bytes"])

    # Sweep until a full pass over all buckets deletes nothing — a write racing the
    # suspension propagation window can land while an earlier bucket is being drained.
    while True:
        pass_deleted = 0
        buckets = await db_pool.fetch(get_query("list_user_buckets"), account_id)
        for bucket in buckets:
            bucket_id = bucket["bucket_id"]
            await db_pool.execute(
                "DELETE FROM multipart_uploads WHERE bucket_id = $1 AND is_completed = FALSE",
                bucket_id,
            )
            while True:
                await _wait_for_unpin_headroom(redis_queues, db_pool, job_id, config)
                rows = await db_pool.fetch(
                    get_query("purge_soft_delete_objects_batch"),
                    bucket_id,
                    config.purger_batch_size,
                )
                if not rows:
                    break
                for row in rows:
                    # Real (object_id, version=None → all versions) payloads, resolvable
                    # by the unpinner — never nuke_user.py's synthetic ones. delete_backends
                    # is left None so enqueue fans out to the configured delete backends; the
                    # unpinner's own per-request lookup is authoritative about what each
                    # backend holds (see purge_soft_delete_objects_batch.sql for why the batch
                    # no longer resolves backends itself).
                    await enqueue_unpin_request(
                        payload=UnpinChainRequest(
                            address=account_id,
                            object_id=str(row["object_id"]),
                            object_version=None,
                            ray_id=f"purge-{job_id}",
                            delete_backends=None,
                        )
                    )
                deleted_objects += len(rows)
                deleted_bytes += sum(int(r["total_bytes"]) for r in rows)
                pass_deleted += len(rows)
                await db_pool.execute(
                    get_query("update_purge_job_progress"),
                    job_id,
                    deleted_objects,
                    deleted_bytes,
                    json.dumps({"phase": "objects", "bucket": bucket["bucket_name"]}),
                )
        if pass_deleted == 0:
            break
        logger.info(f"purger: job={job_id} pass deleted {pass_deleted} objects, sweeping again")

    for bucket in await db_pool.fetch(get_query("list_user_buckets"), account_id):
        await db_pool.fetchrow(get_query("soft_delete_bucket"), bucket["bucket_id"])

    # Gateway-side credentials: drop the account's sub-token scope rows (the backend
    # re-provisions if the user returns) and invalidate their gateway cache entries.
    scope_keys = await db_pool.fetch("SELECT access_key_id FROM sub_token_scopes WHERE account_id = $1", account_id)
    await db_pool.execute("DELETE FROM sub_token_scopes WHERE account_id = $1", account_id)
    for record in scope_keys:
        try:
            await redis_cache.delete(scope_cache_key(record["access_key_id"]))
        except RedisError as exc:
            logger.warning(f"purger: scope cache invalidation failed (entry expires in <=60s): {exc}")

    await db_pool.execute(
        get_query("update_purge_job_progress"),
        job_id,
        deleted_objects,
        deleted_bytes,
        json.dumps({"phase": "finished"}),
    )
    return deleted_objects, deleted_bytes


async def process_one_job(db_pool: Any, redis_queues: Any, redis_cache: Any, config: Config) -> bool:
    """Claim and run a single purge job. Returns False when the queue is empty."""
    job = await db_pool.fetchrow(get_query("claim_purge_job"), float(config.purger_lease_seconds))
    if job is None:
        return False

    job_id = job["job_id"]
    metrics = get_metrics_collector()
    started = time.monotonic()
    logger.info(f"purger: claimed job={job_id} account={job['account_id']}")
    try:
        deleted_objects, deleted_bytes = await _purge_account(db_pool, redis_queues, redis_cache, job, config)
    except Exception as exc:
        # Deliberate top-level catch: the job row must record the failure so the
        # backend's poll sees state='failed' instead of a forever-'running' job.
        logger.exception(f"purger: job={job_id} failed")
        await db_pool.execute(
            "UPDATE purge_jobs SET state = 'failed', error = $2, finished_at = now() WHERE job_id = $1",
            job_id,
            f"{type(exc).__name__}: {exc}",
        )
        metrics.record_purger_job(success=False, deleted_objects=0, duration=time.monotonic() - started)
        return True

    await db_pool.execute(
        "UPDATE purge_jobs SET state = 'done', finished_at = now(), heartbeat_at = now() WHERE job_id = $1",
        job_id,
    )
    metrics.record_purger_job(success=True, deleted_objects=deleted_objects, duration=time.monotonic() - started)
    logger.info(f"purger: job={job_id} done — {deleted_objects} objects, {deleted_bytes} logical bytes")
    return True


async def run_purger_loop() -> None:
    """Poll purge_jobs and drive account purges through the existing
    soft-delete → unpinner → janitor pipeline. Single replica suffices (claiming is
    SKIP LOCKED race-safe regardless). Heavy deps imported lazily so the module stays
    importable by unit tests."""
    import asyncpg
    import redis.asyncio as async_redis

    from hippius_s3.monitoring import initialize_metrics_collector
    from hippius_s3.queue import initialize_queue_client
    from hippius_s3.redis_utils import create_redis_client

    config = get_config()
    # command_timeout (client) + statement_timeout (server) for the same xmin-horizon
    # reasons as the mpu-reaper pool — see mpu_reaper_statement_timeout_seconds.
    db_pool = await asyncpg.create_pool(
        config.database_url,
        min_size=1,
        max_size=5,
        command_timeout=config.purger_statement_timeout_seconds,
        server_settings={"statement_timeout": f"{config.purger_statement_timeout_seconds * 1000}"},
    )
    redis_queues = async_redis.Redis.from_url(config.redis_queues_url)
    redis_cache = create_redis_client(config.redis_url)
    initialize_queue_client(redis_queues)
    initialize_metrics_collector(redis_cache)

    logger.info(
        f"purger: started (interval={config.purger_interval_seconds}s "
        f"batch={config.purger_batch_size} high_water={config.purger_unpin_queue_high_water} "
        f"lease={config.purger_lease_seconds}s)"
    )
    # Closing on the way out is what tells Postgres and Redis this client is gone —
    # an uncancelled worker keeps its backend alive and pins the xmin horizon.
    try:
        while True:
            worked = await process_one_job(db_pool, redis_queues, redis_cache, config)
            if not worked:
                await asyncio.sleep(config.purger_interval_seconds)
    finally:
        await db_pool.close()
        await redis_queues.aclose()
        await redis_cache.aclose()
