import asyncio
import logging
import uuid
from typing import Any
from typing import Optional

from hippius_s3.queue import Chunk
from hippius_s3.queue import UploadChainRequest
from hippius_s3.queue import enqueue_upload_to_backends
from hippius_s3.utils import get_query


logger = logging.getLogger(__name__)


async def promote_part(
    db_pool: Any,
    config: Any,
    *,
    object_id: str,
    object_version: int,
    part_number: int,
) -> bool:
    """Enqueue the backend upload for a single part that has replicated to ceph.

    The drain-gated counterpart to the api's PUT-time enqueue: once a part is on the
    shared pool, the workers can read it, so we build the (now fully derivable)
    UploadChainRequest by object_id and fan it out to the configured backends. Per-part
    so MPU parts pipeline to the backends as each one lands. Returns False (and logs)
    when the version row or its address is missing — the sweep will retry.
    """
    oid = uuid.UUID(object_id)
    async with db_pool.acquire() as conn:
        row = await conn.fetchrow(get_query("promoter_build_request"), oid, int(object_version))
        # upload_id is only set for MPU objects; the uploader also re-derives it, but
        # passing it avoids a redundant lookup there.
        upload_id = await conn.fetchval(
            "SELECT upload_id FROM multipart_uploads WHERE object_id = $1 ORDER BY initiated_at DESC LIMIT 1",
            oid,
        )

    if row is None or row["address"] is None:
        logger.warning(
            "promoter: cannot build request (missing version row or address) object_id=%s v=%s part=%s",
            object_id,
            object_version,
            part_number,
        )
        return False

    payload = UploadChainRequest(
        address=row["address"],
        bucket_name=row["bucket_name"],
        object_key=row["object_key"],
        object_id=object_id,
        object_version=int(object_version),
        chunks=[Chunk(id=int(part_number))],
        upload_id=str(upload_id) if upload_id is not None else None,
        upload_backends=config.upload_backends,
    )
    await enqueue_upload_to_backends(payload)
    logger.info(
        "Promoted part to backend upload object_id=%s v=%s part=%s backends=%s",
        object_id,
        object_version,
        part_number,
        config.upload_backends,
    )
    return True


def _parse_notification(payload: str) -> Optional[tuple[str, int, int]]:
    """Parse a `cephor_replicated` payload `<object_id>:<version>:<part>`.

    object_id is a UUID (no colons), so a plain split is unambiguous. Returns None on a
    malformed payload (logged by the caller) rather than killing the listen loop.
    """
    parts = payload.split(":")
    if len(parts) != 3:
        return None
    object_id, version, part_number = parts
    if not version.isdigit() or not part_number.isdigit():
        return None
    return object_id, int(version), int(part_number)


async def run_upload_promoter_loop() -> None:
    import asyncpg
    from redis.asyncio import Redis

    from hippius_s3.config import get_config
    from hippius_s3.queue import initialize_queue_client

    config = get_config()

    # The drain emits cephor_replicated unconditionally, but the api only stops its
    # PUT-time enqueue when the flag is on. So if we promoted while the flag is off, the
    # api's enqueue AND ours would both fire — a double upload. Idle until enabled (the
    # pod stays up; flipping the flag restarts it). With the flag off, notifications go
    # to no listener and are harmlessly dropped.
    if not config.drain_gated_upload_enabled:
        logger.info("drain-gated upload disabled; upload-promoter idling (set HIPPIUS_DRAIN_GATED_UPLOAD_ENABLED=true)")
        # Park forever (until the pod is restarted by a flag change) without spinning.
        await asyncio.Event().wait()
        return

    db_pool = await asyncpg.create_pool(config.database_url, min_size=2, max_size=10)
    redis_queues_client = Redis.from_url(config.redis_queues_url)
    initialize_queue_client(redis_queues_client)

    # Work items are `(object_id, version, part)` tuples fed by BOTH the LISTEN
    # callback (low-latency) and the periodic sweep (backstop). A bounded queue applies
    # backpressure if promotion can't keep up with a replication burst.
    work_q: asyncio.Queue[tuple[str, int, int]] = asyncio.Queue(maxsize=10000)

    # Dedicated connection for LISTEN — pooled connections get recycled, which would
    # silently drop the subscription.
    listen_conn = await asyncpg.connect(config.database_url)

    def _on_notify(_conn: Any, _pid: int, _channel: str, payload: str) -> None:
        parsed = _parse_notification(payload)
        if parsed is None:
            logger.warning("promoter: ignoring malformed cephor_replicated payload %r", payload)
            return
        try:
            work_q.put_nowait(parsed)
        except asyncio.QueueFull:
            # Dropped here is harmless: the sweep re-finds any un-promoted part.
            logger.warning("promoter: work queue full, dropping notify (sweep will recover) %r", payload)

    await listen_conn.add_listener("cephor_replicated", _on_notify)

    async def _sweeper() -> None:
        while True:
            await asyncio.sleep(config.promoter_sweep_interval_seconds)
            try:
                async with db_pool.acquire() as conn:
                    rows = await conn.fetch(get_query("promoter_sweep_unpromoted"), int(config.promoter_sweep_batch))
                for r in rows:
                    work_q.put_nowait((str(r["object_id"]), int(r["version"]), int(r["part_number"])))
                if rows:
                    logger.info("promoter sweep enqueued %d replicated-but-unpromoted parts", len(rows))
            except asyncio.QueueFull:
                logger.warning("promoter: work queue full during sweep; will retry next interval")
            except Exception as e:
                logger.error("promoter sweep failed: %s", e)

    sweeper_task = asyncio.create_task(_sweeper())
    logger.info(
        "Starting upload-promoter (sweep_interval=%ss batch=%s backends=%s)",
        config.promoter_sweep_interval_seconds,
        config.promoter_sweep_batch,
        config.upload_backends,
    )

    try:
        while True:
            object_id, object_version, part_number = await work_q.get()
            try:
                await promote_part(
                    db_pool,
                    config,
                    object_id=object_id,
                    object_version=object_version,
                    part_number=part_number,
                )
            except Exception as e:
                # A failed promote is not fatal — the sweep re-finds the part (it still
                # has no chunk_backend rows) and retries. Surface it and keep draining.
                logger.error(
                    "promoter: promote_part failed object_id=%s v=%s part=%s: %s",
                    object_id,
                    object_version,
                    part_number,
                    e,
                )
    except (KeyboardInterrupt, asyncio.CancelledError):
        logger.info("upload-promoter stopping…")
    finally:
        sweeper_task.cancel()
        await asyncio.gather(sweeper_task, return_exceptions=True)
        await listen_conn.remove_listener("cephor_replicated", _on_notify)
        await listen_conn.close()
        await redis_queues_client.aclose()
        await db_pool.close()
