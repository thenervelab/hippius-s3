import contextlib
import json
import logging
import os
import time
import uuid
from typing import Optional

import redis.asyncio as async_redis
from pydantic import BaseModel
from pydantic import ConfigDict

from hippius_s3.backend_routing import compute_effective_backends
from hippius_s3.config import get_config


logger = logging.getLogger(__name__)


def _normalize_queue_name(name: str) -> str:
    """Strip whitespace and quotes from queue name."""
    return name.strip().strip('"').strip("'")


_queue_client: Optional[async_redis.Redis] = None


def initialize_queue_client(redis_client: async_redis.Redis) -> None:
    """Initialize the queue client singleton. Call once during app/worker startup."""
    global _queue_client
    _queue_client = redis_client
    logger.info("Queue client initialized")


def get_queue_client() -> async_redis.Redis:
    """Get the initialized queue Redis client."""
    if _queue_client is None:
        raise RuntimeError(
            "Queue client not initialized. Call initialize_queue_client() first during app/worker startup."
        )
    return _queue_client


class Chunk(BaseModel):
    id: int


class PartChunkSpec(BaseModel):
    index: int
    # CID is required for legacy (IPFS-backed) objects but intentionally optional
    # for storage_version>=4 where chunks are addressed by deterministic keys.
    cid: str | None = None
    cipher_size_bytes: int | None = None


class PartToDownload(BaseModel):
    part_number: int
    chunks: list[PartChunkSpec]


class RetryableRequest(BaseModel):
    # Important: queue payloads are persisted. We must tolerate older/newer producers
    # sending fields that this version of the code doesn't know about.
    model_config = ConfigDict(extra="ignore")

    request_id: str | None = None
    attempts: int = 0
    first_enqueued_at: float | None = None
    last_error: str | None = None
    ray_id: str | None = None


class UploadChainRequest(RetryableRequest):
    address: str
    bucket_name: str
    object_key: str
    object_id: str
    object_version: int
    chunks: list[Chunk]
    upload_id: str | None = None
    upload_backends: list[str] | None = None  # Set by API at enqueue time
    bypass_billing: bool = False

    @property
    def name(self) -> str:
        if self.upload_id is not None:
            return f"multipart::{self.object_id}::{self.upload_id}::{self.address}"
        return f"simple::{self.object_id}::{self.address}"


class UnpinChainRequest(RetryableRequest):
    address: str
    object_id: str
    object_version: int | None = None  # None = all versions
    cid: str | None = None  # DEPRECATED — transitional, for in-flight queue compat only
    delete_backends: list[str] | None = None  # Set by API at enqueue time

    @property
    def name(self) -> str:
        ident = self.cid or self.object_id
        return f"unpin::{ident}::{self.address}::{self.object_id}"


class DownloadChainRequest(RetryableRequest):
    object_id: str
    object_version: int
    object_key: str
    bucket_name: str
    # Deprecated: retained for backward compatibility with older queued payloads.
    # Workers no longer depend on storage_version to populate the chunk cache.
    object_storage_version: int | None = None
    address: str
    subaccount: str
    substrate_url: str
    size: int
    multipart: bool
    chunks: list[PartToDownload]
    expire_at: float | None = None
    download_backends: list[str] | None = None  # Set by API at enqueue time

    @property
    def name(self) -> str:
        return f"download::{self.request_id}::{self.object_id}::{self.address}"


async def enqueue_upload_to_backends(request: UploadChainRequest) -> None:
    """Enqueue upload request to per-backend upload queues.

    Reads backends from ``request.upload_backends``; falls back to
    ``config.upload_backends`` when the field is not set.
    """
    client = get_queue_client()
    if request.request_id is None:
        request.request_id = uuid.uuid4().hex
    if request.first_enqueued_at is None:
        request.first_enqueued_at = time.time()
    if request.attempts is None:
        request.attempts = 0

    config = get_config()
    effective = compute_effective_backends(
        request.upload_backends,
        config.upload_backends,
        context={
            "request_id": request.request_id,
            "object_id": request.object_id,
            "object_version": request.object_version,
            "bucket_name": request.bucket_name,
            "object_key": request.object_key,
        },
        raise_on_empty=True,
    )
    request.upload_backends = effective or config.upload_backends

    raw = request.model_dump_json()
    for backend in request.upload_backends:
        queue_name = f"{backend}_upload_requests"
        await client.lpush(queue_name, raw)  # ty: ignore[invalid-await]
    logger.info(f"Enqueued upload request {request.name=} backends={request.upload_backends}")


async def enqueue_upload_request(payload: UploadChainRequest) -> None:
    """Convenience wrapper — delegates to enqueue_upload_to_backends."""
    await enqueue_upload_to_backends(payload)


async def dequeue_upload_request(queue_name: str) -> UploadChainRequest | None:
    """Get the next upload request from the Redis queue."""
    client = get_queue_client()

    result = await client.brpop(_normalize_queue_name(queue_name), timeout=0.5)  # ty: ignore[invalid-await, invalid-argument-type]
    if result:
        _, queue_data = result
        queue_data = json.loads(queue_data)
        return UploadChainRequest.model_validate(queue_data)
    return None


async def dequeue_upload_request_reliable(queue_name: str) -> tuple[UploadChainRequest, str] | None:
    """At-least-once variant of `dequeue_upload_request`: returns (request, handle). The caller
    MUST `ack_reliable(queue_name, handle)` once the upload is terminal (done, retry-scheduled,
    or DLQ'd); an un-acked handle is redelivered after the visibility window, so an uploader
    crash mid-upload re-drives the request instead of dropping it (idempotent `chunk_backend`)."""
    raw = await reliable_brpop(queue_name, timeout=0.5)
    if raw is None:
        return None
    return UploadChainRequest.model_validate(json.loads(raw)), raw


# Per-backend retry handling (ZSET with score = next_attempt_unix_ts)


def _upload_retry_zset(backend: str) -> str:
    return f"{backend}_upload_retries"


async def enqueue_retry_request(
    payload: UploadChainRequest,
    *,
    backend_name: str,
    delay_seconds: float,
    last_error: str | None = None,
) -> None:
    client = get_queue_client()
    if payload.request_id is None:
        payload.request_id = uuid.uuid4().hex
    payload.attempts = int((payload.attempts or 0) + 1)
    payload.last_error = last_error
    if payload.first_enqueued_at is None:
        payload.first_enqueued_at = time.time()
    next_ts = time.time() + max(0.0, float(delay_seconds))
    member = payload.model_dump_json()
    zset_key = _upload_retry_zset(backend_name)
    await client.zadd(zset_key, {member: next_ts})
    logger.info(
        f"Scheduled retry for {payload.name=} backend={backend_name} attempts={payload.attempts} next_at={int(next_ts)}"
    )


async def move_due_upload_retries(
    *,
    backend_name: str,
    now_ts: float | None = None,
    max_items: int = 64,
) -> int:
    """Move due retry items back to the backend's upload queue. Returns number moved."""
    client = get_queue_client()
    target_queue = f"{backend_name}_upload_requests"
    zset_key = _upload_retry_zset(backend_name)

    now_ts = time.time() if now_ts is None else now_ts
    members = await client.zrangebyscore(zset_key, min="-inf", max=now_ts, start=0, num=max_items)
    moved = 0
    for m in members:
        try:
            async with client.pipeline(transaction=True) as pipe:
                pipe.zrem(zset_key, m)
                pipe.lpush(target_queue, m)
                await pipe.execute()
            moved += 1
        except Exception:
            logger.exception(f"Failed to move retry item back to {target_queue}")
    return moved


async def enqueue_unpin_request(payload: UnpinChainRequest, *, queue_name: str | None = None) -> None:
    """Add an unpin request to the Redis queue(s) for processing by unpinner workers.

    If queue_name is provided, enqueue to that single queue only.
    Otherwise fan out to all configured unpin queues.
    """
    client = get_queue_client()
    if payload.request_id is None:
        payload.request_id = uuid.uuid4().hex
    if payload.first_enqueued_at is None:
        payload.first_enqueued_at = time.time()
    if payload.attempts is None:
        payload.attempts = 0

    raw = payload.model_dump_json()

    if queue_name is not None:
        await client.lpush(_normalize_queue_name(queue_name), raw)  # ty: ignore[invalid-await]
        logger.info(f"Enqueued unpin request {payload.name=} queue={queue_name}")
    else:
        config = get_config()
        effective = compute_effective_backends(
            payload.delete_backends,
            config.delete_backends,
            context={
                "request_id": payload.request_id,
                "object_id": payload.object_id,
                "object_version": payload.object_version,
            },
            raise_on_empty=False,
        )
        if payload.delete_backends is not None and effective is None:
            logger.error(
                "All requested delete backends disallowed by config; not enqueuing. requested=%s allowed=%s context=%s",
                payload.delete_backends,
                config.delete_backends,
                {
                    "request_id": payload.request_id,
                    "object_id": payload.object_id,
                    "object_version": payload.object_version,
                },
            )
            return
        backends = effective or config.delete_backends
        queue_names = [f"{b}_unpin_requests" for b in backends]
        for qname in queue_names:
            await client.lpush(qname, raw)  # ty: ignore[invalid-await]
        logger.info(f"Enqueued unpin request {payload.name=} queues={queue_names}")


async def dequeue_unpin_request(queue_name: str = "unpin_requests") -> UnpinChainRequest | None:
    """Get the next unpin request from the Redis queue."""
    client = get_queue_client()
    result = await client.brpop(_normalize_queue_name(queue_name), timeout=3)  # ty: ignore[invalid-await, invalid-argument-type]
    if result:
        _, queue_data = result
        return UnpinChainRequest.model_validate_json(queue_data)
    return None


async def dequeue_unpin_request_reliable(queue_name: str = "unpin_requests") -> tuple[UnpinChainRequest, str] | None:
    """At-least-once variant of `dequeue_unpin_request`: returns (request, handle). The caller
    MUST `ack_reliable(queue_name, handle)` when the unpin is terminal; an un-acked handle is
    redelivered after the visibility window (idempotent soft-delete makes redelivery safe)."""
    raw = await reliable_brpop(queue_name, timeout=3)
    if raw is None:
        return None
    return UnpinChainRequest.model_validate_json(raw), raw


def _unpin_retry_zset(backend: str) -> str:
    return f"{backend}_unpin_retries"


async def enqueue_unpin_retry_request(
    payload: UnpinChainRequest,
    *,
    backend_name: str,
    delay_seconds: float,
    last_error: str | None = None,
) -> None:
    client = get_queue_client()
    if payload.request_id is None:
        payload.request_id = uuid.uuid4().hex
    payload.attempts = int((payload.attempts or 0) + 1)
    payload.last_error = last_error
    if payload.first_enqueued_at is None:
        payload.first_enqueued_at = time.time()
    next_ts = time.time() + max(0.0, float(delay_seconds))
    member = payload.model_dump_json()
    zset_key = _unpin_retry_zset(backend_name)
    await client.zadd(zset_key, {member: next_ts})
    logger.info(
        f"Scheduled unpin retry for {payload.name=} backend={backend_name} attempts={payload.attempts} next_at={int(next_ts)}"
    )


async def move_due_unpin_retries(
    *,
    backend_name: str,
    now_ts: float | None = None,
    max_items: int = 64,
) -> int:
    """Move due unpin retry items back to the backend's unpin queue. Returns number moved."""
    client = get_queue_client()
    target_queue = f"{backend_name}_unpin_requests"
    zset_key = _unpin_retry_zset(backend_name)

    now_ts = time.time() if now_ts is None else now_ts
    members = await client.zrangebyscore(zset_key, min="-inf", max=now_ts, start=0, num=max_items)
    moved = 0
    for m in members:
        try:
            async with client.pipeline(transaction=True) as pipe:
                pipe.zrem(zset_key, m)
                pipe.lpush(target_queue, m)
                await pipe.execute()
            moved += 1
        except Exception:
            logger.exception(f"Failed to move unpin retry item back to {target_queue}")
    return moved


async def enqueue_download_request(payload: DownloadChainRequest) -> None:
    """Add a download request to per-backend download queues."""
    client = get_queue_client()

    config = get_config()
    effective = compute_effective_backends(
        payload.download_backends,
        config.download_backends,
        context={
            "request_id": payload.request_id,
            "object_id": payload.object_id,
            "object_version": payload.object_version,
            "bucket_name": payload.bucket_name,
            "object_key": payload.object_key,
        },
        raise_on_empty=False,
    )
    if payload.download_backends is not None and effective is None:
        logger.error(
            "All requested download backends disallowed by config; not enqueuing. requested=%s allowed=%s context=%s",
            payload.download_backends,
            config.download_backends,
            {
                "request_id": payload.request_id,
                "object_id": payload.object_id,
                "object_version": payload.object_version,
            },
        )
        return
    payload.download_backends = effective or config.download_backends

    raw = payload.model_dump_json()
    queue_names = [f"{b}_download_requests" for b in payload.download_backends]

    for qname in queue_names:
        await client.lpush(qname, raw)  # ty: ignore[invalid-await]

    logger.info(f"Enqueued download request {payload.name=} queues={queue_names}")


async def dequeue_download_request(queue_name: str) -> DownloadChainRequest | None:
    """Get the next download request from a backend-specific download queue."""
    client = get_queue_client()
    result = await client.brpop(_normalize_queue_name(queue_name), timeout=5)  # ty: ignore[invalid-await, invalid-argument-type]
    if result:
        _, queue_data = result
        return DownloadChainRequest.model_validate_json(queue_data)
    return None


# ---------------------------------------------------------------------------------------------
# A12: at-least-once reliable dequeue (in-flight ZSET + reaper).
#
# A plain BRPOP removes the request the instant it is read, so a consumer that crashes between
# the pop and completing the work LOSES the request — and a reader waiting on the resulting chunk
# hangs to the cache TTL (~1h). These helpers make dequeue at-least-once: the popped raw member is
# recorded in a per-queue in-flight ZSET scored by a visibility deadline; the consumer ACKs
# (ZREM) on completion, and a periodic reaper moves any member past its deadline back onto the
# main list (a crash redelivery). Redelivery is safe because every consumer is idempotent
# (deterministic chunk writes, `insert_chunk_backend ON CONFLICT`, idempotent soft-delete), so a
# duplicate is harmless. The visibility window must exceed the worst-case processing time or a
# still-in-flight request is redelivered as a (harmless but wasteful) duplicate. Mirrors the
# existing retry-ZSET move pattern (`move_due_upload_retries`).
# ---------------------------------------------------------------------------------------------

DEFAULT_QUEUE_VISIBILITY_SECONDS = float(os.getenv("HIPPIUS_QUEUE_VISIBILITY_SECONDS", "600"))


def _inflight_zset(queue_name: str) -> str:
    return f"{_normalize_queue_name(queue_name)}:inflight"


async def reliable_brpop(
    queue_name: str,
    *,
    timeout: float = 5,
    visibility_seconds: float = DEFAULT_QUEUE_VISIBILITY_SECONDS,
) -> Optional[str]:
    """BRPOP the next raw member and record it in the in-flight ZSET with a visibility deadline.

    Returns the raw member string (which is also the ACK/reap handle) or None on timeout. After
    this returns, the member is owned by this consumer until it `ack_reliable`s it or the
    visibility window lapses and the reaper redelivers it.
    """
    client = get_queue_client()
    qn = _normalize_queue_name(queue_name)
    result = await client.brpop(qn, timeout=timeout)  # ty: ignore[invalid-await, invalid-argument-type]
    if not result:
        return None
    _, raw = result
    deadline = time.time() + max(1.0, float(visibility_seconds))
    await client.zadd(_inflight_zset(qn), {raw: deadline})
    return raw


async def ack_reliable(queue_name: str, handle: str) -> None:
    """Mark an in-flight member done (remove it from the in-flight ZSET) so the reaper never
    redelivers it. Best-effort: a failed ack just risks one harmless idempotent redelivery."""
    client = get_queue_client()
    with contextlib.suppress(Exception):
        await client.zrem(_inflight_zset(queue_name), handle)  # ty: ignore[invalid-await]


async def requeue_stale_inflight(
    queue_name: str,
    *,
    now_ts: float | None = None,
    max_items: int = 128,
) -> int:
    """Redeliver in-flight members past their visibility deadline (a consumer crashed mid-work):
    move each back onto the main list. Returns the number redelivered. Mirrors
    `move_due_upload_retries`."""
    client = get_queue_client()
    qn = _normalize_queue_name(queue_name)
    zkey = _inflight_zset(qn)
    now_ts = time.time() if now_ts is None else now_ts
    members = await client.zrangebyscore(zkey, min="-inf", max=now_ts, start=0, num=max_items)
    moved = 0
    for m in members:
        try:
            async with client.pipeline(transaction=True) as pipe:
                pipe.zrem(zkey, m)
                pipe.lpush(qn, m)
                await pipe.execute()
            moved += 1
        except Exception:
            logger.exception(f"Failed to redeliver a stale in-flight member to {qn}")
    if moved:
        logger.warning(f"Redelivered {moved} stale in-flight request(s) to {qn} (a consumer crashed mid-processing)")
    return moved


async def dequeue_download_request_reliable(queue_name: str) -> tuple[DownloadChainRequest, str] | None:
    """At-least-once variant of `dequeue_download_request`: returns (request, handle). The caller
    MUST `ack_reliable(queue_name, handle)` once the download completes (success or terminal
    discard); an un-acked handle is redelivered by `requeue_stale_inflight` after the visibility
    window, so a downloader crash no longer strands the reader on a chunk that never arrives."""
    raw = await reliable_brpop(queue_name, timeout=5)
    if raw is None:
        return None
    return DownloadChainRequest.model_validate_json(raw), raw
