import json
import logging
import time
import uuid
from typing import Optional
from typing import Union

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
    subaccount_seed_phrase: str
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
        await client.lpush(queue_name, raw)
    logger.info(f"Enqueued upload request {request.name=} backends={request.upload_backends}")


async def enqueue_upload_request(payload: UploadChainRequest) -> None:
    """Convenience wrapper — delegates to enqueue_upload_to_backends."""
    await enqueue_upload_to_backends(payload)


async def dequeue_upload_request(queue_name: str) -> UploadChainRequest | None:
    """Get the next upload request from the Redis queue."""
    client = get_queue_client()

    result = await client.brpop(_normalize_queue_name(queue_name), timeout=0.5)
    if result:
        _, queue_data = result
        queue_data = json.loads(queue_data)
        return UploadChainRequest.model_validate(queue_data)
    return None


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
        await client.lpush(_normalize_queue_name(queue_name), raw)
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
            await client.lpush(qname, raw)
        logger.info(f"Enqueued unpin request {payload.name=} queues={queue_names}")


async def dequeue_unpin_request(queue_name: str = "unpin_requests") -> Union[UnpinChainRequest, None]:
    """Get the next unpin request from the Redis queue."""
    client = get_queue_client()
    result = await client.brpop(_normalize_queue_name(queue_name), timeout=3)
    if result:
        _, queue_data = result
        return UnpinChainRequest.model_validate_json(queue_data)
    return None


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
        await client.lpush(qname, raw)

    logger.info(f"Enqueued download request {payload.name=} queues={queue_names}")


async def dequeue_download_request(queue_name: str) -> Union[DownloadChainRequest, None]:
    """Get the next download request from a backend-specific download queue."""
    client = get_queue_client()
    result = await client.brpop(_normalize_queue_name(queue_name), timeout=5)
    if result:
        _, queue_data = result
        return DownloadChainRequest.model_validate_json(queue_data)
    return None
