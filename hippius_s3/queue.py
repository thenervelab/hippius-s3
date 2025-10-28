import json
import logging
import time
import uuid
from typing import List
from typing import Union

import redis.asyncio as async_redis
from pydantic import BaseModel

from hippius_s3.config import get_config


logger = logging.getLogger(__name__)


class Chunk(BaseModel):
    id: int


class PartChunkSpec(BaseModel):
    index: int
    cid: str
    cipher_size_bytes: int | None = None


class PartToDownload(BaseModel):
    part_number: int
    chunks: list[PartChunkSpec]


class ChainRequest(BaseModel):
    substrate_url: str
    ipfs_node: str
    address: str
    subaccount: str
    subaccount_seed_phrase: str
    bucket_name: str
    object_key: str
    # Deprecated: retained for backward compatibility; uploads are pre-encrypted
    should_encrypt: bool = False
    object_id: str
    object_version: int

    # Retry & tracing metadata
    request_id: str | None = None
    attempts: int = 0
    first_enqueued_at: float | None = None
    last_error: str | None = None

    @property
    def name(self):
        # Unified request uses upload_id; legacy multipart uses multipart_upload_id
        if hasattr(self, "upload_id") and self.upload_id is not None:
            return f"multipart::{self.object_id}::{self.upload_id}::{self.address}"
        if hasattr(self, "multipart_upload_id") and self.multipart_upload_id is not None:
            return f"multipart::{self.object_id}::{self.multipart_upload_id}::{self.address}"
        return f"simple::{self.object_id}::{self.address}"


class UploadChainRequest(ChainRequest):
    """Unified upload request type that combines simple and multipart uploads."""

    chunks: list[Chunk]
    upload_id: str | None = None


class UnpinChainRequest(ChainRequest):
    cid: str


class DownloadChainRequest(BaseModel):
    request_id: str
    object_id: str
    object_version: int
    object_key: str
    bucket_name: str
    address: str
    subaccount: str
    subaccount_seed_phrase: str
    substrate_url: str
    ipfs_node: str
    should_decrypt: bool
    size: int
    multipart: bool
    chunks: list[PartToDownload]

    @property
    def name(self):
        return f"download::{self.request_id}::{self.object_id}::{self.address}"


class SubstratePinningRequest(BaseModel):
    cids: list[str]
    address: str
    object_id: str
    object_version: int
    request_id: str | None = None
    attempts: int = 0
    first_enqueued_at: float | None = None
    last_error: str | None = None

    @property
    def name(self):
        return f"substrate::{self.object_id}::{self.address}"


async def enqueue_upload_request(
    payload: UploadChainRequest,
    redis_client: async_redis.Redis,
) -> None:
    """Add an upload request to the Redis queue for processing by workers."""
    # Initialize tracing metadata if missing
    if payload.request_id is None:
        payload.request_id = uuid.uuid4().hex
    if payload.first_enqueued_at is None:
        payload.first_enqueued_at = time.time()
    if payload.attempts is None:
        payload.attempts = 0

    # Fan-out to one or more queues per configuration
    config = get_config()
    raw = payload.model_dump_json()
    queue_names_str: str = getattr(config, "upload_queue_names", "upload_requests")

    def _norm(name: str) -> str:
        return name.strip().strip('"').strip("'")

    queue_names: List[str] = [_norm(q) for q in queue_names_str.split(",") if _norm(q)]
    for qname in queue_names:
        await redis_client.lpush(qname, raw)
    logger.info(f"Enqueued upload request {payload.name=} queues={queue_names}")


async def dequeue_upload_request(
    redis_client: async_redis.Redis,
) -> UploadChainRequest | None:
    """Get the next upload request from the Redis queue."""
    config = get_config()
    raw_qname: str = getattr(config, "pinner_consume_queue", "upload_requests")
    queue_name: str = raw_qname.strip().strip('"').strip("'")
    # Use a short blocking timeout to enable timely batch flushes
    result = await redis_client.brpop(queue_name, timeout=0.5)
    if result:
        _, queue_data = result
        queue_data = json.loads(queue_data)

        return UploadChainRequest.model_validate(queue_data)

    return None


# Retry handling (ZSET with score = next_attempt_unix_ts)
RETRY_ZSET = "upload_retries"


async def enqueue_retry_request(
    payload: UploadChainRequest,
    redis_client: async_redis.Redis,
    *,
    delay_seconds: float,
    last_error: str | None = None,
) -> None:
    if payload.request_id is None:
        payload.request_id = uuid.uuid4().hex
    payload.attempts = int((payload.attempts or 0) + 1)
    payload.last_error = last_error
    if payload.first_enqueued_at is None:
        payload.first_enqueued_at = time.time()
    next_ts = time.time() + max(0.0, float(delay_seconds))
    member = payload.model_dump_json()
    await redis_client.zadd(RETRY_ZSET, {member: next_ts})
    logger.info(f"Scheduled retry for {payload.name=} attempts={payload.attempts} next_at={int(next_ts)}")


async def move_due_retries_to_primary(
    redis_client: async_redis.Redis,
    *,
    now_ts: float | None = None,
    max_items: int = 64,
) -> int:
    """Move due retry items back to the primary queue. Returns number moved."""
    now_ts = time.time() if now_ts is None else now_ts
    # Fetch due items
    members = await redis_client.zrangebyscore(RETRY_ZSET, min="-inf", max=now_ts, start=0, num=max_items)
    moved = 0
    for m in members:
        try:
            # Move atomically: remove from ZSET, push to primary
            async with redis_client.pipeline(transaction=True) as pipe:
                pipe.zrem(RETRY_ZSET, m)
                pipe.lpush("upload_requests", m)
                await pipe.execute()
            moved += 1
        except Exception:
            logger.exception("Failed to move retry item back to primary queue")
    return moved


async def enqueue_unpin_request(
    payload: UnpinChainRequest,
    redis_client: async_redis.Redis,
) -> None:
    """Add an unpin request to the Redis queue for processing by unpinner."""

    await redis_client.lpush(
        "unpin_requests",
        payload.model_dump_json(),
    )
    logger.info(f"Enqueued unpin request for {payload.name=}")


async def dequeue_unpin_request(
    redis_client: async_redis.Redis,
) -> Union[
    UnpinChainRequest,
    None,
]:
    """Get the next unpin request from the Redis queue."""
    queue_name = "unpin_requests"
    result = await redis_client.brpop(
        queue_name,
        timeout=3,
    )
    if result:
        _, queue_data = result
        return UnpinChainRequest.model_validate_json(queue_data)

    return None


DOWNLOAD_QUEUE = "download_requests"


async def enqueue_download_request(
    payload: DownloadChainRequest,
    redis_client: async_redis.Redis,
) -> None:
    """Add a download request to the Redis queue for processing by downloader."""
    await redis_client.lpush(
        DOWNLOAD_QUEUE,
        payload.model_dump_json(),
    )
    logger.info(f"Enqueued download request {payload.name=}")


async def dequeue_download_request(
    redis_client: async_redis.Redis,
) -> Union[DownloadChainRequest, None]:
    """Get the next download request from the Redis queue."""
    result = await redis_client.brpop(
        DOWNLOAD_QUEUE,
        timeout=5,
    )
    if result:
        _, queue_data = result
        return DownloadChainRequest.model_validate_json(queue_data)

    return None


SUBSTRATE_QUEUE = "substrate_requests"


async def enqueue_substrate_request(
    payload: SubstratePinningRequest,
    redis_client: async_redis.Redis,
) -> None:
    """Add a substrate pinning request to the Redis queue."""
    if payload.request_id is None:
        payload.request_id = uuid.uuid4().hex
    if payload.first_enqueued_at is None:
        payload.first_enqueued_at = time.time()
    if payload.attempts is None:
        payload.attempts = 0

    await redis_client.lpush(SUBSTRATE_QUEUE, payload.model_dump_json())
    logger.info(f"Enqueued substrate request {payload.name=}")


async def dequeue_substrate_request(
    redis_client: async_redis.Redis,
) -> SubstratePinningRequest | None:
    """Get the next substrate pinning request from the Redis queue."""
    result = await redis_client.brpop(SUBSTRATE_QUEUE, timeout=5)
    if result:
        _, queue_data = result
        return SubstratePinningRequest.model_validate_json(queue_data)
    return None
