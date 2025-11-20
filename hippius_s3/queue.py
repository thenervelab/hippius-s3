import json
import logging
import time
import uuid
from typing import List
from typing import Optional
from typing import Union

import redis.asyncio as async_redis
from pydantic import BaseModel

from hippius_s3.config import get_config


logger = logging.getLogger(__name__)


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
    cid: str
    cipher_size_bytes: int | None = None


class PartToDownload(BaseModel):
    part_number: int
    chunks: list[PartChunkSpec]


class RetryableRequest(BaseModel):
    request_id: str | None = None
    attempts: int = 0
    first_enqueued_at: float | None = None
    last_error: str | None = None


class UploadChainRequest(RetryableRequest):
    address: str
    bucket_name: str
    object_key: str
    object_id: str
    object_version: int
    chunks: list[Chunk]
    upload_id: str | None = None

    @property
    def name(self):
        if self.upload_id is not None:
            return f"multipart::{self.object_id}::{self.upload_id}::{self.address}"
        return f"simple::{self.object_id}::{self.address}"


class UnpinChainRequest(RetryableRequest):
    address: str
    object_id: str
    object_version: int
    cid: str

    @property
    def name(self):
        return f"unpin::{self.cid}::{self.address}::{self.object_id}"


class DownloadChainRequest(RetryableRequest):
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


class SubstratePinningRequest(RetryableRequest):
    cids: list[str]
    address: str
    object_id: str
    object_version: int

    @property
    def name(self):
        return f"substrate::{self.object_id}::{self.address}"


async def enqueue_upload_request(payload: UploadChainRequest) -> None:
    """Add an upload request to the Redis queue for processing by workers."""
    client = get_queue_client()
    if payload.request_id is None:
        payload.request_id = uuid.uuid4().hex
    if payload.first_enqueued_at is None:
        payload.first_enqueued_at = time.time()
    if payload.attempts is None:
        payload.attempts = 0

    config = get_config()
    raw = payload.model_dump_json()
    queue_names_str: str = getattr(config, "upload_queue_names", "upload_requests")

    def _norm(name: str) -> str:
        return name.strip().strip('"').strip("'")

    queue_names: List[str] = [_norm(q) for q in queue_names_str.split(",") if _norm(q)]
    for qname in queue_names:
        await client.lpush(qname, raw)
    logger.info(f"Enqueued upload request {payload.name=} queues={queue_names}")


async def dequeue_upload_request() -> UploadChainRequest | None:
    """Get the next upload request from the Redis queue."""
    client = get_queue_client()
    config = get_config()
    raw_qname: str = getattr(config, "pinner_consume_queue", "upload_requests")
    queue_name: str = raw_qname.strip().strip('"').strip("'")
    result = await client.brpop(queue_name, timeout=0.5)
    if result:
        _, queue_data = result
        queue_data = json.loads(queue_data)
        return UploadChainRequest.model_validate(queue_data)
    return None


# Retry handling (ZSET with score = next_attempt_unix_ts)
RETRY_ZSET = "upload_retries"


async def enqueue_retry_request(
    payload: UploadChainRequest,
    *,
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
    await client.zadd(RETRY_ZSET, {member: next_ts})
    logger.info(f"Scheduled retry for {payload.name=} attempts={payload.attempts} next_at={int(next_ts)}")


async def move_due_retries_to_primary(
    *,
    now_ts: float | None = None,
    max_items: int = 64,
) -> int:
    """Move due retry items back to the primary queue. Returns number moved."""
    client = get_queue_client()
    now_ts = time.time() if now_ts is None else now_ts
    members = await client.zrangebyscore(RETRY_ZSET, min="-inf", max=now_ts, start=0, num=max_items)
    moved = 0
    for m in members:
        try:
            async with client.pipeline(transaction=True) as pipe:
                pipe.zrem(RETRY_ZSET, m)
                pipe.lpush("upload_requests", m)
                await pipe.execute()
            moved += 1
        except Exception:
            logger.exception("Failed to move retry item back to primary queue")
    return moved


async def enqueue_unpin_request(payload: UnpinChainRequest) -> None:
    """Add an unpin request to the Redis queue for processing by unpinner."""
    client = get_queue_client()
    await client.lpush("unpin_requests", payload.model_dump_json())
    logger.info(f"Enqueued unpin request for {payload.name=}")


async def dequeue_unpin_request() -> Union[UnpinChainRequest, None]:
    """Get the next unpin request from the Redis queue."""
    client = get_queue_client()
    queue_name = "unpin_requests"
    result = await client.brpop(queue_name, timeout=3)
    if result:
        _, queue_data = result
        return UnpinChainRequest.model_validate_json(queue_data)
    return None


UNPIN_RETRY_ZSET = "unpin_retries"


async def enqueue_unpin_retry_request(
    payload: UnpinChainRequest,
    *,
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
    await client.zadd(UNPIN_RETRY_ZSET, {member: next_ts})
    logger.info(f"Scheduled unpin retry for {payload.name=} attempts={payload.attempts} next_at={int(next_ts)}")


async def move_due_unpin_retries_to_primary(
    *,
    now_ts: float | None = None,
    max_items: int = 64,
) -> int:
    """Move due unpin retry items back to the primary queue. Returns number moved."""
    client = get_queue_client()
    now_ts = time.time() if now_ts is None else now_ts
    members = await client.zrangebyscore(UNPIN_RETRY_ZSET, min="-inf", max=now_ts, start=0, num=max_items)
    moved = 0
    for m in members:
        try:
            async with client.pipeline(transaction=True) as pipe:
                pipe.zrem(UNPIN_RETRY_ZSET, m)
                pipe.lpush("unpin_requests", m)
                await pipe.execute()
            moved += 1
        except Exception:
            logger.exception("Failed to move unpin retry item back to primary queue")
    return moved


DOWNLOAD_QUEUE = "download_requests"


async def enqueue_download_request(payload: DownloadChainRequest) -> None:
    """Add a download request to the Redis queue for processing by downloader."""
    client = get_queue_client()
    await client.lpush(DOWNLOAD_QUEUE, payload.model_dump_json())
    logger.info(f"Enqueued download request {payload.name=}")


async def dequeue_download_request() -> Union[DownloadChainRequest, None]:
    """Get the next download request from the Redis queue."""
    client = get_queue_client()
    result = await client.brpop(DOWNLOAD_QUEUE, timeout=5)
    if result:
        _, queue_data = result
        return DownloadChainRequest.model_validate_json(queue_data)
    return None


SUBSTRATE_QUEUE = "substrate_requests"


async def enqueue_substrate_request(payload: SubstratePinningRequest) -> None:
    """Add a substrate pinning request to the Redis queue."""
    client = get_queue_client()
    if payload.request_id is None:
        payload.request_id = uuid.uuid4().hex
    if payload.first_enqueued_at is None:
        payload.first_enqueued_at = time.time()
    if payload.attempts is None:
        payload.attempts = 0
    await client.lpush(SUBSTRATE_QUEUE, payload.model_dump_json())
    logger.info(f"Enqueued substrate request {payload.name=}")
