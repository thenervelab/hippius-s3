import json
import logging

import redis.asyncio as async_redis
from pydantic import BaseModel


logger = logging.getLogger(__name__)


class ChainRequest(BaseModel):
    substrate_url: str
    ipfs_node: str
    address: str
    subaccount: str
    subaccount_seed_phrase: str
    bucket_name: str
    object_key: str


class UnpinChainRequest(ChainRequest):
    cid: str


class Chunk(BaseModel):
    id: str
    redis_key: str


class MultipartUploadRequest(ChainRequest):
    upload_id: str
    chunks: list[Chunk]


async def enqueue_upload_request(
    payload: MultipartUploadRequest,
    redis_client: async_redis.Redis,
) -> None:
    """Add an upload request to the Redis queue for processing by pinner."""
    await redis_client.lpush(
        "upload_requests",
        json.dumps(
            payload.model_dump(),
        ),
    )
    logger.info(f"Enqueued upload request {payload.upload_id=}")


async def dequeue_upload_request(
    redis_client: async_redis.Redis,
) -> MultipartUploadRequest | None:
    """Get the next upload request from the Redis queue."""
    result = await redis_client.brpop(
        "upload_requests",
        timeout=3,
    )
    if result:
        _, queue_data = result
        return MultipartUploadRequest.model_validate_json(queue_data)

    return None


async def enqueue_unpin_request(
    payload: UnpinChainRequest,
    redis_client: async_redis.Redis,
) -> None:
    """Add an unpin request to the Redis queue for processing by unpinner."""

    await redis_client.lpush("unpin_requests", json.dumps(payload.model_dump()))
    logger.info(f"Enqueued unpin request for CID={payload.cid}")


async def dequeue_unpin_request(
    redis_client: async_redis.Redis,
) -> UnpinChainRequest | None:
    """Get the next unpin request from the Redis queue."""
    result = await redis_client.brpop("unpin_requests", timeout=1)
    if result:
        _, queue_data = result
        return UnpinChainRequest.model_validate_json(queue_data)
    return None
