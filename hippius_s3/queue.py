import json
import logging
from typing import Union

import redis.asyncio as async_redis
from pydantic import BaseModel


logger = logging.getLogger(__name__)


class Chunk(BaseModel):
    id: int
    redis_key: str


class ChunkToDownload(BaseModel):
    cid: str
    part_id: int
    redis_key: str


class ChainRequest(BaseModel):
    substrate_url: str
    ipfs_node: str
    address: str
    subaccount: str
    subaccount_seed_phrase: str
    bucket_name: str
    object_key: str
    should_encrypt: bool
    object_id: str

    @property
    def name(self):
        if hasattr(self, "upload_id"):
            return f"multipart::{self.object_id}::{self.multipart_upload_id}::{self.address}"

        return f"simple::{self.object_id}::{self.address}"


class MultipartUploadChainRequest(ChainRequest):
    multipart_upload_id: str
    chunks: list[Chunk]


class SimpleUploadChainRequest(ChainRequest):
    chunk: Chunk


class UnpinChainRequest(ChainRequest):
    cid: str


class DownloadChainRequest(BaseModel):
    request_id: str
    object_id: str
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
    chunks: list[ChunkToDownload]

    @property
    def name(self):
        return f"download::{self.request_id}::{self.object_id}::{self.address}"


async def enqueue_upload_request(
    payload: Union[
        MultipartUploadChainRequest,
        SimpleUploadChainRequest,
    ],
    redis_client: async_redis.Redis,
) -> None:
    """Add an upload request to the Redis queue for processing by pinner."""
    await redis_client.lpush(
        "upload_requests",
        payload.model_dump_json(),
    )
    logger.info(f"Enqueued upload request {payload.name=}")


async def dequeue_upload_request(
    redis_client: async_redis.Redis,
) -> Union[MultipartUploadChainRequest, SimpleUploadChainRequest, None]:
    """Get the next upload request from the Redis queue."""
    queue_name = "upload_requests"
    result = await redis_client.brpop(
        queue_name,
        timeout=3,
    )
    if result:
        _, queue_data = result
        queue_data = json.loads(queue_data)

        if "chunk" in queue_data:
            return SimpleUploadChainRequest.model_validate(queue_data)

        return MultipartUploadChainRequest.model_validate(queue_data)

    return None


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
