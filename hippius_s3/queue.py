import json
import logging
from typing import Any
from typing import Dict

import redis.asyncio as async_redis


logger = logging.getLogger(__name__)


async def enqueue_upload_request(
    redis_client: async_redis.Redis,
    s3_result: Any,
    seed_phrase: str,
    owner: str,
) -> None:
    """Add an upload request to the Redis queue for processing by pinner."""
    queue_item = {
        "cid": s3_result.cid,
        "subaccount": s3_result.subaccount,
        "file_path": s3_result.file_path,
        "pin_node": s3_result.pin_node,
        "substrate_url": s3_result.substrate_url,
        "seed_phrase": seed_phrase,
        "owner": owner,
    }

    await redis_client.lpush("upload_requests", json.dumps(queue_item))
    logger.info(f"Enqueued upload request for CID={s3_result.cid}")


async def dequeue_upload_request(redis_client: async_redis.Redis) -> Dict[str, Any] | None:
    """Get the next upload request from the Redis queue."""
    result = await redis_client.brpop("upload_requests", timeout=1)
    if result:
        _, queue_data = result
        return json.loads(queue_data)
    return None


async def enqueue_unpin_request(
    redis_client: async_redis.Redis,
    cid: str,
    subaccount: str,
    seed_phrase: str,
    file_name: str,
    owner: str,
) -> None:
    """Add an unpin request to the Redis queue for processing by unpinner."""
    queue_item = {
        "cid": cid,
        "subaccount": subaccount,
        "seed_phrase": seed_phrase,
        "file_name": file_name,
        "owner": owner,
    }

    await redis_client.lpush("unpin_requests", json.dumps(queue_item))
    logger.info(f"Enqueued unpin request for CID={cid}")


async def dequeue_unpin_request(redis_client: async_redis.Redis) -> Dict[str, Any] | None:
    """Get the next unpin request from the Redis queue."""
    result = await redis_client.brpop("unpin_requests", timeout=1)
    if result:
        _, queue_data = result
        return json.loads(queue_data)
    return None


async def enqueue_s3_publish_request(
    redis_client: async_redis.Redis,
    object_id: str,
    file_name: str,
    owner: str,
    chunks: list[dict],
    seed_phrase: str,
    bucket_name: str,
    subaccount_id: str,
    should_encrypt: bool,
    store_node: str,
    pin_node: str,
    substrate_url: str,
) -> None:
    """Add an s3_publish request to the Redis queue for background processing."""
    queue_item = {
        "object_id": object_id,
        "file_name": file_name,
        "owner": owner,
        "chunks": chunks,
        "seed_phrase": seed_phrase,
        "bucket_name": bucket_name,
        "subaccount_id": subaccount_id,
        "should_encrypt": should_encrypt,
        "store_node": store_node,
        "pin_node": pin_node,
        "substrate_url": substrate_url,
    }

    await redis_client.lpush("s3_publish_requests", json.dumps(queue_item))
    logger.info(f"Enqueued s3_publish request for object_id={object_id}, file={file_name}")


async def dequeue_s3_publish_request(redis_client: async_redis.Redis) -> Dict[str, Any] | None:
    """Get the next s3_publish request from the Redis queue."""
    result = await redis_client.brpop("s3_publish_requests", timeout=1)
    if result:
        _, queue_data = result
        return json.loads(queue_data)
    return None
