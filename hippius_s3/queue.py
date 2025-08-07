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
) -> None:
    """Add an upload request to the Redis queue for processing by pinner."""
    queue_item = {
        "cid": s3_result.cid,
        "subaccount": s3_result.subaccount,
        "file_path": s3_result.file_path,
        "pin_node": s3_result.pin_node,
        "substrate_url": s3_result.substrate_url,
        "seed_phrase": seed_phrase,
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
