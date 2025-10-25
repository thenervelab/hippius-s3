import logging
from typing import Optional

import redis.asyncio as async_redis


logger = logging.getLogger(__name__)


_cache_client: Optional[async_redis.Redis] = None


def initialize_cache_client(redis_client: async_redis.Redis) -> None:
    """Initialize the cache Redis client singleton. Call once during app/worker startup."""
    global _cache_client
    _cache_client = redis_client
    logger.info("Cache Redis client initialized")


def get_cache_client() -> async_redis.Redis:
    """Get the initialized cache Redis client."""
    if _cache_client is None:
        raise RuntimeError(
            "Cache Redis client not initialized. Call initialize_cache_client() first during app/worker startup."
        )
    return _cache_client
