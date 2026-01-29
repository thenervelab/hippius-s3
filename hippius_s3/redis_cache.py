import logging
from typing import Optional
from typing import Union

from redis.asyncio import Redis
from redis.asyncio.cluster import RedisCluster


logger = logging.getLogger(__name__)


_cache_client: Optional[Union[Redis, RedisCluster]] = None


def initialize_cache_client(redis_client: Union[Redis, RedisCluster]) -> None:
    """Initialize the cache Redis client singleton. Call once during app/worker startup."""
    global _cache_client
    _cache_client = redis_client
    logger.info("Cache Redis client initialized")


def get_cache_client() -> Union[Redis, RedisCluster]:
    """Get the initialized cache Redis client."""
    if _cache_client is None:
        raise RuntimeError(
            "Cache Redis client not initialized. Call initialize_cache_client() first during app/worker startup."
        )
    return _cache_client
