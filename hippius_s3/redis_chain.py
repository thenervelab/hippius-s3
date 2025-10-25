import logging
from typing import Optional

import redis.asyncio as async_redis


logger = logging.getLogger(__name__)


_chain_client: Optional[async_redis.Redis] = None


def initialize_chain_client(redis_client: async_redis.Redis) -> None:
    """Initialize the chain Redis client singleton. Call once during app/worker startup."""
    global _chain_client
    _chain_client = redis_client
    logger.info("Chain Redis client initialized")


def get_chain_client() -> async_redis.Redis:
    """Get the initialized chain Redis client."""
    if _chain_client is None:
        raise RuntimeError(
            "Chain Redis client not initialized. Call initialize_chain_client() first during app/worker startup."
        )
    return _chain_client
