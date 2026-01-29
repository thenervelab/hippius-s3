import asyncio
import contextlib
import logging
from typing import Awaitable
from typing import Callable
from typing import TypeVar
from typing import Union

from redis.asyncio import Redis
from redis.asyncio.cluster import RedisCluster
from redis.exceptions import BusyLoadingError
from redis.exceptions import ConnectionError as RedisConnectionError
from redis.exceptions import TimeoutError as RedisTimeoutError


logger = logging.getLogger(__name__)

T = TypeVar("T")


def create_redis_client(redis_url: str) -> Union[Redis, RedisCluster]:
    """
    Create a Redis client from URL, automatically detecting cluster vs standalone.

    Args:
        redis_url: Redis connection URL (redis://host:port or redis://host:port?cluster=true)

    Returns:
        Redis or RedisCluster client based on URL parameters
    """
    if "cluster=true" in redis_url or "redis-cluster" in redis_url:
        host_port = redis_url.replace("redis://", "").split("?")[0].split("/")[0]
        host, port = host_port.split(":") if ":" in host_port else (host_port, "6379")
        logger.info(f"Creating Redis Cluster client for {host}:{port}")
        return RedisCluster(host=host, port=int(port))
    logger.info(f"Creating Redis client for {redis_url}")
    return Redis.from_url(redis_url)


async def with_redis_retry(
    func: Callable[[Union[Redis, RedisCluster]], Awaitable[T]],
    redis_client: Union[Redis, RedisCluster],
    redis_url: str,
    operation_name: str = "redis operation",
    max_retries: int = 5,
    retry_sleep: float = 2.0,
) -> tuple[T, Union[Redis, RedisCluster]]:
    """
    Execute a Redis operation with automatic retry and reconnection on transient errors.

    Handles BusyLoadingError, ConnectionError, and TimeoutError by:
    - Closing the old connection
    - Waiting before retry
    - Creating a new connection
    - Retrying the operation

    Args:
        func: Async function to execute (receives redis_client as first argument)
        redis_client: Current Redis client instance
        redis_url: Redis connection URL for reconnection
        operation_name: Description for logging
        max_retries: Maximum number of retry attempts
        retry_sleep: Seconds to wait between retries

    Returns:
        Tuple of (result, new_redis_client)
        - result: Return value from func
        - new_redis_client: Updated Redis client (may be same or reconnected)

    Raises:
        Last exception after max_retries exhausted
    """
    current_client = redis_client
    last_error = None

    for attempt in range(max_retries):
        try:
            result = await func(current_client)
            return result, current_client
        except (BusyLoadingError, RedisConnectionError, RedisTimeoutError) as e:
            last_error = e
            if attempt < max_retries - 1:
                logger.warning(
                    f"Redis error during {operation_name} (attempt {attempt + 1}/{max_retries}): {e}. "
                    f"Reconnecting in {retry_sleep}s..."
                )
                with contextlib.suppress(Exception):
                    await current_client.close()
                await asyncio.sleep(retry_sleep)
                current_client = create_redis_client(redis_url)
            else:
                logger.error(f"Redis error during {operation_name} after {max_retries} attempts: {e}")

    if last_error is None:
        raise RuntimeError(f"Redis operation {operation_name} failed with no exception recorded")
    raise last_error
