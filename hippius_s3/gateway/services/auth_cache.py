import json
import logging

from redis.asyncio import Redis

from hippius_s3.monitoring import get_metrics_collector
from hippius_s3.services.hippius_api_service import HippiusApiClient
from hippius_s3.services.hippius_api_service import TokenAuthResponse


logger = logging.getLogger(__name__)

AUTH_CACHE_TTL_SECONDS = 60
AUTH_CACHE_PREFIX = "hippius_auth:"


async def cached_auth(
    access_key: str, redis_client: Redis, api_client: "HippiusApiClient | None" = None
) -> TokenAuthResponse:
    """
    Authenticate an access key, caching the result in Redis for 60 seconds.

    On cache hit, deserializes and returns the cached TokenAuthResponse.
    On cache miss, calls HippiusApiClient.auth(), caches the result, and returns it.

    NET-5: pass a shared, long-lived api_client (held in gateway app.state) to reuse its warm
    connection pool across cache misses; without one, a per-miss client is built and torn down.
    """
    cache_key = f"{AUTH_CACHE_PREFIX}{access_key}"

    cached = await redis_client.get(cache_key)
    if cached is not None:
        logger.debug(f"Auth cache hit for key: {access_key[:8]}***")
        get_metrics_collector().record_auth_cache(hit=True)
        return TokenAuthResponse.model_validate(json.loads(cached))

    logger.debug(f"Auth cache miss for key: {access_key[:8]}***")
    get_metrics_collector().record_auth_cache(hit=False)

    if api_client is not None:
        token_response = await api_client.auth(access_key)
    else:
        async with HippiusApiClient() as owned_client:
            token_response = await owned_client.auth(access_key)

    await redis_client.setex(
        cache_key,
        AUTH_CACHE_TTL_SECONDS,
        token_response.model_dump_json(),
    )

    return token_response
