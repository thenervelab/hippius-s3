import hashlib
import json
import logging

from redis.asyncio import Redis

from gateway.services.account_service import fetch_account
from hippius_s3.models.account import HippiusAccount
from hippius_s3.monitoring import get_metrics_collector
from hippius_s3.services.hippius_api_service import HippiusApiClient
from hippius_s3.services.hippius_api_service import TokenAuthResponse


logger = logging.getLogger(__name__)

AUTH_CACHE_TTL_SECONDS = 60
AUTH_CACHE_PREFIX = "hippius_auth:"

SEED_AUTH_CACHE_TTL_SECONDS = 60
SEED_AUTH_CACHE_PREFIX = "hippius_seed_auth:"


async def cached_auth(access_key: str, redis_client: Redis) -> TokenAuthResponse:
    """
    Authenticate an access key, caching the result in Redis for 60 seconds.

    On cache hit, deserializes and returns the cached TokenAuthResponse.
    On cache miss, calls HippiusApiClient.auth(), caches the result, and returns it.
    """
    cache_key = f"{AUTH_CACHE_PREFIX}{access_key}"

    cached = await redis_client.get(cache_key)
    if cached is not None:
        logger.debug(f"Auth cache hit for key: {access_key[:8]}***")
        get_metrics_collector().record_auth_cache(hit=True)
        return TokenAuthResponse.model_validate(json.loads(cached))

    logger.debug(f"Auth cache miss for key: {access_key[:8]}***")
    get_metrics_collector().record_auth_cache(hit=False)

    async with HippiusApiClient() as api_client:
        token_response = await api_client.auth(access_key)

    await redis_client.setex(
        cache_key,
        AUTH_CACHE_TTL_SECONDS,
        token_response.model_dump_json(),
    )

    return token_response


async def cached_seed_auth(
    seed_phrase: str,
    redis_client: Redis,
    redis_accounts_client: Redis,
    substrate_url: str,
) -> HippiusAccount:
    """
    Resolve a seed phrase to a HippiusAccount, caching the result in Redis for 60 seconds.

    The cache key is derived from a SHA-256 hash of the seed phrase so the raw
    mnemonic is never stored as a Redis key.

    On cache hit, deserializes and returns the cached HippiusAccount.
    On cache miss, calls fetch_account() (which internally uses redis_accounts
    and Substrate), caches the result, and returns it.
    """
    seed_hash = hashlib.sha256(seed_phrase.encode()).hexdigest()
    cache_key = f"{SEED_AUTH_CACHE_PREFIX}{seed_hash}"

    cached = await redis_client.get(cache_key)
    if cached is not None:
        logger.debug(f"Seed auth cache hit for hash: {seed_hash[:12]}...")
        get_metrics_collector().record_seed_auth_cache(hit=True)
        return HippiusAccount.model_validate(json.loads(cached))

    logger.debug(f"Seed auth cache miss for hash: {seed_hash[:12]}...")
    get_metrics_collector().record_seed_auth_cache(hit=False)

    account = await fetch_account(seed_phrase, redis_accounts_client, substrate_url)

    await redis_client.setex(
        cache_key,
        SEED_AUTH_CACHE_TTL_SECONDS,
        account.model_dump_json(),
    )

    return account
