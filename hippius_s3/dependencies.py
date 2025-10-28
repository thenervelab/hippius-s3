import asyncio
import logging
from dataclasses import dataclass
from typing import Any
from typing import AsyncGenerator

import redis.asyncio as async_redis
from cachetools import TTLCache
from fastapi import HTTPException
from fastapi import Request
from hippius_sdk.substrate import SubstrateClient
from starlette import status

from hippius_s3.config import Config
from hippius_s3.repositories import BucketRepository
from hippius_s3.repositories import ObjectRepository
from hippius_s3.repositories import UserRepository
from hippius_s3.services.object_reader import ObjectReader


logger = logging.getLogger(__name__)

# maxsize=1000 allows caching up to 1000 different main accounts
credit_cache: TTLCache[str, bool] = TTLCache(maxsize=1000, ttl=60)


class DBConnection:
    """
    Database connection wrapper that automatically releases the connection
    when it's garbage collected or when the close method is called.
    """

    def __init__(self, conn: Any, pool: Any) -> None:
        self.conn = conn
        self.pool = pool

    async def close(self) -> None:
        if self.conn is not None:
            await self.pool.release(self.conn)
            self.conn = None

    def __del__(self) -> None:
        if self.conn is not None and self.pool is not None:
            asyncio.create_task(self.pool.release(self.conn))

    def __getattr__(self, name: str) -> Any:
        return getattr(self.conn, name)


async def get_postgres(request: Request) -> AsyncGenerator["DBConnection", None]:
    """
    Dependency that provides a database connection and automatically
    releases it when the request is finished.
    """
    conn = await request.app.state.postgres_pool.acquire()
    db = DBConnection(conn, request.app.state.postgres_pool)
    try:
        yield db
    finally:
        await db.close()


def get_config(request: Request) -> Config:
    """Extract the application Config from the request."""
    config: Config = request.app.state.config
    return config


def get_redis(request: Request) -> async_redis.Redis:
    """Extract the Redis client from the request."""
    redis_client: async_redis.Redis = request.app.state.redis_client
    return redis_client


def get_redis_accounts(request: Request) -> async_redis.Redis:
    """Extract the Redis accounts client from the request."""
    redis_accounts_client: async_redis.Redis = request.app.state.redis_accounts_client
    return redis_accounts_client


def get_object_reader(request: Request) -> ObjectReader:
    """Provide an ObjectReader service configured with app Config."""
    return ObjectReader(request.app.state.config)


@dataclass
class RequestContext:
    main_account_id: str
    seed_phrase: str


def get_request_context(request: Request) -> RequestContext:
    """Lightweight context extracted from request.state for passing to services."""
    main_account_id = getattr(request.state.account, "main_account", "") or ""
    seed_phrase = getattr(request.state, "seed_phrase", "")
    return RequestContext(main_account_id=main_account_id, seed_phrase=seed_phrase)


def get_user_repo(db: Any = None):
    if db is None:
        raise RuntimeError("get_user_repo requires DB dependency injection")
    return UserRepository(db)


def get_bucket_repo(db: Any = None):
    if db is None:
        raise RuntimeError("get_bucket_repo requires DB dependency injection")
    return BucketRepository(db)


def get_object_repo(db: Any = None):
    if db is None:
        raise RuntimeError("get_object_repo requires DB dependency injection")
    return ObjectRepository(db)


async def extract_seed_phrase(request: Request) -> str:
    """
    FastAPI dependency that extracts the seed phrase from request.state.
    The HMAC middleware should have already extracted and stored the seed phrase.

    Returns:
        str: The extracted seed phrase

    Raises:
        HTTPException: If the seed phrase is not found in request.state
    """
    if hasattr(request.state, "seed_phrase"):
        seed_phrase: str = request.state.seed_phrase
        return seed_phrase

    raise HTTPException(
        status_code=status.HTTP_401_UNAUTHORIZED,
        detail="Seed phrase not found in request state - HMAC middleware may not be configured",
    )


async def check_account_has_credit(
    subaccount: str,
    main_account,
    seed_phrase: str,
    substrate_url: str,
) -> bool:
    """
    Check if the account associated with the seed phrase has enough credit.

    Results are cached for 60 seconds to improve performance and reduce
    substrate network calls.

    Args:
        subaccount: Substrate account name.
        main_account: The main account to check for enough credit
        seed_phrase: The seed phrase of the account to check
        substrate_url: the substrate url to use for the credit check.

    Returns:
        bool: True if the account has credit, False otherwise
    """
    # Check cache first
    if subaccount in credit_cache:
        logger.debug(f"credit_check_cache_hit {subaccount=}")
        return bool(credit_cache[subaccount])

    logger.debug(f"credit_check_cache_miss {subaccount=}")

    try:
        substrate_client = SubstrateClient(
            url=substrate_url,
            password=None,
            account_name=None,
        )
        substrate_client.connect(seed_phrase=seed_phrase)
        credit = await substrate_client.get_free_credits(
            account_address=main_account,
            seed_phrase=seed_phrase,
        )

        has_credit = bool(credit > 0)

        # Cache the result for 60 seconds
        credit_cache[subaccount] = has_credit
        logger.debug(f"Cached credit result for subaccount={subaccount} {main_account=} {has_credit} {int(credit)=}")

        return has_credit

    except Exception as e:
        logger.exception(f"Error in account credit verification: {e}")
        # Cache negative result for shorter time to allow retries
        credit_cache[subaccount] = False
        return False
