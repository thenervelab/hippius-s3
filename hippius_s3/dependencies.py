import asyncio
import logging
from dataclasses import dataclass
from typing import Any
from typing import AsyncGenerator
from typing import Union

from fastapi import HTTPException
from fastapi import Request
from redis.asyncio import Redis
from redis.asyncio.cluster import RedisCluster
from starlette import status

from hippius_s3.config import Config
from hippius_s3.repositories import BucketRepository
from hippius_s3.repositories import ObjectRepository
from hippius_s3.repositories import UserRepository


logger = logging.getLogger(__name__)


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


def get_redis(request: Request) -> Union[Redis, RedisCluster]:
    """Extract the Redis client from the request."""
    redis_client: Union[Redis, RedisCluster] = request.app.state.redis_client
    return redis_client


def get_redis_accounts(request: Request) -> Union[Redis, RedisCluster]:
    """Extract the Redis accounts client from the request."""
    redis_accounts_client: Union[Redis, RedisCluster] = request.app.state.redis_accounts_client
    return redis_accounts_client


@dataclass
class RequestContext:
    main_account_id: str
    seed_phrase: str


def get_request_context(request: Request) -> RequestContext:
    """Lightweight context extracted from request.state for passing to services."""
    main_account_id = getattr(request.state.account, "main_account", "") or ""
    seed_phrase = getattr(request.state, "seed_phrase", "")
    return RequestContext(main_account_id=main_account_id, seed_phrase=seed_phrase)


def get_user_repo(db: DBConnection) -> UserRepository:
    return UserRepository(db)


def get_bucket_repo(db: DBConnection) -> BucketRepository:
    return BucketRepository(db)


def get_object_repo(db: DBConnection) -> ObjectRepository:
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
