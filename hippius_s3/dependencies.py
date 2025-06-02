import asyncio
import logging
from typing import Any
from typing import AsyncGenerator

from cachetools import TTLCache
from fastapi import HTTPException
from fastapi import Request
from hippius_sdk.substrate import SubstrateClient
from starlette import status

from hippius_s3.config import Config
from hippius_s3.ipfs_service import IPFSService


logger = logging.getLogger(__name__)

# TTL cache for credit checks - cache results for 60 seconds
# maxsize=1000 allows caching up to 1000 different seed phrases
credit_cache = TTLCache(maxsize=1000, ttl=60)


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


def get_ipfs_service(request: Request) -> IPFSService:
    """Extract the IPFS service from the request."""
    service: IPFSService = request.app.state.ipfs_service
    return service


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


async def check_account_has_credit(seed_phrase: str) -> bool:
    """
    Check if the account associated with the seed phrase has enough credit.

    Results are cached for 60 seconds to improve performance and reduce
    substrate network calls.

    Args:
        seed_phrase: The seed phrase of the account to check

    Returns:
        bool: True if the account has credit, False otherwise
    """
    # Check cache first
    if seed_phrase in credit_cache:
        logger.debug(f"Credit check cache HIT for seed phrase: {seed_phrase[:20]}...")
        return credit_cache[seed_phrase]

    logger.debug(f"Credit check cache MISS for seed phrase: {seed_phrase[:20]}...")

    try:
        substrate_client = SubstrateClient(password=None, account_name=None)
        substrate_client.connect(seed_phrase=seed_phrase)
        account_address = substrate_client._account_address
        credit = await substrate_client.get_free_credits(
            account_address=account_address,
            seed_phrase=seed_phrase,
        )

        has_credit = bool(credit > 0)

        # Cache the result for 60 seconds
        credit_cache[seed_phrase] = has_credit
        logger.debug(f"Cached credit result for {account_address}: {has_credit}")

        return has_credit

    except Exception as e:
        logger.exception(f"Error in account credit verification: {e}")
        # Cache negative result for shorter time to allow retries
        credit_cache[seed_phrase] = False
        return False
