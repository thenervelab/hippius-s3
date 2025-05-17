import asyncio
import logging
import re
from typing import Any
from typing import AsyncGenerator

from fastapi import HTTPException
from fastapi import Request
from hippius_sdk.substrate import SubstrateClient
from starlette import status

from hippius_s3.config import Config
from hippius_s3.ipfs_service import IPFSService


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


def get_ipfs_service(request: Request) -> IPFSService:
    """Extract the IPFS service from the request."""
    service: IPFSService = request.app.state.ipfs_service
    return service


def get_seed_phrase_from_auth_header(auth_header: str) -> str:
    """
    Extract seed phrase from S3 authorization header.

    The seed phrase is expected to be the account name in the AWS4 authorization header.
    Format: Authorization: AWS4-HMAC-SHA256 Credential=ACCOUNT_NAME/DATE/REGION/...

    Args:
        auth_header: The authorization header value

    Returns:
        str: The extracted seed phrase

    Raises:
        ValueError: If the authorization header is invalid
    """
    # Extract account name (which contains seed phrase)
    # Format: AWS4-HMAC-SHA256 Credential=ACCOUNT_NAME/DATE/REGION/...
    credential_match = re.search(r"Credential=([^/]+)/", auth_header)
    if not credential_match:
        raise ValueError("Invalid authorization header format")

    return credential_match.group(1)


async def extract_seed_phrase(request: Request) -> str:
    """
    FastAPI dependency that extracts the seed phrase from request.state.
    If not found in request.state, extracts it from the authorization header.

    Returns:
        str: The extracted seed phrase

    Raises:
        HTTPException: If the seed phrase cannot be extracted
    """
    # First check if seed phrase is already in request.state
    if hasattr(request.state, "seed_phrase"):
        seed_phrase: str = request.state.seed_phrase
        return seed_phrase

    # Otherwise extract it from the authorization header
    auth_header = request.headers.get("authorization")
    if not auth_header:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Missing authorization header",
        )

    try:
        return get_seed_phrase_from_auth_header(auth_header)
    except ValueError as e:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid authorization header format",
        ) from e


async def check_account_has_credit(seed_phrase: str) -> bool:
    """
    Check if the account associated with the seed phrase has enough credit.

    Args:
        seed_phrase: The seed phrase of the account to check

    Returns:
        bool: True if the account has credit, False otherwise
    """
    try:
        substrate_client = SubstrateClient(password=None, account_name=None)
        substrate_client.connect(seed_phrase=seed_phrase)
        account_address = substrate_client._account_address
        credit = await substrate_client.get_free_credits(
            account_address=account_address,
            seed_phrase=seed_phrase,
        )

        return bool(credit > 0)

    except Exception as e:
        logger.exception(f"Error in account credit verification: {e}")
        return False
