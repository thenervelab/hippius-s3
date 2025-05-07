import asyncio
from typing import Any
from typing import AsyncGenerator

from fastapi import Request

from hippius_s3.config import Config
from hippius_s3.ipfs_service import IPFSService


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
