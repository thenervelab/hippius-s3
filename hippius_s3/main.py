"""Main application module for Hippius S3 service."""

import logging
import os
from contextlib import asynccontextmanager
from typing import AsyncGenerator

import asyncpg
from dotenv import load_dotenv
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from hippius_s3.api.s3.endpoints import router as s3_router
from hippius_s3.api.s3.multipart import router as multipart_router
from hippius_s3.config import get_config
from hippius_s3.ipfs_service import IPFSService


load_dotenv()

logger = logging.getLogger(__name__)


async def postgres_create_pool(database_url: str) -> asyncpg.Pool:
    """Create and return a PostgreSQL connection pool.

    Args:
        database_url: PostgreSQL connection URL

    Returns:
        Connection pool for PostgreSQL
    """
    return await asyncpg.create_pool(database_url)


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncGenerator[None, None]:
    """FastAPI application lifespan handler."""
    try:
        app.state.config = get_config()
        config = app.state.config

        app.state.postgres_pool = await postgres_create_pool(config.database_url)
        logger.info("PostgreSQL connection pool created")

        app.state.ipfs_service = IPFSService(config)
        logger.info("IPFS service initialized")

        yield

    finally:
        try:
            await app.state.postgres_pool.close()
            logger.info("PostgreSQL connection pool closed")
        except Exception:
            logger.exception("Error shutting down postgres pool")


app = FastAPI(
    title="Hippius S3",
    description="S3 Gateway for Hippius' IPFS storage",
    lifespan=lifespan,
    debug=os.getenv("DEBUG", "false").lower() == "true",
)

# noinspection PyTypeChecker
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(s3_router)
app.include_router(multipart_router)
