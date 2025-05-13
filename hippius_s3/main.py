"""Main application module for Hippius S3 service."""

import logging
import os
import sys
from contextlib import asynccontextmanager
from typing import AsyncGenerator

import asyncpg
from dotenv import load_dotenv
from fastapi import FastAPI
from fastapi import Response
from fastapi.middleware.cors import CORSMiddleware

from hippius_s3.api.s3.endpoints import router as s3_router
from hippius_s3.api.s3.multipart import router as multipart_router
from hippius_s3.config import get_config
from hippius_s3.ipfs_service import IPFSService


load_dotenv()

# Configure logging
is_debug = os.getenv("DEBUG", "false").lower() == "true"
log_level = logging.DEBUG if is_debug else logging.INFO

# Configure the root logger
logging.basicConfig(
    level=log_level,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)

# Set logging level for all loggers
for name in logging.root.manager.loggerDict:
    logging.getLogger(name).setLevel(log_level)

# Configure specific loggers
uvicorn_logger = logging.getLogger("uvicorn")
uvicorn_logger.setLevel(log_level)
uvicorn_access_logger = logging.getLogger("uvicorn.access")
uvicorn_access_logger.setLevel(log_level)

# Set highest logging level for our application code
logging.getLogger("hippius_s3").setLevel(logging.DEBUG)
logging.getLogger("hippius_s3.api.s3.multipart").setLevel(logging.DEBUG)
logging.getLogger("hippius_s3.api.s3.endpoints").setLevel(logging.DEBUG)


# Reduce HTTP client log noise
logging.getLogger("httpcore").setLevel(logging.WARNING)
logging.getLogger("httpcore.http11").setLevel(logging.WARNING)
logging.getLogger("urllib3").setLevel(logging.WARNING)
logging.getLogger("httpx").setLevel(logging.WARNING)

# Set up this module's logger
logger = logging.getLogger(__name__)


async def postgres_create_pool(database_url: str) -> asyncpg.Pool:
    """Create and return a Postgres connection pool.

    Args:
        database_url: Postgres connection URL

    Returns:
        Connection pool for Postgres
    """
    return await asyncpg.create_pool(database_url)


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncGenerator[None, None]:
    """FastAPI application lifespan handler."""
    try:
        app.state.config = get_config()
        config = app.state.config

        app.state.postgres_pool = await postgres_create_pool(config.database_url)
        logger.info("Postgres connection pool created")

        app.state.ipfs_service = IPFSService(config)
        logger.info("IPFS service initialized")

        yield

    finally:
        try:
            await app.state.postgres_pool.close()
            logger.info("Postgres connection pool closed")
        except Exception:
            logger.exception("Error shutting down postgres pool")


app = FastAPI(
    title="Hippius S3",
    description="S3 Gateway for Hippius' IPFS storage",
    lifespan=lifespan,
    debug=os.getenv("DEBUG", "false").lower() == "true",
    default_response_class=Response,
)

# noinspection PyTypeChecker
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


# Include routers in the correct order! Do not change this por favor.
app.include_router(s3_router, prefix="")
app.include_router(multipart_router, prefix="")
