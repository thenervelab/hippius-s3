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

# Set HTTP client log levels based on global level
if is_debug:
    # In debug mode, show all HTTP client logs
    logging.getLogger("httpcore").setLevel(logging.DEBUG)
    logging.getLogger("httpcore.http11").setLevel(logging.DEBUG)
    logging.getLogger("urllib3").setLevel(logging.DEBUG)
    logging.getLogger("httpx").setLevel(logging.DEBUG)
else:
    # In non-debug mode, reduce HTTP client log noise
    logging.getLogger("httpcore").setLevel(logging.WARNING)
    logging.getLogger("httpcore.http11").setLevel(logging.WARNING)
    logging.getLogger("urllib3").setLevel(logging.INFO)
    logging.getLogger("httpx").setLevel(logging.INFO)

# Set up this module's logger
logger = logging.getLogger(__name__)
logger.info(f"Logging configured at level: {logging.getLevelName(log_level)}")


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
    default_response_class=Response,  # Use plain Response as default to allow XML responses
)

# noinspection PyTypeChecker
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


# Include routers in the correct order
# We previously had multipart_router first, but that causes standard object operations to fail
# because the multipart router was intercepting non-multipart requests
# Regular S3 router should be first, since the multipart router is designed to return None
# for non-multipart requests, allowing them to fall through to the main S3 router
app.include_router(s3_router, prefix="")
app.include_router(multipart_router, prefix="")
