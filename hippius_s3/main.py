"""Main application module for Hippius S3 service."""

import logging
import os
import sys
from contextlib import asynccontextmanager
from typing import AsyncGenerator

import asyncpg
import redis.asyncio as async_redis
from dotenv import load_dotenv
from fastapi import FastAPI
from fastapi import Response
from fastapi.middleware.cors import CORSMiddleware
from fastapi.openapi.utils import get_openapi

from hippius_s3.api.middlewares.banhammer import BanHammerService
from hippius_s3.api.middlewares.banhammer import banhammer_middleware
from hippius_s3.api.middlewares.credit_check import check_credit_for_all_operations
from hippius_s3.api.middlewares.hmac import verify_hmac_middleware
from hippius_s3.api.middlewares.rate_limit import RateLimitService
from hippius_s3.api.middlewares.rate_limit import rate_limit_middleware
from hippius_s3.api.s3.endpoints import router as s3_router
from hippius_s3.api.s3.multipart import router as multipart_router
from hippius_s3.config import get_config
from hippius_s3.ipfs_service import IPFSService


load_dotenv()

log_level = os.getenv("LOG_LEVEL", "INFO")
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

        # Initialize Redis connection
        redis_url = os.getenv("REDIS_URL", "redis://redis:6379/0")
        app.state.redis_client = async_redis.from_url(redis_url)
        logger.info("Redis client initialized")

        # Initialize rate limiting service
        app.state.rate_limit_service = RateLimitService(app.state.redis_client)
        logger.info("Rate limiting service initialized")

        # Initialize banhammer service
        app.state.banhammer_service = BanHammerService(app.state.redis_client)
        logger.info("Banhammer service initialized")

        app.state.ipfs_service = IPFSService(config)
        logger.info("IPFS service initialized")

        yield

    finally:
        try:
            await app.state.redis_client.close()
            logger.info("Redis client closed")
        except Exception:
            logger.exception("Error shutting down Redis client")

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


def custom_openapi():
    if app.openapi_schema:
        return app.openapi_schema
    openapi_schema = get_openapi(
        title=app.title,
        version="1.0.0",
        description=app.description,
        routes=app.routes,
    )
    openapi_schema["components"]["securitySchemes"] = {
        "Base64 encoded seed phrase": {
            "type": "http",
            "scheme": "bearer",
            "description": "Enter your base64-encoded seed phrase",
        }
    }
    # Add global security requirement
    openapi_schema["security"] = [{"Base64 encoded seed phrase": []}]
    app.openapi_schema = openapi_schema
    return app.openapi_schema


app.openapi = custom_openapi


# Register middlewares (order matters - executed in reverse order of registration)
# 4. Banhammer (IP-based protection - executes FIRST)
async def banhammer_wrapper(request, call_next):
    return await banhammer_middleware(
        request,
        call_next,
        request.app.state.banhammer_service,
    )


app.middleware("http")(banhammer_wrapper)


# 3. Rate limiting (per seed phrase - executes SECOND)
async def rate_limit_wrapper(request, call_next):
    return await rate_limit_middleware(
        request,
        call_next,
        request.app.state.rate_limit_service,
        max_requests=100,
        window_seconds=60,
    )


app.middleware("http")(rate_limit_wrapper)

# 2. Credit verification (executes THIRD)
app.middleware("http")(check_credit_for_all_operations)

# 1. HMAC authentication (extract seed phrase - executes FOURTH)
app.middleware("http")(verify_hmac_middleware)

# 5. CORS middleware must be added LAST so it executes FIRST
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
