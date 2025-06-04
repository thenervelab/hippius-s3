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
from fastapi.openapi.utils import get_openapi

from hippius_s3.api.middlewares.banhammer import BanHammerService
from hippius_s3.api.middlewares.banhammer import banhammer_wrapper
from hippius_s3.api.middlewares.cors import cors_middleware
from hippius_s3.api.middlewares.credit_check import check_credit_for_all_operations
from hippius_s3.api.middlewares.frontend_hmac import verify_frontend_hmac_middleware
from hippius_s3.api.middlewares.hmac import verify_hmac_middleware
from hippius_s3.api.middlewares.rate_limit import RateLimitService
from hippius_s3.api.middlewares.rate_limit import rate_limit_wrapper
from hippius_s3.api.s3.endpoints import router as s3_router
from hippius_s3.api.s3.multipart import router as multipart_router
from hippius_s3.api.user import router as user_router
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

        app.state.redis_client = async_redis.from_url(config.redis_url)
        logger.info("Redis client initialized")

        app.state.rate_limit_service = RateLimitService(app.state.redis_client)
        logger.info("Rate limiting service initialized")

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


def custom_openapi() -> dict:
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


# Override the openapi schema generation
app.openapi = custom_openapi  # type: ignore[method-assign]

# Custom middlewares - middleware("http") executes in REVERSE order
# 1. Credit verification (executes LAST, needs seed phrase)
app.middleware("http")(check_credit_for_all_operations)
# Frontend HMAC verification for /user/ endpoints (executes after credit check)
app.middleware("http")(verify_frontend_hmac_middleware)
# 2. Rate limiting (per seed phrase - executes FOURTH, needs seed phrase)
app.middleware("http")(rate_limit_wrapper)
# 3. HMAC authentication (extract seed phrase - executes THIRD)
app.middleware("http")(verify_hmac_middleware)
# 4. Banhammer (IP-based protection - executes SECOND)
app.middleware("http")(banhammer_wrapper)
# 5. CORS (executes FIRST - outermost layer)
app.middleware("http")(cors_middleware)
app.include_router(user_router, prefix="/user")
app.include_router(s3_router, prefix="")
app.include_router(multipart_router, prefix="")
