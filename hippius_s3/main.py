"""Main application module for Hippius S3 service."""

import logging
import sys
from contextlib import asynccontextmanager
from typing import AsyncGenerator

import asyncpg
import redis.asyncio as async_redis
from dotenv import load_dotenv
from fastapi import FastAPI
from fastapi import Response
from fastapi.openapi.utils import get_openapi

from hippius_s3.api.middlewares.audit_log import audit_log_middleware
from hippius_s3.api.middlewares.backend_hmac import verify_hmac_middleware
from hippius_s3.api.middlewares.banhammer import BanHammerService
from hippius_s3.api.middlewares.banhammer import banhammer_wrapper
from hippius_s3.api.middlewares.cors import cors_middleware
from hippius_s3.api.middlewares.credit_check import check_credit_for_all_operations
from hippius_s3.api.middlewares.frontend_hmac import verify_frontend_hmac_middleware
from hippius_s3.api.middlewares.profiler import SpeedscopeProfilerMiddleware
from hippius_s3.api.middlewares.rate_limit import RateLimitService
from hippius_s3.api.middlewares.rate_limit import rate_limit_wrapper
from hippius_s3.api.s3.endpoints import router as s3_router
from hippius_s3.api.s3.multipart import router as multipart_router
from hippius_s3.api.user import router as user_router
from hippius_s3.cache import RedisDownloadChunksCache
from hippius_s3.cache import RedisObjectPartsCache
from hippius_s3.config import get_config
from hippius_s3.ipfs_service import IPFSService


load_dotenv()
config = get_config()

# Configure the root logger
logging.basicConfig(
    level=config.log_level,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)

# Set up this module's logger
logger = logging.getLogger(__name__)


async def postgres_create_pool(database_url: str) -> asyncpg.Pool:
    """Create and return a Postgres connection pool.

    Args:
        database_url: Postgres connection URL

    Returns:
        Connection pool for Postgres
    """
    return await asyncpg.create_pool(
        database_url,
        min_size=5,
        max_size=20,
        max_queries=50000,
        max_inactive_connection_lifetime=300,
    )


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

        app.state.redis_accounts_client = async_redis.from_url(config.redis_accounts_url)
        logger.info("Redis accounts client initialized")

        app.state.rate_limit_service = RateLimitService(app.state.redis_client)
        logger.info("Rate limiting service initialized")

        app.state.banhammer_service = BanHammerService(app.state.redis_client)
        logger.info("Banhammer service initialized")

        app.state.ipfs_service = IPFSService(config, app.state.redis_client)
        logger.info("IPFS service initialized with Redis client")

        # Cache repositories
        app.state.obj_cache = RedisObjectPartsCache(app.state.redis_client)
        app.state.dl_cache = RedisDownloadChunksCache(app.state.redis_client)
        logger.info("Cache repositories initialized")

        yield

    finally:
        try:
            await app.state.redis_client.close()
            logger.info("Redis client closed")
        except Exception:
            logger.exception("Error shutting down Redis client")

        try:
            await app.state.redis_accounts_client.close()
            logger.info("Redis accounts client closed")
        except Exception:
            logger.exception("Error shutting down Redis accounts client")

        try:
            await app.state.postgres_pool.close()
            logger.info("Postgres connection pool closed")
        except Exception:
            logger.exception("Error shutting down postgres pool")


app = FastAPI(
    title="Hippius S3",
    description="S3 Gateway for Hippius' IPFS storage",
    docs_url="/docs" if config.enable_api_docs else None,
    redoc_url="/redoc" if config.enable_api_docs else None,
    lifespan=lifespan,
    debug=config.debug,
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
# 1. Rate limiting (executes LAST, needs account from credit check)
app.middleware("http")(rate_limit_wrapper)
# 2. Audit logging (executes EIGHTH, logs all operations after auth)
app.middleware("http")(audit_log_middleware)
# 3. Credit verification (executes SEVENTH, sets account info, needs seed phrase)
app.middleware("http")(check_credit_for_all_operations)
# 4. Frontend HMAC verification for /user/ endpoints (executes SIXTH)
app.middleware("http")(verify_frontend_hmac_middleware)
# 5. HMAC authentication (extract seed phrase - executes FIFTH)
app.middleware("http")(verify_hmac_middleware)
# 6. Input validation (AWS S3 compliance - executes FOURTH)
# app.middleware("http")(input_validation_middleware)  # noqa: ERA001
# 7. Banhammer (IP-based protection - executes THIRD)
if config.enable_banhammer:
    app.middleware("http")(banhammer_wrapper)
# 8. CORS (executes SECOND)
app.middleware("http")(cors_middleware)
if config.enable_request_profiling:
    # 9. Profiler (executes FIRST - outermost layer, profiles entire request including auth)
    app.add_middleware(SpeedscopeProfilerMiddleware)


@app.get("/robots.txt", include_in_schema=False)
async def robots_txt():
    """Serve robots.txt to prevent crawler indexing."""
    content = """User-agent: *
Disallow: /

# Explicitly disallow common crawlers
User-agent: Googlebot
Disallow: /

User-agent: Bingbot
Disallow: /

User-agent: Slurp
Disallow: /

User-agent: DuckDuckBot
Disallow: /

User-agent: Baiduspider
Disallow: /

User-agent: YandexBot
Disallow: /

User-agent: facebookexternalhit
Disallow: /

User-agent: Twitterbot
Disallow: /"""
    return Response(
        content=content,
        media_type="text/plain",
    )


app.include_router(user_router, prefix="/user")
app.include_router(s3_router, prefix="")
app.include_router(multipart_router, prefix="")
