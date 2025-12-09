"""Main application module for Hippius S3 service."""

import logging
from contextlib import asynccontextmanager
from pathlib import Path
from typing import AsyncGenerator

import asyncpg
import redis.asyncio as async_redis
from dotenv import load_dotenv
from fastapi import FastAPI
from fastapi import Response
from fastapi.openapi.utils import get_openapi
from fastapi.responses import JSONResponse
from fastapi.staticfiles import StaticFiles

from hippius_s3.api.middlewares.ip_whitelist import ip_whitelist_middleware
from hippius_s3.api.middlewares.metrics import metrics_middleware
from hippius_s3.api.middlewares.parse_internal_headers import parse_internal_headers_middleware
from hippius_s3.api.middlewares.profiler import SpeedscopeProfilerMiddleware
from hippius_s3.api.middlewares.tracing import tracing_middleware
from hippius_s3.api.s3 import errors as s3_errors
from hippius_s3.api.s3.multipart import router as multipart_router
from hippius_s3.api.s3.public_router import router as public_router
from hippius_s3.api.s3.router import router as s3_router_new
from hippius_s3.api.user import router as user_router
from hippius_s3.cache import FileSystemPartsStore
from hippius_s3.cache import RedisDownloadChunksCache
from hippius_s3.cache import RedisObjectPartsCache
from hippius_s3.config import get_config
from hippius_s3.logging_config import setup_loki_logging
from hippius_s3.metrics_collector_task import BackgroundMetricsCollector


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

        from hippius_s3.orm import initialize_engine

        sqlalchemy_url = config.database_url.replace("postgresql://", "postgresql+asyncpg://")
        app.state.sqlalchemy_engine = initialize_engine(sqlalchemy_url)
        logger.info("SQLAlchemy async engine initialized")

        app.state.redis_client = async_redis.from_url(config.redis_url)
        logger.info("Redis client initialized")

        app.state.redis_accounts_client = async_redis.from_url(config.redis_accounts_url)
        logger.info("Redis accounts client initialized")

        app.state.redis_chain_client = async_redis.from_url(config.redis_chain_url)
        logger.info("Redis chain client initialized")

        app.state.redis_rate_limiting_client = async_redis.from_url(config.redis_rate_limiting_url)
        logger.info("Redis rate limiting client initialized")

        app.state.redis_queues_client = async_redis.from_url(config.redis_queues_url)
        logger.info("Redis queues client initialized")

        from hippius_s3.queue import initialize_queue_client
        from hippius_s3.redis_cache import initialize_cache_client
        from hippius_s3.redis_chain import initialize_chain_client

        initialize_queue_client(app.state.redis_queues_client)
        logger.info("Queue client initialized")

        initialize_chain_client(app.state.redis_chain_client)
        logger.info("Chain Redis client initialized")

        initialize_cache_client(app.state.redis_client)
        logger.info("Cache Redis client initialized")

        # IPFS service not needed in API container; workers own IPFS interactions

        # Cache repositories
        app.state.obj_cache = RedisObjectPartsCache(app.state.redis_client)
        app.state.dl_cache = RedisDownloadChunksCache(app.state.redis_client)
        app.state.fs_store = FileSystemPartsStore(config.object_cache_dir)
        logger.info("Cache repositories initialized")

        from hippius_s3.monitoring import MetricsCollector
        from hippius_s3.monitoring import set_metrics_collector

        app.state.metrics_collector = MetricsCollector(app.state.redis_client)
        set_metrics_collector(app.state.metrics_collector)

        logger.info("Metrics collector initialized")
        logger.info("Tracing and metrics handled by opentelemetry-instrument wrapper")

        # Start background metrics collection
        app.state.background_metrics_collector = BackgroundMetricsCollector(
            app.state.metrics_collector,
            app.state.redis_client,
            app.state.redis_accounts_client,
            app.state.redis_chain_client,
            app.state.redis_rate_limiting_client,
            app.state.redis_queues_client,
        )
        await app.state.background_metrics_collector.start()
        logger.info("Background metrics collection started")

        yield

    finally:
        try:
            # Stop background metrics collection
            if hasattr(app.state, "background_metrics_collector"):
                await app.state.background_metrics_collector.stop()
                logger.info("Background metrics collection stopped")
        except Exception:
            logger.exception("Error shutting down background metrics collector")

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
            await app.state.redis_chain_client.close()
            logger.info("Redis chain client closed")
        except Exception:
            logger.exception("Error shutting down Redis chain client")

        try:
            await app.state.redis_rate_limiting_client.close()
            logger.info("Redis rate limiting client closed")
        except Exception:
            logger.exception("Error shutting down Redis rate limiting client")

        try:
            await app.state.redis_queues_client.close()
            logger.info("Redis queues client closed")
        except Exception:
            logger.exception("Error shutting down Redis queues client")

        try:
            await app.state.postgres_pool.close()
            logger.info("Postgres connection pool closed")
        except Exception:
            logger.exception("Error shutting down postgres pool")


def factory() -> FastAPI:
    """Factory function to create and configure the FastAPI application."""
    load_dotenv()
    config = get_config()
    setup_loki_logging(config, "api")

    app = FastAPI(
        title="Hippius S3",
        description="Hippius S3 Gateway",
        docs_url="/docs" if config.enable_api_docs else None,
        redoc_url="/redoc" if config.enable_api_docs else None,
        swagger_favicon_url="/static/favicon.ico",
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
            "Access Key": {
                "type": "http",
                "scheme": "bearer",
                "description": (
                    "Bearer token authentication using Hippius access keys. "
                    "Format: 'hip_' followed by alphanumeric characters. "
                    "Example: hip_abc123def456ghi789. "
                    "Obtain from https://console.hippius.com/dashboard/settings"
                ),
            }
        }
        openapi_schema["security"] = [{"Access Key": []}]
        app.openapi_schema = openapi_schema
        return app.openapi_schema

    app.openapi = custom_openapi  # type: ignore[method-assign]

    # Custom middlewares - middleware("http") executes in REVERSE order
    # Backend now relies on gateway for authentication/authorization
    # All middleware here assume X-Hippius-* headers are already set by gateway
    # Audit logging has been moved to gateway (which sees real client IPs)
    app.middleware("http")(metrics_middleware)
    app.middleware("http")(tracing_middleware)
    app.middleware("http")(parse_internal_headers_middleware)
    app.middleware("http")(ip_whitelist_middleware)
    if config.enable_request_profiling:
        app.add_middleware(SpeedscopeProfilerMiddleware)

    @app.exception_handler(Exception)
    async def global_exception_handler(request, exc):  # type: ignore[no-untyped-def]
        if exc.__class__.__name__ == "DownloadNotReadyError" or str(exc) in {"initial_stream_timeout"}:
            return s3_errors.s3_error_response(
                code="SlowDown",
                message="Object not ready for download yet. Please retry.",
                status_code=503,
            )
        raise exc

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

    @app.get("/health", include_in_schema=False, response_class=JSONResponse)
    async def health():
        """Health check endpoint for monitoring."""
        return JSONResponse(content={"status": "healthy"})

    app.include_router(user_router, prefix="/user")
    app.include_router(public_router, prefix="")
    app.include_router(s3_router_new, prefix="")
    app.include_router(multipart_router, prefix="")

    static_dir = Path(__file__).parent / "static"
    app.mount("/static", StaticFiles(directory=str(static_dir)), name="static")

    return app


app = factory()
