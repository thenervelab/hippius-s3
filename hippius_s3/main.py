"""Main application module for Hippius S3 service."""

import logging
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
from hippius_s3.api.middlewares.metrics import metrics_middleware
from hippius_s3.api.middlewares.profiler import SpeedscopeProfilerMiddleware
from hippius_s3.api.middlewares.rate_limit import RateLimitService
from hippius_s3.api.middlewares.rate_limit import rate_limit_wrapper
from hippius_s3.api.middlewares.trailing_slash import trailing_slash_normalizer
from hippius_s3.api.s3 import errors as s3_errors
from hippius_s3.api.s3.multipart import router as multipart_router
from hippius_s3.api.s3.public_router import router as public_router
from hippius_s3.api.s3.router import router as s3_router_new
from hippius_s3.api.user import router as user_router
from hippius_s3.cache import RedisDownloadChunksCache
from hippius_s3.cache import RedisObjectPartsCache
from hippius_s3.config import get_config
from hippius_s3.ipfs_service import IPFSService
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

        app.state.redis_client = async_redis.from_url(config.redis_url)
        logger.info("Redis client initialized")

        app.state.redis_accounts_client = async_redis.from_url(config.redis_accounts_url)
        logger.info("Redis accounts client initialized")

        app.state.redis_chain_client = async_redis.from_url(config.redis_chain_url)
        logger.info("Redis chain client initialized")

        app.state.redis_rate_limiting_client = async_redis.from_url(config.redis_rate_limiting_url)
        logger.info("Redis rate limiting client initialized")

        app.state.rate_limit_service = RateLimitService(app.state.redis_rate_limiting_client)
        logger.info("Rate limiting service initialized")

        app.state.banhammer_service = BanHammerService(app.state.redis_rate_limiting_client)
        logger.info("Banhammer service initialized")

        app.state.ipfs_service = IPFSService(config, app.state.redis_client)
        logger.info("IPFS service initialized with Redis client")

        # Cache repositories
        app.state.obj_cache = RedisObjectPartsCache(app.state.redis_client)
        app.state.dl_cache = RedisDownloadChunksCache(app.state.redis_client)
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
            await app.state.redis_substrate_client.close()
            logger.info("Redis substrate client closed")
        except Exception:
            logger.exception("Error shutting down Redis substrate client")

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
        openapi_schema["security"] = [{"Base64 encoded seed phrase": []}]
        app.openapi_schema = openapi_schema
        return app.openapi_schema

    app.openapi = custom_openapi  # type: ignore[method-assign]

    # Custom middlewares - middleware("http") executes in REVERSE order
    app.middleware("http")(rate_limit_wrapper)
    app.middleware("http")(metrics_middleware)
    app.middleware("http")(audit_log_middleware)
    app.middleware("http")(check_credit_for_all_operations)
    app.middleware("http")(trailing_slash_normalizer)
    app.middleware("http")(verify_frontend_hmac_middleware)
    app.middleware("http")(verify_hmac_middleware)
    if config.enable_banhammer:
        app.middleware("http")(banhammer_wrapper)
    app.middleware("http")(cors_middleware)
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

    app.include_router(user_router, prefix="/user")
    app.include_router(public_router, prefix="")
    app.include_router(s3_router_new, prefix="")
    app.include_router(multipart_router, prefix="")

    from opentelemetry.instrumentation.fastapi import FastAPIInstrumentor

    FastAPIInstrumentor.instrument_app(app)

    return app


app = factory()
