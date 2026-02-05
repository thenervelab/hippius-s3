"""Main application module for Hippius S3 service."""

import logging
import platform
import re
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Any
from typing import AsyncGenerator

import asyncpg
import redis.asyncio as async_redis
from dotenv import load_dotenv
from fastapi import FastAPI
from fastapi import Request
from fastapi import Response
from fastapi.openapi.utils import get_openapi
from fastapi.responses import JSONResponse
from fastapi.staticfiles import StaticFiles

from hippius_s3.api.middlewares.fs_cache_pressure import fs_cache_pressure_middleware
from hippius_s3.api.middlewares.input_validation import input_validation_middleware
from hippius_s3.api.middlewares.ip_whitelist import ip_whitelist_middleware
from hippius_s3.api.middlewares.metrics import metrics_middleware
from hippius_s3.api.middlewares.parse_internal_headers import parse_internal_headers_middleware
from hippius_s3.api.middlewares.profiler import SpeedscopeProfilerMiddleware
from hippius_s3.api.middlewares.tracing import tracing_middleware
from hippius_s3.api.s3 import errors as s3_errors
from hippius_s3.api.s3.public_router import router as public_router
from hippius_s3.api.s3.router import router as s3_router_new
from hippius_s3.api.user import router as user_router
from hippius_s3.cache import FileSystemPartsStore
from hippius_s3.cache import RedisDownloadChunksCache
from hippius_s3.cache import RedisObjectPartsCache
from hippius_s3.config import Config
from hippius_s3.config import get_config
from hippius_s3.logging_config import setup_loki_logging
from hippius_s3.metrics_collector_task import BackgroundMetricsCollector
from hippius_s3.storage_version import UnsupportedStorageVersionError


logger = logging.getLogger(__name__)


def _warn_if_no_aes_hw_accel() -> None:
    """Best-effort warning when AES-NI (x86) isn't advertised.

    This is a heuristic for performance expectations when using AES-GCM. On Linux,
    we check /proc/cpuinfo for the 'aes' flag. In containers, this generally reflects
    the host CPU flags exposed to the workload.
    """
    try:
        if platform.system().lower() != "linux":
            return
        txt = Path("/proc/cpuinfo").read_text(encoding="utf-8").lower()
        # cpuinfo lines include: "flags : ... aes ..."
        if "flags" in txt and re.search(r"\baes\b", txt) is None:
            logger.warning(
                "AES hardware acceleration flag not detected in /proc/cpuinfo. "
                "AES-GCM may be significantly slower on this node."
            )
    except Exception:
        # Don't fail startup for a best-effort performance hint.
        return


async def postgres_create_pool(database_url: str, config: Config) -> asyncpg.Pool:
    """Create and return a Postgres connection pool.

    Args:
        database_url: Postgres connection URL
        config: Application configuration with pool settings

    Returns:
        Connection pool for Postgres
    """
    return await asyncpg.create_pool(
        database_url,
        min_size=config.db_pool_min_size,
        max_size=config.db_pool_max_size,
        max_queries=config.db_pool_max_queries,
        max_inactive_connection_lifetime=config.db_pool_max_inactive_lifetime,
        command_timeout=config.db_pool_command_timeout,
    )


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncGenerator[None, None]:
    """FastAPI application lifespan handler."""
    try:
        app.state.config = get_config()
        config = app.state.config
        _warn_if_no_aes_hw_accel()

        # Initialize KMS client (fail-fast in required mode, no-op in disabled mode)
        from hippius_s3.services.kek_service import init_kms_client

        await init_kms_client(config)

        app.state.postgres_pool = await postgres_create_pool(config.database_url, config)
        logger.info(f"Postgres connection pool created: min={config.db_pool_min_size}, max={config.db_pool_max_size}")

        from hippius_s3.redis_utils import create_redis_client

        app.state.redis_client = create_redis_client(config.redis_url)
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

        async def collect_pool_metrics() -> None:
            import asyncio

            while True:
                await asyncio.sleep(60)
                if hasattr(app.state, "postgres_pool") and hasattr(app.state, "metrics_collector"):
                    pool = app.state.postgres_pool
                    size = pool.get_size()
                    free = pool.get_idle_size()
                    app.state.metrics_collector.update_db_pool_metrics(size, free)

        import asyncio

        asyncio.create_task(collect_pool_metrics())
        logger.info("Pool metrics collection task started")

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

        try:
            from hippius_s3.services.kek_service import close_kek_pool

            await close_kek_pool()
            logger.info("KEK connection pool closed")
        except Exception:
            logger.exception("Error shutting down KEK pool")


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

    def custom_openapi() -> dict[str, Any]:
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
    app.middleware("http")(fs_cache_pressure_middleware)
    app.middleware("http")(input_validation_middleware)
    if config.enable_request_profiling:
        app.add_middleware(SpeedscopeProfilerMiddleware)

    @app.exception_handler(Exception)
    async def global_exception_handler(request: Request, exc: Exception) -> Response:
        if exc.__class__.__name__ == "DownloadNotReadyError" or str(exc) in {"initial_stream_timeout"}:
            return s3_errors.s3_error_response(
                code="SlowDown",
                message="Object not ready for download yet. Please retry.",
                status_code=503,
            )
        if isinstance(exc, UnsupportedStorageVersionError):
            return s3_errors.s3_error_response(
                code="NotImplemented",
                message=(f"Object uses unsupported storage version (sv={exc.storage_version}). Migrate object to v4."),
                status_code=501,
            )
        raise exc

    @app.get("/robots.txt", include_in_schema=False)
    async def robots_txt() -> Response:
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
    async def health() -> JSONResponse:
        """Health check endpoint for monitoring."""
        return JSONResponse(content={"status": "healthy"})

    app.include_router(user_router, prefix="/user")
    app.include_router(public_router, prefix="")
    app.include_router(s3_router_new, prefix="")

    static_dir = Path(__file__).parent / "static"
    app.mount("/static", StaticFiles(directory=str(static_dir)), name="static")

    return app


app = factory()
