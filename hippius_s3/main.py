"""Main application module for Hippius S3 service."""

import logging
import sys
from contextlib import asynccontextmanager
from typing import AsyncGenerator

import asyncpg
import redis.asyncio as async_redis
from dotenv import load_dotenv
from fastapi import FastAPI
from fastapi import Request
from fastapi import Response
from fastapi.openapi.utils import get_openapi
from prometheus_client import CONTENT_TYPE_LATEST
from prometheus_client import generate_latest

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
from hippius_s3.metrics_collector_task import BackgroundMetricsCollector
from hippius_s3.monitoring import get_metrics_collector


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


async def metrics_middleware(request: Request, call_next):
    """Middleware for recording HTTP metrics."""
    import time

    start_time = time.time()
    response = await call_next(request)
    duration = time.time() - start_time

    # Extract account info from request state if available
    main_account = None
    subaccount_id = None

    if hasattr(request.state, "account"):
        main_account = getattr(request.state.account, "main_account", None)
        subaccount_id = getattr(request.state.account, "id", None)

    # Get endpoint function name from request scope
    endpoint_name = "unknown"
    try:
        if "route" in request.scope:
            route = request.scope["route"]
            if hasattr(route, "endpoint") and hasattr(route.endpoint, "__name__"):
                endpoint_name = route.endpoint.__name__
    except Exception:
        pass

    # Record metrics
    metrics_collector = get_metrics_collector()
    if metrics_collector:
        # Create a custom attributes dict to override the handler
        attributes = {
            "method": request.method,
            "handler": endpoint_name,  # Use function name instead of path
            "status_code": str(response.status_code),
        }

        if main_account:
            attributes["main_account"] = main_account
        if subaccount_id:
            attributes["subaccount_id"] = subaccount_id

        metrics_collector.http_requests_total.add(1, attributes=attributes)
        metrics_collector.http_request_duration.record(duration, attributes=attributes)

    return response


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

        # Initialize metrics collector and set global instance
        from opentelemetry.exporter.prometheus import PrometheusMetricReader
        from opentelemetry.sdk.metrics import MeterProvider
        from opentelemetry.sdk.resources import Resource

        from hippius_s3.monitoring import MetricsCollector
        from hippius_s3.monitoring import metrics

        # Set up OpenTelemetry
        resource = Resource.create({"service.name": "hippius-s3-api", "service.version": "1.0.0"})
        prometheus_reader = PrometheusMetricReader()
        provider = MeterProvider(resource=resource, metric_readers=[prometheus_reader])
        metrics.set_meter_provider(provider)

        # Prometheus metrics are served via /metrics route, no separate server needed

        app.state.metrics_collector = MetricsCollector(app.state.redis_client)

        # Set global metrics collector instance
        import hippius_s3.monitoring

        hippius_s3.monitoring.metrics_collector = app.state.metrics_collector

        logger.info("Metrics collector and Prometheus server initialized")

        # Start background metrics collection
        app.state.background_metrics_collector = BackgroundMetricsCollector(
            app.state.metrics_collector, app.state.redis_client
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


@app.get("/metrics")
async def metrics_endpoint():
    """Prometheus metrics endpoint."""
    return Response(content=generate_latest(), media_type=CONTENT_TYPE_LATEST)


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
# 2. Metrics collection (executes NINTH, records after all processing)
app.middleware("http")(metrics_middleware)
# 3. Audit logging (executes EIGHTH, logs all operations after auth)
app.middleware("http")(audit_log_middleware)
# 3. Credit verification (executes SEVENTH, sets account info, needs seed phrase)
app.middleware("http")(check_credit_for_all_operations)
# 4. Trailing slash normalization (executes SIXTH, after AWS signature verification)
app.middleware("http")(trailing_slash_normalizer)
# 5. Frontend HMAC verification for /user/ endpoints (executes FIFTH)
app.middleware("http")(verify_frontend_hmac_middleware)
# 6. HMAC authentication (extract seed phrase - executes FOURTH)
app.middleware("http")(verify_hmac_middleware)
# 7. Input validation (AWS S3 compliance - executes THIRD)
# app.middleware("http")(input_validation_middleware)  # noqa: ERA001
# 8. Banhammer (IP-based protection - executes SECOND)
if config.enable_banhammer:
    app.middleware("http")(banhammer_wrapper)
# 9. CORS (executes FIRST)
app.middleware("http")(cors_middleware)
if config.enable_request_profiling:
    # 10. Profiler (executes FIRST - outermost layer, profiles entire request including auth)
    app.add_middleware(SpeedscopeProfilerMiddleware)


# Map streaming readiness failures to S3 503 error
@app.exception_handler(Exception)
async def global_exception_handler(request, exc):  # type: ignore[no-untyped-def]
    # Handle preflight stream readiness failures from ObjectReader
    if exc.__class__.__name__ == "DownloadNotReadyError" or str(exc) in {"initial_stream_timeout"}:
        return s3_errors.s3_error_response(
            code="SlowDown",
            message="Object not ready for download yet. Please retry.",
            status_code=503,
        )
    # Let FastAPI default handler deal with others
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
app.include_router(public_router, prefix="")  # before the generic S3 routes
app.include_router(s3_router_new, prefix="")
app.include_router(multipart_router, prefix="")
