import asyncio
import time
from typing import Dict

import asyncpg
from fastapi import FastAPI
from fastapi import Request
from fastapi import Response

from gateway.config import get_config
from gateway.middlewares.account import account_middleware
from gateway.middlewares.acl import acl_middleware
from gateway.middlewares.ats_purge import ats_purge_middleware
from gateway.middlewares.audit_log import audit_log_middleware
from gateway.middlewares.auth_router import auth_router_middleware
from gateway.middlewares.cache_control import cache_control_middleware
from gateway.middlewares.cors import cors_middleware
from gateway.middlewares.frontend_hmac import verify_frontend_hmac_middleware
from gateway.middlewares.input_validation import input_validation_middleware
from gateway.middlewares.metrics import metrics_middleware
from gateway.middlewares.ray_id import ray_id_middleware
from gateway.middlewares.read_only import read_only_middleware
from gateway.middlewares.tracing import tracing_middleware
from gateway.middlewares.trailing_slash import trailing_slash_normalizer
from gateway.routers.acl import router as acl_router
from gateway.routers.docs import router as docs_router
from gateway.services.acl_service import ACLService
from gateway.services.docs_proxy_service import DocsProxyService
from gateway.services.forward_service import ForwardService
from hippius_s3.logging_config import setup_loki_logging
from hippius_s3.monitoring import MetricsCollector
from hippius_s3.monitoring import set_metrics_collector
from hippius_s3.sentry import init_sentry
from hippius_s3.services.arion_service import ArionClient


def factory() -> FastAPI:
    from hippius_s3.otel_setup import configure_otel

    config = get_config()
    logger = setup_loki_logging(config, "hippius-s3-gateway")

    configure_otel("hippius-s3-gateway")

    init_sentry("hippius-s3-gateway")
    app = FastAPI(
        title="Hippius S3 API",
        version="1.0.0",
        docs_url=None,
        redoc_url=None,
        openapi_url=None,
    )

    @app.on_event("startup")
    async def startup() -> None:
        from hippius_s3.redis_utils import create_redis_client

        logger.info("Starting Hippius S3 Gateway...")

        app.state.postgres_pool = await asyncpg.create_pool(
            config.database_url,
            min_size=config.db_pool_min_size,
            max_size=config.db_pool_max_size,
            max_queries=config.db_pool_max_queries,
            max_inactive_connection_lifetime=config.db_pool_max_inactive_lifetime,
            command_timeout=config.db_pool_command_timeout,
            timeout=10,
        )
        logger.info(f"PostgreSQL pool created: min={config.db_pool_min_size}, max={config.db_pool_max_size}")

        app.state.redis_client = create_redis_client(config.redis_url)
        logger.info("Connected to Redis")

        from redis.asyncio import Redis

        app.state.redis_accounts = Redis.from_url(config.redis_accounts_url, decode_responses=False)
        logger.info("Connected to Redis (accounts)")

        app.state.redis_chain = Redis.from_url(config.redis_chain_url, decode_responses=False)
        logger.info("Connected to Redis (chain)")

        app.state.redis_rate_limiting = Redis.from_url(config.redis_rate_limiting_url, decode_responses=False)
        logger.info("Connected to Redis (rate limiting)")

        app.state.redis_acl = Redis.from_url(config.redis_acl_url, decode_responses=True)
        logger.info("Connected to Redis (ACL cache)")

        app.state.metrics_collector = MetricsCollector(app.state.redis_client)
        set_metrics_collector(app.state.metrics_collector)
        logger.info("Metrics collector initialized")
        logger.info("Tracing and metrics handled by programmatic OTel init")

        app.state.forward_service = ForwardService(config.backend_url)
        logger.info(f"ForwardService initialized with backend: {config.backend_url}")

        logger.info("Rate limiting and banhammer disabled")

        app.state.acl_service = ACLService(
            db_pool=app.state.postgres_pool,
            redis_client=app.state.redis_acl,
            cache_ttl=config.acl_cache_ttl_seconds,
        )
        logger.info("ACLService initialized")

        app.state.docs_proxy_service = DocsProxyService(
            backend_url=config.backend_url,
            redis_client=app.state.redis_client,
            cache_ttl=config.docs_cache_ttl_seconds,
        )
        logger.info("DocsProxyService initialized")

        app.state.arion_client = ArionClient(
            base_url=config.arion_base_url,
            service_key=config.arion_service_key,
        )
        logger.info("ArionClient initialized")

        async def collect_pool_metrics() -> None:
            while True:
                await asyncio.sleep(60)
                if hasattr(app.state, "postgres_pool") and hasattr(app.state, "metrics_collector"):
                    pool = app.state.postgres_pool
                    size = pool.get_size()
                    free = pool.get_idle_size()
                    app.state.metrics_collector.update_db_pool_metrics(size, free)

        asyncio.create_task(collect_pool_metrics())
        logger.info("Pool metrics collection task started")

        logger.info("Gateway startup complete")

    @app.on_event("shutdown")
    async def shutdown() -> None:
        from gateway.services import ats_cache_client

        logger.info("Shutting down Hippius S3 Gateway...")

        if hasattr(app.state, "arion_client"):
            await app.state.arion_client.close()
            logger.info("ArionClient closed")

        await ats_cache_client.close()
        logger.info("ATS cache client closed")

        if hasattr(app.state, "forward_service"):
            await app.state.forward_service.close()

        if hasattr(app.state, "postgres_pool"):
            await app.state.postgres_pool.close()
            logger.info("PostgreSQL pool closed")

        if hasattr(app.state, "redis_client"):
            await app.state.redis_client.close()
            logger.info("Redis client closed")

        if hasattr(app.state, "redis_accounts"):
            await app.state.redis_accounts.close()
            logger.info("Redis accounts client closed")

        if hasattr(app.state, "redis_chain"):
            await app.state.redis_chain.close()
            logger.info("Redis chain client closed")

        if hasattr(app.state, "redis_rate_limiting"):
            await app.state.redis_rate_limiting.close()
            logger.info("Redis rate limiting client closed")

        if hasattr(app.state, "redis_acl"):
            await app.state.redis_acl.close()
            logger.info("Redis ACL client closed")

        logger.info("Gateway shutdown complete")

    @app.get("/health")
    async def health() -> Dict[str, str]:
        return {"status": "healthy", "service": "gateway"}

    app.include_router(docs_router)
    app.include_router(acl_router)

    @app.api_route("/{path:path}", methods=["GET", "POST", "PUT", "DELETE", "HEAD", "PATCH"])
    async def forward_all(request: Request, path: str) -> Response:
        if hasattr(request.state, "gateway_start_time"):
            request.state.gateway_overhead_ms = (time.time() - request.state.gateway_start_time) * 1000
        forward_service = request.app.state.forward_service
        return await forward_service.forward_request(request)

    # Starlette executes middleware last-registered-first (last = outermost).
    # The list below runs innermost → outermost on the request path.
    app.middleware("http")(ray_id_middleware)
    if config.enable_audit_logging:
        app.middleware("http")(audit_log_middleware)
    app.middleware("http")(metrics_middleware)
    app.middleware("http")(tracing_middleware)
    app.middleware("http")(verify_frontend_hmac_middleware)
    app.middleware("http")(acl_middleware)
    app.middleware("http")(account_middleware)
    app.middleware("http")(trailing_slash_normalizer)
    app.middleware("http")(auth_router_middleware)
    app.middleware("http")(input_validation_middleware)
    if config.read_only_mode:
        app.middleware("http")(read_only_middleware)
    # Inside CORS so Cache-Control lands before CORS wraps the response.
    app.middleware("http")(ats_purge_middleware)
    app.middleware("http")(cache_control_middleware)
    # Outermost: CORS must wrap everything so error responses get CORS headers
    app.middleware("http")(cors_middleware)

    return app


if __name__ == "__main__":
    import os

    import uvicorn

    config = get_config()
    debug_mode = os.getenv("DEBUG", "false").lower() == "true"
    uvicorn.run(
        "gateway.main:factory", host="0.0.0.0", port=config.port, reload=debug_mode, access_log=True, factory=True
    )
