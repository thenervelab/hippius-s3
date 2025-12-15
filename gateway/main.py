import asyncio
from typing import Callable
from typing import Dict

import asyncpg
import redis.asyncio as redis
from fastapi import FastAPI
from fastapi import Request
from fastapi import Response

from gateway.config import get_config
from gateway.middlewares.account import account_middleware
from gateway.middlewares.acl import acl_middleware
from gateway.middlewares.audit_log import audit_log_middleware
from gateway.middlewares.auth_router import auth_router_middleware
from gateway.middlewares.banhammer import BanHammerService
from gateway.middlewares.banhammer import banhammer_middleware
from gateway.middlewares.cors import cors_middleware
from gateway.middlewares.frontend_hmac import verify_frontend_hmac_middleware
from gateway.middlewares.metrics import metrics_middleware
from gateway.middlewares.rate_limit import RateLimitService
from gateway.middlewares.rate_limit import rate_limit_middleware
from gateway.middlewares.ray_id import ray_id_middleware
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


config = get_config()
logger = setup_loki_logging(config, "hippius-s3-gateway")


def factory() -> FastAPI:
    app = FastAPI(
        title="Hippius S3 API",
        version="1.0.0",
        docs_url=None,
        redoc_url=None,
        openapi_url=None,
    )

    @app.on_event("startup")
    async def startup() -> None:
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

        app.state.redis_client = redis.from_url(config.redis_url, decode_responses=False)
        logger.info("Connected to Redis")

        app.state.redis_accounts = redis.from_url(config.redis_accounts_url, decode_responses=False)
        logger.info("Connected to Redis (accounts)")

        app.state.redis_chain = redis.from_url(config.redis_chain_url, decode_responses=False)
        logger.info("Connected to Redis (chain)")

        app.state.redis_rate_limiting = redis.from_url(config.redis_rate_limiting_url, decode_responses=False)
        logger.info("Connected to Redis (rate limiting)")

        app.state.redis_acl = redis.from_url(config.redis_acl_url, decode_responses=True)
        logger.info("Connected to Redis (ACL cache)")

        app.state.metrics_collector = MetricsCollector(app.state.redis_client)
        set_metrics_collector(app.state.metrics_collector)
        logger.info("Metrics collector initialized")
        logger.info("Tracing and metrics handled by opentelemetry-instrument wrapper")

        app.state.forward_service = ForwardService(config.backend_url)
        logger.info(f"ForwardService initialized with backend: {config.backend_url}")

        app.state.rate_limit_service = RateLimitService(app.state.redis_rate_limiting)
        logger.info("RateLimitService initialized")

        app.state.banhammer_service = BanHammerService(app.state.redis_rate_limiting)
        logger.info("BanHammerService initialized")

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

        async def collect_pool_metrics() -> None:
            while True:
                await asyncio.sleep(60)
                if hasattr(app.state, "postgres_pool") and hasattr(app.state, "metrics_collector"):
                    pool = app.state.postgres_pool
                    size = pool.get_size()
                    free = pool.get_idle_size()
                    app.state.metrics_collector.update_db_pool_metrics(size, free)

        await asyncio.create_task(collect_pool_metrics())
        logger.info("Pool metrics collection task started")

        logger.info("Gateway startup complete")

    @app.on_event("shutdown")
    async def shutdown() -> None:
        logger.info("Shutting down Hippius S3 Gateway...")

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
        forward_service = request.app.state.forward_service
        return await forward_service.forward_request(request)  # type: ignore[no-any-return]

    async def rate_limit_wrapper(request: Request, call_next: Callable) -> Response:
        return await rate_limit_middleware(
            request,
            call_next,
            request.app.state.rate_limit_service,
            max_requests=config.rate_limit_per_minute,
            window_seconds=60,
        )

    async def banhammer_wrapper(request: Request, call_next: Callable) -> Response:
        return await banhammer_middleware(
            request,
            call_next,
            request.app.state.banhammer_service,
        )

    # Register middleware in REVERSE order (outermost first)
    # IMPORTANT: auth_router must execute BEFORE account (so register AFTER)
    # IMPORTANT: trailing_slash must execute AFTER auth_router (so register BEFORE)
    # IMPORTANT: audit_log must execute AFTER account (so register BEFORE) to access account info
    # IMPORTANT: ray_id must execute FIRST (so register LAST) to make ray_id available to all middlewares
    app.middleware("http")(ray_id_middleware)
    if config.enable_audit_logging:
        app.middleware("http")(audit_log_middleware)
    app.middleware("http")(metrics_middleware)
    app.middleware("http")(tracing_middleware)
    app.middleware("http")(cors_middleware)
    if config.enable_banhammer:
        app.middleware("http")(banhammer_wrapper)
    app.middleware("http")(verify_frontend_hmac_middleware)
    app.middleware("http")(rate_limit_wrapper)
    app.middleware("http")(acl_middleware)
    app.middleware("http")(account_middleware)
    app.middleware("http")(trailing_slash_normalizer)
    app.middleware("http")(auth_router_middleware)

    return app


app = factory()

if __name__ == "__main__":
    import os

    import uvicorn

    debug_mode = os.getenv("DEBUG", "false").lower() == "true"
    uvicorn.run("gateway.main:app", host="0.0.0.0", port=config.port, reload=debug_mode, access_log=True)
