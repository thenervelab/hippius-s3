import logging

import asyncpg
import redis.asyncio as redis
from fastapi import FastAPI
from fastapi import Request

from gateway.config import get_config
from gateway.services.forward_service import ForwardService


logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)

logger = logging.getLogger(__name__)

config = get_config()


def factory() -> FastAPI:
    app = FastAPI(title="Hippius S3 Gateway", version="1.0.0")

    @app.on_event("startup")
    async def startup():
        logger.info("Starting Hippius S3 Gateway...")

        app.state.postgres_pool = await asyncpg.create_pool(config.database_url)
        logger.info("Connected to PostgreSQL")

        app.state.redis_client = redis.from_url(config.redis_url, decode_responses=False)
        logger.info("Connected to Redis")

        app.state.redis_accounts = redis.from_url(
            config.redis_accounts_url, decode_responses=False
        )
        logger.info("Connected to Redis (accounts)")

        app.state.redis_chain = redis.from_url(config.redis_chain_url, decode_responses=False)
        logger.info("Connected to Redis (chain)")

        app.state.redis_rate_limiting = redis.from_url(
            config.redis_rate_limiting_url, decode_responses=False
        )
        logger.info("Connected to Redis (rate limiting)")

        app.state.forward_service = ForwardService(config.backend_url)
        logger.info(f"ForwardService initialized with backend: {config.backend_url}")

        logger.info("Gateway startup complete")

    @app.on_event("shutdown")
    async def shutdown():
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

        logger.info("Gateway shutdown complete")

    @app.get("/health")
    async def health():
        return {"status": "healthy", "service": "gateway"}

    @app.api_route("/{path:path}", methods=["GET", "POST", "PUT", "DELETE", "HEAD", "PATCH"])
    async def forward_all(request: Request, path: str):
        forward_service = request.app.state.forward_service
        return await forward_service.forward_request(request)

    return app


app = factory()


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(
        "gateway.main:app", host="0.0.0.0", port=config.port, reload=False, access_log=True
    )
