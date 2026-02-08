import logging
import time
from typing import Any

import httpx


logger = logging.getLogger(__name__)


class DocsProxyService:
    def __init__(self, backend_url: str, cache_ttl: int = 300):
        self.backend_url = backend_url
        self.cache_ttl = cache_ttl
        self.memory_cache: dict[Any, Any] | None = None
        self.cache_timestamp: float = 0.0
        logger.info(f"DocsProxyService initialized with backend: {backend_url}, cache TTL: {cache_ttl}s")

    async def get_openapi_schema(self) -> dict[Any, Any]:
        if self.memory_cache and (time.time() - self.cache_timestamp) < self.cache_ttl:
            logger.debug("OpenAPI schema cache hit (memory)")
            return self.memory_cache

        logger.info("OpenAPI schema cache miss, fetching from backend")
        schema = await self.fetch_from_backend()

        self.memory_cache = schema
        self.cache_timestamp = time.time()

        return schema

    async def fetch_from_backend(self) -> dict[Any, Any]:
        url = f"{self.backend_url}/openapi.json"
        async with httpx.AsyncClient(timeout=10.0) as client:
            try:
                logger.debug(f"Fetching OpenAPI schema from {url}")
                response = await client.get(url)
                response.raise_for_status()
                schema: dict[Any, Any] = response.json()
                logger.info("Successfully fetched OpenAPI schema from backend")
                return schema
            except httpx.HTTPError as e:
                logger.error(f"Failed to fetch OpenAPI schema from backend: {e}")
                raise
            except Exception as e:
                logger.error(f"Unexpected error fetching OpenAPI schema: {e}")
                raise

    async def clear_cache(self) -> None:
        self.memory_cache = None
        self.cache_timestamp = 0.0
        logger.info("Cleared OpenAPI schema from memory cache")
