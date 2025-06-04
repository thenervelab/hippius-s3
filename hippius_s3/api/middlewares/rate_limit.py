import base64
import logging
import secrets
from typing import Awaitable
from typing import Callable

import redis.asyncio as async_redis
from starlette import status
from starlette.requests import Request
from starlette.responses import Response

from hippius_s3.api.s3.errors import s3_error_response


logger = logging.getLogger(__name__)


class RateLimitService:
    def __init__(self, redis: async_redis.Redis):
        self.redis = redis
        logger.info("RateLimitService was instantiated")

    async def log_request(
        self,
        seed_phrase: str,
        ttl_seconds: int,
    ) -> None:
        """Logs a request for a given seed phrase.

        Args:
            seed_phrase (str): The seed phrase identifier.
            ttl_seconds (int): The time span for which the request count is valid.
        """
        seed_b64 = base64.b64encode(seed_phrase.encode()).decode()
        key = f"hippius_rate_limit:{seed_b64}:{secrets.token_hex(8)}"
        await self.redis.set(key, "", ex=ttl_seconds)
        logger.debug(f"{key=}")

    async def count_requests(
        self,
        seed_phrase: str,
    ) -> int:
        """Get the total number of logged requests for a seed phrase.

        Args:
            seed_phrase (str): The seed phrase to count requests for.

        Returns:
            int: The total number of requests made by the specified seed phrase.
        """
        seed_b64 = base64.b64encode(seed_phrase.encode()).decode()
        pattern = f"hippius_rate_limit:{seed_b64}:*"

        cursor = 0
        requests = []

        cursor, items = await self.redis.scan(cursor=cursor, match=pattern)
        requests.extend(items)

        while cursor != 0:
            cursor, items = await self.redis.scan(cursor=cursor, match=pattern)
            requests.extend(items)

        return len(requests)


async def rate_limit_middleware(
    request: Request,
    call_next: Callable[[Request], Awaitable[Response]],
    rate_limit_service: RateLimitService,
    max_requests: int = 100,
    window_seconds: int = 60,
) -> Response:
    """
    Rate limiting middleware for S3 API requests based on seed phrases.

    Args:
        request: The incoming request
        call_next: The next middleware/endpoint to call
        rate_limit_service: The rate limiting service
        max_requests: Maximum requests allowed per window (default: 100)
        window_seconds: Time window in seconds (default: 60)
    """
    if request.method == "OPTIONS":
        return await call_next(request)

    # Skip rate limiting for documentation and health check endpoints
    skip_paths = ["/openapi.json", "/docs", "/redoc", "/health"]
    if request.url.path in skip_paths:
        return await call_next(request)

    # dont rate limit the front end
    if request.url.path.startswith("/user/"):
        return await call_next(request)

    # Ensure seed phrase is available from authentication
    if not hasattr(request.state, "seed_phrase"):
        logger.error(f"Rate limiting failed: no seed_phrase found for {request.url.path}")
        return s3_error_response(
            code="AccessDenied",
            message="Authentication required for this endpoint",
            status_code=status.HTTP_403_FORBIDDEN,
        )

    seed_phrase = request.state.seed_phrase

    try:
        current_count = await rate_limit_service.count_requests(seed_phrase)

        if current_count >= max_requests:
            logger.warning(f"'{seed_phrase}' rate limit exceeded ({current_count}/{max_requests})")
            return s3_error_response(
                code="SlowDown",
                message=f"Rate limit exceeded. Maximum {max_requests} requests per {window_seconds} seconds allowed.",
                status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            )

        await rate_limit_service.log_request(
            seed_phrase,
            window_seconds,
        )

        return await call_next(request)

    except Exception as e:
        logger.error(f"Rate limiting error: {e}")
        # allow the request to proceed rather than blocking all traffic
        return await call_next(request)


async def rate_limit_wrapper(request: Request, call_next: Callable) -> Response:
    return await rate_limit_middleware(
        request,
        call_next,
        request.app.state.rate_limit_service,
        max_requests=100,
        window_seconds=60,
    )
