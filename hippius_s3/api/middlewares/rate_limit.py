import logging
import time
from typing import Awaitable
from typing import Callable

import redis.asyncio as async_redis
from starlette import status
from starlette.requests import Request
from starlette.responses import Response

from hippius_s3.api.s3.errors import s3_error_response
from hippius_s3.config import get_config


config = get_config()
logger = logging.getLogger(__name__)


class RateLimitService:
    def __init__(self, redis: async_redis.Redis):
        self.redis = redis
        logger.info("RateLimitService was instantiated")

    async def increment_and_get_count(
        self,
        main_account_id: str,
        window_seconds: int,
    ) -> int:
        """Atomically increment and return the request count in the current window.

        Uses a fixed window counter implemented with INCR and EXPIRE for O(1) performance.
        """
        # Fixed window bucket (e.g., per 60s)
        window_bucket = int(time.time()) // window_seconds
        key = f"hippius_rate_limit:{main_account_id}:{window_bucket}"

        # INCR returns the incremented value; set TTL only when the key is first created
        count = await self.redis.incr(key)
        if count == 1:
            await self.redis.expire(key, window_seconds)
        return int(count)


async def rate_limit_middleware(
    request: Request,
    call_next: Callable[[Request], Awaitable[Response]],
    rate_limit_service: RateLimitService,
    max_requests: int = 7200,
    window_seconds: int = 60,
) -> Response:
    """
    Rate limiting middleware for S3 API requests based on main account ID.

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
    skip_paths = ["/openapi.json", "/docs", "/redoc", "/health", "/robots.txt"]
    if request.url.path in skip_paths:
        return await call_next(request)

    # dont rate limit the front end
    if request.url.path.startswith("/user/"):
        return await call_next(request)

    # Ensure account is available from authentication
    if not hasattr(request.state, "account") or not request.state.account:
        logger.error(f"Rate limiting failed: no account found for {request.url.path}")
        return s3_error_response(
            code="AccessDenied",
            message="Authentication required for this endpoint",
            status_code=status.HTTP_403_FORBIDDEN,
        )

    main_account_id = request.state.account.main_account

    try:
        current_count = await rate_limit_service.increment_and_get_count(main_account_id, window_seconds)

        if current_count > max_requests:
            logger.warning(f"Main account '{main_account_id}' rate limit exceeded ({current_count}/{max_requests})")
            return s3_error_response(
                code="SlowDown",
                message=f"Rate limit exceeded. Maximum {max_requests} requests per {window_seconds} seconds allowed.",
                status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
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
        max_requests=config.rate_limit_per_minute,
        window_seconds=60,
    )
