import logging
import secrets
from typing import Callable

import redis.asyncio as async_redis
from fastapi import Request
from fastapi import Response
from starlette import status

from hippius_s3.api.s3.errors import s3_error_response


logger = logging.getLogger(__name__)


def get_client_ip(request: Request) -> str:
    """Extract the real client IP from request headers."""
    # Check X-Real-IP first (most reliable for HAProxy setups)
    real_ip = request.headers.get("X-Real-IP")
    if real_ip:
        return real_ip.strip()

    # Check X-Forwarded-For as fallback (for other proxy setups)
    xff = request.headers.get("X-Forwarded-For")
    if xff:
        # Take the first IP in the chain (the original client)
        return xff.split(",")[0].strip()

    # Fall back to direct client IP
    if request.client and request.client.host:
        return request.client.host

    raise ValueError(f"Could not find client origin IP {request.headers=}")


class BanHammerService:
    def __init__(
        self,
        redis: async_redis.Redis,
        infringement_window_seconds: int = 300,
        infringement_cooldown_seconds: int = 3600,  # 1 hour
        infringement_max: int = 50,
    ):
        self.redis = redis
        self.infringement_window_seconds = infringement_window_seconds
        self.infringement_cooldown_seconds = infringement_cooldown_seconds
        self.infringement_max = infringement_max
        logger.info(
            f"BanHammerService initialized: {infringement_max} infringements in {infringement_window_seconds}s = {infringement_cooldown_seconds}s ban"
        )

    async def is_blocked(self, ip: str) -> int | None:
        """Check if an IP is currently banned. Returns seconds until unban, or None if not banned."""
        key = f"hippius_banhammer:block:{ip}"
        ttl = await self.redis.ttl(key)

        # ttl == -2: key doesn't exist (not banned)
        # ttl == -1: key exists but no TTL (shouldn't happen)
        # ttl > 0: key exists with time remaining
        if ttl > 0:
            return ttl
        return None

    async def add_infringement(self, ip: str, reason: str = ""):
        """Add an infringement for an IP and check if it should be banned."""
        # Add an infringement with unique key
        infringement_key = f"hippius_banhammer:infringement:{ip}:{secrets.token_hex(8)}"
        await self.redis.set(infringement_key, reason, ex=self.infringement_window_seconds)
        logger.info(f"Added infringement for {ip}: {reason}")

        pattern = f"hippius_banhammer:infringement:{ip}:*"
        cursor = 0
        infringements = []

        cursor, items = await self.redis.scan(
            cursor=cursor,
            match=pattern,
        )
        infringements.extend(items)

        while cursor != 0:
            cursor, items = await self.redis.scan(
                cursor=cursor,
                match=pattern,
            )
            infringements.extend(items)

        infringement_count = len(infringements)
        logger.debug(f"IP {ip} has {infringement_count}/{self.infringement_max} infringements")

        # Ban if threshold exceeded
        if infringement_count >= self.infringement_max:
            block_key = f"hippius_banhammer:block:{ip}"
            await self.redis.set(
                block_key,
                f"banned_for_{infringement_count}_infringements",
                ex=self.infringement_cooldown_seconds,
            )
            logger.warning(
                f"BANNED IP {ip} for {self.infringement_cooldown_seconds}s due to {infringement_count} infringements"
            )


async def banhammer_middleware(
    request: Request,
    call_next: Callable,
    banhammer_service: BanHammerService,
) -> Response:
    """
    Banhammer middleware to protect against abusive IPs.

    This middleware:
    1. Extracts client IP from headers
    2. Checks if IP is currently banned
    3. Monitors for suspicious behavior patterns
    4. Automatically bans IPs that exceed infringement thresholds
    """

    # Extract client IP
    client_ip = get_client_ip(request)
    if client_ip == "unknown":
        logger.warning("Could not determine client IP, allowing request")
        return await call_next(request)

    try:
        # Check if IP is currently banned
        ban_ttl = await banhammer_service.is_blocked(client_ip)
        if ban_ttl:
            logger.warning(f"Blocked request from banned IP {client_ip} ({ban_ttl}s remaining)")
            return s3_error_response(
                code="AccessDenied",
                message=f"Your IP address has been temporarily banned due to suspicious activity. Try again in {ban_ttl} seconds.",
                status_code=status.HTTP_403_FORBIDDEN,
            )

        # Continue with request
        response = await call_next(request)

        # Post-request checks for suspicious behavior
        await _post_request_checks(
            request,
            response,
            client_ip,
            banhammer_service,
        )

        return response

    except Exception as e:
        logger.error(f"Banhammer middleware error: {e}")
        # On errors, allow request through rather than blocking all traffic
        return await call_next(request)


async def _post_request_checks(
    request: Request,
    response: Response,
    client_ip: str,
    banhammer_service: BanHammerService,
):
    """Run post-request checks to detect suspicious behavior."""

    # Check 1: Too many 4xx errors (client errors)
    if 400 <= response.status_code < 500:
        await banhammer_service.add_infringement(
            client_ip,
            f"client_error_{response.status_code}_{request.method}_{request.url.path}",
        )

    # Check 2: Malformed requests (specific S3 errors that indicate scanning/probing)
    if response.status_code == 400:
        await banhammer_service.add_infringement(
            client_ip,
            f"malformed_request_{request.method}_{request.url.path}",
        )

    # Check 3: Authentication failures
    if response.status_code == 403:
        await banhammer_service.add_infringement(
            client_ip,
            f"auth_failure_{request.method}_{request.url.path}",
        )
