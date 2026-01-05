import logging
import time
from typing import Awaitable
from typing import Callable

import redis.asyncio as async_redis
from starlette import status
from starlette.requests import Request
from starlette.responses import Response

from gateway.utils.errors import s3_error_response


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
        *,
        allowlist_ips: set[str] | None = None,
        unauth_infringement_window_seconds: int = 60,
        unauth_infringement_cooldown_seconds: int = 3600,  # 1h
        unauth_infringement_max: int = 50,
        auth_infringement_window_seconds: int = 60,
        auth_infringement_cooldown_seconds: int = 300,  # 5m (short ban for authenticated)
        auth_infringement_max: int = 200,
        unauth_404_methods: set[str] | None = None,
    ):
        self.redis = redis
        self.unauth_infringement_window_seconds = int(unauth_infringement_window_seconds)
        self.unauth_infringement_cooldown_seconds = int(unauth_infringement_cooldown_seconds)
        self.unauth_infringement_max = int(unauth_infringement_max)

        self.auth_infringement_window_seconds = int(auth_infringement_window_seconds)
        self.auth_infringement_cooldown_seconds = int(auth_infringement_cooldown_seconds)
        self.auth_infringement_max = int(auth_infringement_max)

        self.allowlist_ips = set(allowlist_ips or set())
        self.unauth_404_methods = {m.upper() for m in (unauth_404_methods or {"GET", "HEAD"})}
        logger.info(
            "BanHammerService initialized: unauth=%s infringements/%ss => %ss ban; auth=%s infringements/%ss => %ss ban",
            self.unauth_infringement_max,
            self.unauth_infringement_window_seconds,
            self.unauth_infringement_cooldown_seconds,
            self.auth_infringement_max,
            self.auth_infringement_window_seconds,
            self.auth_infringement_cooldown_seconds,
        )

    def is_allowlisted(self, ip: str) -> bool:
        return str(ip).strip() in self.allowlist_ips

    def _profile_for(self, is_authenticated: bool) -> str:
        return "auth" if is_authenticated else "unauth"

    def _block_key(self, ip: str) -> str:
        # Shared "toxic IP" ban across both auth/unauth traffic.
        return f"hippius_banhammer:block:{ip}"

    def _count_key(self, profile: str, ip: str, window_bucket: int) -> str:
        return f"hippius_banhammer:infringements:{profile}:{ip}:{window_bucket}"

    def _limits_for(self, profile: str) -> tuple[int, int, int]:
        if profile == "auth":
            return (
                self.auth_infringement_window_seconds,
                self.auth_infringement_cooldown_seconds,
                self.auth_infringement_max,
            )
        return (
            self.unauth_infringement_window_seconds,
            self.unauth_infringement_cooldown_seconds,
            self.unauth_infringement_max,
        )

    async def is_blocked(self, ip: str) -> int | None:
        """Check if an IP is currently banned. Returns seconds until unban, or None if not banned."""
        # NOTE: shared ban key; auth/unauth counters are separate but bans apply to all.
        key = self._block_key(ip)
        ttl = int(await self.redis.ttl(key))

        # ttl == -2: key doesn't exist (not banned)
        # ttl == -1: key exists but no TTL (shouldn't happen)
        # ttl > 0: key exists with time remaining
        if ttl > 0:
            return ttl
        return None

    def should_add_infringement(
        self,
        *,
        is_authenticated: bool,
        request_method: str,
        status_code: int,
    ) -> bool:
        """Classification rules for what counts as an infringement.

        Keep this intentionally simple:
        - Strict for unauthenticated probing (counts 400/401/403/405 and 404 for selected methods)
        - Lenient for authenticated traffic (counts only 400)
        """
        sc = int(status_code)

        # Never count server-side issues.
        if sc >= 500:
            return False

        # Never count rate limiting responses.
        if sc in (429, 503):
            return False

        # Authenticated: only malformed requests count.
        if is_authenticated:
            return sc == 400

        # Unauthenticated: strict.
        # Include 401 (common unauth response) and 405 (method probing).
        if sc in (400, 401, 403, 405):
            return True
        return sc == 404 and str(request_method).upper() in self.unauth_404_methods

    async def add_infringement(self, ip: str, *, is_authenticated: bool = False, reason: str = "") -> None:
        """Add an infringement for an IP and check if it should be banned.

        Uses a fixed-window counter (INCR + EXPIRE) for O(1) performance.
        """
        profile = self._profile_for(is_authenticated)
        window_seconds, cooldown_seconds, infringement_max = self._limits_for(profile)

        # Increment counter for the current window bucket
        window_bucket = int(time.time()) // int(window_seconds)
        count_key = self._count_key(profile, ip, window_bucket)
        count = await self.redis.incr(count_key)
        if count == 1:
            await self.redis.expire(count_key, int(window_seconds))

        logger.info("banhammer_infringement ip=%s profile=%s count=%s/%s reason=%s", ip, profile, count, infringement_max, reason)

        # Ban if threshold exceeded
        if count >= int(infringement_max):
            # Shared ban: if already banned for longer, keep the longer TTL.
            block_key = self._block_key(ip)
            ban_seconds = int(cooldown_seconds)
            value = f"banned_for_{count}_infringements:{profile}:{reason}"
            script = """
            local key = KEYS[1]
            local val = ARGV[1]
            local new_ttl = tonumber(ARGV[2])
            local ttl = redis.call('TTL', key)
            if ttl == -2 then
              redis.call('SET', key, val, 'EX', new_ttl)
              return new_ttl
            end
            if ttl == -1 then
              -- key exists without TTL; treat as permanently banned and keep as-is
              return ttl
            end
            if ttl < new_ttl then
              redis.call('SET', key, val, 'EX', new_ttl)
              return new_ttl
            end
            return ttl
            """
            with_ttl = await self.redis.eval(script, 1, block_key, value, str(ban_seconds))
            logger.warning(
                "banhammer_ban ip=%s profile=%s ban_seconds=%s effective_ttl=%s count=%s reason=%s",
                ip,
                profile,
                ban_seconds,
                str(with_ttl),
                count,
                reason,
            )


async def banhammer_middleware(
    request: Request,
    call_next: Callable[[Request], Awaitable[Response]],
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

    def _is_request_authenticated(req: Request) -> bool:
        # Centralise auth detection to avoid sprinkling conventions throughout middleware.
        return bool(getattr(req.state, "account", None))

    # Extract client IP
    try:
        client_ip = get_client_ip(request)
    except Exception:
        logger.warning("Could not determine client IP, allowing request", exc_info=True)
        return await call_next(request)

    if not client_ip or str(client_ip).strip().lower() == "unknown":
        logger.warning("Could not determine client IP (unknown/empty), allowing request")
        return await call_next(request)

    if banhammer_service.is_allowlisted(client_ip):
        return await call_next(request)

    is_authenticated = _is_request_authenticated(request)

    try:
        # Check if IP is currently banned
        ban_ttl = await banhammer_service.is_blocked(client_ip)
        if ban_ttl:
            logger.debug(f"Blocked request from banned IP {client_ip} ({ban_ttl}s remaining)")
            return s3_error_response(
                code="AccessDenied",
                message=f"Your IP address has been temporarily banned due to suspicious activity. Try again in {ban_ttl} seconds.",
                status_code=status.HTTP_403_FORBIDDEN,
            )

        # Continue with request
        response = await call_next(request)

        if banhammer_service.should_add_infringement(
            is_authenticated=is_authenticated,
            request_method=str(request.method),
            status_code=int(response.status_code),
        ):
            await banhammer_service.add_infringement(
                client_ip,
                is_authenticated=is_authenticated,
                reason=f"{response.status_code}_{request.method}_{request.url.path}",
            )

        return response

    except Exception as e:
        logger.error(f"Banhammer middleware error: {e}")
        # On errors, allow request through rather than blocking all traffic
        return await call_next(request)


async def banhammer_wrapper(request: Request, call_next: Callable) -> Response:
    return await banhammer_middleware(
        request,
        call_next,
        request.app.state.banhammer_service,
    )
