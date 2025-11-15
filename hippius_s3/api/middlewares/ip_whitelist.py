"""IP whitelist middleware to ensure backend only accepts requests from gateway."""

import logging
from typing import Awaitable
from typing import Callable

from fastapi import Request
from fastapi import Response


logger = logging.getLogger(__name__)


async def ip_whitelist_middleware(
    request: Request,
    call_next: Callable[[Request], Awaitable[Response]],
) -> Response:
    """
    Ensure backend only accepts requests from the gateway (Docker internal network).

    This is a security measure to prevent direct access to the backend API.
    Only requests from Docker internal networks (172.x, 10.x) or localhost are allowed.
    """
    client_ip = request.client.host if request.client else None

    if not client_ip:
        logger.warning("Could not determine client IP, denying request")
        return Response(status_code=403, content="Access denied")

    if not (
        client_ip.startswith("172.") or client_ip.startswith("10.") or client_ip == "127.0.0.1" or client_ip == "::1"
    ):
        logger.warning(f"Denied request from non-internal IP: {client_ip}")
        return Response(status_code=403, content="Access denied")

    logger.debug(f"Allowed request from internal IP: {client_ip}")
    return await call_next(request)
