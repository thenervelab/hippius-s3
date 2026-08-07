"""IP whitelist middleware to ensure backend only accepts requests from gateway."""

import ipaddress
import logging
from typing import Awaitable
from typing import Callable

from fastapi import Request
from fastapi import Response

from hippius_s3.config import get_config


logger = logging.getLogger(__name__)


def _denied(reason: str) -> Response:
    logger.warning(reason)
    return Response(status_code=403, content="Access denied")


async def ip_whitelist_middleware(
    request: Request,
    call_next: Callable[[Request], Awaitable[Response]],
) -> Response:
    """
    Ensure backend only accepts requests from the gateway (private cluster networks).

    Matched against config.api_ip_whitelist_cidrs, which defaults to RFC1918 plus loopback. Note
    that RFC1918's middle block is 172.16.0.0/12, not all of 172/8 — 172.15.x and 172.32.x are
    public address space and must not be admitted.

    /health endpoint is exempted to allow Kubernetes health probes from node IPs.
    """
    # Allow health checks from anywhere in the cluster (including kubelet on nodes)
    if request.url.path == "/health":
        return await call_next(request)

    client_ip = request.client.host if request.client else None

    if not client_ip:
        return _denied("Denied request with no client address")

    # This is an authorization boundary, so an address we cannot parse is a denial, never a 500 —
    # ip_address() raises on anything malformed and that must not become a server error.
    try:
        address = ipaddress.ip_address(client_ip)
    except ValueError:
        return _denied(f"Denied request from unparseable client IP: {client_ip!r}")

    # A dual-stack listener reports the gateway as ::ffff:10.0.0.1, which matches no IPv4 network —
    # so without this every forwarded request 403s and the api is hard-down from a listener config
    # change whose symptom looks nothing like its cause. The mapped form is a representation of the
    # same host, not a separate address class, so normalising it makes the check correct rather than
    # more permissive: ::ffff:172.15.0.1 is still denied.
    if isinstance(address, ipaddress.IPv6Address) and address.ipv4_mapped:
        address = address.ipv4_mapped

    if not any(address in network for network in get_config().api_ip_whitelist_cidrs):
        return _denied(f"Denied request from non-internal IP: {client_ip}")

    logger.debug(f"Allowed request from internal IP: {client_ip}")
    return await call_next(request)
