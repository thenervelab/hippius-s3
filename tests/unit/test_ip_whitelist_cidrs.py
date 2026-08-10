"""The api's authorization boundary: only the private ranges the gateway actually runs in."""

from __future__ import annotations

import pytest
from starlette.requests import Request

from hippius_s3.api.middlewares.ip_whitelist import ip_whitelist_middleware


def _request(client_ip: str | None, path: str = "/some-bucket/key") -> Request:
    scope: dict = {
        "type": "http",
        "method": "GET",
        "path": path,
        "scheme": "http",
        "server": ("testserver", 80),
        "query_string": b"",
        "headers": [],
    }
    if client_ip is not None:
        scope["client"] = (client_ip, 12345)
    return Request(scope)


async def _admitted(client_ip: str | None, path: str = "/some-bucket/key") -> bool:
    """True when the middleware passed the request through to the app."""
    reached = False

    async def call_next(_request: Request):
        nonlocal reached
        reached = True
        return "ok"

    response = await ip_whitelist_middleware(_request(client_ip, path), call_next)
    if not reached:
        assert response.status_code == 403
    return reached


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("client_ip", "expected"),
    [
        # RFC1918 block 172.16.0.0/12 — the two boundaries either side are PUBLIC address space,
        # and a `startswith("172.")` prefix test admits both of them.
        ("172.15.255.255", False),
        ("172.16.0.0", True),
        ("172.31.255.255", True),
        ("172.32.0.0", False),
        # 10.0.0.0/8 and its lower boundary.
        ("10.0.0.1", True),
        ("9.255.255.255", False),
        # RFC1918 but deliberately not in the default: nothing here runs on 192.168/16, and an
        # unused range is authorization boundary carried for free. Denied on purpose, not by
        # oversight — see _DEFAULT_IP_WHITELIST_CIDRS.
        ("192.168.1.1", False),
        ("127.0.0.1", True),
        ("::1", True),
        # Anything unparseable must fail closed rather than raise.
        ("not-an-ip", False),
        ("", False),
        ("172.16.0.0.1", False),
    ],
)
async def test_only_private_ranges_are_admitted(client_ip: str, expected: bool) -> None:
    assert await _admitted(client_ip) is expected


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("client_ip", "expected"),
    [
        ("::ffff:10.0.0.1", True),
        ("::ffff:127.0.0.1", True),
        ("::ffff:172.15.255.255", False),
        ("::ffff:203.0.113.7", False),
    ],
)
async def test_ipv4_mapped_addresses_decide_the_same_as_their_ipv4_form(client_ip: str, expected: bool) -> None:
    """A dual-stack listener reports the gateway as ::ffff:10.0.0.1. That is the same host as
    10.0.0.1, so it has to reach the same verdict — admitting it is correctness, and the denied
    cases prove normalising the form does not blanket-admit the mapped range."""
    assert await _admitted(client_ip) is expected


@pytest.mark.asyncio
async def test_a_request_with_no_client_is_denied() -> None:
    assert await _admitted(None) is False


@pytest.mark.asyncio
async def test_health_stays_reachable_from_any_address() -> None:
    """Kubelet probes arrive from the node IP, which is outside the pod network."""
    assert await _admitted("203.0.113.7", path="/health") is True
