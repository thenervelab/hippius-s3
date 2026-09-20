"""ArionClient construction: the read path needs tight per-request bounds, the uploader needs the
long default. Both are the same class, so the bounds are constructor-scoped — a class-wide change
would silently apply a 10 s read timeout to 4 MiB Arion uploads that legitimately take minutes.
"""

from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import patch

import httpcore
import httpx

from hippius_s3.services import arion_service
from hippius_s3.services.arion_service import ArionClient


def _cfg() -> SimpleNamespace:
    return SimpleNamespace(
        arion_base_url="https://arion.example/",
        arion_service_key="k",
        arion_verify_ssl=True,
    )


def test_default_timeout_is_unchanged_for_the_uploader() -> None:
    with patch.object(arion_service, "get_config", return_value=_cfg()):
        client = ArionClient()
    assert client._client.timeout == httpx.Timeout(60.0, connect=10.0)


def test_caller_supplied_timeout_and_limits_are_applied() -> None:
    timeout = httpx.Timeout(connect=4.0, read=10.0, write=10.0, pool=4.0)
    limits = httpx.Limits(max_connections=32, max_keepalive_connections=32)
    with patch.object(arion_service, "get_config", return_value=_cfg()):
        client = ArionClient(timeout=timeout, limits=limits)
    assert client._client.timeout == timeout
    # httpx does not expose limits on the client; the transport's pool carries them.
    transport = client._client._transport
    assert isinstance(transport, httpx.AsyncHTTPTransport)
    pool = transport._pool
    assert isinstance(pool, httpcore.AsyncConnectionPool)
    assert pool._max_connections == 32
    assert pool._max_keepalive_connections == 32
