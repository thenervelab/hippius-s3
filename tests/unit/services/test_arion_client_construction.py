from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import patch

import httpcore
import httpx
import pytest

from hippius_s3.services import arion_service
from hippius_s3.services.arion_service import ArionClient


def _cfg() -> SimpleNamespace:
    return SimpleNamespace(
        arion_base_url="https://arion.example/",
        arion_service_key="k",
        arion_verify_ssl=True,
    )


def _client(timeout: httpx.Timeout | None = None, limits: httpx.Limits | None = None) -> ArionClient:
    with patch.object(arion_service, "get_config", return_value=_cfg()):
        return ArionClient(timeout=timeout, limits=limits)


def test_default_timeout_is_unchanged_for_the_uploader() -> None:
    """The read path needs tight per-request bounds, the uploader the long default; both are the
    same class, so the bounds are constructor-scoped — a class-wide change would silently apply a
    10 s read timeout to 4 MiB Arion uploads that legitimately take minutes."""
    client = _client()
    assert client._client.timeout == httpx.Timeout(60.0, connect=10.0)


def test_caller_supplied_timeout_and_limits_are_applied() -> None:
    timeout = httpx.Timeout(connect=4.0, read=10.0, write=10.0, pool=2.0)
    limits = httpx.Limits(max_connections=32, max_keepalive_connections=32)
    client = _client(timeout=timeout, limits=limits)
    assert client._client.timeout == timeout
    # httpx does not expose limits on the client; the transport's pool carries them.
    transport = client._client._transport
    assert isinstance(transport, httpx.AsyncHTTPTransport)
    pool = transport._pool
    assert isinstance(pool, httpcore.AsyncConnectionPool)
    assert pool._max_connections == 32
    assert pool._max_keepalive_connections == 32


class _Conn:
    def __init__(self, state: str) -> None:
        self._state = state

    def is_closed(self) -> bool:
        return self._state == "closed"

    def has_expired(self) -> bool:
        return self._state == "expired"

    def is_idle(self) -> bool:
        return self._state in ("idle", "expired")


def test_pool_snapshot_counts_connections_by_state(monkeypatch: pytest.MonkeyPatch) -> None:
    # The snapshot is the only record of what a wedged pool looked like; it is taken before the
    # client is thrown away, so it has to read httpcore's private state.
    client = _client()
    states = ["in_use", "in_use", "in_use", "idle", "idle", "expired", "closed"]
    monkeypatch.setattr(client._client._transport, "_pool", SimpleNamespace(connections=[_Conn(s) for s in states]))
    assert client.pool_snapshot() == "connections=7 idle=2 expired=1 closed=1 in_use=3"


def test_pool_snapshot_of_a_fresh_client_is_empty() -> None:
    assert _client().pool_snapshot() == "connections=0 idle=0 expired=0 closed=0 in_use=0"


def test_pool_snapshot_never_raises(monkeypatch: pytest.MonkeyPatch) -> None:
    class _Broken:
        @property
        def connections(self) -> list[object]:
            raise RuntimeError("shape changed")

    client = _client()
    monkeypatch.setattr(client._client._transport, "_pool", _Broken())
    assert client.pool_snapshot() == "unavailable"

    # A transport that is not httpx's own (a test MockTransport, say) has no pool to read.
    monkeypatch.setattr(client._client, "_transport", httpx.MockTransport(lambda request: httpx.Response(200)))
    assert client.pool_snapshot() == "unavailable"
