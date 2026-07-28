"""A client that hangs up mid-body is not a server error.

`request.stream()` raises ClientDisconnect when the peer goes away while we are still pumping
its body upstream. That happens inside the body iterator httpx consumes from
`client.stream(...).__aenter__()`, so it escapes through the whole middleware chain and uvicorn
records the request as a 500 — for what is ordinary client behaviour (SDK timeout, ^C, reset).

These tests pin the abort to 499 and, just as importantly, pin that a *real* upstream failure
is still allowed to surface.
"""

from __future__ import annotations

from contextlib import asynccontextmanager
from typing import Any
from unittest.mock import Mock

import httpx
import pytest
from starlette.requests import ClientDisconnect

from gateway.services.forward_service import ForwardService


def _request(method: str, headers: dict[str, str] | None = None, *, disconnect_after: int = 0) -> Any:
    """A request whose body stream yields `disconnect_after` chunks then drops the client."""
    request = Mock()
    request.method = method
    request.headers = headers or {"content-length": "17"}
    request.scope = {"path": "/bucket/key.bin"}
    request.url.query = ""
    request.url.path = "/bucket/key.bin"
    request.state = Mock(spec=[])

    async def _stream():
        for _ in range(disconnect_after):
            yield b"chunk"
        raise ClientDisconnect()

    request.stream = _stream
    return request


class _ConsumingStreamStub:
    """Stands in for client.stream, draining the request body the way httpx does."""

    def __init__(self) -> None:
        self.calls = 0
        self.consumed = 0

    def __call__(self, **kwargs: Any) -> Any:
        self.calls += 1
        content = kwargs.get("content")
        outer = self

        @asynccontextmanager
        async def _cm():
            if content is not None:
                async for chunk in content:
                    outer.consumed += len(chunk)
            response = Mock()
            response.status_code = 200
            response.headers = httpx.Headers({"content-type": "application/octet-stream"})

            async def _aiter_bytes():
                yield b"payload"

            response.aiter_bytes = _aiter_bytes
            yield response

        return _cm()


@pytest.fixture
def service() -> ForwardService:
    return ForwardService("http://api:8000")


@pytest.mark.asyncio
@pytest.mark.parametrize("method", ["PUT", "POST"])
async def test_client_abort_is_499_not_500(service: ForwardService, method: str) -> None:
    stub = _ConsumingStreamStub()
    service.client.stream = stub  # type: ignore[method-assign]

    response = await service.forward_request(_request(method))

    assert response.status_code == 499, "a client-initiated abort must not be booked as a server error"


@pytest.mark.asyncio
async def test_client_abort_partway_through_a_body_is_still_499(service: ForwardService) -> None:
    """The disconnect usually lands after some bytes are already upstream, not on the first chunk."""
    stub = _ConsumingStreamStub()
    service.client.stream = stub  # type: ignore[method-assign]

    response = await service.forward_request(_request("PUT", disconnect_after=3))

    assert response.status_code == 499
    assert stub.consumed == 15, "the bytes that did arrive should still have been forwarded"


@pytest.mark.asyncio
async def test_client_abort_is_not_retried(service: ForwardService) -> None:
    """request.stream() is one-shot — a re-send would ship a truncated body."""
    stub = _ConsumingStreamStub()
    service.client.stream = stub  # type: ignore[method-assign]

    await service.forward_request(_request("PUT"))

    assert stub.calls == 1


@pytest.mark.asyncio
async def test_upstream_failure_still_surfaces(service: ForwardService) -> None:
    """Guard the blast radius: only ClientDisconnect is swallowed, not upstream breakage."""

    def _stream(**kwargs: Any) -> Any:
        @asynccontextmanager
        async def _cm():
            raise httpx.ConnectError("api unreachable")
            yield  # pragma: no cover

        return _cm()

    service.client.stream = _stream  # type: ignore[method-assign]

    with pytest.raises(httpx.ConnectError):
        await service.forward_request(_request("PUT"))
