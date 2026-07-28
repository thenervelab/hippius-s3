"""A client that hangs up mid-body is not a server error.

`request.stream()` raises ClientDisconnect when the peer goes away while we are still pumping
its body upstream. That happens inside the body iterator httpx consumes from
`client.stream(...).__aenter__()`, so it escapes through the whole middleware chain and uvicorn
logs a full ASGI exception traceback — for what is ordinary client behaviour (SDK timeout, ^C,
reset). No access-log line is written and no response is sent: the peer is already gone.

These tests pin the abort to 499 and, just as importantly, pin that a *real* upstream failure
is still allowed to surface.
"""

from __future__ import annotations

import logging
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
@pytest.mark.parametrize(
    "headers",
    [
        {"content-length": "17"},
        {"transfer-encoding": "chunked"},
        # AWS CLI v2 / SigV4 streaming: a real content-length plus aws-chunked framing.
        {"content-length": "17", "content-encoding": "aws-chunked", "x-amz-decoded-content-length": "5"},
    ],
    ids=["content-length", "chunked", "aws-chunked"],
)
async def test_every_body_framing_reaches_the_handler(service: ForwardService, headers: dict[str, str]) -> None:
    """`has_body` gates whether the body iterator is consumed at all — if a refactor drops the
    transfer-encoding clause, that framing silently stops hitting the ClientDisconnect branch."""
    stub = _ConsumingStreamStub()
    service.client.stream = stub  # type: ignore[method-assign]

    response = await service.forward_request(_request("PUT", headers))

    assert response.status_code == 499


@pytest.mark.asyncio
async def test_client_abort_logs_what_was_received(service: ForwardService, caplog: Any) -> None:
    """With the client gone there is no response and no access-log line, so this WARNING is the
    only artifact the abort leaves behind. It has to carry the byte count to be worth anything."""
    stub = _ConsumingStreamStub()
    service.client.stream = stub  # type: ignore[method-assign]

    with caplog.at_level(logging.WARNING, logger="gateway.services.forward_service"):
        await service.forward_request(_request("PUT", disconnect_after=3))

    assert "Client disconnected while sending request body" in caplog.text
    assert "15 bytes received" in caplog.text


@pytest.mark.asyncio
async def test_a_body_bearing_request_is_never_eligible_for_retry(service: ForwardService) -> None:
    """The reason the abort is safe to swallow: ClientDisconnect can only arise when a body is
    being sent, and a body-bearing request is never retriable, so the two can never interact.
    Asserting the invariant directly — a call-count check on a PUT would hold with or without
    the fix, since PUT is already single-attempt.
    """
    for method in ("GET", "HEAD", "PUT", "POST", "DELETE"):
        for headers in ({"content-length": "17"}, {"transfer-encoding": "chunked"}):
            stub = _ConsumingStreamStub()
            service.client.stream = stub  # type: ignore[method-assign]

            await service.forward_request(_request(method, headers))

            assert stub.calls == 1, f"{method} with a body must be single-attempt"


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
