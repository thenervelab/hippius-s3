"""Retrying a dead upstream connection, and knowing when not to.

A pooled keep-alive connection to the API can be closed at any moment — most visibly when
an api pod is rolled. httpx surfaces that as RemoteProtocolError from the stream context
manager's __aenter__, i.e. before any response byte exists, so a re-send is invisible to
the client.

The bound on this is that `request.stream()` is one-shot. Anything with a body cannot be
replayed, so it must not be retried.
"""

from __future__ import annotations

from contextlib import asynccontextmanager
from typing import Any
from unittest.mock import Mock

import httpx
import pytest

from gateway.services.forward_service import ForwardService


def _request(method: str, headers: dict[str, str] | None = None) -> Any:
    request = Mock()
    request.method = method
    request.headers = headers or {}
    request.scope = {"path": "/bucket/key.bin"}
    request.url.query = ""
    request.url.path = "/bucket/key.bin"
    request.state = Mock(spec=[])

    async def _stream():
        yield b""

    request.stream = _stream
    return request


def _upstream_response(status: int = 200) -> Mock:
    response = Mock()
    response.status_code = status
    response.headers = httpx.Headers({"content-type": "application/octet-stream"})

    async def _aiter_bytes():
        yield b"payload"

    response.aiter_bytes = _aiter_bytes
    return response


class _StreamStub:
    """Stands in for client.stream, failing the first N calls at __aenter__."""

    def __init__(self, failures: int, exc: Exception) -> None:
        self.failures = failures
        self.exc = exc
        self.calls = 0

    def __call__(self, **kwargs: Any) -> Any:
        self.calls += 1
        should_fail = self.calls <= self.failures
        exc = self.exc

        @asynccontextmanager
        async def _cm():
            if should_fail:
                raise exc
            yield _upstream_response()

        return _cm()


@pytest.fixture
def service() -> ForwardService:
    return ForwardService("http://api:8000")


@pytest.mark.asyncio
@pytest.mark.parametrize("method", ["GET", "HEAD"])
async def test_retries_once_when_upstream_dies_before_responding(service: ForwardService, method: str) -> None:
    stub = _StreamStub(failures=1, exc=httpx.RemoteProtocolError("Server disconnected without sending a response."))
    service.client.stream = stub  # type: ignore[method-assign]

    response = await service.forward_request(_request(method))

    assert stub.calls == 2, "a bodyless idempotent request should be re-sent exactly once"
    assert response.status_code == 200


@pytest.mark.asyncio
async def test_gives_up_after_one_retry(service: ForwardService) -> None:
    """Two dead connections in a row is a real outage, not a rolled pod — surface it."""
    stub = _StreamStub(failures=2, exc=httpx.RemoteProtocolError("Server disconnected without sending a response."))
    service.client.stream = stub  # type: ignore[method-assign]

    with pytest.raises(httpx.RemoteProtocolError):
        await service.forward_request(_request("GET"))

    assert stub.calls == 2, "must not retry indefinitely"


@pytest.mark.asyncio
@pytest.mark.parametrize("method", ["PUT", "POST", "DELETE"])
async def test_never_retries_a_request_it_cannot_replay(service: ForwardService, method: str) -> None:
    """request.stream() is consumed on the first attempt; a re-send would ship an empty body.

    DELETE has no body but is excluded on purpose: re-sending after an ambiguous failure
    can delete a second time.
    """
    stub = _StreamStub(failures=1, exc=httpx.RemoteProtocolError("Server disconnected without sending a response."))
    service.client.stream = stub  # type: ignore[method-assign]

    with pytest.raises(httpx.RemoteProtocolError):
        await service.forward_request(_request(method, {"content-length": "17"}))

    assert stub.calls == 1, f"{method} must not be retried"


@pytest.mark.asyncio
async def test_never_retries_a_get_that_carries_a_body(service: ForwardService) -> None:
    stub = _StreamStub(failures=1, exc=httpx.RemoteProtocolError("boom"))
    service.client.stream = stub  # type: ignore[method-assign]

    with pytest.raises(httpx.RemoteProtocolError):
        await service.forward_request(_request("GET", {"content-length": "12"}))

    assert stub.calls == 1, "a GET with a body cannot be replayed either"


@pytest.mark.asyncio
async def test_does_not_retry_application_errors(service: ForwardService) -> None:
    """A 500 from the API is the API's answer — forward it rather than doubling the load."""
    stub = _StreamStub(failures=0, exc=RuntimeError("unused"))
    service.client.stream = stub  # type: ignore[method-assign]

    response = await service.forward_request(_request("GET"))

    assert stub.calls == 1
    assert response.status_code == 200
