"""`Incomplete upstream stream` must only fire for a genuinely truncated body.

The check compares bytes_sent against the upstream Content-Length. Some replies carry a
Content-Length but legitimately send no body at all — a HEAD reply advertises the length GET
*would* return (RFC 9110 9.3.2), and 204/304 are defined bodyless. For those, bytes_sent is
always 0 and always < Content-Length, so the warning fired unconditionally: ~561-1373/hr on
the production gateway, which buried the real mid-stream truncations it exists to surface.

These tests pin the suppression AND pin that a real truncation still warns.
"""

from __future__ import annotations

import logging
from contextlib import asynccontextmanager
from typing import Any
from unittest.mock import Mock

import httpx
import pytest

from gateway.services.forward_service import ForwardService


def _request(method: str) -> Any:
    request = Mock()
    request.method = method
    request.headers = {}
    request.scope = {"path": "/bucket/key.bin"}
    request.url.query = ""
    request.url.path = "/bucket/key.bin"
    request.state = Mock(spec=[])

    async def _stream():
        return
        yield  # pragma: no cover - never reached, keeps this an async generator

    request.stream = _stream
    return request


def _stream_stub(*, status_code: int, content_length: int, body: bytes) -> Any:
    """Upstream that advertises `content_length` but actually emits `body`."""

    def _call(**kwargs: Any) -> Any:
        @asynccontextmanager
        async def _cm():
            response = Mock()
            response.status_code = status_code
            response.headers = httpx.Headers({"content-length": str(content_length)})

            async def _aiter_bytes():
                if body:
                    yield body

            response.aiter_bytes = _aiter_bytes
            yield response

        return _cm()

    return _call


async def _drain(service: ForwardService, request: Any) -> None:
    response = await service.forward_request(request)
    body = getattr(response, "body_iterator", None)
    if body is not None:
        async for _ in body:
            pass


@pytest.fixture
def service() -> ForwardService:
    return ForwardService("http://api:8000")


@pytest.mark.asyncio
async def test_head_reply_does_not_warn(service: ForwardService, caplog: Any) -> None:
    """A HEAD reply sends no body by definition — that is not a truncation."""
    service.client.stream = _stream_stub(status_code=200, content_length=25165824, body=b"")  # type: ignore[method-assign]

    with caplog.at_level(logging.WARNING, logger="gateway.services.forward_service"):
        await _drain(service, _request("HEAD"))

    assert "Incomplete upstream stream" not in caplog.text


@pytest.mark.asyncio
@pytest.mark.parametrize("status_code", [204, 304])
async def test_bodyless_status_codes_do_not_warn(service: ForwardService, caplog: Any, status_code: int) -> None:
    """204 and 304 are defined to carry no body, whatever Content-Length says."""
    service.client.stream = _stream_stub(status_code=status_code, content_length=1024, body=b"")  # type: ignore[method-assign]

    with caplog.at_level(logging.WARNING, logger="gateway.services.forward_service"):
        await _drain(service, _request("GET"))

    assert "Incomplete upstream stream" not in caplog.text


@pytest.mark.asyncio
async def test_a_real_truncation_still_warns(service: ForwardService, caplog: Any) -> None:
    """The signal has to survive: a GET that stops short of Content-Length must still warn.

    This is the 2026-07-28 shape — 200 OK, full Content-Length, body dies partway.
    """
    service.client.stream = _stream_stub(status_code=200, content_length=25165824, body=b"x" * 5242880)  # type: ignore[method-assign]

    with caplog.at_level(logging.WARNING, logger="gateway.services.forward_service"):
        await _drain(service, _request("GET"))

    assert "Incomplete upstream stream" in caplog.text
    assert "sent=5242880" in caplog.text


@pytest.mark.asyncio
async def test_complete_get_does_not_warn(service: ForwardService, caplog: Any) -> None:
    service.client.stream = _stream_stub(status_code=200, content_length=7, body=b"payload")  # type: ignore[method-assign]

    with caplog.at_level(logging.WARNING, logger="gateway.services.forward_service"):
        await _drain(service, _request("GET"))

    assert "Incomplete upstream stream" not in caplog.text
