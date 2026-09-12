"""Draining a rejected request's body is bounded: past the bound we close the connection instead."""

from __future__ import annotations

from types import SimpleNamespace
from typing import Any

import pytest
from starlette.datastructures import Headers

from hippius_s3.utils_core import DRAIN_MAX_BYTES
from hippius_s3.utils_core import drain_request_body
from hippius_s3.utils_core import respond_before_body


def _request(headers: dict[str, str], chunks: list[bytes], read: list[bytes]) -> Any:
    def stream() -> Any:
        async def gen() -> Any:
            for chunk in chunks:
                read.append(chunk)
                yield chunk

        return gen()

    return SimpleNamespace(headers=Headers(headers), stream=stream)


@pytest.mark.asyncio
async def test_a_small_body_is_drained() -> None:
    read: list[bytes] = []
    assert await drain_request_body(_request({"content-length": "4"}, [b"body"], read)) is True
    assert read == [b"body"]


@pytest.mark.asyncio
async def test_a_body_declared_too_large_is_not_read_at_all() -> None:
    read: list[bytes] = []
    request = _request({"content-length": str(DRAIN_MAX_BYTES + 1)}, [b"x" * 1024], read)

    assert await drain_request_body(request) is False
    assert read == []


@pytest.mark.asyncio
async def test_an_undeclared_body_stops_at_the_limit() -> None:
    """No Content-Length (aws-chunked, or a lying client): we stop once we have read the bound."""
    read: list[bytes] = []
    oversized = [b"x" * (DRAIN_MAX_BYTES // 2 + 1)] * 4
    request = _request({}, oversized, read)

    assert await drain_request_body(request) is False
    assert len(read) < len(oversized)


@pytest.mark.asyncio
async def test_the_response_closes_the_connection_when_the_body_was_not_drained() -> None:
    from hippius_s3.api.s3.errors import s3_error_response

    read: list[bytes] = []
    big = _request({"content-length": str(DRAIN_MAX_BYTES + 1)}, [b"x"], read)
    small = _request({"content-length": "1"}, [b"x"], read)

    closed = await respond_before_body(big, s3_error_response("InvalidDigest", "bad", status_code=400))
    kept = await respond_before_body(small, s3_error_response("InvalidDigest", "bad", status_code=400))

    assert closed.headers["connection"] == "close"
    assert "connection" not in kept.headers
