from typing import AsyncIterator

import pytest
from starlette.requests import Request

from hippius_s3.utils_core import iter_request_body


def _make_request(body: bytes, headers: dict[str, str]) -> Request:
    scope = {
        "type": "http",
        "method": "PUT",
        "path": "/",
        "headers": [(k.encode("latin-1"), v.encode("latin-1")) for k, v in headers.items()],
    }
    messages = [{"type": "http.request", "body": body, "more_body": False}]

    async def receive() -> dict:
        if messages:
            return messages.pop(0)
        return {"type": "http.disconnect"}

    return Request(scope, receive)


@pytest.mark.asyncio
async def test_iter_request_body_does_not_false_positive_chunked():
    raw = b"4\r\nTEST\r\nTAIL"
    request = _make_request(raw, headers={})
    pieces = [chunk async for chunk in iter_request_body(request)]
    assert b"".join(pieces) == raw


@pytest.mark.asyncio
async def test_iter_request_body_decodes_aws_chunked():
    raw = b"4\r\nTEST\r\n0\r\n\r\n"
    request = _make_request(raw, headers={"content-encoding": "aws-chunked"})
    pieces = [chunk async for chunk in iter_request_body(request)]
    assert b"".join(pieces) == b"TEST"
