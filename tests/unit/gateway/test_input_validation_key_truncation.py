"""Object keys must be validated on what the client actually sent, not on a truncated path.

`request.url.path` is built by Starlette with `urlsplit` over the already-decoded URL, and
`urlsplit` treats `#` as the fragment delimiter. So a key sent as `report%23v1.txt` decoded to
`report#v1.txt` and then lost everything from the `#` — the middleware only ever saw `report`.

Two consequences, both live in prod before this fix:

1. `#` could never be rejected, despite sitting in OBJECT_KEY_AVOID_CHARS the whole time.
2. The truncated key reached storage. `report#v1.txt` and `report#v2.txt` both landed as
   `report`, so the second silently overwrote the first — 200 OK on both, one object gone.

These tests drive the middleware over a real ASGI scope, because the bug lives entirely in how
the path is read off that scope. A test that passes an already-parsed key would not see it.
"""

from __future__ import annotations

from typing import Any
from unittest.mock import AsyncMock
from urllib.parse import unquote

import pytest
from fastapi import Request
from fastapi import Response

from gateway.middlewares.input_validation import input_validation_middleware


def _request(raw_path: bytes) -> Request:
    """A Request whose scope carries `raw_path` exactly as a client would send it on the wire.

    `path` is set to what Starlette actually derives — decode FIRST, then drop the fragment.
    Order matters: decoding after splitting would leave `%23` intact, and the old code would then
    reject it for containing `%` — the test would pass for the wrong reason and never see the
    truncation bug at all.
    """
    starlette_path = unquote(raw_path.decode()).split("#", 1)[0]
    return Request(
        {
            "type": "http",
            "http_version": "1.1",
            "method": "PUT",
            "scheme": "https",
            "server": ("s3.hippius.com", 443),
            "path": starlette_path,
            "raw_path": raw_path,
            "query_string": b"",
            "root_path": "",
            "headers": [(b"host", b"s3.hippius.com")],
        }
    )


async def _run(raw_path: bytes) -> Response:
    call_next = AsyncMock(return_value=Response(status_code=200))
    return await input_validation_middleware(_request(raw_path), call_next)


@pytest.mark.asyncio
async def test_encoded_hash_in_key_is_rejected_not_truncated() -> None:
    """The exact prod shape: a correctly percent-encoded `#` must not slip through."""
    response = await _run(b"/bucket/report%23v1.txt")

    assert response.status_code == 400, (
        "a key containing '#' must be rejected; before this fix the '#' was parsed off as a URL "
        "fragment, so the key silently became 'report' and was stored under the wrong name"
    )


@pytest.mark.asyncio
async def test_literal_hash_in_key_is_rejected() -> None:
    """Some clients send `#` unencoded; that must not bypass validation either."""
    response = await _run(b"/bucket/report#v1.txt")

    assert response.status_code == 400


@pytest.mark.asyncio
async def test_two_keys_differing_only_after_the_hash_cannot_collide() -> None:
    """The data-loss case, stated as an invariant.

    Both of these truncate to 'report'. If either is allowed through, they land on the same
    destination key and one object is destroyed.
    """
    for raw in (b"/bucket/report%23v1.txt", b"/bucket/report%23v2.txt"):
        assert (await _run(raw)).status_code == 400


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "raw_path",
    [
        b"/bucket/normal.txt",
        b"/bucket/nested/deep/file.bin",
        b"/bucket/with%20space.txt",  # space is allowed — AWS permits it, and it is not on the avoid list
        b"/bucket/dash-and_underscore.txt",
        b"/bucket/unicode-%C3%A9%C3%A8.txt",
    ],
)
async def test_valid_keys_still_pass(raw_path: bytes) -> None:
    """The fix must not start rejecting keys that were always fine."""
    assert (await _run(raw_path)).status_code == 200


@pytest.mark.asyncio
@pytest.mark.parametrize("encoded", ["%5B", "%7B", "%7C", "%5E", "%25"])
async def test_other_avoid_chars_still_rejected(encoded: str) -> None:
    """These already rejected correctly; pin them so the raw_path switch does not regress them."""
    response = await _run(f"/bucket/bad{encoded}key.txt".encode())

    assert response.status_code == 400


@pytest.mark.asyncio
async def test_missing_raw_path_falls_back_without_crashing() -> None:
    """Not every ASGI server populates raw_path; degrade to the old behaviour, never 500."""
    scope: dict[str, Any] = {
        "type": "http",
        "http_version": "1.1",
        "method": "PUT",
        "scheme": "https",
        "server": ("s3.hippius.com", 443),
        "path": "/bucket/normal.txt",
        "query_string": b"",
        "root_path": "",
        "headers": [(b"host", b"s3.hippius.com")],
    }
    call_next = AsyncMock(return_value=Response(status_code=200))
    response = await input_validation_middleware(Request(scope), call_next)

    assert response.status_code == 200
