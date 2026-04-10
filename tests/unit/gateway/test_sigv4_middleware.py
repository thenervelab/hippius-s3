"""Tests for SigV4 utility functions."""

from typing import Any

import pytest
from starlette.requests import Request as StarletteRequest

from gateway.middlewares.sigv4 import canonical_path_from_scope


def _make_request(path: str, raw_path: bytes | None = None) -> StarletteRequest:
    """
    Helper to construct a Starlette Request with a specific raw_path.

    This lets us assert how canonical_path_from_scope derives the canonical path used
    for signing, especially for keys containing spaces and other characters
    that require percent-encoding.
    """
    scope: dict[str, Any] = {
        "type": "http",
        "method": "PUT",
        "path": path,
        "headers": [],
        "query_string": b"",
        "server": ("testserver", 80),
        "scheme": "http",
    }
    # Only include raw_path in the scope when explicitly provided so we can
    # test both the raw_path and fallback code paths in canonical_path_from_scope.
    if raw_path is not None:
        scope["raw_path"] = raw_path
    return StarletteRequest(scope)


def test_canonical_path_uses_raw_path_when_available() -> None:
    """
    When raw_path is present in the ASGI scope, canonical_path_from_scope should use it
    verbatim (decoded from bytes) for the canonical path, preserving the
    client's percent-encoding of spaces and special characters.
    """
    logical_path = "/bucket/conflict65 (3).jpg"
    wire_path = b"/bucket/conflict65%20(3).jpg"

    request = _make_request(path=logical_path, raw_path=wire_path)
    result = canonical_path_from_scope(request)

    assert result == "/bucket/conflict65%20(3).jpg"


def test_canonical_path_raises_when_raw_path_missing() -> None:
    """
    If raw_path is not available, canonical_path_from_scope should fail fast rather than
    attempting to re-encode the logical URL path, which could lead to subtle
    signature mismatches.
    """
    logical_path = "/bucket/conflict65 (3).jpg"

    request = _make_request(path=logical_path, raw_path=None)
    with pytest.raises(RuntimeError):
        canonical_path_from_scope(request)
