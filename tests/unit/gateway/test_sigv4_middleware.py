"""Smoke tests for SigV4 verification middleware."""

from typing import Any

import pytest
from fastapi import FastAPI
from httpx import ASGITransport
from httpx import AsyncClient
from starlette.requests import Request as StarletteRequest

from gateway.middlewares.sigv4 import SigV4Verifier


@pytest.fixture  # type: ignore[misc]
def sigv4_app() -> Any:
    from gateway.middlewares.sigv4 import sigv4_middleware

    app = FastAPI()

    @app.get("/test")
    async def test_endpoint() -> dict[str, str]:
        return {"message": "ok"}

    @app.put("/test")
    async def put_test_endpoint() -> dict[str, str]:
        return {"message": "ok"}

    @app.get("/health")
    async def health_endpoint() -> dict[str, str]:
        return {"status": "healthy"}

    app.middleware("http")(sigv4_middleware)

    return app


def _make_request(path: str, raw_path: bytes | None = None) -> StarletteRequest:
    """
    Helper to construct a Starlette Request with a specific raw_path.

    This lets us assert how SigV4Verifier derives the canonical path used
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
    # test both the raw_path and fallback code paths in SigV4Verifier.
    if raw_path is not None:
        scope["raw_path"] = raw_path
    return StarletteRequest(scope)


@pytest.mark.asyncio
async def test_missing_auth_header_returns_403(sigv4_app: Any) -> None:
    async with AsyncClient(transport=ASGITransport(app=sigv4_app), base_url="http://test") as client:
        response = await client.put("/test", content=b"test data")

    assert response.status_code == 403
    assert b"SignatureDoesNotMatch" in response.content


@pytest.mark.asyncio
async def test_exempt_paths_bypass_auth(sigv4_app: Any) -> None:
    async with AsyncClient(transport=ASGITransport(app=sigv4_app), base_url="http://test") as client:
        response = await client.get("/health")

    assert response.status_code == 200


@pytest.mark.asyncio
async def test_options_request_bypasses_auth(sigv4_app: Any) -> None:
    async with AsyncClient(transport=ASGITransport(app=sigv4_app), base_url="http://test") as client:
        response = await client.options("/test")

    # Middleware bypasses auth for OPTIONS, but endpoint doesn't support OPTIONS method
    # So we get 405 Method Not Allowed, not 403 Forbidden
    assert response.status_code == 405


def test_sigv4_verifier_uses_raw_path_when_available() -> None:
    """
    When raw_path is present in the ASGI scope, SigV4Verifier should use it
    verbatim (decoded from bytes) for the canonical path, preserving the
    client's percent-encoding of spaces and special characters.
    """
    logical_path = "/bucket/conflict65 (3).jpg"
    wire_path = b"/bucket/conflict65%20(3).jpg"

    request = _make_request(path=logical_path, raw_path=wire_path)
    verifier = SigV4Verifier(request)

    assert verifier.path == "/bucket/conflict65%20(3).jpg"


def test_sigv4_verifier_raises_when_raw_path_missing() -> None:
    """
    If raw_path is not available, SigV4Verifier should fail fast rather than
    attempting to re-encode the logical URL path, which could lead to subtle
    signature mismatches.
    """
    logical_path = "/bucket/conflict65 (3).jpg"

    request = _make_request(path=logical_path, raw_path=None)
    with pytest.raises(RuntimeError):
        SigV4Verifier(request)
