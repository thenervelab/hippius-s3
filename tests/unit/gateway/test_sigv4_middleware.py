"""Smoke tests for the SigV4 path canonicalization helper and the auth router middleware."""

from typing import Any

import pytest
from fastapi import FastAPI
from httpx import ASGITransport
from httpx import AsyncClient
from starlette.requests import Request as StarletteRequest

from gateway.middlewares.sigv4 import canonical_path_from_scope


@pytest.fixture  # type: ignore[misc]
def auth_router_app() -> Any:
    from gateway.middlewares.auth_router import auth_router_middleware

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

    app.middleware("http")(auth_router_middleware)

    return app


def _make_request(path: str, raw_path: bytes | None = None) -> StarletteRequest:
    """
    Helper to construct a Starlette Request with a specific raw_path.

    This lets us assert how canonical_path_from_scope derives the canonical
    path used for signing, especially for keys containing spaces and other
    characters that require percent-encoding.
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
    # test both the raw_path and fallback code paths.
    if raw_path is not None:
        scope["raw_path"] = raw_path
    return StarletteRequest(scope)


@pytest.mark.asyncio
async def test_missing_auth_header_returns_403(auth_router_app: Any) -> None:
    async with AsyncClient(transport=ASGITransport(app=auth_router_app), base_url="http://test") as client:
        response = await client.put("/test", content=b"test data")

    # No Authorization header on a non-GET/HEAD request -> rejected before
    # any signature is even attempted, so the code is InvalidAccessKeyId
    # rather than SignatureDoesNotMatch.
    assert response.status_code == 403
    assert b"InvalidAccessKeyId" in response.content


@pytest.mark.asyncio
async def test_invalid_credential_format_returns_403(auth_router_app: Any) -> None:
    headers = {
        "authorization": "AWS4-HMAC-SHA256 Credential=not-a-hip-key/20240101/us-east-1/s3/aws4_request, "
        "SignedHeaders=host, Signature=deadbeef",
        "x-amz-date": "20240101T000000Z",
    }
    async with AsyncClient(transport=ASGITransport(app=auth_router_app), base_url="http://test") as client:
        response = await client.put("/test", content=b"test data", headers=headers)

    # Seed phrase / non-hip_ credentials are no longer supported.
    assert response.status_code == 403
    assert b"InvalidAccessKeyId" in response.content


@pytest.mark.asyncio
async def test_exempt_paths_bypass_auth(auth_router_app: Any) -> None:
    async with AsyncClient(transport=ASGITransport(app=auth_router_app), base_url="http://test") as client:
        response = await client.get("/health")

    assert response.status_code == 200


@pytest.mark.asyncio
async def test_options_request_bypasses_auth(auth_router_app: Any) -> None:
    async with AsyncClient(transport=ASGITransport(app=auth_router_app), base_url="http://test") as client:
        response = await client.options("/test")

    # Middleware bypasses auth for OPTIONS, but the endpoint doesn't support
    # OPTIONS, so we get 405 Method Not Allowed, not 403 Forbidden.
    assert response.status_code == 405


@pytest.mark.asyncio
async def test_anonymous_get_bypasses_auth(auth_router_app: Any) -> None:
    """GET/HEAD requests with no Authorization header are allowed as anonymous."""
    async with AsyncClient(transport=ASGITransport(app=auth_router_app), base_url="http://test") as client:
        response = await client.get("/test")

    assert response.status_code == 200


def test_canonical_path_uses_raw_path_when_available() -> None:
    """
    When raw_path is present in the ASGI scope, canonical_path_from_scope
    should use it verbatim (decoded from bytes) for the canonical path,
    preserving the client's percent-encoding of spaces and special
    characters.
    """
    logical_path = "/bucket/conflict65 (3).jpg"
    wire_path = b"/bucket/conflict65%20(3).jpg"

    request = _make_request(path=logical_path, raw_path=wire_path)

    assert canonical_path_from_scope(request) == "/bucket/conflict65%20(3).jpg"


def test_canonical_path_raises_when_raw_path_missing() -> None:
    """
    If raw_path is not available, canonical_path_from_scope should fail fast
    rather than attempting to re-encode the logical URL path, which could
    lead to subtle signature mismatches.
    """
    logical_path = "/bucket/conflict65 (3).jpg"

    request = _make_request(path=logical_path, raw_path=None)
    with pytest.raises(RuntimeError):
        canonical_path_from_scope(request)
