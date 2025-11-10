"""Smoke tests for SigV4 verification middleware."""

from typing import Any

import pytest
from fastapi import FastAPI
from httpx import ASGITransport
from httpx import AsyncClient


@pytest.fixture  # type: ignore[misc]
def sigv4_app() -> Any:
    from gateway.middlewares.sigv4 import sigv4_middleware

    app = FastAPI()

    @app.get("/test")
    async def test_endpoint() -> dict[str, str]:
        return {"message": "ok"}

    @app.get("/health")
    async def health_endpoint() -> dict[str, str]:
        return {"status": "healthy"}

    app.middleware("http")(sigv4_middleware)

    return app


@pytest.mark.asyncio
async def test_missing_auth_header_returns_403(sigv4_app: Any) -> None:
    async with AsyncClient(transport=ASGITransport(app=sigv4_app), base_url="http://test") as client:
        response = await client.get("/test")

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
