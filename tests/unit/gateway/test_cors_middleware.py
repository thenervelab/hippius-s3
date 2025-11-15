"""Smoke tests for CORS middleware."""

from typing import Any

import pytest
from fastapi import FastAPI
from httpx import ASGITransport
from httpx import AsyncClient


@pytest.fixture  # type: ignore[misc]
def cors_app() -> Any:
    from gateway.middlewares.cors import cors_middleware

    app = FastAPI()

    @app.get("/test")
    async def test_endpoint() -> dict[str, str]:
        return {"message": "ok"}

    app.middleware("http")(cors_middleware)

    return app


@pytest.mark.asyncio
async def test_options_request_returns_204(cors_app: Any) -> None:
    async with AsyncClient(transport=ASGITransport(app=cors_app), base_url="http://test") as client:
        response = await client.options("/test")

    assert response.status_code == 204


@pytest.mark.asyncio
async def test_cors_headers_added_to_response(cors_app: Any) -> None:
    async with AsyncClient(transport=ASGITransport(app=cors_app), base_url="http://test") as client:
        response = await client.get("/test")

    assert response.status_code == 200
    assert response.headers["Access-Control-Allow-Origin"] == "*"
    assert "Access-Control-Allow-Methods" in response.headers
    assert "Access-Control-Allow-Headers" in response.headers


@pytest.mark.asyncio
async def test_security_headers_added(cors_app: Any) -> None:
    async with AsyncClient(transport=ASGITransport(app=cors_app), base_url="http://test") as client:
        response = await client.get("/test")

    assert response.headers["X-Robots-Tag"] == "noindex, nofollow, nosnippet, noarchive"
    assert response.headers["X-Content-Type-Options"] == "nosniff"
    assert response.headers["X-XSS-Protection"] == "1; mode=block"
    assert "Strict-Transport-Security" in response.headers
