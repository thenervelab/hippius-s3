"""Integration tests for the complete authentication middleware chain"""

from typing import Any

import pytest
from fastapi import FastAPI
from fastapi import Request
from httpx import ASGITransport
from httpx import AsyncClient


@pytest.fixture  # type: ignore[misc]
def integration_app() -> Any:
    """
    Create a FastAPI app with the full auth middleware chain for smoke testing.
    """
    from gateway.middlewares.account import account_middleware
    from gateway.middlewares.auth_router import auth_router_middleware

    app = FastAPI()

    @app.get("/test")
    async def test_endpoint(request: Request) -> dict[str, Any]:
        auth_method = getattr(request.state, "auth_method", None)
        return {"auth_method": auth_method}

    @app.get("/health")
    async def health_endpoint() -> dict[str, str]:
        return {"status": "healthy"}

    app.middleware("http")(account_middleware)
    app.middleware("http")(auth_router_middleware)

    return app


@pytest.mark.asyncio
async def test_exempt_path_bypasses_auth(integration_app: Any) -> None:
    """Test that exempt paths like /health bypass all auth"""
    async with AsyncClient(transport=ASGITransport(app=integration_app), base_url="http://test") as client:
        response = await client.get("/health")

    assert response.status_code == 200
    data = response.json()
    assert data["status"] == "healthy"


@pytest.mark.asyncio
async def test_anonymous_get_request(integration_app: Any) -> None:
    """Test that GET requests without auth header are allowed as anonymous"""
    async with AsyncClient(transport=ASGITransport(app=integration_app), base_url="http://test") as client:
        response = await client.get("/test")

    assert response.status_code == 200
    data = response.json()
    assert data["auth_method"] == "anonymous"


@pytest.mark.asyncio
async def test_missing_auth_for_put_returns_403(integration_app: Any) -> None:
    """Test that PUT requests without auth header return 403"""
    async with AsyncClient(transport=ASGITransport(app=integration_app), base_url="http://test") as client:
        response = await client.put("/test", content=b"data")

    assert response.status_code == 403


@pytest.mark.asyncio
async def test_invalid_auth_header_returns_403(integration_app: Any) -> None:
    """Test that invalid auth header format returns 403"""
    async with AsyncClient(transport=ASGITransport(app=integration_app), base_url="http://test") as client:
        response = await client.put(
            "/test",
            headers={"Authorization": "InvalidFormat"},
            content=b"data",
        )

    assert response.status_code == 403
