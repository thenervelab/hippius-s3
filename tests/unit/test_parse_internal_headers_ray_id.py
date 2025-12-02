from typing import Any

import pytest
from fastapi import FastAPI
from fastapi import Request
from httpx import ASGITransport
from httpx import AsyncClient


@pytest.fixture
def api_app() -> Any:
    from hippius_s3.api.middlewares.parse_internal_headers import parse_internal_headers_middleware

    app = FastAPI()

    @app.get("/test")
    async def test_endpoint(request: Request) -> dict[str, Any]:
        return {
            "message": "ok",
            "ray_id": getattr(request.state, "ray_id", "not-set"),
            "has_logger": hasattr(request.state, "logger"),
            "account_id": getattr(request.state, "account_id", "not-set"),
        }

    app.middleware("http")(parse_internal_headers_middleware)

    return app


@pytest.mark.asyncio
async def test_parse_internal_headers_extracts_ray_id(api_app: Any) -> None:
    async with AsyncClient(transport=ASGITransport(app=api_app), base_url="http://test") as client:
        response = await client.get("/test", headers={"X-Hippius-Ray-ID": "a1b2c3d4e5f67890"})

    assert response.status_code == 200
    data = response.json()
    assert data["ray_id"] == "a1b2c3d4e5f67890"


@pytest.mark.asyncio
async def test_parse_internal_headers_defaults_ray_id(api_app: Any) -> None:
    async with AsyncClient(transport=ASGITransport(app=api_app), base_url="http://test") as client:
        response = await client.get("/test")

    assert response.status_code == 200
    data = response.json()
    assert data["ray_id"] == "no-ray-id"


@pytest.mark.asyncio
async def test_parse_internal_headers_sets_logger(api_app: Any) -> None:
    async with AsyncClient(transport=ASGITransport(app=api_app), base_url="http://test") as client:
        response = await client.get("/test", headers={"X-Hippius-Ray-ID": "a1b2c3d4e5f67890"})

    assert response.status_code == 200
    data = response.json()
    assert data["has_logger"] is True


@pytest.mark.asyncio
async def test_parse_internal_headers_preserves_other_headers(api_app: Any) -> None:
    async with AsyncClient(transport=ASGITransport(app=api_app), base_url="http://test") as client:
        response = await client.get(
            "/test",
            headers={
                "X-Hippius-Ray-ID": "a1b2c3d4e5f67890",
                "X-Hippius-Request-User": "test-account-123",
            },
        )

    assert response.status_code == 200
    data = response.json()
    assert data["ray_id"] == "a1b2c3d4e5f67890"
    assert data["account_id"] == "test-account-123"
