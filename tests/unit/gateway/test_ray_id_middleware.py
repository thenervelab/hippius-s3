import re
from typing import Any

import pytest
from fastapi import FastAPI
from fastapi import Request
from httpx import ASGITransport
from httpx import AsyncClient


@pytest.fixture
def ray_id_app() -> Any:
    from gateway.middlewares.ray_id import ray_id_middleware

    app = FastAPI()

    @app.get("/test")
    async def test_endpoint(request: Request) -> dict[str, Any]:
        return {
            "message": "ok",
            "ray_id": getattr(request.state, "ray_id", "not-set"),
            "has_logger": hasattr(request.state, "logger"),
        }

    app.middleware("http")(ray_id_middleware)

    return app


@pytest.mark.asyncio
async def test_ray_id_middleware_generates_ray_id(ray_id_app: Any) -> None:
    async with AsyncClient(transport=ASGITransport(app=ray_id_app), base_url="http://test") as client:
        response = await client.get("/test")

    assert response.status_code == 200
    data = response.json()
    assert data["ray_id"] != "not-set"
    assert len(data["ray_id"]) == 16


@pytest.mark.asyncio
async def test_ray_id_middleware_sets_request_state(ray_id_app: Any) -> None:
    async with AsyncClient(transport=ASGITransport(app=ray_id_app), base_url="http://test") as client:
        response = await client.get("/test")

    assert response.status_code == 200
    data = response.json()
    assert data["ray_id"] != "not-set"
    assert data["has_logger"] is True


@pytest.mark.asyncio
async def test_ray_id_middleware_adds_response_header(ray_id_app: Any) -> None:
    async with AsyncClient(transport=ASGITransport(app=ray_id_app), base_url="http://test") as client:
        response = await client.get("/test")

    assert response.status_code == 200
    assert "X-Hippius-Ray-ID" in response.headers
    ray_id = response.headers["X-Hippius-Ray-ID"]
    assert len(ray_id) == 16
    assert re.match(r"^[0-9a-f]{16}$", ray_id), f"Ray ID should be 16 hex chars, got: {ray_id}"


@pytest.mark.asyncio
async def test_ray_id_middleware_ray_id_format(ray_id_app: Any) -> None:
    async with AsyncClient(transport=ASGITransport(app=ray_id_app), base_url="http://test") as client:
        response = await client.get("/test")

    assert response.status_code == 200
    data = response.json()
    ray_id = data["ray_id"]
    assert isinstance(ray_id, str)
    assert len(ray_id) == 16
    assert ray_id == ray_id.lower()
    assert re.match(r"^[0-9a-f]{16}$", ray_id)


@pytest.mark.asyncio
async def test_ray_id_middleware_unique_per_request(ray_id_app: Any) -> None:
    ray_ids = set()

    async with AsyncClient(transport=ASGITransport(app=ray_id_app), base_url="http://test") as client:
        for _ in range(10):
            response = await client.get("/test")
            assert response.status_code == 200
            data = response.json()
            ray_ids.add(data["ray_id"])

    assert len(ray_ids) == 10, "Ray IDs should be unique per request"


@pytest.mark.asyncio
async def test_ray_id_middleware_response_header_matches_state(ray_id_app: Any) -> None:
    async with AsyncClient(transport=ASGITransport(app=ray_id_app), base_url="http://test") as client:
        response = await client.get("/test")

    assert response.status_code == 200
    data = response.json()
    state_ray_id = data["ray_id"]
    header_ray_id = response.headers["X-Hippius-Ray-ID"]

    assert state_ray_id == header_ray_id, "Ray ID in state should match header"
