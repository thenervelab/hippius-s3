from typing import Any

import pytest
from fastapi import FastAPI
from fastapi import Request
from fastapi import Response
from httpx import ASGITransport
from httpx import AsyncClient

from gateway.middlewares.read_only import read_only_middleware


@pytest.fixture  # type: ignore[misc]
def read_only_app() -> Any:
    app = FastAPI()
    app.middleware("http")(read_only_middleware)

    @app.get("/health")
    async def health() -> dict[str, str]:
        return {"status": "healthy"}

    @app.api_route("/{path:path}", methods=["GET", "POST", "PUT", "DELETE", "HEAD", "PATCH", "OPTIONS"])
    async def catch_all(request: Request) -> Response:
        return Response(status_code=200, content="ok")

    return app


@pytest.mark.asyncio
async def test_get_request_allowed(read_only_app: Any) -> None:
    async with AsyncClient(transport=ASGITransport(app=read_only_app), base_url="http://test") as client:
        response = await client.get("/my-bucket/my-key")
    assert response.status_code == 200


@pytest.mark.asyncio
async def test_head_request_allowed(read_only_app: Any) -> None:
    async with AsyncClient(transport=ASGITransport(app=read_only_app), base_url="http://test") as client:
        response = await client.head("/my-bucket/my-key")
    assert response.status_code == 200


@pytest.mark.asyncio
async def test_options_request_allowed(read_only_app: Any) -> None:
    async with AsyncClient(transport=ASGITransport(app=read_only_app), base_url="http://test") as client:
        response = await client.options("/my-bucket")
    assert response.status_code == 200


@pytest.mark.asyncio
async def test_put_request_rejected(read_only_app: Any) -> None:
    async with AsyncClient(transport=ASGITransport(app=read_only_app), base_url="http://test") as client:
        response = await client.put("/my-bucket/my-key", content=b"data")
    assert response.status_code == 405
    assert b"MethodNotAllowed" in response.content
    assert b"read-only" in response.content


@pytest.mark.asyncio
async def test_post_request_rejected(read_only_app: Any) -> None:
    async with AsyncClient(transport=ASGITransport(app=read_only_app), base_url="http://test") as client:
        response = await client.post("/my-bucket?delete", content=b"<Delete/>")
    assert response.status_code == 405


@pytest.mark.asyncio
async def test_delete_request_rejected(read_only_app: Any) -> None:
    async with AsyncClient(transport=ASGITransport(app=read_only_app), base_url="http://test") as client:
        response = await client.delete("/my-bucket/my-key")
    assert response.status_code == 405


@pytest.mark.asyncio
async def test_patch_request_rejected(read_only_app: Any) -> None:
    async with AsyncClient(transport=ASGITransport(app=read_only_app), base_url="http://test") as client:
        response = await client.patch("/my-bucket/my-key", content=b"data")
    assert response.status_code == 405


@pytest.mark.asyncio
async def test_health_endpoint_always_allowed(read_only_app: Any) -> None:
    async with AsyncClient(transport=ASGITransport(app=read_only_app), base_url="http://test") as client:
        response = await client.get("/health")
    assert response.status_code == 200


@pytest.mark.asyncio
async def test_rejected_response_is_xml(read_only_app: Any) -> None:
    async with AsyncClient(transport=ASGITransport(app=read_only_app), base_url="http://test") as client:
        response = await client.put("/my-bucket", content=b"data")
    assert response.status_code == 405
    assert response.headers["content-type"] == "application/xml"


@pytest.mark.asyncio
async def test_list_objects_get_allowed(read_only_app: Any) -> None:
    async with AsyncClient(transport=ASGITransport(app=read_only_app), base_url="http://test") as client:
        response = await client.get("/my-bucket?list-type=2&prefix=photos/")
    assert response.status_code == 200
