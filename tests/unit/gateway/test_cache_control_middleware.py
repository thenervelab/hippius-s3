"""Tests for cache_control_middleware — Cache-Control header injection by method/status/ACL."""

from typing import Any

import pytest
from fastapi import FastAPI
from fastapi import Request
from fastapi import Response
from httpx import ASGITransport
from httpx import AsyncClient

from gateway.config import get_config
from gateway.middlewares.cache_control import PRIVATE_CACHE_CONTROL
from gateway.middlewares.cache_control import REVALIDATE_ALWAYS
from gateway.middlewares.cache_control import STANDARD_PUBLIC
from gateway.middlewares.cache_control import cache_control_middleware


@pytest.fixture  # type: ignore[misc]
def app() -> Any:
    app = FastAPI()
    app.middleware("http")(cache_control_middleware)

    @app.api_route("/{path:path}", methods=["GET", "POST", "PUT", "DELETE", "HEAD", "PATCH"])
    async def catch_all(request: Request) -> Response:
        status = int(request.headers.get("x-test-status", "200"))
        # Simulate acl_middleware wiring
        if request.headers.get("x-test-anon-read") == "true":
            request.state.anonymous_read_allowed = True
        else:
            request.state.anonymous_read_allowed = False
        return Response(status_code=status, content=b"ok")

    return app


@pytest.mark.asyncio
async def test_private_bucket_gets_no_store(app: Any) -> None:
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
        r = await client.get("/private-bucket/foo.txt")
    assert r.headers["Cache-Control"] == PRIVATE_CACHE_CONTROL


@pytest.mark.asyncio
async def test_public_bucket_default_is_revalidate_always(app: Any) -> None:
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
        r = await client.get("/public-bucket/foo.txt", headers={"x-test-anon-read": "true"})
    assert r.headers["Cache-Control"] == REVALIDATE_ALWAYS


@pytest.mark.asyncio
async def test_offload_bucket_gets_standard_public(app: Any, monkeypatch: pytest.MonkeyPatch) -> None:
    get_config().ats_cache_offload_buckets.add("assets")
    try:
        async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
            r = await client.get("/assets/icon.png", headers={"x-test-anon-read": "true"})
        assert r.headers["Cache-Control"] == STANDARD_PUBLIC
    finally:
        get_config().ats_cache_offload_buckets.discard("assets")


@pytest.mark.asyncio
async def test_head_public_bucket_gets_revalidate_always(app: Any) -> None:
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
        r = await client.head("/public-bucket/foo.txt", headers={"x-test-anon-read": "true"})
    assert r.headers["Cache-Control"] == REVALIDATE_ALWAYS


@pytest.mark.asyncio
async def test_put_gets_no_cache_control_header(app: Any) -> None:
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
        r = await client.put("/public-bucket/foo.txt", content=b"x", headers={"x-test-anon-read": "true"})
    assert "Cache-Control" not in r.headers


@pytest.mark.asyncio
async def test_delete_gets_no_cache_control_header(app: Any) -> None:
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
        r = await client.delete("/public-bucket/foo.txt", headers={"x-test-anon-read": "true"})
    assert "Cache-Control" not in r.headers


@pytest.mark.asyncio
async def test_error_responses_get_no_cache_control(app: Any) -> None:
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
        r = await client.get(
            "/public-bucket/foo.txt",
            headers={"x-test-status": "500", "x-test-anon-read": "true"},
        )
    assert "Cache-Control" not in r.headers


@pytest.mark.asyncio
async def test_304_response_still_gets_cache_control(app: Any) -> None:
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
        r = await client.get(
            "/public-bucket/foo.txt",
            headers={"x-test-status": "304", "x-test-anon-read": "true"},
        )
    assert r.status_code == 304
    assert r.headers["Cache-Control"] == REVALIDATE_ALWAYS


@pytest.mark.asyncio
async def test_partial_content_206_gets_cache_control(app: Any) -> None:
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
        r = await client.get(
            "/public-bucket/foo.txt",
            headers={"x-test-status": "206", "x-test-anon-read": "true"},
        )
    assert r.status_code == 206
    assert r.headers["Cache-Control"] == REVALIDATE_ALWAYS


@pytest.mark.asyncio
async def test_bucket_listing_gets_private(app: Any) -> None:
    """GET on a bucket (no key) should never be marked publicly cacheable."""
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
        r = await client.get("/public-bucket", headers={"x-test-anon-read": "true"})
    assert r.headers["Cache-Control"] == PRIVATE_CACHE_CONTROL


@pytest.mark.asyncio
async def test_missing_flag_defaults_to_private(app: Any) -> None:
    """If acl middleware didn't wire the flag (e.g., request was a healthcheck), default private."""
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
        r = await client.get("/public-bucket/foo.txt")  # no x-test-anon-read header
    assert r.headers["Cache-Control"] == PRIVATE_CACHE_CONTROL
