"""Tests for request.state.bucket_is_cache_warm — the flag cache_control_middleware reads.

Pinned to acl_middleware so we know the warm flag from the BucketLookup makes it onto
request.state for every request shape (existing bucket / missing bucket / non-warm bucket).
"""

from typing import Any
from typing import Awaitable
from typing import Callable
from unittest.mock import AsyncMock

import pytest
from fastapi import FastAPI
from fastapi import Request
from fastapi import Response
from httpx import ASGITransport
from httpx import AsyncClient

from gateway import config as gateway_config
from gateway.middlewares.acl import acl_middleware
from gateway.services.acl_service import BucketLookup


@pytest.fixture(autouse=True)  # type: ignore[misc]
def _ats_enabled(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("ATS_CACHE_ENDPOINT", "http://ats.local:8080")
    gateway_config._config = None
    yield
    gateway_config._config = None


def _build_app(acl_service: Any, *, account_id: str | None = None) -> Any:
    app = FastAPI()
    app.state.acl_service = acl_service

    @app.api_route("/{path:path}", methods=["GET", "HEAD", "PUT", "DELETE"])
    async def catch_all(request: Request) -> dict[str, Any]:
        return {
            "bucket_is_cache_warm": bool(getattr(request.state, "bucket_is_cache_warm", False)),
            "bucket_is_cache_warm_set": hasattr(request.state, "bucket_is_cache_warm"),
        }

    async def stub_auth(request: Request, call_next: Callable[[Request], Awaitable[Response]]) -> Response:
        request.state.account_id = account_id
        request.state.auth_method = None
        request.state.token_type = None
        return await call_next(request)

    app.middleware("http")(acl_middleware)
    app.middleware("http")(stub_auth)
    return app


def _make_service(*, lookup: BucketLookup | None, primary_permits: bool = True, anon_permits: bool = True) -> Any:
    service = AsyncMock()
    service.get_bucket_owner_and_id = AsyncMock(return_value=lookup)

    async def check_permission(
        *, account_id: str | None, bucket: str, key: str | None, permission: Any, access_key: Any, bucket_owner_id: Any
    ) -> bool:
        if account_id is None:
            return anon_permits
        return primary_permits

    service.check_permission = AsyncMock(side_effect=check_permission)
    return service


@pytest.mark.asyncio
async def test_warm_bucket_sets_flag_true_on_request_state() -> None:
    lookup = BucketLookup(owner_id="owner-1", bucket_id="b-1", is_cache_warm=True)
    app = _build_app(_make_service(lookup=lookup))
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
        r = await client.get("/warm-bucket/foo.txt")
    assert r.status_code == 200
    assert r.json()["bucket_is_cache_warm"] is True


@pytest.mark.asyncio
async def test_cold_bucket_sets_flag_false_on_request_state() -> None:
    lookup = BucketLookup(owner_id="owner-1", bucket_id="b-1", is_cache_warm=False)
    app = _build_app(_make_service(lookup=lookup))
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
        r = await client.get("/cold-bucket/foo.txt")
    assert r.status_code == 200
    assert r.json()["bucket_is_cache_warm"] is False
    assert r.json()["bucket_is_cache_warm_set"] is True


@pytest.mark.asyncio
async def test_missing_bucket_sets_flag_false_on_request_state() -> None:
    """Bucket not found in DB → flag explicitly False (so cache_control_middleware doesn't see stale state)."""
    app = _build_app(_make_service(lookup=None, primary_permits=False, anon_permits=False))
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
        r = await client.get("/missing-bucket/foo.txt")
    # ACL middleware passes through to backend on missing bucket — handler runs and reports state
    assert r.json()["bucket_is_cache_warm"] is False
    assert r.json()["bucket_is_cache_warm_set"] is True


@pytest.mark.asyncio
async def test_warm_bucket_head_request_sets_flag() -> None:
    """HEAD requests must wire the warm flag too — they're cacheable just like GET."""
    lookup = BucketLookup(owner_id="owner-1", bucket_id="b-1", is_cache_warm=True)
    captured: list[bool] = []

    app = FastAPI()
    app.state.acl_service = _make_service(lookup=lookup)

    @app.api_route("/{path:path}", methods=["HEAD"])
    async def head_handler(request: Request) -> Response:
        captured.append(bool(getattr(request.state, "bucket_is_cache_warm", False)))
        return Response(status_code=200)

    async def stub_auth(request: Request, call_next: Callable[[Request], Awaitable[Response]]) -> Response:
        request.state.account_id = None
        request.state.auth_method = None
        request.state.token_type = None
        return await call_next(request)

    app.middleware("http")(acl_middleware)
    app.middleware("http")(stub_auth)

    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
        r = await client.head("/warm-bucket/foo.txt")
    assert r.status_code == 200
    assert captured == [True]


@pytest.mark.asyncio
async def test_warm_flag_does_not_leak_between_requests() -> None:
    """Each request must compute its own flag — no cross-request state leakage."""
    warm_lookup = BucketLookup(owner_id="owner-1", bucket_id="b-1", is_cache_warm=True)
    cold_lookup = BucketLookup(owner_id="owner-1", bucket_id="b-2", is_cache_warm=False)

    service = AsyncMock()
    call_log: list[str] = []

    async def lookup_dispatch(bucket: str) -> BucketLookup:
        call_log.append(bucket)
        return warm_lookup if bucket == "warm" else cold_lookup

    service.get_bucket_owner_and_id = AsyncMock(side_effect=lookup_dispatch)
    service.check_permission = AsyncMock(return_value=True)

    app = _build_app(service)

    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
        warm_resp = await client.get("/warm/foo.txt")
        cold_resp = await client.get("/cold/foo.txt")
        warm_resp_2 = await client.get("/warm/bar.txt")

    assert warm_resp.json()["bucket_is_cache_warm"] is True
    assert cold_resp.json()["bucket_is_cache_warm"] is False
    assert warm_resp_2.json()["bucket_is_cache_warm"] is True
    assert call_log == ["warm", "cold", "warm"]


@pytest.mark.asyncio
async def test_warm_flag_set_on_write_requests_too() -> None:
    """ats_purge_middleware doesn't read bucket_is_cache_warm, but acl_middleware should still
    populate it so that any future read-side downstream middleware doesn't see undefined state."""
    lookup = BucketLookup(owner_id="owner-1", bucket_id="b-1", is_cache_warm=True)
    app = _build_app(_make_service(lookup=lookup), account_id="owner-1")
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
        r = await client.put("/warm-bucket/foo.txt", content=b"x")
    assert r.status_code == 200
    assert r.json()["bucket_is_cache_warm"] is True


@pytest.mark.asyncio
async def test_copy_source_warm_flag_does_not_leak_to_destination() -> None:
    """Invariant: when COPY source bucket is warm but destination is cold, request.state.bucket_is_cache_warm
    must reflect the DESTINATION (cold) — never the source. Pins against future refactors that might
    accidentally overwrite the destination flag with the source lookup result."""
    dest_lookup = BucketLookup(owner_id="owner-1", bucket_id="dest-id", is_cache_warm=False)
    src_lookup = BucketLookup(owner_id="owner-1", bucket_id="src-id", is_cache_warm=True)

    service = AsyncMock()

    async def lookup_dispatch(bucket: str) -> BucketLookup:
        return dest_lookup if bucket == "dest-bucket" else src_lookup

    service.get_bucket_owner_and_id = AsyncMock(side_effect=lookup_dispatch)
    service.check_permission = AsyncMock(return_value=True)

    app = _build_app(service, account_id="owner-1")

    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
        r = await client.put(
            "/dest-bucket/dest-key",
            content=b"x",
            headers={"x-amz-copy-source": "/src-bucket/src-key"},
        )
    assert r.status_code == 200
    # Destination is cold; warm source must NOT leak.
    assert r.json()["bucket_is_cache_warm"] is False
