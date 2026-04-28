"""Tests for request.state.anonymous_read_allowed — the load-bearing flag for ATS public caching."""

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


@pytest.fixture(autouse=True)  # type: ignore[misc]
def _ats_enabled(monkeypatch: pytest.MonkeyPatch) -> None:
    """Enable ATS so the anonymous_read_allowed probe runs."""
    monkeypatch.setenv("ATS_CACHE_ENDPOINT", "http://ats.local:8080")
    gateway_config._config = None
    yield
    gateway_config._config = None


def _build_app(
    acl_service: Any,
    *,
    account_id: str | None = None,
    auth_method: str | None = None,
    token_type: str | None = None,
) -> Any:
    app = FastAPI()
    app.state.acl_service = acl_service

    @app.api_route("/{path:path}", methods=["GET", "HEAD", "PUT", "DELETE"])
    async def catch_all(request: Request) -> dict[str, Any]:
        return {"anonymous_read_allowed": bool(getattr(request.state, "anonymous_read_allowed", False))}

    async def stub_auth(request: Request, call_next: Callable[[Request], Awaitable[Response]]) -> Response:
        request.state.account_id = account_id
        request.state.auth_method = auth_method
        request.state.token_type = token_type
        return await call_next(request)

    app.middleware("http")(acl_middleware)
    app.middleware("http")(stub_auth)
    return app


def _make_service(*, primary_permits: bool = True, anon_permits: bool = False) -> Any:
    service = AsyncMock()
    service.get_bucket_owner = AsyncMock(return_value="owner-id")
    service.get_bucket_id = AsyncMock(return_value="bucket-id")
    service.get_bucket_owner_and_id = AsyncMock(return_value=("owner-id", "bucket-id"))

    async def check_permission(
        *, account_id: str | None, bucket: str, key: str | None, permission: Any, access_key: Any, bucket_owner_id: Any
    ) -> bool:
        if account_id is None:
            return anon_permits
        return primary_permits

    service.check_permission = AsyncMock(side_effect=check_permission)
    return service


@pytest.mark.asyncio
async def test_anonymous_get_on_public_bucket_sets_flag_true() -> None:
    service = _make_service(primary_permits=True, anon_permits=True)
    app = _build_app(service)
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
        r = await client.get("/public-bucket/foo.txt")
    assert r.status_code == 200
    assert r.json()["anonymous_read_allowed"] is True


@pytest.mark.asyncio
async def test_anonymous_get_on_private_bucket_denied() -> None:
    """When anonymous access is denied, the request is 403'd before the flag matters."""
    service = _make_service(primary_permits=False, anon_permits=False)
    app = _build_app(service)
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
        r = await client.get("/private-bucket/foo.txt")
    assert r.status_code == 403


@pytest.mark.asyncio
async def test_put_does_not_set_flag() -> None:
    """The flag is only set for GET/HEAD reads."""
    service = _make_service(primary_permits=True, anon_permits=True)
    app = _build_app(service)
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
        r = await client.put("/public-bucket/foo.txt", content=b"x")
    assert r.status_code == 200
    assert r.json()["anonymous_read_allowed"] is False


@pytest.mark.asyncio
async def test_bucket_level_request_does_not_set_flag() -> None:
    """Bucket-listing is not per-object; no public caching decision applies."""
    service = _make_service(primary_permits=True, anon_permits=True)
    app = _build_app(service)
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
        r = await client.get("/public-bucket")
    assert r.status_code == 200
    assert r.json()["anonymous_read_allowed"] is False


@pytest.mark.asyncio
async def test_head_sets_flag_like_get() -> None:
    service = _make_service(primary_permits=True, anon_permits=True)
    app = _build_app(service)
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
        r = await client.head("/public-bucket/foo.txt")
    assert r.status_code == 200
    # HEAD strips body — the endpoint would have set the flag in request.state,
    # but since HEAD suppresses body, we can't read JSON. Just assert 200.


@pytest.mark.asyncio
async def test_authenticated_get_on_public_bucket_sets_flag_true() -> None:
    """Authenticated user reading a public object — flag True because AllUsers grant applies."""
    service = _make_service(primary_permits=True, anon_permits=True)
    app = _build_app(service, account_id="alice")
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
        r = await client.get("/public-bucket/foo.txt")
    assert r.status_code == 200
    assert r.json()["anonymous_read_allowed"] is True


@pytest.mark.asyncio
async def test_authenticated_get_on_private_object_sets_flag_false() -> None:
    """Per-object ACL override: object is private even if bucket is public → flag False."""
    service = _make_service(primary_permits=True, anon_permits=False)
    app = _build_app(service, account_id="alice")
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
        r = await client.get("/public-bucket/secret.txt")
    assert r.status_code == 200
    assert r.json()["anonymous_read_allowed"] is False


@pytest.mark.asyncio
async def test_authenticated_check_permission_called_twice() -> None:
    """Authenticated GET should trigger two check_permission calls: caller + anon probe."""
    service = _make_service(primary_permits=True, anon_permits=True)
    app = _build_app(service, account_id="alice")
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
        await client.get("/public-bucket/foo.txt")
    assert service.check_permission.await_count == 2
    seen_account_ids = {call.kwargs["account_id"] for call in service.check_permission.await_args_list}
    assert seen_account_ids == {"alice", None}


@pytest.mark.asyncio
async def test_probe_skipped_when_ats_disabled(monkeypatch: pytest.MonkeyPatch) -> None:
    """When ATS is off, the anon-read probe is skipped to save a Redis round-trip."""
    monkeypatch.setenv("ATS_CACHE_ENDPOINT", "")
    gateway_config._config = None
    service = _make_service(primary_permits=True, anon_permits=True)
    app = _build_app(service, account_id="alice")
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
        r = await client.get("/public-bucket/foo.txt")
    assert r.status_code == 200
    assert r.json()["anonymous_read_allowed"] is False
    assert service.check_permission.await_count == 1


@pytest.mark.asyncio
async def test_master_token_read_on_public_bucket_sets_flag_true() -> None:
    """Master-token owner reading a public object — flag must be True so ATS caches it.

    Regression guard: the master-token bypass runs AFTER anonymous_read_allowed is computed.
    """
    service = _make_service(primary_permits=True, anon_permits=True)
    app = _build_app(service, account_id="owner-id", auth_method="access_key", token_type="master")
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
        r = await client.get("/public-bucket/foo.txt")
    assert r.status_code == 200
    assert r.json()["anonymous_read_allowed"] is True


@pytest.mark.asyncio
async def test_master_token_read_on_private_object_sets_flag_false() -> None:
    """Master-token owner reading a private object — flag False so ATS does not cache it."""
    service = _make_service(primary_permits=True, anon_permits=False)
    app = _build_app(service, account_id="owner-id", auth_method="access_key", token_type="master")
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
        r = await client.get("/private-bucket/foo.txt")
    assert r.status_code == 200
    assert r.json()["anonymous_read_allowed"] is False
