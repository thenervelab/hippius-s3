"""Integration: full gateway middleware stack (ACL + cache-control + ats-purge)."""

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
from gateway.middlewares.ats_purge import ats_purge_middleware
from gateway.middlewares.cache_control import PRIVATE_CACHE_CONTROL
from gateway.middlewares.cache_control import PUBLIC_CACHE_CONTROL
from gateway.middlewares.cache_control import cache_control_middleware


@pytest.fixture(autouse=True)  # type: ignore[misc]
def _ats_enabled(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("ATS_CACHE_ENDPOINT", "http://ats.local:8080")
    gateway_config._config = None
    yield
    gateway_config._config = None


def _make_service(*, primary: bool = True, anon: bool = False) -> Any:
    service = AsyncMock()
    service.get_bucket_owner = AsyncMock(return_value="owner-id")
    service.get_bucket_id = AsyncMock(return_value="bucket-id")
    service.get_bucket_owner_and_id = AsyncMock(return_value=("owner-id", "bucket-id"))

    async def check_permission(
        *, account_id: str | None, bucket: str, key: str | None, permission: Any, access_key: Any, bucket_owner_id: Any
    ) -> bool:
        if account_id is None:
            return anon
        return primary

    service.check_permission = AsyncMock(side_effect=check_permission)
    return service


def _build_app(
    acl_service: Any,
    *,
    captured_purges: list[tuple[str, str]],
    account_id: str | None = None,
    monkeypatch: pytest.MonkeyPatch,
) -> Any:
    app = FastAPI()
    app.state.acl_service = acl_service

    @app.api_route("/{path:path}", methods=["GET", "HEAD", "PUT", "POST", "DELETE"])
    async def catch_all(request: Request) -> Response:
        return Response(status_code=200, content=b"ok")

    def fake_schedule_purge(host: str, key: str) -> None:
        captured_purges.append((host, key))

    monkeypatch.setattr("gateway.middlewares.ats_purge.schedule_purge", fake_schedule_purge)

    async def stub_auth(request: Request, call_next: Callable[[Request], Awaitable[Response]]) -> Response:
        request.state.account_id = account_id
        return await call_next(request)

    # Register inner-to-outer — last registered is outermost
    app.middleware("http")(ats_purge_middleware)
    app.middleware("http")(cache_control_middleware)
    app.middleware("http")(acl_middleware)
    app.middleware("http")(stub_auth)

    return app


@pytest.mark.asyncio
async def test_anon_get_public_bucket_emits_public_cache_control(monkeypatch: pytest.MonkeyPatch) -> None:
    service = _make_service(primary=True, anon=True)
    purges: list[tuple[str, str]] = []
    app = _build_app(service, captured_purges=purges, monkeypatch=monkeypatch)
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://s3.hippius.com") as client:
        r = await client.get("/public-bucket/foo.txt")
    assert r.status_code == 200
    assert r.headers["Cache-Control"] == PUBLIC_CACHE_CONTROL
    assert purges == []


@pytest.mark.asyncio
async def test_anon_get_private_bucket_denied_no_cache_header(monkeypatch: pytest.MonkeyPatch) -> None:
    service = _make_service(primary=False, anon=False)
    purges: list[tuple[str, str]] = []
    app = _build_app(service, captured_purges=purges, monkeypatch=monkeypatch)
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://s3.hippius.com") as client:
        r = await client.get("/private-bucket/foo.txt")
    assert r.status_code == 403
    assert "Cache-Control" not in r.headers
    assert purges == []


@pytest.mark.asyncio
async def test_authenticated_get_private_object_emits_private(monkeypatch: pytest.MonkeyPatch) -> None:
    service = _make_service(primary=True, anon=False)
    purges: list[tuple[str, str]] = []
    app = _build_app(service, captured_purges=purges, account_id="alice", monkeypatch=monkeypatch)
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://s3.hippius.com") as client:
        r = await client.get("/private-bucket/foo.txt")
    assert r.status_code == 200
    assert r.headers["Cache-Control"] == PRIVATE_CACHE_CONTROL
    assert purges == []


@pytest.mark.asyncio
async def test_put_fires_purge_and_does_not_set_cache_control(monkeypatch: pytest.MonkeyPatch) -> None:
    service = _make_service(primary=True, anon=False)
    purges: list[tuple[str, str]] = []
    app = _build_app(service, captured_purges=purges, account_id="alice", monkeypatch=monkeypatch)
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://s3.hippius.com") as client:
        r = await client.put("/public-bucket/foo.txt", content=b"data")
    assert r.status_code == 200
    assert "Cache-Control" not in r.headers
    assert purges == [("s3.hippius.com", "public-bucket/foo.txt")]


@pytest.mark.asyncio
async def test_authenticated_denied_no_purge_no_cache_header(monkeypatch: pytest.MonkeyPatch) -> None:
    service = _make_service(primary=False, anon=False)
    purges: list[tuple[str, str]] = []
    app = _build_app(service, captured_purges=purges, account_id="alice", monkeypatch=monkeypatch)
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://s3.hippius.com") as client:
        r = await client.put("/public-bucket/foo.txt", content=b"data")
    assert r.status_code == 403
    assert purges == []
    assert "Cache-Control" not in r.headers


@pytest.mark.asyncio
async def test_bucket_acl_flip_does_not_fire_purge(monkeypatch: pytest.MonkeyPatch) -> None:
    """Stock ATS HTTP PURGE is single-key only; bucket-level invalidation is intentionally unsupported."""
    service = _make_service(primary=True, anon=False)
    purges: list[tuple[str, str]] = []
    app = _build_app(service, captured_purges=purges, account_id="alice", monkeypatch=monkeypatch)
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://s3.hippius.com") as client:
        r = await client.put("/public-bucket?acl", content=b"<AccessControlPolicy/>")
    assert r.status_code == 200
    assert purges == []


@pytest.mark.asyncio
async def test_object_acl_put_fires_single_key_purge(monkeypatch: pytest.MonkeyPatch) -> None:
    """PUT /{bucket}/{key}?acl changes object ACL — only that key's cache needs invalidating."""
    service = _make_service(primary=True, anon=False)
    purges: list[tuple[str, str]] = []
    app = _build_app(service, captured_purges=purges, account_id="alice", monkeypatch=monkeypatch)
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://s3.hippius.com") as client:
        r = await client.put("/public-bucket/foo.txt?acl", content=b"<AccessControlPolicy/>")
    assert r.status_code == 200
    assert purges == [("s3.hippius.com", "public-bucket/foo.txt")]
