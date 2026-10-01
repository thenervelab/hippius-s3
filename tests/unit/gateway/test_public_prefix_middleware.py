"""The prefix grant is decided in the middleware with the bucket the request is reading.

A copy authorizes the source bucket. A version id is s3:GetObjectVersion and is not
covered by a prefix. The cache flag is set only when the ACL alone would have denied
the anonymous read.
"""

from __future__ import annotations

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

from hippius_s3 import config as gateway_config
from hippius_s3.gateway.middlewares.acl import acl_middleware
from hippius_s3.gateway.services.acl_service import BucketLookup
from tests.unit.gateway._suspension_fakes import install_no_suspension_state


@pytest.fixture(autouse=True)
def _ats_enabled(monkeypatch: pytest.MonkeyPatch) -> Any:
    monkeypatch.setenv("ATS_CACHE_ENDPOINT", "http://ats.local:8080")
    gateway_config.reset_config()
    yield
    gateway_config.reset_config()


def _lookup(owner: str, bucket_id: str) -> BucketLookup:
    return BucketLookup(owner_id=owner, bucket_id=bucket_id, is_cache_warm=False)


def _build(
    service: Any, *, account_id: str | None = None, auth_method: str | None = None, token_type: str | None = None
) -> FastAPI:
    app = FastAPI()
    app.state.acl_service = service
    install_no_suspension_state(app)

    @app.api_route("/{path:path}", methods=["GET", "HEAD", "PUT", "DELETE"])
    async def catch_all(request: Request) -> dict[str, Any]:
        return {
            "anonymous_read_allowed": bool(getattr(request.state, "anonymous_read_allowed", False)),
            "anonymous_read_via_prefix": bool(getattr(request.state, "anonymous_read_via_prefix", False)),
        }

    async def stub_auth(request: Request, call_next: Callable[[Request], Awaitable[Response]]) -> Response:
        request.state.account_id = account_id
        request.state.auth_method = auth_method
        request.state.token_type = token_type
        request.state.access_key = None
        return await call_next(request)

    app.middleware("http")(acl_middleware)
    app.middleware("http")(stub_auth)
    return app


def _service(decide: Callable[..., bool]) -> tuple[Any, list[dict[str, Any]]]:
    calls: list[dict[str, Any]] = []

    async def check_permission(**kwargs: Any) -> bool:
        calls.append(kwargs)
        return decide(**kwargs)

    async def get_bucket_owner_and_id(bucket: str) -> BucketLookup:
        if bucket == "src":
            return _lookup("src-owner", "src-id")
        return _lookup("dest-owner", "dest-id")

    service = AsyncMock()
    service.get_bucket_owner_and_id = AsyncMock(side_effect=get_bucket_owner_and_id)
    service.check_permission = AsyncMock(side_effect=check_permission)
    return service, calls


@pytest.mark.asyncio
async def test_anonymous_prefix_read_sets_the_short_cache_flag() -> None:
    def decide(**kwargs: Any) -> bool:
        return kwargs["account_id"] is None and kwargs.get("allow_public_prefix", True)

    service, calls = _service(decide)
    app = _build(service)
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
        response = await client.get("/evidence/public/a")

    assert response.status_code == 200
    assert response.json() == {"anonymous_read_allowed": True, "anonymous_read_via_prefix": True}
    assert [call["bucket_id"] for call in calls] == ["dest-id", "dest-id", "dest-id"]
    assert [call["allow_public_prefix"] for call in calls] == [True, False, True]


@pytest.mark.asyncio
async def test_a_bucket_acl_grant_does_not_take_the_prefix_flag() -> None:
    def decide(**kwargs: Any) -> bool:
        return kwargs["account_id"] is None

    service, calls = _service(decide)
    app = _build(service)
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
        response = await client.get("/evidence/public/a")

    assert response.json()["anonymous_read_via_prefix"] is False
    assert calls[1]["allow_public_prefix"] is False


@pytest.mark.asyncio
async def test_version_id_disables_the_prefix_on_the_probe_and_the_check() -> None:
    service, calls = _service(lambda **_kwargs: False)
    app = _build(service)
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
        response = await client.get("/evidence/public/a?VersionId=1")

    assert response.status_code == 403
    assert calls
    assert all(call["allow_public_prefix"] is False for call in calls)
    assert all(call["bucket_id"] == "dest-id" for call in calls)


@pytest.mark.asyncio
async def test_copy_source_uses_the_source_bucket_and_pins_version_id() -> None:
    service, calls = _service(lambda **_kwargs: False)
    app = _build(service, account_id="caller")
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
        plain = await client.put("/dest/out.txt", headers={"x-amz-copy-source": "src/public/a.txt"})
        pinned = await client.put(
            "/dest/out.txt",
            headers={"x-amz-copy-source": "src/public/a.txt%3FversionId%3D9"},
        )

    assert plain.status_code == pinned.status_code == 403
    assert calls[0]["bucket"] == "src"
    assert calls[0]["bucket_id"] == "src-id"
    assert calls[0]["key"] == "public/a.txt"
    assert calls[0]["allow_public_prefix"] is True
    assert calls[1]["bucket_id"] == "src-id"
    assert calls[1]["key"] == "public/a.txt"
    assert calls[1]["allow_public_prefix"] is False
