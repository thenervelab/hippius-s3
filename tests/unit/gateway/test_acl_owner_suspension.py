"""Owner-suspension check inside acl_middleware (issue #421).

The suspension middleware only sees the REQUESTER's identity. Anonymous public reads
and cross-account access carry a different (or no) identity, so acl_middleware blocks
them based on the BUCKET OWNER's suspension after resolving ownership.
"""

from typing import Any
from unittest.mock import AsyncMock

import pytest
from fastapi import FastAPI
from httpx import ASGITransport
from httpx import AsyncClient

from hippius_s3.gateway.middlewares.acl import acl_middleware
from hippius_s3.gateway.services.acl_service import BucketLookup
from hippius_s3.gateway.services.suspension import MODE_FULL
from hippius_s3.gateway.services.suspension import MODE_READ_ONLY


OWNER = "5FHneW46xGXgs5mUiveU4sbTyGBzmstUspZC92UhjJM694ty"


class FakeRedis:
    def __init__(self) -> None:
        self.store: dict[str, bytes] = {}

    async def get(self, key: str) -> bytes | None:
        return self.store.get(key)

    async def setex(self, key: str, ttl: int, value: Any) -> None:
        self.store[key] = value if isinstance(value, bytes) else str(value).encode("utf-8")

    async def delete(self, key: str) -> None:
        self.store.pop(key, None)


class FakePool:
    def __init__(self, mode: str | None = None) -> None:
        self.mode = mode

    async def fetchrow(self, query: str, *args: Any) -> Any:
        return {"mode": self.mode} if self.mode else None


def _app(owner_mode: str | None) -> FastAPI:
    app = FastAPI()
    app.state.postgres_pool = FakePool(mode=owner_mode)
    app.state.redis_client = FakeRedis()

    acl_service = AsyncMock()
    acl_service.check_permission = AsyncMock(return_value=True)
    acl_service.get_bucket_owner_and_id = AsyncMock(
        return_value=BucketLookup(owner_id=OWNER, bucket_id="bucket-uuid", is_cache_warm=False)
    )
    app.state.acl_service = acl_service

    @app.get("/{bucket}/{key:path}")
    async def get_object(bucket: str, key: str) -> dict[str, str]:
        return {"bucket": bucket, "key": key}

    @app.put("/{bucket}/{key:path}")
    async def put_object(bucket: str, key: str) -> dict[str, str]:
        return {"bucket": bucket, "key": key}

    app.middleware("http")(acl_middleware)
    return app


@pytest.mark.asyncio
async def test_anonymous_read_of_active_owner_bucket_passes() -> None:
    app = _app(owner_mode=None)
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
        response = await client.get("/public-bucket/key.txt")
    assert response.status_code == 200


@pytest.mark.asyncio
async def test_anonymous_read_of_fully_suspended_owner_bucket_is_denied() -> None:
    app = _app(owner_mode=MODE_FULL)
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
        response = await client.get("/public-bucket/key.txt")
    assert response.status_code == 403
    assert "AccessDenied" in response.text


@pytest.mark.asyncio
async def test_anonymous_read_of_read_only_owner_bucket_passes() -> None:
    app = _app(owner_mode=MODE_READ_ONLY)
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
        response = await client.get("/public-bucket/key.txt")
    assert response.status_code == 200


@pytest.mark.asyncio
async def test_cross_account_write_to_read_only_owner_bucket_is_denied() -> None:
    """A contractor with a bucket-ACL WRITE grant must not grow a read_only account's data."""
    app = _app(owner_mode=MODE_READ_ONLY)
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
        response = await client.put("/public-bucket/key.txt", content=b"data")
    assert response.status_code == 403
