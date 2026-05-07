"""Unit tests for bucket soft-delete behavior.

Verifies the Phase 1 contract:
- 204 on first DeleteBucket (UPDATE matches one row).
- 404 NoSuchBucket on a repeat DeleteBucket — NOT 403 (the previous endpoint
  returned 403 here, conflating "no permission" with "already deleted").
- 409 BucketNotEmpty when objects or in-flight MPUs exist.

There is intentionally no Redis queue; the Phase 2 reaper polls
`buckets WHERE deleted_at IS NOT NULL` (via idx_buckets_deleted_at_pending)
as the source of truth.
"""

from contextlib import asynccontextmanager
from typing import Any
from unittest.mock import AsyncMock

import pytest
from fastapi import FastAPI
from fastapi import Request
from httpx import ASGITransport
from httpx import AsyncClient

from hippius_s3.api.s3.buckets.router import router
from hippius_s3.dependencies import get_db_pool
from hippius_s3.dependencies import get_redis
from hippius_s3.models.account import HippiusAccount


def _make_mock_pool(
    *,
    bucket_row: dict[str, Any] | None,
    objects_in_bucket: list[dict[str, Any]],
    mpus_in_bucket: list[dict[str, Any]],
    soft_delete_returns: dict[str, Any] | None,
) -> Any:
    """Build a mock pool whose `acquire()` yields a connection that returns
    the prepared rows for each get_query lookup the endpoint performs."""
    mock_db = AsyncMock()

    async def fetchrow(query: str, *args: Any, **kwargs: Any) -> Any:
        if "FROM buckets" in query and "deleted_at IS NULL" in query and "RETURNING" not in query:
            return bucket_row
        if "UPDATE buckets" in query and "RETURNING" in query:
            return soft_delete_returns
        return None

    async def fetch(query: str, *args: Any, **kwargs: Any) -> list[Any]:
        if "FROM objects" in query:
            return objects_in_bucket
        if "FROM multipart_uploads" in query:
            return mpus_in_bucket
        return []

    mock_db.fetchrow = AsyncMock(side_effect=fetchrow)
    mock_db.fetch = AsyncMock(side_effect=fetch)

    @asynccontextmanager
    async def acquire() -> Any:
        yield mock_db

    mock_pool = AsyncMock()
    mock_pool.acquire = acquire
    return mock_pool


def _bucket_app(pool_factory: Any) -> Any:
    app = FastAPI()
    app.include_router(router)

    @app.middleware("http")
    async def inject_account(request: Request, call_next: Any) -> Any:
        request.state.account = HippiusAccount(
            id="test-subaccount",
            main_account="test-main-account",
            upload=True,
            delete=True,
            has_credits=True,
        )
        request.state.ray_id = "test-ray"
        return await call_next(request)

    app.dependency_overrides[get_db_pool] = pool_factory
    app.dependency_overrides[get_redis] = lambda: AsyncMock()
    return app


@pytest.mark.asyncio
async def test_delete_bucket_returns_204_on_success() -> None:
    bucket_id = "bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb"
    pool = _make_mock_pool(
        bucket_row={"bucket_id": bucket_id, "bucket_name": "alpha", "main_account_id": "test-main-account"},
        objects_in_bucket=[],
        mpus_in_bucket=[],
        soft_delete_returns={"bucket_id": bucket_id, "bucket_name": "alpha"},
    )
    app = _bucket_app(lambda: pool)

    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
        response = await client.delete("/alpha")

    assert response.status_code == 204


@pytest.mark.asyncio
async def test_delete_bucket_idempotent_returns_404_not_403() -> None:
    """A repeat DeleteBucket against a soft-deleted bucket must return 404
    NoSuchBucket per AWS S3 spec — never 403 AccessDenied. The legacy
    endpoint returned 403 from the same code path, which is a contract bug
    that's easy to miss."""
    pool = _make_mock_pool(
        # First lookup still finds the bucket (race window: another caller
        # just soft-deleted between the SELECT and the UPDATE). With the
        # filter on get_bucket_by_name, this branch only triggers if the
        # UPDATE itself loses a race. Either way, we must return 404.
        bucket_row={
            "bucket_id": "bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb",
            "bucket_name": "alpha",
            "main_account_id": "test-main-account",
        },
        objects_in_bucket=[],
        mpus_in_bucket=[],
        soft_delete_returns=None,
    )
    app = _bucket_app(lambda: pool)

    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
        response = await client.delete("/alpha")

    assert response.status_code == 404
    assert "NoSuchBucket" in response.text
    assert "AccessDenied" not in response.text


@pytest.mark.asyncio
async def test_delete_bucket_returns_404_when_already_gone() -> None:
    """Repeat DeleteBucket after the soft-delete commit: get_bucket_by_name
    filters on deleted_at IS NULL, so the row is invisible — 404."""
    pool = _make_mock_pool(
        bucket_row=None,
        objects_in_bucket=[],
        mpus_in_bucket=[],
        soft_delete_returns=None,
    )
    app = _bucket_app(lambda: pool)

    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
        response = await client.delete("/alpha")

    assert response.status_code == 404
    assert "NoSuchBucket" in response.text


@pytest.mark.asyncio
async def test_delete_bucket_with_objects_returns_409() -> None:
    pool = _make_mock_pool(
        bucket_row={
            "bucket_id": "bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb",
            "bucket_name": "alpha",
            "main_account_id": "test-main-account",
        },
        objects_in_bucket=[{"object_key": "k1"}],
        mpus_in_bucket=[],
        soft_delete_returns=None,
    )
    app = _bucket_app(lambda: pool)

    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
        response = await client.delete("/alpha")

    assert response.status_code == 409
    assert "BucketNotEmpty" in response.text


@pytest.mark.asyncio
async def test_delete_bucket_with_inflight_mpu_returns_409() -> None:
    pool = _make_mock_pool(
        bucket_row={
            "bucket_id": "bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb",
            "bucket_name": "alpha",
            "main_account_id": "test-main-account",
        },
        objects_in_bucket=[],
        mpus_in_bucket=[{"upload_id": "u1"}],
        soft_delete_returns=None,
    )
    app = _bucket_app(lambda: pool)

    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
        response = await client.delete("/alpha")

    assert response.status_code == 409
    assert "BucketNotEmpty" in response.text
