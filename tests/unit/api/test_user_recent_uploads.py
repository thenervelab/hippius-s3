"""Unit tests for /user/recent_uploads endpoint."""

import json
from datetime import datetime
from datetime import timezone
from typing import Any
from unittest.mock import AsyncMock
from unittest.mock import MagicMock
from uuid import UUID

import pytest
from fastapi import FastAPI
from httpx import ASGITransport
from httpx import AsyncClient

from hippius_s3.api.user import RECENT_UPLOADS_CACHE_TTL_SECONDS
from hippius_s3.api.user import router
from hippius_s3.dependencies import get_postgres
from hippius_s3.dependencies import get_redis
from hippius_s3.utils import get_query


def _make_row(
    object_id: str,
    object_key: str,
    bucket_id: str,
    bucket_name: str,
    uploaded_at: datetime,
    size_bytes: int = 100,
    content_type: str = "application/octet-stream",
    md5_hash: str = "deadbeef",
    ipfs_cid: str | None = "Qm...",
) -> dict[str, Any]:
    return {
        "object_id": UUID(object_id),
        "object_key": object_key,
        "bucket_id": UUID(bucket_id),
        "bucket_name": bucket_name,
        "size_bytes": size_bytes,
        "content_type": content_type,
        "md5_hash": md5_hash,
        "ipfs_cid": ipfs_cid,
        "uploaded_at": uploaded_at,
    }


@pytest.fixture
def mocks() -> tuple[MagicMock, MagicMock]:
    mock_db = MagicMock()
    mock_db.fetch = AsyncMock(return_value=[])

    mock_redis = MagicMock()
    mock_redis.get = AsyncMock(return_value=None)
    mock_redis.setex = AsyncMock(return_value=True)

    return mock_db, mock_redis


@pytest.fixture
def app(mocks: tuple[MagicMock, MagicMock]) -> FastAPI:
    mock_db, mock_redis = mocks

    application = FastAPI()
    application.include_router(router, prefix="/user")
    application.dependency_overrides[get_postgres] = lambda: mock_db
    application.dependency_overrides[get_redis] = lambda: mock_redis

    return application


@pytest.mark.asyncio
async def test_cache_hit_skips_db(app: FastAPI, mocks: tuple[MagicMock, MagicMock]) -> None:
    mock_db, mock_redis = mocks
    cached_payload = {
        "main_account_id": "acct-1",
        "count": 1,
        "uploads": [
            {
                "object_id": "00000000-0000-0000-0000-000000000001",
                "object_key": "cached.txt",
                "bucket_id": "00000000-0000-0000-0000-0000000000aa",
                "bucket_name": "b1",
                "size_bytes": 7,
                "content_type": "text/plain",
                "md5_hash": "abc",
                "ipfs_cid": None,
                "uploaded_at": "2026-04-27T12:00:00+00:00",
            }
        ],
    }
    mock_redis.get.return_value = json.dumps(cached_payload)

    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
        response = await client.get("/user/recent_uploads?main_account_id=acct-1")

    assert response.status_code == 200
    assert response.json() == cached_payload

    mock_redis.get.assert_awaited_once_with("recent_uploads:acct-1")
    mock_db.fetch.assert_not_called()
    mock_redis.setex.assert_not_called()


@pytest.mark.asyncio
async def test_cache_miss_queries_db_and_populates_cache(app: FastAPI, mocks: tuple[MagicMock, MagicMock]) -> None:
    mock_db, mock_redis = mocks
    rows = [
        _make_row(
            "00000000-0000-0000-0000-000000000001",
            "newest.bin",
            "00000000-0000-0000-0000-0000000000aa",
            "alpha",
            datetime(2026, 4, 27, 12, 0, 0, tzinfo=timezone.utc),
        ),
        _make_row(
            "00000000-0000-0000-0000-000000000002",
            "older.bin",
            "00000000-0000-0000-0000-0000000000bb",
            "beta",
            datetime(2026, 4, 26, 8, 30, 0, tzinfo=timezone.utc),
            ipfs_cid=None,
        ),
    ]
    mock_db.fetch.return_value = rows

    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
        response = await client.get("/user/recent_uploads?main_account_id=acct-9")

    assert response.status_code == 200
    body = response.json()

    assert body["main_account_id"] == "acct-9"
    assert body["count"] == 2
    assert len(body["uploads"]) == 2

    first = body["uploads"][0]
    assert first["object_id"] == "00000000-0000-0000-0000-000000000001"
    assert first["object_key"] == "newest.bin"
    assert first["bucket_id"] == "00000000-0000-0000-0000-0000000000aa"
    assert first["bucket_name"] == "alpha"
    assert first["uploaded_at"] == "2026-04-27T12:00:00+00:00"
    assert first["ipfs_cid"] == "Qm..."

    second = body["uploads"][1]
    assert second["ipfs_cid"] is None

    mock_db.fetch.assert_awaited_once()
    args = mock_db.fetch.await_args.args
    assert args[1] == "acct-9"
    assert len(args) == 2

    mock_redis.setex.assert_awaited_once()
    setex_args = mock_redis.setex.await_args.args
    assert setex_args[0] == "recent_uploads:acct-9"
    assert setex_args[1] == RECENT_UPLOADS_CACHE_TTL_SECONDS
    assert json.loads(setex_args[2]) == body


@pytest.mark.asyncio
async def test_empty_result_still_caches(app: FastAPI, mocks: tuple[MagicMock, MagicMock]) -> None:
    mock_db, mock_redis = mocks
    mock_db.fetch.return_value = []

    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
        response = await client.get("/user/recent_uploads?main_account_id=fresh-acct")

    assert response.status_code == 200
    body = response.json()
    assert body == {"main_account_id": "fresh-acct", "count": 0, "uploads": []}

    mock_redis.setex.assert_awaited_once()
    setex_args = mock_redis.setex.await_args.args
    assert setex_args[0] == "recent_uploads:fresh-acct"
    assert setex_args[1] == RECENT_UPLOADS_CACHE_TTL_SECONDS


@pytest.mark.asyncio
async def test_missing_main_account_id_returns_422(app: FastAPI) -> None:
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
        response = await client.get("/user/recent_uploads")

    assert response.status_code == 422


@pytest.mark.asyncio
async def test_sql_contract() -> None:
    sql = get_query("get_recent_uploads_for_account")

    assert "LIMIT 10" in sql
    assert "$1" in sql
    assert "$2" not in sql

    assert "ov.last_modified" in sql
    assert "o.last_modified" not in sql

    assert "deleted_at IS NULL" in sql
    assert "ov.status <> 'failed'" in sql


@pytest.mark.asyncio
async def test_response_uses_cached_blob_verbatim_round_trip(app: FastAPI, mocks: tuple[MagicMock, MagicMock]) -> None:
    mock_db, mock_redis = mocks
    rows = [
        _make_row(
            "00000000-0000-0000-0000-0000000000ff",
            "round-trip.bin",
            "00000000-0000-0000-0000-0000000000bb",
            "gamma",
            datetime(2026, 4, 27, 9, 15, 30, tzinfo=timezone.utc),
        ),
    ]
    mock_db.fetch.return_value = rows

    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
        first = await client.get("/user/recent_uploads?main_account_id=rt-acct")

    cached_blob = mock_redis.setex.await_args.args[2]

    mock_db.fetch.reset_mock()
    mock_redis.get.return_value = cached_blob

    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
        second = await client.get("/user/recent_uploads?main_account_id=rt-acct")

    assert first.status_code == 200
    assert second.status_code == 200
    assert first.json() == second.json()
    mock_db.fetch.assert_not_called()
