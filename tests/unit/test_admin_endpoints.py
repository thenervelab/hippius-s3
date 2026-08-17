"""Unit tests for the /admin account-management endpoints (issue #421)."""

import asyncio
import uuid
from typing import Any
from typing import Callable

import pytest
from fastapi import FastAPI
from httpx import ASGITransport
from httpx import AsyncClient

from hippius_s3.api.admin import router as admin_router
from hippius_s3.dependencies import get_postgres


ACCOUNT = "5FHneW46xGXgs5mUiveU4sbTyGBzmstUspZC92UhjJM694ty"


class FakeDB:
    """Routes fetchrow/fetch/execute by SQL substring; records every call."""

    def __init__(self, router: Callable[[str, tuple], Any]) -> None:
        self.router = router
        self.calls: list[tuple[str, str, tuple]] = []

    async def fetchrow(self, query: str, *args: Any, **kwargs: Any) -> Any:
        self.calls.append(("fetchrow", query, args))
        return self.router(query, args)

    async def fetch(self, query: str, *args: Any, **kwargs: Any) -> Any:
        self.calls.append(("fetch", query, args))
        return self.router(query, args)

    async def execute(self, query: str, *args: Any, **kwargs: Any) -> Any:
        self.calls.append(("execute", query, args))
        return self.router(query, args)


class FakeRedis:
    def __init__(self) -> None:
        self.store: dict[str, Any] = {}
        self.deleted: list[str] = []

    async def setex(self, key: str, ttl: int, value: Any) -> None:
        self.store[key] = value

    async def delete(self, key: str) -> None:
        self.deleted.append(key)
        self.store.pop(key, None)


def _make_app(db: FakeDB) -> tuple[FastAPI, FakeRedis]:
    app = FastAPI()
    app.include_router(admin_router, prefix="/admin")
    redis = FakeRedis()
    app.state.redis_client = redis

    async def _override() -> Any:
        yield db

    app.dependency_overrides[get_postgres] = _override
    return app, redis


def _client(app: FastAPI) -> AsyncClient:
    return AsyncClient(transport=ASGITransport(app=app), base_url="http://test")


@pytest.mark.asyncio
async def test_suspend_rejects_invalid_ss58() -> None:
    app, _ = _make_app(FakeDB(lambda q, a: None))
    async with _client(app) as client:
        response = await client.post("/admin/accounts/not-an-address!/suspend", json={})
    assert response.status_code == 400


@pytest.mark.asyncio
async def test_suspend_defaults_to_full_and_writes_through_cache() -> None:
    def route(query: str, args: tuple) -> Any:
        if "INSERT INTO account_suspensions" in query:
            return {"account_id": args[0], "mode": args[1]}
        raise AssertionError(f"unexpected query: {query}")

    app, redis = _make_app(FakeDB(route))
    async with _client(app) as client:
        response = await client.post(f"/admin/accounts/{ACCOUNT}/suspend", json={})

    assert response.status_code == 200
    assert response.json() == {"account_id": ACCOUNT, "state": "suspended"}
    assert redis.store[f"hippius_suspension:{ACCOUNT}"] == "full"


@pytest.mark.asyncio
async def test_suspend_read_only_returns_read_only_state() -> None:
    def route(query: str, args: tuple) -> Any:
        if "INSERT INTO account_suspensions" in query:
            return {"account_id": args[0], "mode": args[1]}
        raise AssertionError(f"unexpected query: {query}")

    app, redis = _make_app(FakeDB(route))
    async with _client(app) as client:
        response = await client.post(f"/admin/accounts/{ACCOUNT}/suspend", json={"mode": "read_only"})

    assert response.status_code == 200
    assert response.json()["state"] == "read_only"
    assert redis.store[f"hippius_suspension:{ACCOUNT}"] == "read_only"


@pytest.mark.asyncio
async def test_reactivate_is_idempotent_and_invalidates_cache() -> None:
    def route(query: str, args: tuple) -> Any:
        if "state IN ('queued', 'running')" in query:
            return None
        if "DELETE FROM account_suspensions" in query:
            return None
        raise AssertionError(f"unexpected query: {query}")

    app, redis = _make_app(FakeDB(route))
    async with _client(app) as client:
        response = await client.post(f"/admin/accounts/{ACCOUNT}/reactivate")

    assert response.status_code == 200
    assert response.json() == {"account_id": ACCOUNT, "state": "active"}
    assert redis.deleted == [f"hippius_suspension:{ACCOUNT}"]


@pytest.mark.asyncio
async def test_reactivate_conflicts_while_purge_active() -> None:
    job_id = uuid.uuid4()

    def route(query: str, args: tuple) -> Any:
        if "state IN ('queued', 'running')" in query:
            return {"job_id": job_id, "state": "running"}
        raise AssertionError(f"unexpected query: {query}")

    app, _ = _make_app(FakeDB(route))
    async with _client(app) as client:
        response = await client.post(f"/admin/accounts/{ACCOUNT}/reactivate")

    assert response.status_code == 409
    assert response.json()["detail"]["code"] == "PurgeInProgress"


@pytest.mark.asyncio
async def test_status_reports_state_and_stats() -> None:
    def route(query: str, args: tuple) -> Any:
        if "FROM account_suspensions" in query:
            return {"account_id": ACCOUNT, "mode": "read_only", "created_at": None, "updated_at": None}
        if "AS buckets" in query:
            return {"buckets": 3, "bytes": 1234567}
        raise AssertionError(f"unexpected query: {query}")

    app, _ = _make_app(FakeDB(route))
    async with _client(app) as client:
        response = await client.get(f"/admin/accounts/{ACCOUNT}/status")

    assert response.status_code == 200
    assert response.json() == {"account_id": ACCOUNT, "state": "read_only", "buckets": 3, "bytes": 1234567}


@pytest.mark.asyncio
async def test_status_degrades_to_null_counts_on_timeout() -> None:
    def route(query: str, args: tuple) -> Any:
        if "FROM account_suspensions" in query:
            return None
        if "AS buckets" in query:
            raise asyncio.TimeoutError()
        raise AssertionError(f"unexpected query: {query}")

    app, _ = _make_app(FakeDB(route))
    async with _client(app) as client:
        response = await client.get(f"/admin/accounts/{ACCOUNT}/status")

    assert response.status_code == 200
    assert response.json() == {"account_id": ACCOUNT, "state": "active", "buckets": None, "bytes": None}


@pytest.mark.asyncio
async def test_purge_creates_job_and_implies_full_suspension() -> None:
    def route(query: str, args: tuple) -> Any:
        if "INSERT INTO account_suspensions" in query:
            assert args[1] == "full"
            return {"account_id": args[0], "mode": args[1]}
        if "INSERT INTO purge_jobs" in query:
            return {"job_id": args[0]}
        raise AssertionError(f"unexpected query: {query}")

    db = FakeDB(route)
    app, redis = _make_app(db)
    async with _client(app) as client:
        response = await client.delete(f"/admin/accounts/{ACCOUNT}/data")

    assert response.status_code == 202
    uuid.UUID(response.json()["job_id"])
    assert redis.store[f"hippius_suspension:{ACCOUNT}"] == "full"
    assert any("INSERT INTO account_suspensions" in q for _, q, _a in db.calls)


@pytest.mark.asyncio
async def test_purge_returns_existing_active_job() -> None:
    existing = uuid.uuid4()

    def route(query: str, args: tuple) -> Any:
        if "INSERT INTO account_suspensions" in query:
            return {"account_id": args[0], "mode": args[1]}
        if "INSERT INTO purge_jobs" in query:
            return None
        if "state IN ('queued', 'running')" in query:
            return {"job_id": existing, "state": "queued"}
        raise AssertionError(f"unexpected query: {query}")

    app, _ = _make_app(FakeDB(route))
    async with _client(app) as client:
        first = await client.delete(f"/admin/accounts/{ACCOUNT}/data")

    assert first.status_code == 202
    assert first.json()["job_id"] == str(existing)


@pytest.mark.asyncio
async def test_purge_job_status_roundtrip() -> None:
    job_id = uuid.uuid4()

    def route(query: str, args: tuple) -> Any:
        if "FROM purge_jobs" in query and "WHERE job_id" in query:
            return {
                "job_id": job_id,
                "account_id": ACCOUNT,
                "state": "running",
                "deleted_objects": 42,
                "deleted_bytes": 1024,
                "error": None,
                "created_at": None,
                "started_at": None,
                "finished_at": None,
            }
        raise AssertionError(f"unexpected query: {query}")

    app, _ = _make_app(FakeDB(route))
    async with _client(app) as client:
        response = await client.get(f"/admin/purge-jobs/{job_id}")

    assert response.status_code == 200
    body = response.json()
    assert body["state"] == "running"
    assert body["deleted_objects"] == 42
    assert body["deleted_bytes"] == 1024
    assert body["error"] is None


@pytest.mark.asyncio
async def test_purge_job_status_unknown_returns_404() -> None:
    app, _ = _make_app(FakeDB(lambda q, a: None))
    async with _client(app) as client:
        response = await client.get(f"/admin/purge-jobs/{uuid.uuid4()}")
    assert response.status_code == 404


@pytest.mark.asyncio
async def test_purge_job_status_invalid_uuid_returns_400() -> None:
    app, _ = _make_app(FakeDB(lambda q, a: None))
    async with _client(app) as client:
        response = await client.get("/admin/purge-jobs/not-a-uuid")
    assert response.status_code == 400
