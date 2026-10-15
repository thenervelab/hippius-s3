"""Unit tests for the /admin account-management endpoints (issue #421)."""

import asyncio
import uuid
from typing import Any
from typing import Callable

import pytest
from fastapi import FastAPI
from fastapi import Response
from httpx import ASGITransport
from httpx import AsyncClient

from hippius_s3.api.admin import USAGE_PAGE_TIMEOUT_SECONDS
from hippius_s3.api.admin import router as admin_router
from hippius_s3.dependencies import get_postgres
from hippius_s3.utils import get_query


ACCOUNT = "5FHneW46xGXgs5mUiveU4sbTyGBzmstUspZC92UhjJM694ty"


class FakeDB:
    """Routes fetchrow/fetch/execute by SQL substring; records every call."""

    def __init__(self, router: Callable[[str, tuple], Any]) -> None:
        self.router = router
        self.calls: list[tuple[str, str, tuple]] = []
        self.kwarg_calls: list[dict[str, Any]] = []

    async def fetchrow(self, query: str, *args: Any, **kwargs: Any) -> Any:
        self.calls.append(("fetchrow", query, args))
        self.kwarg_calls.append(kwargs)
        return self.router(query, args)

    async def fetch(self, query: str, *args: Any, **kwargs: Any) -> Any:
        self.calls.append(("fetch", query, args))
        self.kwarg_calls.append(kwargs)
        return self.router(query, args)

    async def execute(self, query: str, *args: Any, **kwargs: Any) -> Any:
        self.calls.append(("execute", query, args))
        self.kwarg_calls.append(kwargs)
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
    # Match the real app (hippius_s3/main.py): default_response_class=Response, whose
    # render() 500s on a dict/model. This is what caught the prod bug where the admin
    # endpoints lacked JSONResponse — a bare FastAPI() defaults to JSONResponse and hid it.
    app = FastAPI(default_response_class=Response)
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


def _usage_row(account_id: str | None, buckets: int | None, total_bytes: int | None, *, ready: bool = True) -> dict:
    return {"ready": ready, "account_id": account_id, "buckets": buckets, "bytes": total_bytes}


def test_usage_route_lives_under_admin_prefix() -> None:
    app, _ = _make_app(FakeDB(lambda q, a: None))
    assert app.url_path_for("account_usage") == "/admin/usage"


def test_usage_sql_clamps_per_bucket_excludes_deleted_and_orders_c() -> None:
    sql = " ".join(get_query("list_admin_usage_accounts").split())
    assert "COALESCE(SUM(GREATEST(0, bsu.bytes_used)), 0)::bigint AS bytes" in sql
    assert "GREATEST(0, SUM" not in sql
    assert "b.deleted_at IS NULL" in sql
    assert "main_account_id > COALESCE($1::text, '')" in sql
    assert "main_account_id >=" not in sql
    assert 'ORDER BY b.main_account_id COLLATE "C"' in sql
    assert "storage_usage_rollup_state" in sql
    assert "account_suspensions" not in sql


@pytest.mark.asyncio
async def test_usage_page_sets_next_after_when_an_extra_row_exists() -> None:
    def route(query: str, args: tuple) -> Any:
        assert "bucket_storage_usage" in query
        assert args == (None, 3)
        return [
            _usage_row("acct-a", 1, 5),
            _usage_row("acct-b", 2, 10),
            _usage_row("acct-c", 1, 1),
        ]

    db = FakeDB(route)
    app, _ = _make_app(db)
    async with _client(app) as client:
        response = await client.get("/admin/usage", params={"limit": "2"})

    assert response.status_code == 200
    assert response.json() == {
        "accounts": [
            {"account_id": "acct-a", "buckets": 1, "bytes": 5},
            {"account_id": "acct-b", "buckets": 2, "bytes": 10},
        ],
        "next_after": "acct-b",
    }
    assert db.kwarg_calls[0]["timeout"] == USAGE_PAGE_TIMEOUT_SECONDS


@pytest.mark.asyncio
async def test_usage_last_page_has_null_next_after() -> None:
    def route(query: str, args: tuple) -> Any:
        assert args == ("acct-b", 3)
        return [_usage_row("acct-c", 1, 1)]

    app, _ = _make_app(FakeDB(route))
    async with _client(app) as client:
        response = await client.get("/admin/usage", params={"limit": "2", "after": "acct-b"})

    assert response.status_code == 200
    assert response.json() == {
        "accounts": [{"account_id": "acct-c", "buckets": 1, "bytes": 1}],
        "next_after": None,
    }


@pytest.mark.asyncio
async def test_usage_default_limit_is_1000_and_empty_page_stays_200_when_ready() -> None:
    def route(query: str, args: tuple) -> Any:
        assert args == (None, 1001)
        return [_usage_row(None, None, None, ready=True)]

    app, _ = _make_app(FakeDB(route))
    async with _client(app) as client:
        response = await client.get("/admin/usage")

    assert response.status_code == 200
    assert response.json() == {"accounts": [], "next_after": None}


@pytest.mark.asyncio
async def test_usage_after_cursor_is_exclusive_in_the_query() -> None:
    def route(query: str, args: tuple) -> Any:
        assert "main_account_id > COALESCE($1::text, '')" in " ".join(query.split())
        assert args[0] == "acct-b"
        return [_usage_row("acct-c", 1, 4)]

    app, _ = _make_app(FakeDB(route))
    async with _client(app) as client:
        response = await client.get("/admin/usage", params={"after": "acct-b", "limit": "10"})

    assert response.status_code == 200
    ids = [row["account_id"] for row in response.json()["accounts"]]
    assert ids == ["acct-c"]
    assert "acct-b" not in ids


@pytest.mark.asyncio
async def test_usage_rollup_not_ready_is_503_even_with_account_rows() -> None:
    def route(query: str, args: tuple) -> Any:
        return [_usage_row("acct-a", 2, 7, ready=False)]

    app, _ = _make_app(FakeDB(route))
    async with _client(app) as client:
        response = await client.get("/admin/usage")

    assert response.status_code == 503
    assert response.json() == {"detail": "usage unavailable; retry"}
    assert "acct-a" not in response.text


@pytest.mark.asyncio
async def test_usage_rollup_not_ready_on_empty_page_is_503() -> None:
    def route(query: str, args: tuple) -> Any:
        return [_usage_row(None, None, None, ready=False)]

    app, _ = _make_app(FakeDB(route))
    async with _client(app) as client:
        response = await client.get("/admin/usage")

    assert response.status_code == 503
    assert response.json() == {"detail": "usage unavailable; retry"}


def _usage_db_must_not_run(query: str, args: tuple) -> Any:
    raise AssertionError(f"db must not be queried: {query}")


@pytest.mark.asyncio
@pytest.mark.parametrize("limit", ["0", "-1", "10001", "abc", "1.5", "1e3", "++2", "", " "])
async def test_usage_bad_limit_is_422(limit: str) -> None:
    db = FakeDB(_usage_db_must_not_run)
    app, _ = _make_app(db)
    async with _client(app) as client:
        response = await client.get("/admin/usage", params={"limit": limit})

    assert response.status_code == 422
    assert response.json() == {"detail": "limit must be an integer from 1 to 10000"}
    assert db.calls == []


@pytest.mark.asyncio
@pytest.mark.parametrize("limit", ["+2", " 2", "2 ", "0002"])
async def test_usage_limit_accepts_trimmed_integers(limit: str) -> None:
    def route(query: str, args: tuple) -> Any:
        assert args == (None, 3)
        return [_usage_row("acct-a", 1, 5)]

    app, _ = _make_app(FakeDB(route))
    async with _client(app) as client:
        response = await client.get("/admin/usage", params={"limit": limit})

    assert response.status_code == 200
    assert response.json()["accounts"] == [{"account_id": "acct-a", "buckets": 1, "bytes": 5}]
    assert response.json()["next_after"] is None


@pytest.mark.asyncio
async def test_usage_timeout_is_503() -> None:
    def route(query: str, args: tuple) -> Any:
        raise asyncio.TimeoutError()

    app, _ = _make_app(FakeDB(route))
    async with _client(app) as client:
        response = await client.get("/admin/usage")

    assert response.status_code == 503
    assert response.json() == {"detail": "usage unavailable; retry"}
