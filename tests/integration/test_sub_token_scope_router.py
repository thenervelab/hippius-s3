"""Integration tests for the /user/sub-tokens/{access_key_id}/scope endpoints."""

from __future__ import annotations

from typing import Any
from unittest.mock import AsyncMock
from unittest.mock import MagicMock
from unittest.mock import patch
from uuid import uuid4

import pytest
from fastapi import FastAPI
from httpx import ASGITransport
from httpx import AsyncClient

from hippius_s3.api.sub_token_scopes import router as sub_token_scopes_router
from hippius_s3.dependencies import DBConnection
from hippius_s3.dependencies import get_postgres
from hippius_s3.repositories.sub_token_scope_repository import SubTokenScope


ACCT_A = "5FHneW46xGXgs5mUiveU4sbTyGBzmstUspZC92UhjJM694ty"
ACCT_B = "5GrwvaEF5zXb26Fz9rcQpDWS57CtERHpNehXCPcNoHGKutQY"
KEY_X = "hip_sub_abcdef012345"


@pytest.fixture  # type: ignore[misc]
def scope_app(monkeypatch: pytest.MonkeyPatch) -> Any:
    """FastAPI app with the real sub_token_scopes router + mocked deps.

    Always mocks `cached_auth` to succeed (returning a sub-token owned by ACCT_A)
    so tests that don't care about the token-validation path don't need to opt in.
    Tests that exercise rejection paths (P2.5) override the mock locally.
    """
    app = FastAPI()

    # Mock repo.
    repo = MagicMock()
    repo.get = AsyncMock(return_value=None)
    repo.upsert = AsyncMock()
    repo.delete = AsyncMock(return_value=True)
    app.state.sub_token_scope_repo = repo

    # Mock Redis (cache invalidation).
    redis_client = AsyncMock()
    redis_client.delete = AsyncMock(return_value=1)
    app.state.redis_client = redis_client

    # Mock DB connection — handlers call `db.fetch(...)` (PUT bucket resolution)
    # and `db.fetchrow(...)` (GET via the get_sub_token_scope_with_buckets query).
    mock_conn = MagicMock()
    mock_conn.fetch = AsyncMock(return_value=[])
    mock_conn.fetchrow = AsyncMock(return_value=None)
    pool = MagicMock()
    db = DBConnection(conn=mock_conn, pool=pool)

    async def _fake_get_postgres():
        yield db

    app.dependency_overrides[get_postgres] = _fake_get_postgres
    app.include_router(sub_token_scopes_router, prefix="/user/sub-tokens")

    # Default: target access_key is a valid sub-token owned by ACCT_A. Tests that
    # exercise rejection paths override this mock with `side_effect` raising
    # HTTPException or swap it for one that raises a specific error.
    monkeypatch.setattr(
        "hippius_s3.api.sub_token_scopes._verify_access_key_owned_by_account",
        AsyncMock(return_value=None),
    )

    app.state._test_mock_conn = mock_conn
    app.state._test_mock_repo = repo
    app.state._test_mock_redis = redis_client
    app.state._test_mock_db = db

    return app


def _bucket_row(name: str, owner: str, bucket_id: str | None = None) -> dict[str, Any]:
    return {"bucket_name": name, "bucket_id": bucket_id or str(uuid4()), "main_account_id": owner}


# ---- PUT: happy path ------------------------------------------------------


@pytest.mark.asyncio
async def test_put_scope_stores_and_invalidates_cache(scope_app: Any) -> None:
    bucket_id = str(uuid4())
    scope_app.state._test_mock_conn.fetch.return_value = [_bucket_row("bucket-a", ACCT_A, bucket_id)]
    scope_app.state._test_mock_repo.upsert.return_value = SubTokenScope(
        access_key_id=KEY_X,
        account_id=ACCT_A,
        permission="object_read",
        bucket_scope="specific",
        bucket_ids=(bucket_id,),
    )

    async with AsyncClient(transport=ASGITransport(app=scope_app), base_url="http://test") as client:
        resp = await client.put(
            f"/user/sub-tokens/{KEY_X}/scope",
            json={
                "account_id": ACCT_A,
                "permission": "object_read",
                "bucket_scope": "specific",
                "buckets": ["bucket-a"],
            },
        )

    assert resp.status_code == 200
    body = resp.json()
    assert body["permission"] == "object_read"
    assert body["bucket_scope"] == "specific"
    assert body["buckets"] == ["bucket-a"]

    # Cache invalidated.
    scope_app.state._test_mock_redis.delete.assert_awaited_once()

    # Repo upsert called with resolved bucket_id (UUID, not bucket name).
    upsert_args = scope_app.state._test_mock_repo.upsert.await_args.kwargs
    assert upsert_args["bucket_ids"] == [bucket_id]


# ---- P2.5 — PUT rejects orphan access_key_id -----------------------------


def _verify_raises(code: str, message: str, http_status: int) -> AsyncMock:
    """Build a mock for `_verify_access_key_owned_by_account` that raises HTTPException."""
    from fastapi import HTTPException

    async def _raise(*args: Any, **kwargs: Any) -> None:
        raise HTTPException(status_code=http_status, detail={"code": code, "message": message})

    return AsyncMock(side_effect=_raise)


@pytest.mark.asyncio
async def test_put_scope_rejects_unknown_access_key(scope_app: Any) -> None:
    """P2.5: installing scope for an access_key that is not a known sub-token of
    body.account_id must fail (not silently succeed with dead data)."""
    bucket_id = str(uuid4())
    scope_app.state._test_mock_conn.fetch.return_value = [_bucket_row("bucket-a", ACCT_A, bucket_id)]

    with patch(
        "hippius_s3.api.sub_token_scopes._verify_access_key_owned_by_account",
        _verify_raises("InvalidArgument", "Target access key is not a sub-token", 400),
    ):
        async with AsyncClient(transport=ASGITransport(app=scope_app), base_url="http://test") as client:
            resp = await client.put(
                f"/user/sub-tokens/{KEY_X}/scope",
                json={
                    "account_id": ACCT_A,
                    "permission": "object_read",
                    "bucket_scope": "specific",
                    "buckets": ["bucket-a"],
                },
            )

    assert resp.status_code == 400, resp.text
    scope_app.state._test_mock_repo.upsert.assert_not_awaited()


@pytest.mark.asyncio
async def test_put_scope_rejects_access_key_from_different_account(scope_app: Any) -> None:
    """P2.5: body.account_id=A but access_key belongs to account B → reject."""
    bucket_id = str(uuid4())
    scope_app.state._test_mock_conn.fetch.return_value = [_bucket_row("bucket-a", ACCT_A, bucket_id)]

    with patch(
        "hippius_s3.api.sub_token_scopes._verify_access_key_owned_by_account",
        _verify_raises("AccessDenied", "Target sub-token belongs to a different account", 403),
    ):
        async with AsyncClient(transport=ASGITransport(app=scope_app), base_url="http://test") as client:
            resp = await client.put(
                f"/user/sub-tokens/{KEY_X}/scope",
                json={
                    "account_id": ACCT_A,
                    "permission": "object_read",
                    "bucket_scope": "specific",
                    "buckets": ["bucket-a"],
                },
            )

    assert resp.status_code == 403, resp.text
    scope_app.state._test_mock_repo.upsert.assert_not_awaited()


# ---- P2.3 — GET account_id mismatch --------------------------------------


def _scope_row(
    *,
    access_key_id: str = KEY_X,
    account_id: str = ACCT_A,
    permission: str = "object_read",
    bucket_scope: str = "specific",
    bucket_ids: list[str] | None = None,
    live_bucket_names: list[str] | None = None,
    stale_bucket_ids: list[str] | None = None,
) -> dict[str, Any]:
    """Shape-compatible row for the get_sub_token_scope_with_buckets query."""
    return {
        "access_key_id": access_key_id,
        "account_id": account_id,
        "permission": permission,
        "bucket_scope": bucket_scope,
        "bucket_ids": list(bucket_ids or []),
        "live_bucket_names": list(live_bucket_names or []),
        "stale_bucket_ids": list(stale_bucket_ids or []),
    }


@pytest.mark.asyncio
async def test_get_scope_mismatched_account_returns_403_not_404(scope_app: Any) -> None:
    """P2.3: scope exists but belongs to a different account → AccessDenied, not NoSuchScope."""
    other_bucket_id = str(uuid4())
    scope_app.state._test_mock_conn.fetchrow.return_value = _scope_row(
        account_id=ACCT_B,  # stored record belongs to B
        bucket_ids=[other_bucket_id],
        live_bucket_names=["other-bucket"],
    )

    async with AsyncClient(transport=ASGITransport(app=scope_app), base_url="http://test") as client:
        resp = await client.get(f"/user/sub-tokens/{KEY_X}/scope", params={"account_id": ACCT_A})

    assert resp.status_code == 403, resp.text
    assert resp.json()["detail"]["code"] == "AccessDenied"


@pytest.mark.asyncio
async def test_get_scope_not_set_returns_404(scope_app: Any) -> None:
    scope_app.state._test_mock_conn.fetchrow.return_value = None

    async with AsyncClient(transport=ASGITransport(app=scope_app), base_url="http://test") as client:
        resp = await client.get(f"/user/sub-tokens/{KEY_X}/scope", params={"account_id": ACCT_A})

    assert resp.status_code == 404, resp.text
    assert resp.json()["detail"]["code"] == "NoSuchScope"


# ---- P3.8 — stale bucket ids in GET --------------------------------------


@pytest.mark.asyncio
async def test_get_scope_reports_stale_bucket_ids(scope_app: Any) -> None:
    """P3.8: scope references buckets [A, B]; B has been deleted. Response must
    include A in `buckets` AND surface B in `stale_bucket_ids` instead of silently dropping it."""
    live_id = str(uuid4())
    deleted_id = str(uuid4())
    scope_app.state._test_mock_conn.fetchrow.return_value = _scope_row(
        bucket_ids=[live_id, deleted_id],
        live_bucket_names=["live-bucket"],
        stale_bucket_ids=[deleted_id],
    )

    async with AsyncClient(transport=ASGITransport(app=scope_app), base_url="http://test") as client:
        resp = await client.get(f"/user/sub-tokens/{KEY_X}/scope", params={"account_id": ACCT_A})

    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert body["buckets"] == ["live-bucket"]
    assert body["stale_bucket_ids"] == [deleted_id]


# ---- PUT validation rules -------------------------------------------------


@pytest.mark.asyncio
async def test_put_scope_rejects_invalid_access_key_format(scope_app: Any) -> None:
    async with AsyncClient(transport=ASGITransport(app=scope_app), base_url="http://test") as client:
        resp = await client.put(
            "/user/sub-tokens/not_hip_key/scope",
            json={
                "account_id": ACCT_A,
                "permission": "object_read",
                "bucket_scope": "all",
                "buckets": [],
            },
        )
    assert resp.status_code == 400


@pytest.mark.asyncio
async def test_put_scope_rejects_specific_without_buckets(scope_app: Any) -> None:
    async with AsyncClient(transport=ASGITransport(app=scope_app), base_url="http://test") as client:
        resp = await client.put(
            f"/user/sub-tokens/{KEY_X}/scope",
            json={
                "account_id": ACCT_A,
                "permission": "object_read",
                "bucket_scope": "specific",
                "buckets": [],
            },
        )
    assert resp.status_code == 400


@pytest.mark.asyncio
async def test_put_scope_rejects_all_with_buckets(scope_app: Any) -> None:
    async with AsyncClient(transport=ASGITransport(app=scope_app), base_url="http://test") as client:
        resp = await client.put(
            f"/user/sub-tokens/{KEY_X}/scope",
            json={
                "account_id": ACCT_A,
                "permission": "admin_read_write",
                "bucket_scope": "all",
                "buckets": ["bucket-a"],
            },
        )
    assert resp.status_code == 400


@pytest.mark.asyncio
async def test_put_scope_rejects_bucket_not_owned_by_account(scope_app: Any) -> None:
    # Bucket exists but is owned by someone else.
    scope_app.state._test_mock_conn.fetch.return_value = [
        _bucket_row("other-bucket", ACCT_B, str(uuid4())),
    ]

    async with AsyncClient(transport=ASGITransport(app=scope_app), base_url="http://test") as client:
        resp = await client.put(
            f"/user/sub-tokens/{KEY_X}/scope",
            json={
                "account_id": ACCT_A,
                "permission": "object_read",
                "bucket_scope": "specific",
                "buckets": ["other-bucket"],
            },
        )
    assert resp.status_code == 403


@pytest.mark.asyncio
async def test_put_scope_rejects_nonexistent_bucket(scope_app: Any) -> None:
    scope_app.state._test_mock_conn.fetch.return_value = []  # no buckets

    async with AsyncClient(transport=ASGITransport(app=scope_app), base_url="http://test") as client:
        resp = await client.put(
            f"/user/sub-tokens/{KEY_X}/scope",
            json={
                "account_id": ACCT_A,
                "permission": "object_read",
                "bucket_scope": "specific",
                "buckets": ["nope-bucket"],
            },
        )
    assert resp.status_code == 404


# ---- DELETE --------------------------------------------------------------


@pytest.mark.asyncio
async def test_delete_scope_invalidates_cache(scope_app: Any) -> None:
    async with AsyncClient(transport=ASGITransport(app=scope_app), base_url="http://test") as client:
        resp = await client.delete(f"/user/sub-tokens/{KEY_X}/scope")

    assert resp.status_code == 204
    scope_app.state._test_mock_repo.delete.assert_awaited_once_with(KEY_X)
    scope_app.state._test_mock_redis.delete.assert_awaited_once()


@pytest.mark.asyncio
async def test_delete_scope_is_idempotent(scope_app: Any) -> None:
    scope_app.state._test_mock_repo.delete.return_value = False

    async with AsyncClient(transport=ASGITransport(app=scope_app), base_url="http://test") as client:
        resp = await client.delete(f"/user/sub-tokens/{KEY_X}/scope")

    assert resp.status_code == 204
