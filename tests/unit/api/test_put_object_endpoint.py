import asyncio
import uuid
from types import SimpleNamespace
from typing import Any
from unittest.mock import AsyncMock
from unittest.mock import MagicMock

import pytest
from redis.exceptions import ConnectionError as RedisConnectionError
from starlette.datastructures import Headers

from hippius_s3.api.s3.objects import put_object_endpoint
from hippius_s3.api.s3.objects.put_object_endpoint import handle_put_object
from hippius_s3.writer.types import PutResult
from tests.unit._fake_pool import make_fake_pool


def _fake_request(headers: dict[str, str] | None = None) -> Any:
    return SimpleNamespace(
        state=SimpleNamespace(
            account=SimpleNamespace(main_account="acct-main", id="sub-1"),
            ray_id="ray-1",
        ),
        headers=Headers(headers or {}),
        # ObjectWriter is constructed with request.app.state.fs_store; put_simple_stream_full is
        # patched in these tests so the store is never exercised — a sentinel avoids create_fs_store.
        # postgres_pool backs set_object_version_address (drain-direct address write); its execute is
        # a no-op AsyncMock so the address persist neither hits a real DB nor perturbs `pool` acquires.
        app=SimpleNamespace(
            state=SimpleNamespace(fs_store=SimpleNamespace(), postgres_pool=MagicMock(execute=AsyncMock()))
        ),
    )


def _bucket_present_router(method: str, query: str, args: tuple) -> Any:
    q = query or ""
    if "Get bucket by name" in q:
        return {"bucket_id": str(uuid.uuid4()), "bucket_name": "bkt", "main_account_id": "acct-main"}
    # get_object_by_path → no existing object (fresh PUT); user upsert → ignored
    return None


def _bucket_missing_router(method: str, query: str, args: tuple) -> Any:
    return None


class _FakeRedis:
    """Minimal Redis double for the user-seen SET NX cache.

    nx_result mimics redis-py: True when the key was set (first sighting), None when it already
    existed (cache hit). Records calls so tests can assert the cache was consulted.
    """

    def __init__(self, nx_result: Any = True) -> None:
        self.nx_result = nx_result
        self.set_calls: list[tuple[str, bool, Any]] = []

    async def set(self, key: str, value: str, nx: bool = False, ex: Any = None) -> Any:
        self.set_calls.append((key, nx, ex))
        return self.nx_result


def _has_query(pool: Any, needle: str) -> bool:
    return any(needle in (e.get("query") or "") for e in pool.events)


def _patch_writer(monkeypatch: Any, captured: dict[str, Any]) -> None:
    async def fake_put(self: Any, **kw: Any) -> PutResult:
        captured["bucket_id"] = kw["bucket_id"]
        return PutResult(
            object_id=str(uuid.uuid4()),
            etag="etag",
            size_bytes=3,
            upload_id=str(uuid.uuid4()),
            object_version=1,
        )

    async def fake_persist_address(*_a: Any, **_kw: Any) -> None:
        return None

    monkeypatch.setattr(put_object_endpoint.ObjectWriter, "put_simple_stream_full", fake_put)
    monkeypatch.setattr(put_object_endpoint, "set_object_version_address", fake_persist_address)


@pytest.mark.asyncio
async def test_missing_bucket_404_inside_scope() -> None:
    """Missing bucket returns 404 NoSuchBucket; user+bucket resolved in a single acquire and the
    handler returns before any further connection is acquired."""
    pool = make_fake_pool(_bucket_missing_router)
    resp = await handle_put_object(
        bucket_name="nope",
        object_key="k",
        request=_fake_request(),
        pool=pool,
        redis_client=_FakeRedis(),
    )
    assert resp.status_code == 404
    assert b"NoSuchBucket" in bytes(resp.body)
    # Only the user+bucket scope was acquired (we 404'd before the existing-object check).
    assert pool.acquire_count == 1


@pytest.mark.asyncio
async def test_head_lookups_single_acquire(monkeypatch: Any) -> None:
    """user + bucket reads share ONE acquired connection; the existing-object check uses a
    second acquire. Total endpoint acquires for the head = 2 (was 3)."""
    pool = make_fake_pool(_bucket_present_router)

    async def fake_put(self: Any, **kw: Any) -> PutResult:
        return PutResult(
            object_id=str(uuid.uuid4()),
            etag="etag",
            size_bytes=3,
            upload_id=str(uuid.uuid4()),
            object_version=1,
        )

    async def fake_persist_address(*_a: Any, **_kw: Any) -> None:
        return None

    monkeypatch.setattr(put_object_endpoint.ObjectWriter, "put_simple_stream_full", fake_put)
    monkeypatch.setattr(put_object_endpoint, "set_object_version_address", fake_persist_address)

    resp = await handle_put_object(
        bucket_name="bkt",
        object_key="k/o.json",
        request=_fake_request({"Content-Type": "application/json"}),
        pool=pool,
        redis_client=_FakeRedis(),
    )
    assert resp.status_code == 200

    # The two head reads ran on the SAME connection (one acquire).
    fetchrows = [e for e in pool.events if e["method"] == "fetchrow"]
    user_evt = next(
        e for e in fetchrows if "Get or create" in (e["query"] or "") or "users" in (e["query"] or "").lower()
    )
    bucket_evt = next(e for e in fetchrows if "Get bucket by name" in (e["query"] or ""))
    assert user_evt["conn"] == bucket_evt["conn"], "user + bucket did not share one connection"

    # Endpoint acquires (writer is patched out here): user+bucket (1) + existing-object (1)
    # + is_completed-after-enqueue (1) = 3.
    assert pool.acquire_count == 3


@pytest.mark.asyncio
async def test_acquire_timeout_returns_503_not_500() -> None:
    """Pool-saturation (acquire timeout) on the PUT path must surface as a retryable 503 SlowDown,
    not a generic 500 — the endpoint's catch-all must not mask it."""
    pool = make_fake_pool(_bucket_missing_router)

    def _timeout(*, timeout: float | None = None) -> Any:
        raise asyncio.TimeoutError()

    pool.acquire = _timeout  # type: ignore[assignment]

    resp = await handle_put_object(
        bucket_name="bkt",
        object_key="k",
        request=_fake_request(),
        pool=pool,
        redis_client=_FakeRedis(),
    )
    assert resp.status_code == 503
    assert resp.headers.get("x-amz-error-code") == "SlowDown"


@pytest.mark.asyncio
async def test_skips_bucket_lookup_when_forwarded(monkeypatch: Any) -> None:
    """Fix 2: when the gateway forwards X-Hippius-Bucket-Id (request.state.bucket_id), the
    non-append PUT path reuses it and skips its own get_bucket_by_name."""
    pool = make_fake_pool(_bucket_present_router)
    captured: dict[str, Any] = {}
    _patch_writer(monkeypatch, captured)

    fwd_id = str(uuid.uuid4())
    req = _fake_request({"Content-Type": "application/json"})
    req.state.bucket_id = fwd_id

    resp = await handle_put_object(
        bucket_name="bkt",
        object_key="k/o.json",
        request=req,
        pool=pool,
        redis_client=_FakeRedis(nx_result=True),
    )
    assert resp.status_code == 200
    assert not _has_query(pool, "Get bucket by name"), "forwarded bucket_id should skip the API lookup"
    assert captured["bucket_id"] == fwd_id


@pytest.mark.asyncio
async def test_skips_user_upsert_when_cached(monkeypatch: Any) -> None:
    """Fix 5: a Redis SET NX cache-hit (key already present) skips the per-PUT user upsert."""
    pool = make_fake_pool(_bucket_present_router)
    captured: dict[str, Any] = {}
    _patch_writer(monkeypatch, captured)

    redis = _FakeRedis(nx_result=None)  # None == key already existed == cache hit
    resp = await handle_put_object(
        bucket_name="bkt",
        object_key="k/o.json",
        request=_fake_request({"Content-Type": "application/json"}),
        pool=pool,
        redis_client=redis,
    )
    assert resp.status_code == 200
    assert redis.set_calls and redis.set_calls[0][1] is True, "user-seen cache must use SET NX"
    assert not _has_query(pool, "Get or create"), "cached user must not trigger the upsert"


@pytest.mark.asyncio
async def test_runs_user_upsert_on_first_sight(monkeypatch: Any) -> None:
    """Fix 5: the first PUT for an account (SET NX returns truthy) still runs the upsert."""
    pool = make_fake_pool(_bucket_present_router)
    captured: dict[str, Any] = {}
    _patch_writer(monkeypatch, captured)

    resp = await handle_put_object(
        bucket_name="bkt",
        object_key="k/o.json",
        request=_fake_request({"Content-Type": "application/json"}),
        pool=pool,
        redis_client=_FakeRedis(nx_result=True),
    )
    assert resp.status_code == 200
    assert _has_query(pool, "Get or create"), "first sighting must run the user upsert"


@pytest.mark.asyncio
async def test_user_upsert_runs_when_redis_errors(monkeypatch: Any) -> None:
    """Fix 5 fail-open: a Redis outage must not skip the upsert — it falls back to running it."""
    pool = make_fake_pool(_bucket_present_router)
    captured: dict[str, Any] = {}
    _patch_writer(monkeypatch, captured)

    class _ErrRedis:
        async def set(self, *_a: Any, **_k: Any) -> Any:
            raise RedisConnectionError("redis down")

    resp = await handle_put_object(
        bucket_name="bkt",
        object_key="k/o.json",
        request=_fake_request({"Content-Type": "application/json"}),
        pool=pool,
        redis_client=_ErrRedis(),
    )
    assert resp.status_code == 200
    assert _has_query(pool, "Get or create"), "redis error must fall open to running the upsert"


@pytest.mark.asyncio
async def test_no_head_acquire_when_user_cached_and_bucket_forwarded(monkeypatch: Any) -> None:
    """Fix 2 + Fix 5 compose: a known account on a forwarded-bucket PUT does zero head-scope DB
    work. Only the existing-object check and the post-enqueue is_completed update acquire."""
    pool = make_fake_pool(_bucket_present_router)
    captured: dict[str, Any] = {}
    _patch_writer(monkeypatch, captured)

    req = _fake_request({"Content-Type": "application/json"})
    req.state.bucket_id = str(uuid.uuid4())

    resp = await handle_put_object(
        bucket_name="bkt",
        object_key="k/o.json",
        request=req,
        pool=pool,
        redis_client=_FakeRedis(nx_result=None),
    )
    assert resp.status_code == 200
    assert not _has_query(pool, "Get or create")
    assert not _has_query(pool, "Get bucket by name")
    # existing-object check (1) + is_completed-after-enqueue (1); the head user+bucket acquire is gone.
    assert pool.acquire_count == 2
