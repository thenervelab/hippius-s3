import asyncio
import uuid
from types import SimpleNamespace
from typing import Any

import pytest
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
        app=SimpleNamespace(state=SimpleNamespace(fs_store=SimpleNamespace())),
    )


def _bucket_present_router(method: str, query: str, args: tuple) -> Any:
    q = query or ""
    if "Get bucket by name" in q:
        return {"bucket_id": str(uuid.uuid4()), "bucket_name": "bkt", "main_account_id": "acct-main"}
    # get_object_by_path → no existing object (fresh PUT); user upsert → ignored
    return None


def _bucket_missing_router(method: str, query: str, args: tuple) -> Any:
    return None


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
        redis_client=SimpleNamespace(),
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

    async def fake_enqueue(**_kw: Any) -> None:
        return None

    monkeypatch.setattr(put_object_endpoint.ObjectWriter, "put_simple_stream_full", fake_put)
    monkeypatch.setattr(put_object_endpoint, "writer_enqueue_upload", fake_enqueue)

    resp = await handle_put_object(
        bucket_name="bkt",
        object_key="k/o.json",
        request=_fake_request({"Content-Type": "application/json"}),
        pool=pool,
        redis_client=SimpleNamespace(),
    )
    assert resp.status_code == 200

    # The two head reads ran on the SAME connection (one acquire).
    fetchrows = [e for e in pool.events if e["method"] == "fetchrow"]
    user_evt = next(
        e for e in fetchrows if "Get or create" in (e["query"] or "") or "users" in (e["query"] or "").lower()
    )
    bucket_evt = next(e for e in fetchrows if "Get bucket by name" in (e["query"] or ""))
    assert user_evt["conn"] == bucket_evt["conn"], "user + bucket did not share one connection"

    # existing-object check used a separate acquire → 2 endpoint acquires for the head.
    assert pool.acquire_count == 2


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
        redis_client=SimpleNamespace(),
    )
    assert resp.status_code == 503
    assert resp.headers.get("x-amz-error-code") == "SlowDown"
