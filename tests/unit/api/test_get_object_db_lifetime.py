"""GET must not hold its pooled DB connection across the first-chunk wait.

Observed 2026-09-08: ~60 cold reads (each waiting up to stream_first_chunk_timeout_seconds on a
backend fetch) landed on one api-local pod and pinned every slot of its 4x15 pool. Unrelated PUTs
routed to that pod then timed out on acquire and returned 503 SlowDown, while the other pods sat
idle. The endpoint now holds the connection only through build_stream_context, and acquires it
with the same bound the PUT path uses.
"""

from __future__ import annotations

import asyncio
from types import SimpleNamespace
from typing import Any

import pytest

from hippius_s3.api.s3.objects import get_object_endpoint
from hippius_s3.models.account import HippiusAccount
from hippius_s3.services.object_reader import StreamContext


ACCOUNT = "5OwnerMainAccountSS58"


class _Conn:
    async def fetchrow(self, query: str, *args: Any) -> Any:
        if query == "get_or_create_user_by_main_account":
            return {"id": "user-1"}
        if query == "get_object_for_download_with_permissions":
            return {
                "object_id": "obj-1",
                "bucket_id": "bkt-1",
                "bucket_name": "bucket",
                "object_version": 1,
                "storage_version": 3,
                "size_bytes": 10,
                "multipart": False,
                "download_chunks": None,
                "content_type": "application/octet-stream",
                "created_at": "2026-01-01T00:00:00Z",
                "md5_hash": "d41d8cd98f00b204e9800998ecf8427e",
                "metadata": None,
                "bucket_owner_id": ACCOUNT,
                "encryption_version": None,
                "enc_suite_id": None,
                "enc_chunk_size_bytes": None,
                "kek_id": None,
                "wrapped_dek": None,
            }
        return None

    async def fetchval(self, *_: Any) -> Any:
        return True


class _Pool:
    """Records whether the one connection is checked out, so a test can ask at any point."""

    def __init__(self, *, acquire_hangs: bool = False) -> None:
        self._conn = _Conn()
        self._acquire_hangs = acquire_hangs
        self.held = False
        self.releases = 0

    async def acquire(self, timeout: float | None = None) -> _Conn:  # noqa: ASYNC109 (mirrors asyncpg pool.acquire)
        if self._acquire_hangs:
            # asyncpg's pool.acquire(timeout=...) raises asyncio.TimeoutError when no slot frees up.
            await asyncio.sleep(0)
            raise asyncio.TimeoutError()
        assert not self.held, "acquired twice"
        self.held = True
        return self._conn

    async def release(self, _conn: Any) -> None:
        assert self.held, "released without acquire"
        self.held = False
        self.releases += 1


def _request() -> Any:
    return SimpleNamespace(
        state=SimpleNamespace(
            account=HippiusAccount(id=ACCOUNT, main_account=ACCOUNT, has_credits=True, upload=False, delete=False),
            main_account_id=ACCOUNT,
            ray_id="ray-1",
        ),
        query_params={},
        headers={},
        app=SimpleNamespace(state=SimpleNamespace(redis_client=object(), obj_cache=object())),
    )


def _ctx() -> StreamContext:
    return StreamContext(
        plan=[],
        object_version=1,
        storage_version=3,
        source="cache",
        key_bytes=None,
        suite_id=None,
        bucket_id="bkt-1",
        upload_id="",
    )


@pytest.mark.asyncio
async def test_connection_is_released_before_the_first_chunk_wait(monkeypatch: Any) -> None:
    """build_stream_context still owns the connection; read_response (which contains the
    first-chunk wait) must run with it already back in the pool."""
    monkeypatch.setattr(get_object_endpoint, "get_query", lambda name: name)
    monkeypatch.setattr(get_object_endpoint, "require_supported_storage_version", lambda v: v)

    pool = _Pool()
    seen: dict[str, bool] = {}

    async def _fake_build_stream_context(db: Any, *args: Any, **kwargs: Any) -> StreamContext:
        seen["held_during_build"] = pool.held
        assert db is pool._conn
        return _ctx()

    async def _fake_read_response(**kwargs: Any) -> Any:
        seen["held_during_read"] = pool.held
        assert kwargs["ctx"] is not None
        assert "db" not in kwargs, "read_response must not receive the pooled connection"
        return SimpleNamespace(status_code=200, headers={})

    monkeypatch.setattr("hippius_s3.services.object_reader.build_stream_context", _fake_build_stream_context)
    monkeypatch.setattr("hippius_s3.services.object_reader.read_response", _fake_read_response)

    response = await get_object_endpoint.handle_get_object("bucket", "key.txt", _request(), pool, redis_client=object())

    assert response.status_code == 200
    assert seen["held_during_build"] is True
    assert seen["held_during_read"] is False
    assert pool.releases == 1
    assert pool.held is False


@pytest.mark.asyncio
async def test_saturated_pool_returns_503_slowdown_not_a_hang(monkeypatch: Any) -> None:
    """Same contract as the PUT path: acquire is bounded by db_pool_acquire_timeout and a
    timeout maps to a retryable 503 SlowDown with Retry-After, never a 500 and never a queue."""
    monkeypatch.setattr(get_object_endpoint, "get_query", lambda name: name)

    async def _must_not_run(*args: Any, **kwargs: Any) -> Any:
        raise AssertionError("no stream work may start without a connection")

    monkeypatch.setattr("hippius_s3.services.object_reader.build_stream_context", _must_not_run)
    monkeypatch.setattr("hippius_s3.services.object_reader.read_response", _must_not_run)

    response = await get_object_endpoint.handle_get_object(
        "bucket", "key.txt", _request(), _Pool(acquire_hangs=True), redis_client=object()
    )

    assert response.status_code == 503
    assert response.headers.get("Retry-After") == "3"
    assert b"SlowDown" in bytes(response.body)


@pytest.mark.asyncio
async def test_error_inside_the_db_block_still_releases_the_connection(monkeypatch: Any) -> None:
    """The context manager, not a trailing finally, owns the release: a failure while the
    connection is held returns it exactly once and the endpoint still answers."""
    monkeypatch.setattr(get_object_endpoint, "get_query", lambda name: name)
    monkeypatch.setattr(get_object_endpoint, "require_supported_storage_version", lambda v: v)

    pool = _Pool()

    async def _boom(*args: Any, **kwargs: Any) -> Any:
        raise RuntimeError("catalog exploded")

    monkeypatch.setattr("hippius_s3.services.object_reader.build_stream_context", _boom)

    response = await get_object_endpoint.handle_get_object("bucket", "key.txt", _request(), pool, redis_client=object())

    assert response.status_code == 500
    assert pool.releases == 1
    assert pool.held is False
