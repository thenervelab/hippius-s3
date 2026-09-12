import asyncio
import uuid
from types import SimpleNamespace
from typing import Any
from unittest.mock import AsyncMock
from unittest.mock import MagicMock

import pytest
from redis.exceptions import ConnectionError as RedisConnectionError
from starlette.datastructures import Headers
from starlette.responses import Response

from hippius_s3.api.s3.extensions import append as append_module
from hippius_s3.api.s3.objects import put_object_endpoint
from hippius_s3.api.s3.objects.put_object_endpoint import handle_put_object
from hippius_s3.writer.types import PreconditionFailed
from hippius_s3.writer.types import PutResult
from tests.unit._fake_pool import make_fake_pool


def _fake_request(headers: dict[str, str] | None = None) -> Any:
    return SimpleNamespace(
        state=SimpleNamespace(
            account=SimpleNamespace(main_account="acct-main", id="sub-1"),
            main_account_id="acct-main",
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


def _patch_writer(monkeypatch: Any, captured: dict[str, Any], object_version: int = 1) -> None:
    async def fake_put(self: Any, **kw: Any) -> PutResult:
        captured["bucket_id"] = kw["bucket_id"]
        return PutResult(
            object_id=str(uuid.uuid4()),
            etag="etag",
            size_bytes=3,
            upload_id=str(uuid.uuid4()),
            object_version=object_version,
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
    """user + bucket reads share ONE acquired connection. With the existing-object pre-check
    removed (WU-3), total endpoint acquires = 2 (user+bucket, then is_completed-after-enqueue)."""
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

    # Endpoint acquires (writer is patched out here): user+bucket (1) + is_completed-after-enqueue (1) = 2.
    assert pool.acquire_count == 2


@pytest.mark.asyncio
async def test_no_existing_object_precheck_query(monkeypatch: Any) -> None:
    """WU-3: PUT no longer issues the get_object_by_path existing-object pre-check. It always passes
    a fresh candidate UUID and trusts upsert_object_basic's `ON CONFLICT ... RETURNING object_id`."""
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
    assert not _has_query(pool, "Get object by bucket and key path"), "existing-object pre-check must be gone"


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
    work. With the existing-object pre-check gone (WU-3), only the post-enqueue is_completed update acquires."""
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
    # Only is_completed-after-enqueue (1); the head user+bucket acquire and the existing-object
    # pre-check acquire are both gone.
    assert pool.acquire_count == 1


@pytest.mark.asyncio
@pytest.mark.parametrize("object_version,flag_set", [(1, True), (2, False)])
async def test_created_flag_tracks_allocated_version(monkeypatch: Any, object_version: int, flag_set: bool) -> None:
    """Version 1 (fresh objects row) sets request.state.ats_object_created so the gateway's
    ats_purge middleware skips the creation purge; an overwrite (version >= 2) must leave it
    unset — absent-flag-means-purge is the fail-safe direction."""
    pool = make_fake_pool(_bucket_present_router)
    _patch_writer(monkeypatch, {}, object_version=object_version)

    req = _fake_request({"Content-Type": "text/plain"})
    resp = await handle_put_object(
        bucket_name="bkt",
        object_key="fresh.txt",
        request=req,
        pool=pool,
        redis_client=_FakeRedis(),
    )
    assert resp.status_code == 200
    assert resp.headers.get("x-amz-version-id") == str(object_version)
    assert getattr(req.state, "ats_object_created", False) is flag_set


def _patch_writer_capture(monkeypatch: Any, captured: dict[str, Any], raise_exc: Exception | None = None) -> None:
    async def fake_put(self: Any, **kw: Any) -> PutResult:
        captured.update(kw)
        if raise_exc is not None:
            raise raise_exc
        return PutResult(
            object_id=str(uuid.uuid4()), etag="etag", size_bytes=3, upload_id=str(uuid.uuid4()), object_version=1
        )

    async def fake_persist_address(*_a: Any, **_kw: Any) -> None:
        return None

    monkeypatch.setattr(put_object_endpoint.ObjectWriter, "put_simple_stream_full", fake_put)
    monkeypatch.setattr(put_object_endpoint, "set_object_version_address", fake_persist_address)


async def _put(monkeypatch: Any, headers: dict[str, str], raise_exc: Exception | None = None) -> Any:
    captured: dict[str, Any] = {}
    _patch_writer_capture(monkeypatch, captured, raise_exc)
    req = _fake_request(headers)
    req.state.bucket_id = str(uuid.uuid4())
    resp = await handle_put_object(
        bucket_name="bkt",
        object_key="audit.log",
        request=req,
        pool=make_fake_pool(_bucket_present_router),
        redis_client=_FakeRedis(nx_result=None),
    )
    return resp, captured


@pytest.mark.asyncio
async def test_if_none_match_star_makes_the_write_create_only(monkeypatch: Any) -> None:
    resp, captured = await _put(monkeypatch, {"If-None-Match": "*"})
    assert resp.status_code == 200
    assert captured["if_none_match"] is True


@pytest.mark.asyncio
async def test_no_if_none_match_is_an_ordinary_overwrite(monkeypatch: Any) -> None:
    resp, captured = await _put(monkeypatch, {})
    assert resp.status_code == 200
    assert captured["if_none_match"] is False


@pytest.mark.asyncio
async def test_existing_key_is_precondition_failed(monkeypatch: Any) -> None:
    resp, _ = await _put(monkeypatch, {"If-None-Match": "*"}, raise_exc=PreconditionFailed())
    assert resp.status_code == 412
    assert b"<Code>PreconditionFailed</Code>" in resp.body
    assert b"<Condition>If-None-Match</Condition>" in resp.body


@pytest.mark.asyncio
async def test_etag_valued_if_none_match_is_not_implemented_rather_than_ignored(monkeypatch: Any) -> None:
    resp, captured = await _put(monkeypatch, {"If-None-Match": '"5d41402abc4b2a76b9719d911017c592"'})
    assert resp.status_code == 501
    assert b"<Code>NotImplemented</Code>" in resp.body
    assert captured == {}, "the write must not run"


@pytest.mark.asyncio
async def test_create_only_append_is_judged_inside_append_not_by_a_loose_read(monkeypatch: Any) -> None:
    # The endpoint must NOT pre-check on its own connection: a key created between such a read and
    # the append would be modified under a create-only header. append_stream decides under the same
    # row lock as the version CAS instead, so all the endpoint does is carry the flag down.
    seen_queries: list[str] = []

    def router(method: str, query: str, args: tuple) -> Any:
        seen_queries.append(query or "")
        if "Get bucket by name" in (query or ""):
            return {"bucket_id": str(uuid.uuid4()), "bucket_name": "bkt", "main_account_id": "acct-main"}
        return None

    append_calls: list[Any] = []

    async def fake_append(*a: Any, **kw: Any) -> Any:
        append_calls.append(kw)
        return Response(status_code=200)

    monkeypatch.setattr(put_object_endpoint, "handle_append", fake_append)
    resp = await handle_put_object(
        bucket_name="bkt",
        object_key="audit.log",
        request=_fake_request({"If-None-Match": "*", "x-amz-meta-append": "true"}),
        pool=make_fake_pool(router),
        redis_client=_FakeRedis(nx_result=None),
    )
    assert resp.status_code == 200
    assert append_calls[0]["if_none_match"] is True
    assert not any("exists_live" in q for q in seen_queries), "no unlocked pre-check may run"


class _BodyStream:
    """request.stream() stand-in that records how much of the body the endpoint read."""

    def __init__(self, *chunks: bytes) -> None:
        self.chunks = list(chunks)
        self.read: list[bytes] = []

    def __call__(self) -> Any:
        async def gen() -> Any:
            for c in self.chunks:
                self.read.append(c)
                yield c

        return gen()


@pytest.mark.asyncio
async def test_unsupported_if_none_match_drains_the_body_before_answering(monkeypatch: Any) -> None:
    """An early answer with the body still pending poisons the kept-alive connection: the client's
    next request on it fails with a bare 400 (seen in e2e as a GET after a refused PUT)."""
    captured: dict[str, Any] = {}
    _patch_writer_capture(monkeypatch, captured)
    req = _fake_request({"If-None-Match": "etag"})
    req.stream = body = _BodyStream(b"part-1", b"part-2")
    resp = await handle_put_object("bkt", "k", req, make_fake_pool(_bucket_present_router), _FakeRedis(nx_result=None))
    assert resp.status_code == 501
    assert body.read == [b"part-1", b"part-2"]


@pytest.mark.asyncio
async def test_reserve_time_refusal_drains_the_body_before_answering(monkeypatch: Any) -> None:
    captured: dict[str, Any] = {}
    _patch_writer_capture(monkeypatch, captured, raise_exc=PreconditionFailed())
    req = _fake_request({"If-None-Match": "*"})
    req.state.bucket_id = str(uuid.uuid4())
    req.stream = body = _BodyStream(b"unread body")
    resp = await handle_put_object("bkt", "k", req, make_fake_pool(_bucket_present_router), _FakeRedis(nx_result=None))
    assert resp.status_code == 412
    assert body.read == [b"unread body"]


@pytest.mark.asyncio
async def test_create_only_append_on_an_existing_key_is_412_with_the_body_drained(monkeypatch: Any) -> None:
    # handle_append's own `finally: _drain(body_iter)` covers the refusal, so the connection stays
    # reusable even though the writer rejected the append before consuming the delta.
    read: list[bytes] = []

    async def body_iter() -> Any:
        for chunk in (b"delta-1", b"delta-2"):
            read.append(chunk)
            yield chunk

    async def refuse(self: Any, **kw: Any) -> Any:
        assert kw["if_none_match"] is True
        raise PreconditionFailed()

    monkeypatch.setattr(append_module.ObjectWriter, "append_stream", refuse)
    req = _fake_request({"If-None-Match": "*", "x-amz-meta-append": "true", "x-amz-meta-append-if-version": "3"})
    resp = await append_module.handle_append(
        req,
        make_fake_pool(lambda *a: None),
        _FakeRedis(nx_result=None),
        bucket={"bucket_id": str(uuid.uuid4())},
        bucket_id=str(uuid.uuid4()),
        bucket_name="bkt",
        object_key="audit.log",
        body_iter=body_iter(),
        if_none_match=True,
    )
    assert resp.status_code == 412
    assert b"<Code>PreconditionFailed</Code>" in resp.body
    assert read == [b"delta-1", b"delta-2"]
