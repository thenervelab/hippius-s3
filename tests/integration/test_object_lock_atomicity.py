"""Object Lock writes are atomic with what they protect.

1. A new version's lock commits together with the version becoming serveable.

A sub-token tier that may DELETE ?versionId= (object_read_write_no_delete, for pruning backups) can
list a fresh version and destroy it permanently if the lock lands in a later transaction. So the
writer must store the lock inside the tail transaction that makes the version serveable. This
drives the real ObjectWriter against real Postgres and, at the moment the lock statement runs,
looks at the version from a SECOND connection: it must still be unserveable there.

2. A retention or legal-hold write that loses a race to a version delete answers 404, not a 200
   for a lock that landed nowhere.
"""

from __future__ import annotations

import contextlib
import os
import uuid
from datetime import datetime
from datetime import timedelta
from datetime import timezone
from types import SimpleNamespace
from typing import Any
from typing import AsyncGenerator
from typing import AsyncIterator

import asyncpg
import pytest
import pytest_asyncio

from hippius_s3.api.s3.objects.object_lock_endpoints import handle_put_object_legal_hold
from hippius_s3.api.s3.objects.object_lock_endpoints import handle_put_object_retention
from hippius_s3.cache import FileSystemPartsStore
from hippius_s3.utils import get_query
from hippius_s3.writer import object_writer as writer_mod
from hippius_s3.writer.db import unserve_version_after_address_failure
from hippius_s3.writer.object_writer import ObjectWriter


pytestmark = pytest.mark.asyncio

_DB_URL = os.getenv("DATABASE_URL", "postgresql://postgres:postgres@localhost:5432/hippius?sslmode=disable")
_LOCK_SQL = get_query("set_object_version_lock")
RETAIN_UNTIL = (datetime.now(timezone.utc) + timedelta(days=3)).replace(microsecond=0)


class _NoRedis:
    async def delete(self, *_a: Any, **_k: Any) -> int:
        return 0

    async def setex(self, *_a: Any, **_k: Any) -> None:
        return None

    async def set(self, *_a: Any, **_k: Any) -> None:
        return None


class _ObservedConn:
    """Delegates to the real connection; runs `on_lock` just before the lock statement executes."""

    def __init__(self, conn: Any, on_lock: Any) -> None:
        self._conn = conn
        self._on_lock = on_lock

    def __getattr__(self, name: str) -> Any:
        return getattr(self._conn, name)

    async def execute(self, sql: str, *args: Any) -> Any:
        if sql == _LOCK_SQL:
            await self._on_lock(args[0], args[1])
        return await self._conn.execute(sql, *args)


@pytest_asyncio.fixture
async def env(tmp_path: Any, monkeypatch: Any) -> AsyncGenerator[dict[str, Any], None]:
    try:
        pool = await asyncpg.create_pool(_DB_URL, min_size=1, max_size=5)
    except OSError as exc:  # only an unreachable server is a legitimate skip
        pytest.skip(f"postgres unavailable: {exc}")

    async def fake_kek(*, bucket_id: str) -> tuple[Any, bytes]:
        return uuid.uuid4(), b"\x01" * 32

    monkeypatch.setattr("hippius_s3.services.kek_service.get_or_create_active_bucket_kek", fake_kek)

    account = f"5LCKW{uuid.uuid4().hex[:12]}"
    bucket_id = uuid.uuid4()
    bucket_name = f"lockw-{uuid.uuid4().hex[:10]}"
    await pool.execute("INSERT INTO users (main_account_id, created_at) VALUES ($1, now())", account)
    await pool.execute(
        "INSERT INTO buckets (bucket_id, bucket_name, main_account_id, created_at) VALUES ($1, $2, $3, now())",
        bucket_id,
        bucket_name,
        account,
    )
    writer = ObjectWriter(pool=pool, redis_client=_NoRedis(), fs_store=FileSystemPartsStore(str(tmp_path)))
    try:
        yield {"pool": pool, "writer": writer, "bucket_id": str(bucket_id), "bucket_name": bucket_name}
    finally:
        async with pool.acquire() as conn, conn.transaction():
            rows = await conn.fetch("SELECT object_id FROM objects WHERE bucket_id = $1", bucket_id)
            ids = [str(r["object_id"]) for r in rows]
            await conn.execute("DELETE FROM fs_cache_inventory WHERE object_id::text = ANY($1::text[])", ids)
            await conn.execute("DELETE FROM parts WHERE object_id::text = ANY($1::text[])", ids)
            await conn.execute("DELETE FROM multipart_uploads WHERE bucket_id = $1", bucket_id)
            await conn.execute("DELETE FROM objects WHERE bucket_id = $1", bucket_id)
            await conn.execute("DELETE FROM object_versions WHERE object_id::text = ANY($1::text[])", ids)
            await conn.execute("DELETE FROM buckets WHERE bucket_id = $1", bucket_id)
            await conn.execute("DELETE FROM users WHERE main_account_id = $1", account)
        await pool.close()


async def _body(data: bytes) -> AsyncIterator[bytes]:
    yield data


async def _version_row(pool: Any, object_id: Any, object_version: int) -> Any:
    return await pool.fetchrow(
        "SELECT size_bytes, md5_hash, object_lock_mode, object_lock_retain_until, object_lock_legal_hold "
        "FROM object_versions WHERE object_id = $1::uuid AND object_version = $2",
        str(object_id),
        int(object_version),
    )


async def test_the_lock_commits_with_the_version_becoming_serveable(env: dict[str, Any], monkeypatch: Any) -> None:
    pool = env["pool"]
    seen_from_outside: list[Any] = []

    async def _look_from_outside(object_id: Any, object_version: int) -> None:
        # A separate connection: it sees only what has COMMITTED.
        seen_from_outside.append(await _version_row(pool, object_id, object_version))

    real_acquire = writer_mod.acquire_with_timeout

    @contextlib.asynccontextmanager
    async def _observed_acquire(*a: Any, **k: Any) -> AsyncIterator[Any]:
        async with real_acquire(*a, **k) as conn:
            yield _ObservedConn(conn, _look_from_outside)

    monkeypatch.setattr(writer_mod, "acquire_with_timeout", _observed_acquire)

    res = await env["writer"].put_simple_stream_full(
        bucket_id=env["bucket_id"],
        bucket_name=env["bucket_name"],
        object_id=str(uuid.uuid4()),
        object_key="backups/vm-1/manifest.json",
        account_address="acct",
        content_type="application/json",
        metadata={},
        body_iter=_body(b'{"seq": 1}'),
        lock=lambda: ("COMPLIANCE", RETAIN_UNTIL, False),
    )

    assert len(seen_from_outside) == 1, "the writer did not store the lock itself"
    before = seen_from_outside[0]
    assert int(before["size_bytes"]) == 0 and not before["md5_hash"], (
        "the version was already serveable to other sessions before its lock was written"
    )

    after = await _version_row(pool, res.object_id, res.object_version)
    assert int(after["size_bytes"]) == len(b'{"seq": 1}')
    assert after["object_lock_mode"] == "COMPLIANCE"
    assert after["object_lock_retain_until"] == RETAIN_UNTIL


async def test_no_lock_leaves_the_lock_columns_untouched(env: dict[str, Any]) -> None:
    res = await env["writer"].put_simple_stream_full(
        bucket_id=env["bucket_id"],
        bucket_name=env["bucket_name"],
        object_id=str(uuid.uuid4()),
        object_key="plain",
        account_address="acct",
        content_type="application/octet-stream",
        metadata={},
        body_iter=_body(b"x"),
    )
    row = await _version_row(env["pool"], res.object_id, res.object_version)
    assert row["object_lock_mode"] is None and row["object_lock_retain_until"] is None
    assert not row["object_lock_legal_hold"]


class _DeleteRacingConn:
    """Delegates to the real connection. When the handler takes the objects row lock, a DELETE
    ?versionId= has just committed first: the version is tombstoned from a second connection."""

    def __init__(self, conn: Any, pool: Any, object_id: str, object_version: int) -> None:
        self._conn = conn
        self._pool = pool
        self._target = (object_id, object_version)

    def __getattr__(self, name: str) -> Any:
        return getattr(self._conn, name)

    async def execute(self, sql: str, *args: Any) -> Any:
        if sql == get_query("lock_object_row_for_update"):
            await self._pool.execute(
                "UPDATE object_versions SET deleted_at = now() WHERE object_id = $1::uuid AND object_version = $2",
                *self._target,
            )
        return await self._conn.execute(sql, *args)


def _lock_request() -> Any:
    return SimpleNamespace(
        headers={},
        state=SimpleNamespace(
            bucket_object_lock={"enabled": True},
            account=SimpleNamespace(main_account="acct"),
            bucket_owner_id="acct",
        ),
    )


_RETENTION_BODY = (
    f"<Retention><Mode>GOVERNANCE</Mode><RetainUntilDate>{RETAIN_UNTIL.strftime('%Y-%m-%dT%H:%M:%SZ')}"
    "</RetainUntilDate></Retention>"
).encode()


@pytest.mark.parametrize("which", ["retention", "legal-hold"])
async def test_a_lock_write_that_loses_to_a_version_delete_is_a_404(env: dict[str, Any], which: str) -> None:
    pool = env["pool"]
    res = await env["writer"].put_simple_stream_full(
        bucket_id=env["bucket_id"],
        bucket_name=env["bucket_name"],
        object_id=str(uuid.uuid4()),
        object_key="k",
        account_address="acct",
        content_type="application/octet-stream",
        metadata={},
        body_iter=_body(b"x"),
    )
    async with pool.acquire() as conn:
        racing = _DeleteRacingConn(conn, pool, res.object_id, res.object_version)
        if which == "retention":
            resp = await handle_put_object_retention(
                uuid.UUID(env["bucket_id"]), "k", res.object_version, _lock_request(), racing, _RETENTION_BODY
            )
        else:
            resp = await handle_put_object_legal_hold(
                uuid.UUID(env["bucket_id"]),
                "k",
                res.object_version,
                _lock_request(),
                racing,
                b"<LegalHold><Status>ON</Status></LegalHold>",
            )

    assert resp.status_code == 404, "a lock that landed on nothing was reported as stored"
    row = await _version_row(pool, res.object_id, res.object_version)
    assert row["object_lock_mode"] is None and not row["object_lock_legal_hold"]


async def test_a_lock_write_on_a_live_version_is_stored(env: dict[str, Any]) -> None:
    res = await env["writer"].put_simple_stream_full(
        bucket_id=env["bucket_id"],
        bucket_name=env["bucket_name"],
        object_id=str(uuid.uuid4()),
        object_key="k",
        account_address="acct",
        content_type="application/octet-stream",
        metadata={},
        body_iter=_body(b"x"),
    )
    async with env["pool"].acquire() as conn:
        resp = await handle_put_object_retention(
            uuid.UUID(env["bucket_id"]), "k", res.object_version, _lock_request(), conn, _RETENTION_BODY
        )
    assert resp.status_code == 200
    row = await _version_row(env["pool"], res.object_id, res.object_version)
    assert row["object_lock_mode"] == "GOVERNANCE"


_PARTS = [{"part_number": 1, "etag": "ab" * 16, "size_bytes": 5}]


class _ObservedAcquire:
    def __init__(self, pool: Any, on_lock: Any) -> None:
        self._pool = pool
        self._on_lock = on_lock
        self._ctx: Any = None

    async def __aenter__(self) -> Any:
        self._ctx = self._pool.acquire()
        conn = await self._ctx.__aenter__()
        return _ObservedConn(conn, self._on_lock)

    async def __aexit__(self, *exc: Any) -> Any:
        return await self._ctx.__aexit__(*exc)


class _ObservedPool:
    """pool.acquire() wrapper. mpu_complete checks out its own connection, not acquire_with_timeout."""

    def __init__(self, pool: Any, on_lock: Any) -> None:
        self._pool = pool
        self._on_lock = on_lock

    def acquire(self) -> _ObservedAcquire:
        return _ObservedAcquire(self._pool, self._on_lock)

    def __getattr__(self, name: str) -> Any:
        return getattr(self._pool, name)


async def _reserved(env: dict[str, Any], *, key: str) -> Any:
    """A simple PUT's version, put back in the reserved shape, with its upload still open."""
    res = await env["writer"].put_simple_stream_full(
        bucket_id=env["bucket_id"],
        bucket_name=env["bucket_name"],
        object_id=str(uuid.uuid4()),
        object_key=key,
        account_address="acct",
        content_type="application/octet-stream",
        metadata={},
        body_iter=_body(b"x"),
    )
    await env["pool"].execute(
        "UPDATE object_versions SET size_bytes = 0, md5_hash = '' WHERE object_id = $1::uuid AND object_version = $2",
        res.object_id,
        res.object_version,
    )
    return res


async def _complete(env: dict[str, Any], res: Any, *, key: str, lock: Any, on_lock: Any = None) -> None:
    writer = env["writer"]
    original = writer.pool
    if on_lock is not None:
        writer.pool = _ObservedPool(original, on_lock)
    try:
        await writer.mpu_complete(
            bucket_name=env["bucket_name"],
            object_id=res.object_id,
            object_key=key,
            upload_id=res.upload_id,
            object_version=res.object_version,
            address="acct",
            db_parts=_PARTS,
            lock=lock,
        )
    finally:
        writer.pool = original


async def test_mpu_complete_stores_the_default_lock_before_the_version_is_visible(
    env: dict[str, Any],
) -> None:
    """Bucket-default retention is chosen while other sessions still see a reserved version.

    Fails if the lock statement runs after the size commit (the second connection would already
    see the bytes) or if complete ignores the callback and leaves the version unlocked.
    """
    key = "backups/vm-1/disk.raw"
    res = await _reserved(env, key=key)
    seen: list[Any] = []
    chosen = (datetime.now(timezone.utc) + timedelta(days=3)).replace(microsecond=0)

    async def _look(_object_id: Any, _object_version: int) -> None:
        seen.append(await _version_row(env["pool"], res.object_id, res.object_version))

    await _complete(env, res, key=key, lock=lambda: ("COMPLIANCE", chosen, False), on_lock=_look)

    assert len(seen) == 1, "complete did not write the lock itself"
    assert int(seen[0]["size_bytes"]) == 0 and not seen[0]["md5_hash"]
    assert seen[0]["object_lock_mode"] is None
    row = await _version_row(env["pool"], res.object_id, res.object_version)
    assert int(row["size_bytes"]) == 5
    assert row["object_lock_mode"] == "COMPLIANCE"
    assert row["object_lock_retain_until"] == chosen


async def test_mpu_complete_does_not_overwrite_an_explicit_lock_or_clear_a_hold(env: dict[str, Any]) -> None:
    """Headers stored at initiate are an absolute date. The default applied at complete must not
    replace them, and the legal_hold argument of that default (False) must not clear a hold."""
    key = "backups/vm-1/explicit.raw"
    res = await _reserved(env, key=key)
    explicit = (datetime.now(timezone.utc) + timedelta(days=30)).replace(microsecond=0)
    await env["pool"].execute(
        "UPDATE object_versions SET object_lock_mode = 'COMPLIANCE', object_lock_retain_until = $3, "
        "object_lock_legal_hold = true WHERE object_id = $1::uuid AND object_version = $2",
        res.object_id,
        res.object_version,
        explicit,
    )
    later = explicit + timedelta(days=1)

    def _should_not_be_stored() -> tuple[str, datetime, bool]:
        return "GOVERNANCE", later, False

    await _complete(env, res, key=key, lock=_should_not_be_stored)
    row = await _version_row(env["pool"], res.object_id, res.object_version)
    assert row["object_lock_mode"] == "COMPLIANCE"
    assert row["object_lock_retain_until"] == explicit
    assert row["object_lock_legal_hold"] is True
    assert int(row["size_bytes"]) == 5


async def test_mpu_complete_keeps_a_legal_hold_when_it_applies_the_default(env: dict[str, Any]) -> None:
    key = "backups/vm-1/held.raw"
    res = await _reserved(env, key=key)
    await env["pool"].execute(
        "UPDATE object_versions SET object_lock_legal_hold = true WHERE object_id = $1::uuid AND object_version = $2",
        res.object_id,
        res.object_version,
    )
    chosen = (datetime.now(timezone.utc) + timedelta(days=2)).replace(microsecond=0)
    await _complete(env, res, key=key, lock=lambda: ("GOVERNANCE", chosen, False))
    row = await _version_row(env["pool"], res.object_id, res.object_version)
    assert row["object_lock_mode"] == "GOVERNANCE"
    assert row["object_lock_retain_until"] == chosen
    assert row["object_lock_legal_hold"] is True


async def test_address_failure_revert_keeps_the_lock(env: dict[str, Any]) -> None:
    res = await env["writer"].put_simple_stream_full(
        bucket_id=env["bucket_id"],
        bucket_name=env["bucket_name"],
        object_id=str(uuid.uuid4()),
        object_key="backups/vm-1/manifest.json",
        account_address="acct",
        content_type="application/json",
        metadata={},
        body_iter=_body(b'{"seq": 1}'),
        lock=lambda: ("COMPLIANCE", RETAIN_UNTIL, True),
    )
    async with env["pool"].acquire() as conn:
        await unserve_version_after_address_failure(conn, object_id=res.object_id, object_version=res.object_version)
    row = await _version_row(env["pool"], res.object_id, res.object_version)
    assert int(row["size_bytes"]) == 0 and row["md5_hash"] == ""
    assert row["object_lock_mode"] == "COMPLIANCE"
    assert row["object_lock_retain_until"] == RETAIN_UNTIL
    assert row["object_lock_legal_hold"] is True
