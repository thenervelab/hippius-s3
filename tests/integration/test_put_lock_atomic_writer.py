"""A new version's Object Lock commits together with the version becoming serveable.

A sub-token tier that may DELETE ?versionId= (object_read_write_no_delete, for pruning backups) can
list a fresh version and destroy it permanently if the lock lands in a later transaction. So the
writer must store the lock inside the tail transaction that makes the version serveable. This
drives the real ObjectWriter against real Postgres and, at the moment the lock statement runs,
looks at the version from a SECOND connection: it must still be unserveable there.
"""

from __future__ import annotations

import contextlib
import os
import uuid
from datetime import datetime
from datetime import timedelta
from datetime import timezone
from typing import Any
from typing import AsyncGenerator
from typing import AsyncIterator

import asyncpg
import pytest
import pytest_asyncio

from hippius_s3.cache import FileSystemPartsStore
from hippius_s3.utils import get_query
from hippius_s3.writer import object_writer as writer_mod
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
        lock=("COMPLIANCE", RETAIN_UNTIL, None),
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
