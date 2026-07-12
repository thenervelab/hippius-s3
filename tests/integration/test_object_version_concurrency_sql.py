"""Concurrency regression for object_version allocation in the upsert queries.

Two concurrent writes to the same (bucket_id, object_key) must not collide on
`object_versions_pkey`. Before the fix, `upsert_object_basic.sql` (and its
multipart / with_cid siblings) allocated the next version with a bare
`MAX(object_version)+1` subquery inside `ON CONFLICT DO UPDATE`. Under READ
COMMITTED that subquery's snapshot can miss a sibling transaction's
just-committed version row, so both writers pick the same `object_version` and
the second one 500s with a duplicate-key violation. The fix allocates
`GREATEST(objects.current_object_version, MAX(...)) + 1` — the row-locked
counter is re-read post-lock (EvalPlanQual), so it always reflects the sibling's
committed bump.

Needs a live Postgres (DATABASE_URL); skips otherwise. The seed rows are
committed (both connections must see them) and removed in a finally.
"""

import asyncio
import os
import uuid
from typing import Any

import asyncpg
import pytest

from hippius_s3.utils import get_query
from hippius_s3.writer.db import upsert_object_basic


async def _connect_or_skip() -> asyncpg.Connection:
    dsn = os.environ.get("DATABASE_URL")
    if not dsn:
        pytest.skip("DATABASE_URL not set; skipping live-schema concurrency check")
    try:
        return await asyncpg.connect(dsn=dsn)
    except (OSError, asyncpg.PostgresError) as exc:
        pytest.skip(f"Postgres unreachable on DATABASE_URL: {exc}")


async def _upsert(conn: asyncpg.Connection, bucket_id: uuid.UUID, key: str) -> Any:
    return await upsert_object_basic(
        conn,
        object_id=str(uuid.uuid4()),
        bucket_id=str(bucket_id),
        object_key=key,
        content_type="application/octet-stream",
        metadata={},
        md5_hash="",
        size_bytes=0,
        storage_version=5,
        upload_backends=["arion"],
    )


async def _seed_bucket(conn: asyncpg.Connection, acct: str, bucket_id: uuid.UUID) -> None:
    await conn.execute("INSERT INTO users(main_account_id) VALUES($1) ON CONFLICT DO NOTHING", acct)
    await conn.execute(
        "INSERT INTO buckets(bucket_id, bucket_name, created_at, main_account_id) VALUES($1, $2, now(), $3)",
        bucket_id,
        f"ovrace-{bucket_id}",
        acct,
    )


async def _cleanup(conn: asyncpg.Connection, bucket_id: uuid.UUID, acct: str) -> None:
    # object_versions.object_id -> objects is ON DELETE CASCADE, so deleting the objects
    # row drops its versions too (the reverse objects -> object_versions FK is RESTRICT and
    # cannot be deferred, so we must delete from the objects side first).
    await conn.execute("DELETE FROM objects WHERE bucket_id = $1", bucket_id)
    await conn.execute("DELETE FROM buckets WHERE bucket_id = $1", bucket_id)
    await conn.execute("DELETE FROM users WHERE main_account_id = $1", acct)


@pytest.mark.asyncio
async def test_upsert_object_queries_prepare_against_live_schema() -> None:
    conn = await _connect_or_skip()
    try:
        for name in ("upsert_object_basic", "upsert_object_multipart", "upsert_object_with_cid"):
            await conn.prepare(get_query(name))
    finally:
        await conn.close()


@pytest.mark.asyncio
async def test_concurrent_same_key_write_does_not_collide_on_version_pk() -> None:
    setup = await _connect_or_skip()
    conn_a = await _connect_or_skip()
    conn_b = await _connect_or_skip()
    acct = f"5OVRACE{uuid.uuid4().hex[:12]}"
    bucket_id = uuid.uuid4()
    key = f"ovrace-{uuid.uuid4()}"
    tr_a = conn_a.transaction()
    task_b: asyncio.Task | None = None
    try:
        await _seed_bucket(setup, acct, bucket_id)

        # Seed version 1 (committed) so both writers take the ON CONFLICT DO UPDATE path.
        first = await _upsert(setup, bucket_id, key)
        object_id = first["object_id"]
        assert first["current_object_version"] == 1

        # A reserves version 2 but does NOT commit yet — it holds the objects row lock.
        await tr_a.start()
        row_a = await _upsert(conn_a, bucket_id, key)
        assert row_a["current_object_version"] == 2

        # B issues the same upsert on a second connection. It takes its READ COMMITTED
        # snapshot now (before A commits) and then blocks on A's objects row lock.
        task_b = asyncio.create_task(_upsert(conn_b, bucket_id, key))
        await asyncio.sleep(0.5)
        assert not task_b.done(), "B should be blocked on A's row lock, not finished"

        # A commits v2; B unblocks. With the old MAX()+1 subquery B's stale snapshot would
        # recompute v2 and raise UniqueViolationError on object_versions_pkey. With
        # GREATEST(current_object_version, MAX)+1 it reads A's committed counter and takes v3.
        await tr_a.commit()
        row_b = await task_b

        assert row_b["object_id"] == object_id
        assert row_b["current_object_version"] == 3

        versions = [
            r["object_version"]
            for r in await setup.fetch(
                "SELECT object_version FROM object_versions WHERE object_id = $1 ORDER BY object_version",
                object_id,
            )
        ]
        assert versions == [1, 2, 3]
        current = await setup.fetchval("SELECT current_object_version FROM objects WHERE object_id = $1", object_id)
        assert current == 3
    finally:
        if task_b is not None and not task_b.done():
            task_b.cancel()
        if conn_a.is_in_transaction():
            await tr_a.rollback()
        await _cleanup(setup, bucket_id, acct)
        await conn_a.close()
        await conn_b.close()
        await setup.close()
