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

That GREATEST floor is NOT a full guarantee, though: create_migration_version.sql
inserts a version WITHOUT bumping current_object_version, and the MAX() floor is
itself snapshot-stale under READ COMMITTED, so a migrator racing a write to the
same object can still collide. The writer retries on object_versions_pkey to
cover that residual case; the second test below pins both the residual collision
and that a fresh transaction (what the retry does) resolves it.

Needs a live Postgres (DATABASE_URL); skips otherwise. The seed rows are
committed (both connections must see them) and removed in a finally.
"""

import asyncio
import json
import os
import uuid
from datetime import datetime
from datetime import timezone
from typing import Any

import asyncpg
import pytest

from hippius_s3.utils import get_query
from hippius_s3.writer.db import retry_on_object_version_conflict
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


@pytest.mark.asyncio
async def test_migration_version_race_collides_and_retry_resolves_it() -> None:
    """A create_migration_version committed while an upsert is blocked still collides
    (the GREATEST floor's MAX() is snapshot-stale), and a fresh transaction — what the
    writer's retry-on-object_versions_pkey does — resolves it. Guards the writer retry."""
    setup = await _connect_or_skip()
    conn_m = await _connect_or_skip()
    conn_p = await _connect_or_skip()
    acct = f"5MIGRACE{uuid.uuid4().hex[:11]}"
    bucket_id = uuid.uuid4()
    key = f"migrace-{uuid.uuid4()}"
    tr_m = conn_m.transaction()
    task_p: asyncio.Task | None = None
    try:
        await _seed_bucket(setup, acct, bucket_id)
        first = await _upsert(setup, bucket_id, key)
        object_id = first["object_id"]
        assert first["current_object_version"] == 1

        # Migrator inserts version 2 but does NOT bump current_object_version (it holds the
        # objects row via FOR UPDATE) — leaving current_object_version (1) behind MAX (2).
        await tr_m.start()
        migrated = await conn_m.fetchval(
            get_query("create_migration_version"), str(object_id), "x", json.dumps({}), 5, ["arion"]
        )
        assert migrated == 2

        # A concurrent write blocks on the migrator's row lock with a pre-commit snapshot.
        task_p = asyncio.create_task(_upsert(conn_p, bucket_id, key))
        await asyncio.sleep(0.5)
        assert not task_p.done()

        # Migrator commits v2. The blocked write unblocks and collides: current_object_version is
        # a fresh 1, MAX() is snapshot-stale at 1, so GREATEST(1, 1)+1 = 2 == the migration version.
        await tr_m.commit()
        with pytest.raises(asyncpg.exceptions.UniqueViolationError):
            await task_p

        # The writer's retry re-runs in a fresh transaction: MAX() now sees the committed v2, so
        # GREATEST(1, 2)+1 = 3 and the write succeeds.
        retried = await _upsert(conn_p, bucket_id, key)
        assert retried["current_object_version"] == 3
        assert retried["object_id"] == object_id
    finally:
        if task_p is not None and not task_p.done():
            task_p.cancel()
        if conn_m.is_in_transaction():
            await tr_m.rollback()
        await _cleanup(setup, bucket_id, acct)
        await conn_m.close()
        await conn_p.close()
        await setup.close()


@pytest.mark.asyncio
async def test_migration_race_resolved_by_retry_helper_on_multipart_reserve() -> None:
    """End-to-end wiring: retry_on_object_version_conflict wraps the multipart reserve, so a
    migration-version collision that raises object_versions_pkey is retried in a fresh statement
    and succeeds with v3 — no 500 reaches the caller. Mirrors the raw-collision test above but
    exercises the helper on the (previously unguarded) multipart path."""
    setup = await _connect_or_skip()
    conn_m = await _connect_or_skip()
    conn_p = await _connect_or_skip()  # autocommit — each helper retry is a fresh snapshot
    acct = f"5MPRACE{uuid.uuid4().hex[:12]}"
    bucket_id = uuid.uuid4()
    key = f"mprace-{uuid.uuid4()}"
    tr_m = conn_m.transaction()
    task_p: asyncio.Task | None = None

    async def _reserve_multipart() -> Any:
        return await conn_p.fetchrow(
            get_query("upsert_object_multipart"),
            str(uuid.uuid4()),
            str(bucket_id),
            key,
            "application/octet-stream",
            json.dumps({}),
            "",
            0,
            datetime.now(timezone.utc),
            5,
            ["arion"],
        )

    try:
        await _seed_bucket(setup, acct, bucket_id)
        first = await _upsert(setup, bucket_id, key)
        object_id = first["object_id"]
        assert first["current_object_version"] == 1

        # Migrator inserts v2 without bumping current_object_version, holding the objects row.
        await tr_m.start()
        migrated = await conn_m.fetchval(
            get_query("create_migration_version"), str(object_id), "x", json.dumps({}), 5, ["arion"]
        )
        assert migrated == 2

        # The helper-wrapped multipart reserve blocks on the migrator's row lock (pre-commit snapshot).
        task_p = asyncio.create_task(retry_on_object_version_conflict(_reserve_multipart))
        await asyncio.sleep(0.5)
        assert not task_p.done()

        # Migrator commits v2: attempt 1 collides on object_versions_pkey, the helper retries in a
        # fresh statement that sees the committed MAX and takes v3 — the caller never sees the error.
        await tr_m.commit()
        row_p = await task_p
        assert row_p["object_id"] == object_id
        assert row_p["current_object_version"] == 3
    finally:
        if task_p is not None and not task_p.done():
            task_p.cancel()
        if conn_m.is_in_transaction():
            await tr_m.rollback()
        await _cleanup(setup, bucket_id, acct)
        await conn_m.close()
        await conn_p.close()
        await setup.close()
