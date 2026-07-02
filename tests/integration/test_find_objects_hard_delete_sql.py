"""Correctness + plan checks for find_objects_ready_for_hard_delete.

The janitor's hard-delete query was rewritten to materialise a bounded batch of
soft-deleted candidates first (LIMIT $1) and then index-probe each, instead of a
full hash join over chunk_backend/parts/part_chunks (which caused a ~135 GiB read
storm in prod — see oom-psql-postmortem.md). These tests pin BOTH the preserved
semantics and the index-driven plan.

All seed data is written inside a transaction that is always rolled back, so
nothing persists. Skips if no Postgres is reachable on DATABASE_URL.
"""

import datetime
import os
import uuid

import asyncpg
import pytest

from hippius_s3.utils import get_query


async def _connect_or_skip() -> asyncpg.Connection:
    dsn = os.environ.get("DATABASE_URL")
    if not dsn:
        pytest.skip("DATABASE_URL not set; skipping live-schema check")
    try:
        return await asyncpg.connect(dsn=dsn)
    except (OSError, asyncpg.PostgresError) as exc:
        pytest.skip(f"Postgres unreachable on DATABASE_URL: {exc}")


async def _seed_object(
    conn: asyncpg.Connection,
    bucket_id: uuid.UUID,
    deleted_at: datetime.datetime,
    chunk_backends: list[bool] | None,
) -> uuid.UUID:
    """Seed one soft-deleted object. chunk_backends: per-backend `deleted` flags;
    [] = parts/chunks but zero chunk_backend rows; None = no parts at all."""
    oid = uuid.uuid4()
    key = f"hd-test-{oid}"
    # objects first — its current_object_version FK to object_versions is DEFERRABLE.
    await conn.execute(
        "INSERT INTO objects(object_id, bucket_id, object_key, created_at, current_object_version, deleted_at)"
        " VALUES($1, $2, $3, now(), 1, $4)",
        oid,
        bucket_id,
        key,
        deleted_at,
    )
    await conn.execute(
        "INSERT INTO object_versions(object_id, object_version, storage_version, size_bytes, content_type)"
        " VALUES($1, 1, 5, 100, 'application/octet-stream')",
        oid,
    )
    if chunk_backends is not None:
        upload_id = uuid.uuid4()
        await conn.execute(
            "INSERT INTO multipart_uploads(upload_id, bucket_id, object_key, initiated_at) VALUES($1, $2, $3, now())",
            upload_id,
            bucket_id,
            key,
        )
        part_id = uuid.uuid4()
        await conn.execute(
            "INSERT INTO parts(part_id, upload_id, part_number, size_bytes, etag, uploaded_at, object_id, object_version)"
            " VALUES($1, $2, 1, 100, 'etag', now(), $3, 1)",
            part_id,
            upload_id,
            oid,
        )
        chunk_pk = await conn.fetchval(
            "INSERT INTO part_chunks(part_id, chunk_index, cipher_size_bytes) VALUES($1, 0, 100) RETURNING id",
            part_id,
        )
        for i, is_deleted in enumerate(chunk_backends):
            await conn.execute(
                "INSERT INTO chunk_backend(chunk_id, backend, deleted) VALUES($1, $2, $3)",
                chunk_pk,
                f"backend{i}",
                is_deleted,
            )
    return oid


# Far-past timestamps so the seeded rows are the OLDEST soft-deleted objects in
# the DB (the query orders candidates by deleted_at), making the batch test
# deterministic even against a populated database.
_EPOCH = datetime.timezone.utc
_T_A = datetime.datetime(2000, 1, 1, tzinfo=_EPOCH)  # ready, oldest
_T_D = datetime.datetime(2000, 1, 2, tzinfo=_EPOCH)  # ready
_T_B = datetime.datetime(2000, 1, 3, tzinfo=_EPOCH)  # not-ready (a live backend)
_T_C = datetime.datetime(2000, 1, 4, tzinfo=_EPOCH)  # no chunk_backend rows


@pytest.mark.asyncio
async def test_find_objects_ready_for_hard_delete_semantics() -> None:
    conn = await _connect_or_skip()
    tr = conn.transaction()
    await tr.start()
    try:
        acct = f"5HDTEST{uuid.uuid4().hex[:12]}"
        await conn.execute("INSERT INTO users(main_account_id) VALUES($1) ON CONFLICT DO NOTHING", acct)
        bucket_id = uuid.uuid4()
        await conn.execute(
            "INSERT INTO buckets(bucket_id, bucket_name, created_at, main_account_id) VALUES($1, $2, now(), $3)",
            bucket_id,
            f"hd-test-{bucket_id}",
            acct,
        )
        a = await _seed_object(conn, bucket_id, _T_A, [True, True])  # ready: all backends deleted
        d = await _seed_object(conn, bucket_id, _T_D, [True])  # ready: single deleted backend
        b = await _seed_object(conn, bucket_id, _T_B, [True, False])  # not-ready: one live backend
        c = await _seed_object(conn, bucket_id, _T_C, [])  # excluded: never had a chunk_backend row

        sql = get_query("find_objects_ready_for_hard_delete")

        # Large batch: ready objects returned, not-ready / no-chunks excluded (membership —
        # robust to any other soft-deleted rows already in the DB).
        rows = await conn.fetch(sql, 1000)
        returned = {r["object_id"] for r in rows}
        assert a in returned
        assert d in returned
        assert b not in returned
        assert c not in returned

        # Batch=2: only the two OLDEST candidates (a, d) are even considered, so the
        # newer not-ready/no-chunks rows can't appear — proves LIMIT bounds the scan.
        rows2 = await conn.fetch(sql, 2)
        assert {r["object_id"] for r in rows2} == {a, d}
    finally:
        await tr.rollback()
        await conn.close()


@pytest.mark.asyncio
async def test_find_objects_ready_for_hard_delete_respects_batch_limit() -> None:
    conn = await _connect_or_skip()
    try:
        sql = get_query("find_objects_ready_for_hard_delete")
        rows = await conn.fetch(sql, 1)
        assert len(rows) <= 1
    finally:
        await conn.close()


@pytest.mark.asyncio
async def test_find_objects_ready_for_hard_delete_plan_is_index_driven() -> None:
    """On a prod-sized DB the plan must stay index-driven (no full scan of the big
    tables). Skips on a small/empty test DB where seq scans are legitimately cheapest."""
    conn = await _connect_or_skip()
    try:
        n = await conn.fetchval("SELECT reltuples::bigint FROM pg_class WHERE relname = 'chunk_backend'")
        if not n or n < 1_000_000:
            pytest.skip(f"chunk_backend too small ({n}) for a meaningful plan assertion")
        sql = get_query("find_objects_ready_for_hard_delete")
        plan = "\n".join(r["QUERY PLAN"] for r in await conn.fetch("EXPLAIN " + sql, 5000))
        assert "idx_objects_deleted" in plan
        assert "Seq Scan on chunk_backend" not in plan
        assert "Seq Scan on parts" not in plan
        assert "Seq Scan on part_chunks" not in plan
    finally:
        await conn.close()
