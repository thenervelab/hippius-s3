"""Correctness + plan checks for the janitor hard-delete queries.

find_objects_ready_for_hard_delete was rewritten to materialise a bounded batch of
soft-deleted candidates first (LIMIT $1) and index-probe each, instead of a full
hash join over chunk_backend/parts/part_chunks (a ~135 GiB read storm in prod — see
oom-psql-postmortem.md). hard_delete_object then re-verifies readiness atomically so
a revived object can't be deleted. These tests pin the semantics, the index-driven
plan, and the delete-time race guard.

Seed data is written via the `pg_tx` fixture (auto-rolled-back); read-only checks use
`pg_conn`. Both skip if no Postgres is reachable on DATABASE_URL.
"""

import datetime
import uuid

import asyncpg
import pytest

from hippius_s3.utils import get_query


# Far-past timestamps so seeded rows are the OLDEST soft-deleted objects in the DB
# (the find query orders candidates by deleted_at), making the batch test deterministic
# even against a populated database.
_EPOCH = datetime.timezone.utc
_T_A = datetime.datetime(2000, 1, 1, tzinfo=_EPOCH)  # ready, oldest
_T_D = datetime.datetime(2000, 1, 2, tzinfo=_EPOCH)  # ready
_T_B = datetime.datetime(2000, 1, 3, tzinfo=_EPOCH)  # not-ready (a live backend)
_T_C = datetime.datetime(2000, 1, 4, tzinfo=_EPOCH)  # no chunk_backend rows

# Start-of-sweep keyset cursor: the query pages on (deleted_at, object_id) > ($2, $3).
_EPOCH_CURSOR = datetime.datetime(1970, 1, 1, tzinfo=_EPOCH)
_ZERO_UUID = uuid.UUID("00000000-0000-0000-0000-000000000000")


async def _seed_bucket(conn: asyncpg.Connection) -> uuid.UUID:
    acct = f"5HDTEST{uuid.uuid4().hex[:12]}"
    await conn.execute("INSERT INTO users(main_account_id) VALUES($1) ON CONFLICT DO NOTHING", acct)
    bucket_id = uuid.uuid4()
    await conn.execute(
        "INSERT INTO buckets(bucket_id, bucket_name, created_at, main_account_id) VALUES($1, $2, now(), $3)",
        bucket_id,
        f"hd-test-{bucket_id}",
        acct,
    )
    return bucket_id


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


async def _seed_one(conn: asyncpg.Connection, chunk_backends: list[bool]) -> uuid.UUID:
    """Seed a single standalone ready/not-ready object (own bucket), deleted long ago."""
    return await _seed_object(conn, await _seed_bucket(conn), _T_A, chunk_backends)


@pytest.mark.asyncio
async def test_find_objects_ready_for_hard_delete_semantics(pg_tx: asyncpg.Connection) -> None:
    bucket_id = await _seed_bucket(pg_tx)
    a = await _seed_object(pg_tx, bucket_id, _T_A, [True, True])  # ready: all backends deleted
    d = await _seed_object(pg_tx, bucket_id, _T_D, [True])  # ready: single deleted backend
    b = await _seed_object(pg_tx, bucket_id, _T_B, [True, False])  # not-ready: one live backend
    c = await _seed_object(pg_tx, bucket_id, _T_C, [])  # excluded: never had a chunk_backend row

    sql = get_query("find_objects_ready_for_hard_delete")

    # Large batch from the epoch cursor. The query now returns EVERY scanned candidate with a
    # `ready` flag (so the caller can advance its cursor past un-ready ones), so assert on the
    # flag rather than on mere membership.
    rows = await pg_tx.fetch(sql, 1000, _EPOCH_CURSOR, _ZERO_UUID)
    ready = {r["object_id"] for r in rows if r["ready"]}
    scanned = {r["object_id"] for r in rows}
    assert a in ready
    assert d in ready
    assert b in scanned and b not in ready, "a live backend row means not-ready, but still scanned"
    assert c in scanned and c not in ready, "no chunk_backend row at all means not-ready, but still scanned"

    # Batch=2: only the two OLDEST candidates (a, d) are even considered, so the
    # newer not-ready/no-chunks rows can't appear — proves LIMIT bounds the scan.
    assert {r["object_id"] for r in await pg_tx.fetch(sql, 2, _EPOCH_CURSOR, _ZERO_UUID)} == {a, d}


@pytest.mark.asyncio
async def test_find_objects_ready_for_hard_delete_respects_batch_limit(pg_conn: asyncpg.Connection) -> None:
    rows = await pg_conn.fetch(get_query("find_objects_ready_for_hard_delete"), 1, _EPOCH_CURSOR, _ZERO_UUID)
    assert len(rows) <= 1


@pytest.mark.asyncio
async def test_cursor_advances_past_a_permanently_unready_head(pg_tx: asyncpg.Connection) -> None:
    """THE head-of-line fix. `b` is un-ready forever (a live chunk_backend row) and sorts FIRST.
    Un-cursored, the janitor re-scanned it every cycle and hard-deleted nothing — exactly the prod
    state (`hard_deleted=0` for months while ~33M ready objects queued behind it). Paging past it
    with the keyset cursor must surface the ready object that sits behind it."""
    bucket_id = await _seed_bucket(pg_tx)
    b = await _seed_object(pg_tx, bucket_id, _T_A, [True, False])  # oldest AND permanently un-ready
    a = await _seed_object(pg_tx, bucket_id, _T_D, [True, True])  # ready, but stuck behind b

    sql = get_query("find_objects_ready_for_hard_delete")

    # Page 1 (batch=1 from epoch) sees only the stuck head and finds nothing ready — the old bug.
    page1 = await pg_tx.fetch(sql, 1, _EPOCH_CURSOR, _ZERO_UUID)
    assert [r["object_id"] for r in page1] == [b]
    assert not any(r["ready"] for r in page1), "the stuck head is never ready"

    # Advancing the cursor past it (what the caller now persists) reaches the ready object.
    last = page1[-1]
    page2 = await pg_tx.fetch(sql, 1, last["deleted_at"], last["object_id"])
    assert [r["object_id"] for r in page2] == [a]
    assert page2[0]["ready"], "the ready object behind the stuck head must now be reachable"


@pytest.mark.asyncio
async def test_cursor_is_exclusive_so_a_page_never_repeats_itself(pg_tx: asyncpg.Connection) -> None:
    """Keyset must be strictly greater, otherwise the sweep would re-serve its last row forever."""
    bucket_id = await _seed_bucket(pg_tx)
    first = await _seed_object(pg_tx, bucket_id, _T_A, [True, True])

    sql = get_query("find_objects_ready_for_hard_delete")
    page1 = await pg_tx.fetch(sql, 1, _EPOCH_CURSOR, _ZERO_UUID)
    assert [r["object_id"] for r in page1] == [first]

    last = page1[-1]
    page2 = await pg_tx.fetch(sql, 1, last["deleted_at"], last["object_id"])
    assert first not in {r["object_id"] for r in page2}, "cursor must be exclusive (strictly >)"


@pytest.mark.asyncio
async def test_find_objects_ready_for_hard_delete_plan_is_index_driven(pg_conn: asyncpg.Connection) -> None:
    """On a prod-sized DB the plan must stay index-driven (no full scan of the big
    tables). Skips on a small/empty test DB where seq scans are legitimately cheapest."""
    n = await pg_conn.fetchval("SELECT reltuples::bigint FROM pg_class WHERE relname = 'chunk_backend'")
    if not n or n < 1_000_000:
        pytest.skip(f"chunk_backend too small ({n}) for a meaningful plan assertion")
    sql = get_query("find_objects_ready_for_hard_delete")
    plan = "\n".join(r["QUERY PLAN"] for r in await pg_conn.fetch("EXPLAIN " + sql, 5000))
    assert "idx_objects_deleted" in plan
    assert "Seq Scan on chunk_backend" not in plan
    assert "Seq Scan on parts" not in plan
    assert "Seq Scan on part_chunks" not in plan


@pytest.mark.asyncio
async def test_hard_delete_object_deletes_ready_object(pg_tx: asyncpg.Connection) -> None:
    oid = await _seed_one(pg_tx, [True, True])  # ready: all chunks deleted
    tag = await pg_tx.execute(get_query("hard_delete_object"), oid)
    assert tag == "DELETE 1"
    assert await pg_tx.fetchval("SELECT count(*) FROM objects WHERE object_id=$1", oid) == 0


@pytest.mark.asyncio
async def test_hard_delete_object_skips_revived_object(pg_tx: asyncpg.Connection) -> None:
    """Race guard: an object revived (deleted_at cleared) after the find query must
    NOT be hard-deleted even if its object_id was already selected."""
    oid = await _seed_one(pg_tx, [True, True])  # was ready...
    await pg_tx.execute("UPDATE objects SET deleted_at = NULL WHERE object_id=$1", oid)  # ...then re-PUT revives it
    tag = await pg_tx.execute(get_query("hard_delete_object"), oid)
    assert tag == "DELETE 0"  # guard skipped it
    assert await pg_tx.fetchval("SELECT count(*) FROM objects WHERE object_id=$1", oid) == 1  # still alive


@pytest.mark.asyncio
async def test_hard_delete_object_skips_object_with_live_chunk(pg_tx: asyncpg.Connection) -> None:
    """Race guard: if a live (non-deleted) chunk_backend row appears after the find
    query (e.g. a fresh upload), the delete must skip — never orphan a live chunk."""
    oid = await _seed_one(pg_tx, [True])  # was ready...
    # ...then a live chunk_backend row appears (re-upload).
    await pg_tx.execute(
        "UPDATE chunk_backend SET deleted = false WHERE chunk_id IN"
        " (SELECT pc.id FROM parts p JOIN part_chunks pc ON pc.part_id = p.part_id WHERE p.object_id = $1)",
        oid,
    )
    tag = await pg_tx.execute(get_query("hard_delete_object"), oid)
    assert tag == "DELETE 0"
    assert await pg_tx.fetchval("SELECT count(*) FROM objects WHERE object_id=$1", oid) == 1
