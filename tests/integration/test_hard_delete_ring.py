"""Truth table + keyset walk for the janitor's hard-delete finder and its guarded delete, against
real Postgres.

`find_objects_ready_for_hard_delete.sql` returns a (deleted_at, object_id)-ordered SLICE of ALL
soft-deleted objects past the 1h grace — ready and not — with a per-row `ready` boolean, so the
caller can step a durable ring cursor past a permanently-unready head instead of re-filtering it out
of the oldest slot forever. `hard_delete_object.sql` re-verifies the SAME readiness atomically; the
two MUST agree, or every relaxed candidate would `DELETE 0`-skip and throughput returns to zero.

These pin the four soft-deleted populations against the real migrated schema:
  - unpin-incomplete (a live chunk_backend row)          -> ready=false
  - fully-unpinned    (every chunk_backend row deleted)  -> ready=true
  - zero chunk_backend, aged  > 24h                      -> ready=true  (CopyObject-dest relaxation)
  - zero chunk_backend, fresh < 24h                      -> ready=false (grace for a lagging unpin)

Seed data is written via the `pg_tx` fixture (always rolled back). Each test wipes nothing global —
it filters to the bucket it seeded — because the finder scans ALL soft-deleted objects and the shared
DB may carry others; assertions are membership/subset over the seeded object_ids, never equality over
the whole slice.
"""

from __future__ import annotations

import datetime
import uuid

import asyncpg
import pytest

from hippius_s3.utils import get_query


pytestmark = pytest.mark.asyncio

_UTC = datetime.timezone.utc
_NIL_UUID = "00000000-0000-0000-0000-000000000000"  # the janitor's ring-start object_id sentinel
_EPOCH = datetime.datetime(1970, 1, 1, tzinfo=_UTC)  # the janitor's ring-start deleted_at sentinel
_CURSOR_START = (_EPOCH, _NIL_UUID)


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


async def _seed_soft_deleted(
    conn: asyncpg.Connection,
    bucket_id: uuid.UUID,
    *,
    deleted_age_hours: float,
    chunk_backends: list[tuple[str, bool]] | None,
) -> uuid.UUID:
    """Seed one soft-deleted object -> version -> part -> one chunk (+ optional chunk_backend rows).

    chunk_backends is the list of (backend, deleted) rows for the single chunk; None means the
    CopyObject-destination shape (part + chunk exist, ZERO chunk_backend rows). deleted_age_hours
    sets objects.deleted_at = now() - that many hours. Returns the object_id.
    """
    oid = uuid.uuid4()
    key = f"hd-test-{oid}"
    await conn.execute(
        "INSERT INTO objects(object_id, bucket_id, object_key, created_at, current_object_version, deleted_at)"
        " VALUES($1, $2, $3, now(), 1, now() - make_interval(hours => $4))",
        oid,
        bucket_id,
        key,
        deleted_age_hours,
    )
    await conn.execute(
        "INSERT INTO object_versions(object_id, object_version, storage_version, size_bytes, content_type,"
        " version_type) VALUES($1, 1, 5, 100, 'application/octet-stream', 'user'::version_type)",
        oid,
    )
    upload_id = uuid.uuid4()
    await conn.execute(
        "INSERT INTO multipart_uploads(upload_id, bucket_id, object_key, initiated_at) VALUES($1, $2, $3, now())",
        upload_id,
        bucket_id,
        key,
    )
    part_id = uuid.uuid4()
    await conn.execute(
        "INSERT INTO parts(part_id, upload_id, part_number, size_bytes, chunk_size_bytes, etag, uploaded_at,"
        " object_id, object_version) VALUES($1, $2, 1, 100, 100, 'etag', now(), $3, 1)",
        part_id,
        upload_id,
        oid,
    )
    chunk_pk = await conn.fetchval(
        "INSERT INTO part_chunks(part_id, chunk_index, cipher_size_bytes) VALUES($1, 0, 100) RETURNING id",
        part_id,
    )
    for backend, deleted in chunk_backends or []:
        await conn.execute(
            "INSERT INTO chunk_backend(chunk_id, backend, deleted) VALUES($1, $2, $3)", chunk_pk, backend, deleted
        )
    return oid


async def _find(
    conn: asyncpg.Connection,
    *,
    limit: int = 1000,
    cursor: tuple[datetime.datetime, str] = _CURSOR_START,
) -> list[asyncpg.Record]:
    return await conn.fetch(get_query("find_objects_ready_for_hard_delete"), limit, cursor[0], cursor[1])


def _ready_by_oid(rows: list[asyncpg.Record], wanted: set[uuid.UUID]) -> dict[uuid.UUID, bool]:
    return {r["object_id"]: r["ready"] for r in rows if r["object_id"] in wanted}


# ===================================================== per-row readiness truth table


async def test_slice_reports_ready_per_row(pg_tx: asyncpg.Connection) -> None:
    bucket = await _seed_bucket(pg_tx)
    unpin_incomplete = await _seed_soft_deleted(pg_tx, bucket, deleted_age_hours=3, chunk_backends=[("arion", False)])
    fully_unpinned = await _seed_soft_deleted(pg_tx, bucket, deleted_age_hours=4, chunk_backends=[("arion", True)])
    zero_aged = await _seed_soft_deleted(pg_tx, bucket, deleted_age_hours=25, chunk_backends=None)
    zero_fresh = await _seed_soft_deleted(pg_tx, bucket, deleted_age_hours=2, chunk_backends=None)

    wanted = {unpin_incomplete, fully_unpinned, zero_aged, zero_fresh}
    ready = _ready_by_oid(await _find(pg_tx), wanted)

    assert ready == {
        unpin_incomplete: False,  # a live backend copy remains
        fully_unpinned: True,  # every copy unpinned, replication was real
        zero_aged: True,  # never-replicated but soft-deleted > 24h: aged relaxation
        zero_fresh: False,  # never-replicated and < 24h: grace for a lagging unpin
    }


async def test_within_grace_object_is_not_in_slice(pg_tx: asyncpg.Connection) -> None:
    bucket = await _seed_bucket(pg_tx)
    # Deleted 30 minutes ago: inside the 1h candidate grace, so it is not even a candidate.
    fresh = await _seed_soft_deleted(pg_tx, bucket, deleted_age_hours=0.5, chunk_backends=[("arion", True)])
    assert fresh not in {r["object_id"] for r in await _find(pg_tx)}


# ===================================================== keyset paging


async def test_keyset_pagination_walks_in_order_and_covers_slice(pg_tx: asyncpg.Connection) -> None:
    bucket = await _seed_bucket(pg_tx)
    seeded = {
        await _seed_soft_deleted(pg_tx, bucket, deleted_age_hours=h, chunk_backends=[("arion", True)])
        for h in (3, 4, 25, 6)
    }

    collected: list[tuple[datetime.datetime, uuid.UUID]] = []
    cursor = _CURSOR_START
    while True:
        rows = await _find(pg_tx, limit=2, cursor=cursor)
        if not rows:
            break
        for r in rows:
            if r["object_id"] in seeded:
                collected.append((r["deleted_at"], r["object_id"]))
        last = rows[-1]
        cursor = (last["deleted_at"], str(last["object_id"]))
        if len(rows) < 2:
            break

    assert [oid for _, oid in collected] == [oid for _, oid in sorted(collected)], "walk is (deleted_at, oid)-ordered"
    assert {oid for _, oid in collected} == seeded, "every seeded candidate seen exactly once"
    assert len(collected) == len(set(collected))


# ===================================================== guarded delete agrees with the finder


async def test_hard_delete_object_agrees_with_finder(pg_tx: asyncpg.Connection) -> None:
    bucket = await _seed_bucket(pg_tx)
    unpin_incomplete = await _seed_soft_deleted(pg_tx, bucket, deleted_age_hours=3, chunk_backends=[("arion", False)])
    fully_unpinned = await _seed_soft_deleted(pg_tx, bucket, deleted_age_hours=4, chunk_backends=[("arion", True)])
    zero_aged = await _seed_soft_deleted(pg_tx, bucket, deleted_age_hours=25, chunk_backends=None)
    zero_fresh = await _seed_soft_deleted(pg_tx, bucket, deleted_age_hours=2, chunk_backends=None)

    async def _delete(oid: uuid.UUID) -> str:
        return await pg_tx.execute(get_query("hard_delete_object"), oid)

    # The two ready rows delete; the two unready rows DELETE 0 — exactly the finder's `ready` verdict.
    assert await _delete(fully_unpinned) == "DELETE 1"
    assert await _delete(zero_aged) == "DELETE 1"
    assert await _delete(unpin_incomplete) == "DELETE 0"
    assert await _delete(zero_fresh) == "DELETE 0"

    survivors = {
        r["object_id"] for r in await pg_tx.fetch("SELECT object_id FROM objects WHERE bucket_id = $1", bucket)
    }
    assert survivors == {unpin_incomplete, zero_fresh}, "only the unready rows survive"
