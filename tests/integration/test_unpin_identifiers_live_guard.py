"""The unpinner's chunk-selection guard: never hand back the CURRENT version of a LIVE object.

`get_chunk_backend_identifiers` feeds the unpinner, which DELETES whatever it returns from the
backend. Nothing in the query used to re-check that the object was still deleted, so safety rested
entirely on the caller passing the right `object_version` — held up only by the fact that reviving a
soft-deleted object bumps the version (`upsert_object_basic`: `deleted_at = NULL,
current_object_version = GREATEST(...) + 1`). Any caller passing a stale/NULL version, or a future
change that reuses version numbers, would have deleted live data off the backend.

The guard is `(o.deleted_at IS NOT NULL OR p.object_version <> o.current_object_version)` — NOT a
blunt `deleted_at IS NOT NULL`, because superseded versions of LIVE objects are legitimately
unpinned (overwrite retention, cleanup_migration_versions.py, the unpin DLQ requeue path). These
tests pin both halves: the live-current row is withheld, and every legitimate case still flows.

Seed data goes through the auto-rolled-back `pg_tx` fixture; skips without Postgres on DATABASE_URL.
"""

import uuid

import asyncpg
import pytest

from hippius_s3.utils import get_query


async def _seed_bucket(conn: asyncpg.Connection) -> uuid.UUID:
    acct = f"5HDTEST{uuid.uuid4().hex[:12]}"
    await conn.execute("INSERT INTO users(main_account_id) VALUES($1) ON CONFLICT DO NOTHING", acct)
    bucket_id = uuid.uuid4()
    await conn.execute(
        "INSERT INTO buckets(bucket_id, bucket_name, created_at, main_account_id) VALUES($1, $2, now(), $3)",
        bucket_id,
        f"unpin-guard-{bucket_id}",
        acct,
    )
    return bucket_id


async def _seed_object(
    conn: asyncpg.Connection,
    bucket_id: uuid.UUID,
    *,
    deleted: bool,
    versions: list[int],
    current_version: int,
) -> uuid.UUID:
    """Seed an object with a part+chunk per entry in `versions`, each carrying a live
    `chunk_backend` row for backend 'arion' with a non-null identifier."""
    oid = uuid.uuid4()
    key = f"unpin-guard-{oid}"
    await conn.execute(
        "INSERT INTO objects(object_id, bucket_id, object_key, created_at, current_object_version, deleted_at)"
        " VALUES($1, $2, $3, now(), $4, CASE WHEN $5::bool THEN now() ELSE NULL END)",
        oid,
        bucket_id,
        key,
        current_version,
        deleted,
    )
    upload_id = uuid.uuid4()
    await conn.execute(
        "INSERT INTO multipart_uploads(upload_id, bucket_id, object_key, initiated_at) VALUES($1, $2, $3, now())",
        upload_id,
        bucket_id,
        key,
    )
    for v in versions:
        await conn.execute(
            "INSERT INTO object_versions(object_id, object_version, storage_version, size_bytes, content_type)"
            " VALUES($1, $2, 5, 100, 'application/octet-stream')",
            oid,
            v,
        )
        part_id = uuid.uuid4()
        await conn.execute(
            "INSERT INTO parts(part_id, upload_id, part_number, size_bytes, etag, uploaded_at, object_id, object_version)"
            " VALUES($1, $2, 1, 100, 'etag', now(), $3, $4)",
            part_id,
            upload_id,
            oid,
            v,
        )
        chunk_pk = await conn.fetchval(
            "INSERT INTO part_chunks(part_id, chunk_index, cipher_size_bytes) VALUES($1, 0, 100) RETURNING id",
            part_id,
        )
        await conn.execute(
            "INSERT INTO chunk_backend(chunk_id, backend, backend_identifier, deleted) VALUES($1, 'arion', $2, false)",
            chunk_pk,
            f"path-hash-{chunk_pk}",
        )
    return oid


@pytest.mark.asyncio
async def test_deleted_object_is_unpinnable(pg_tx: asyncpg.Connection) -> None:
    """The normal path: the object is soft-deleted, so its chunks are handed to the unpinner."""
    bucket_id = await _seed_bucket(pg_tx)
    oid = await _seed_object(pg_tx, bucket_id, deleted=True, versions=[1], current_version=1)

    rows = await pg_tx.fetch(get_query("get_chunk_backend_identifiers"), "arion", oid, 1)
    assert len(rows) == 1, "a soft-deleted object's chunks must still be unpinnable"


@pytest.mark.asyncio
async def test_live_object_current_version_is_withheld(pg_tx: asyncpg.Connection) -> None:
    """THE data-loss case: object is LIVE and this is its current version — withhold the rows so
    the unpinner cannot delete live data off the backend."""
    bucket_id = await _seed_bucket(pg_tx)
    oid = await _seed_object(pg_tx, bucket_id, deleted=False, versions=[1], current_version=1)

    rows = await pg_tx.fetch(get_query("get_chunk_backend_identifiers"), "arion", oid, 1)
    assert rows == [], "the current version of a LIVE object must never be handed to the unpinner"


@pytest.mark.asyncio
async def test_live_object_superseded_version_is_still_unpinnable(pg_tx: asyncpg.Connection) -> None:
    """The guard must NOT be a blunt deleted_at check: superseded versions of a live object are
    legitimately unpinned (overwrite retention, cleanup_migration_versions, unpin DLQ requeue)."""
    bucket_id = await _seed_bucket(pg_tx)
    oid = await _seed_object(pg_tx, bucket_id, deleted=False, versions=[1, 2], current_version=2)

    old = await pg_tx.fetch(get_query("get_chunk_backend_identifiers"), "arion", oid, 1)
    assert len(old) == 1, "a superseded version of a live object must remain unpinnable"

    cur = await pg_tx.fetch(get_query("get_chunk_backend_identifiers"), "arion", oid, 2)
    assert cur == [], "...while its current version stays protected"


@pytest.mark.asyncio
async def test_null_version_on_live_object_returns_only_superseded(pg_tx: asyncpg.Connection) -> None:
    """A NULL object_version means 'every version of this object'. On a LIVE object that must
    still exclude the current version — this is the stale-caller case the guard exists for."""
    bucket_id = await _seed_bucket(pg_tx)
    oid = await _seed_object(pg_tx, bucket_id, deleted=False, versions=[1, 2, 3], current_version=3)

    rows = await pg_tx.fetch(get_query("get_chunk_backend_identifiers"), "arion", oid, None)
    versions = {
        await pg_tx.fetchval(
            "SELECT p.object_version FROM part_chunks pc JOIN parts p ON p.part_id = pc.part_id WHERE pc.id = $1",
            r["chunk_id"],
        )
        for r in rows
    }
    assert versions == {1, 2}, f"NULL version must yield only superseded versions, got {versions}"


@pytest.mark.asyncio
async def test_null_version_on_deleted_object_returns_every_version(pg_tx: asyncpg.Connection) -> None:
    """Once the object is deleted, every version — including the last current one — is unpinnable,
    otherwise the object could never become fully unpinned and hard-deletable."""
    bucket_id = await _seed_bucket(pg_tx)
    oid = await _seed_object(pg_tx, bucket_id, deleted=True, versions=[1, 2], current_version=2)

    rows = await pg_tx.fetch(get_query("get_chunk_backend_identifiers"), "arion", oid, None)
    assert len(rows) == 2, "a deleted object must expose all versions so it can be fully unpinned"
