"""backfill_arion_hash against real Postgres.

HCFS's file_records lives in another database in production; here a TEMP table with the same key
shape stands in for it on the same connection, which is enough to exercise every query the script
runs on both sides.
"""

from __future__ import annotations

import os
import uuid
from typing import AsyncGenerator

import asyncpg
import pytest
import pytest_asyncio

from hippius_s3.scripts.backfill_arion_hash import backfill_batch


pytestmark = pytest.mark.asyncio

_DB_URL = os.getenv("DATABASE_URL", "postgresql://postgres:postgres@localhost:5432/hippius?sslmode=disable")

ARION_HASH = "37e055f089a9c58cbc5f63e84670d25cfc8dc015a28b4c2058af1b813cf79b78"
FILE_ID = "129107a090d80a03ebc8328e30e740ca761d4f4a72b907d0c64ae90cab35a70f"


@pytest_asyncio.fixture
async def conn() -> AsyncGenerator[asyncpg.Connection, None]:
    try:
        c = await asyncpg.connect(_DB_URL)
    except OSError as exc:  # only an unreachable server is a legitimate skip
        pytest.skip(f"postgres unavailable: {exc}")
    if not await c.fetchval(
        "SELECT 1 FROM information_schema.columns WHERE table_name = 'chunk_backend' AND column_name = 'arion_hash'"
    ):
        await c.close()
        pytest.fail("chunk_backend.arion_hash is missing — run `python -m hippius_s3.scripts.migrate`")
    tx = c.transaction()
    await tx.start()
    # Stand-in for HCFS's table: same primary key, same columns the script reads.
    await c.execute(
        "CREATE TEMP TABLE file_records (user_id text NOT NULL, path_hash bytea NOT NULL, "
        "arion_hash text NOT NULL DEFAULT '', PRIMARY KEY (user_id, path_hash)) ON COMMIT DROP"
    )
    try:
        yield c
    finally:
        await tx.rollback()
        await c.close()


async def _object(c: asyncpg.Connection, owner: str, n_chunks: int = 1) -> tuple[uuid.UUID, list[int]]:
    """One single-part object with ``n_chunks`` arion chunks that predate arion_hash; returns chunk ids."""
    bucket_id, object_id, upload_id, part_id = uuid.uuid4(), uuid.uuid4(), uuid.uuid4(), uuid.uuid4()
    await c.execute("INSERT INTO users (main_account_id, created_at) VALUES ($1, now()) ON CONFLICT DO NOTHING", owner)
    await c.execute(
        "INSERT INTO buckets (bucket_id, bucket_name, main_account_id, created_at) VALUES ($1, $2, $3, now())",
        bucket_id,
        f"bf-{uuid.uuid4().hex[:10]}",
        owner,
    )
    await c.execute(
        "INSERT INTO objects (object_id, bucket_id, object_key, current_object_version, created_at) "
        "VALUES ($1, $2, 'k.bin', 1, now())",
        object_id,
        bucket_id,
    )
    await c.execute(
        "INSERT INTO object_versions (object_id, object_version, storage_version, size_bytes, md5_hash, "
        "content_type, status) VALUES ($1, 1, 5, 123, 'deadbeef', 'application/octet-stream', 'uploaded')",
        object_id,
    )
    await c.execute(
        "INSERT INTO multipart_uploads (upload_id, bucket_id, object_key, is_completed, initiated_at) "
        "VALUES ($1, $2, 'k.bin', TRUE, now())",
        upload_id,
        bucket_id,
    )
    await c.execute(
        "INSERT INTO parts (part_id, upload_id, object_id, object_version, part_number, size_bytes, etag, uploaded_at) "
        "VALUES ($1, $2, $3, 1, 1, 123, 'deadbeef', now())",
        part_id,
        upload_id,
        object_id,
    )
    chunk_ids = []
    for ci in range(n_chunks):
        chunk_id = await c.fetchval(
            "INSERT INTO part_chunks (part_id, chunk_index, cipher_size_bytes) VALUES ($1, $2, 139) RETURNING id",
            part_id,
            ci,
        )
        file_id = FILE_ID if n_chunks == 1 else f"{ci:02x}" * 32
        await c.execute(
            "INSERT INTO chunk_backend (chunk_id, backend, backend_identifier) VALUES ($1, 'arion', $2)",
            chunk_id,
            file_id,
        )
        chunk_ids.append(chunk_id)
    return object_id, chunk_ids


async def _hcfs_row(c: asyncpg.Connection, owner: str, file_id: str, arion_hash: str) -> None:
    await c.execute(
        "INSERT INTO file_records (user_id, path_hash, arion_hash) VALUES ($1, $2, $3)",
        owner,
        bytes.fromhex(file_id),
        arion_hash,
    )


async def _run(c: asyncpg.Connection, *, dry_run: bool = False) -> None:
    # Start just below our rows so unrelated rows elsewhere in a shared DB cannot eat the page.
    first = await c.fetchval("SELECT min(chunk_id) FROM chunk_backend WHERE arion_hash IS NULL")
    after = int(first) - 1 if first is not None else 0
    await backfill_batch(c, c, after_chunk_id=after, batch_size=1000, dry_run=dry_run)


async def _chunk_hash(c: asyncpg.Connection, chunk_id: int) -> str | None:
    return await c.fetchval("SELECT arion_hash FROM chunk_backend WHERE chunk_id = $1", chunk_id)


async def _version_hash(c: asyncpg.Connection, object_id: uuid.UUID) -> str | None:
    return await c.fetchval("SELECT arion_hash FROM object_versions WHERE object_id = $1", object_id)


async def test_fills_the_chunk_and_rolls_the_version_up(conn: asyncpg.Connection) -> None:
    owner = f"5OWN{uuid.uuid4().hex[:10]}"
    object_id, [chunk_id] = await _object(conn, owner)
    await _hcfs_row(conn, owner, FILE_ID, ARION_HASH)

    await _run(conn)

    assert await _chunk_hash(conn, chunk_id) == ARION_HASH
    assert await _version_hash(conn, object_id) == ARION_HASH


async def test_dry_run_writes_nothing(conn: asyncpg.Connection) -> None:
    owner = f"5OWN{uuid.uuid4().hex[:10]}"
    object_id, [chunk_id] = await _object(conn, owner)
    await _hcfs_row(conn, owner, FILE_ID, ARION_HASH)

    await _run(conn, dry_run=True)

    assert await _chunk_hash(conn, chunk_id) is None
    assert await _version_hash(conn, object_id) is None


async def test_rows_hcfs_cannot_vouch_for_stay_null(conn: asyncpg.Connection) -> None:
    """No HCFS row under the bucket owner, or one with no Arion copy: leave it, don't guess."""
    owner = f"5OWN{uuid.uuid4().hex[:10]}"
    _, [missing] = await _object(conn, owner)
    await _hcfs_row(conn, "5SOMEONE-ELSE", FILE_ID, ARION_HASH)  # same file_id, different account

    owner2 = f"5OWN{uuid.uuid4().hex[:10]}"
    _, [empty] = await _object(conn, owner2)
    await _hcfs_row(conn, owner2, FILE_ID, "")

    await _run(conn)

    assert await _chunk_hash(conn, missing) is None
    assert await _chunk_hash(conn, empty) is None


async def test_multi_chunk_object_fills_chunks_but_not_the_version(conn: asyncpg.Connection) -> None:
    owner = f"5OWN{uuid.uuid4().hex[:10]}"
    object_id, chunk_ids = await _object(conn, owner, n_chunks=2)
    await _hcfs_row(conn, owner, "00" * 32, "a0" * 32)
    await _hcfs_row(conn, owner, "01" * 32, "a1" * 32)

    await _run(conn)

    assert [await _chunk_hash(conn, cid) for cid in chunk_ids] == ["a0" * 32, "a1" * 32]
    assert await _version_hash(conn, object_id) is None


async def test_never_overwrites_a_hash_the_uploader_already_stored(conn: asyncpg.Connection) -> None:
    owner = f"5OWN{uuid.uuid4().hex[:10]}"
    _, [chunk_id] = await _object(conn, owner)
    await conn.execute("UPDATE chunk_backend SET arion_hash = $1 WHERE chunk_id = $2", "b" * 64, chunk_id)
    await _hcfs_row(conn, owner, FILE_ID, ARION_HASH)

    await _run(conn)

    assert await _chunk_hash(conn, chunk_id) == "b" * 64
