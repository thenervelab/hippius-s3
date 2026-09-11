"""The Arion hash: stored per chunk by the uploader, rolled up per version, listed — against real Postgres.

"Arion hash" is the id Arion, the validator, the indexer and the explorer know a file by: the
BLAKE3 of the ciphertext HCFS pushed. Two other identifiers look like it and must not stand in for it:
  * chunk_backend.backend_identifier — HCFS's file_id (its path hash), for our /download and /delete.
  * object_versions.body_blake3      — BLAKE3 of the plaintext, which Arion never sees.
The console once showed body_blake3 as the Arion hash, so explorer lookups found nothing.
"""

from __future__ import annotations

import os
import uuid
from typing import AsyncGenerator

import asyncpg
import pytest
import pytest_asyncio

from hippius_s3.utils import get_query


pytestmark = pytest.mark.asyncio

_DB_URL = os.getenv("DATABASE_URL", "postgresql://postgres:postgres@localhost:5432/hippius?sslmode=disable")

ARION_HASH = "37e055f089a9c58cbc5f63e84670d25cfc8dc015a28b4c2058af1b813cf79b78"
FILE_ID = "129107a090d80a03ebc8328e30e740ca761d4f4a72b907d0c64ae90cab35a70f"
BODY_BLAKE3 = "dd4136dbbe663ead2622121e07c752aab5f68a90fbb3e6e18bedaa05285511db"
KEY = "plan/Hippius_Host_Specs___Load.html"

LISTING_QUERIES = [
    "list_objects",
    "list_objects_delimited",
    "list_object_versions",
    "console_list_objects",
    "get_recent_uploads_for_account",
]


@pytest_asyncio.fixture
async def seeded() -> AsyncGenerator[tuple[asyncpg.Connection, dict], None]:
    try:
        conn = await asyncpg.connect(_DB_URL)
    except OSError as exc:  # only an unreachable server is a legitimate skip
        pytest.skip(f"postgres unavailable: {exc}")

    # Reachable but unmigrated is a failure, not a skip: a skip here would be a silent false green.
    if not await conn.fetchval(
        "SELECT 1 FROM information_schema.columns WHERE table_name = 'chunk_backend' AND column_name = 'arion_hash'"
    ):
        await conn.close()
        pytest.fail("chunk_backend.arion_hash is missing — run `python -m hippius_s3.scripts.migrate`")

    tx = conn.transaction()
    await tx.start()

    account = f"5TEST{uuid.uuid4().hex[:12]}"
    bucket_id, object_id, upload_id = uuid.uuid4(), uuid.uuid4(), uuid.uuid4()
    bucket_name = f"arion-{uuid.uuid4().hex[:10]}"

    await conn.execute("INSERT INTO users (main_account_id, created_at) VALUES ($1, now())", account)
    await conn.execute(
        "INSERT INTO buckets (bucket_id, bucket_name, main_account_id, created_at) VALUES ($1, $2, $3, now())",
        bucket_id,
        bucket_name,
        account,
    )
    await conn.execute(
        "INSERT INTO objects (object_id, bucket_id, object_key, current_object_version, created_at) "
        "VALUES ($1, $2, $3, 1, now())",
        object_id,
        bucket_id,
        KEY,
    )
    await conn.execute(
        "INSERT INTO object_versions "
        "(object_id, object_version, storage_version, size_bytes, md5_hash, content_type, body_blake3, status) "
        "VALUES ($1, 1, 5, 123, 'deadbeef', 'text/html', $2, 'uploaded')",
        object_id,
        BODY_BLAKE3,
    )
    await conn.execute(
        "INSERT INTO multipart_uploads (upload_id, bucket_id, object_key, is_completed, initiated_at) "
        "VALUES ($1, $2, $3, TRUE, now())",
        upload_id,
        bucket_id,
        KEY,
    )

    try:
        yield (
            conn,
            {
                "bucket_id": bucket_id,
                "object_id": object_id,
                "upload_id": upload_id,
                "account": account,
                "name": bucket_name,
            },
        )
    finally:
        await tx.rollback()
        await conn.close()


async def _add_part(conn: asyncpg.Connection, ids: dict, part_number: int, n_chunks: int) -> uuid.UUID:
    part_id = uuid.uuid4()
    await conn.execute(
        "INSERT INTO parts (part_id, upload_id, object_id, object_version, part_number, size_bytes, etag, uploaded_at) "
        "VALUES ($1, $2, $3, 1, $4, 123, 'deadbeef', now())",
        part_id,
        ids["upload_id"],
        ids["object_id"],
        part_number,
    )
    for ci in range(n_chunks):
        await conn.execute(
            "INSERT INTO part_chunks (part_id, chunk_index, cipher_size_bytes) VALUES ($1, $2, 139)", part_id, ci
        )
    return part_id


async def _store_chunk(
    conn: asyncpg.Connection, part_id: uuid.UUID, ci: int, file_id: str, arion_hash: str | None
) -> None:
    await conn.fetchval(get_query("insert_chunk_backend"), part_id, ci, "arion", file_id, arion_hash)


async def _rollup(conn: asyncpg.Connection, ids: dict) -> None:
    await conn.execute(get_query("update_object_version_arion_hash"), ids["object_id"], 1, "arion")


async def _version_arion_hash(conn: asyncpg.Connection, ids: dict) -> str | None:
    return await conn.fetchval(
        "SELECT arion_hash FROM object_versions WHERE object_id = $1 AND object_version = 1", ids["object_id"]
    )


@pytest.mark.parametrize("query_name", LISTING_QUERIES)
async def test_every_listing_query_selects_the_arion_hash(query_name: str) -> None:
    assert "arion_hash" in get_query(query_name), f"{query_name}.sql must project arion_hash"


async def test_single_chunk_object_lists_its_arion_hash(seeded: tuple[asyncpg.Connection, dict]) -> None:
    conn, ids = seeded
    part_id = await _add_part(conn, ids, 1, 1)
    await _store_chunk(conn, part_id, 0, FILE_ID, ARION_HASH)
    await _rollup(conn, ids)

    rows = await conn.fetch(get_query("list_objects"), ids["bucket_id"], None, None, 10, None)
    assert [r["arion_hash"] for r in rows] == [ARION_HASH]
    assert rows[0]["body_blake3"] == BODY_BLAKE3, "the plaintext digest is still there, just not as the Arion hash"

    console = await conn.fetch(get_query("console_list_objects"), ids["bucket_id"], None, 10, 0)
    assert [r["arion_hash"] for r in console] == [ARION_HASH]

    recent = await conn.fetch(get_query("get_recent_uploads_for_account"), ids["account"])
    assert [r["arion_hash"] for r in recent] == [ARION_HASH]


async def test_head_reports_the_arion_hash_not_the_hcfs_file_id(seeded: tuple[asyncpg.Connection, dict]) -> None:
    conn, ids = seeded
    part_id = await _add_part(conn, ids, 1, 1)
    await _store_chunk(conn, part_id, 0, FILE_ID, ARION_HASH)

    head = await conn.fetchrow(get_query("get_object_head_by_path"), ids["name"], KEY)
    assert head["arion_file_hash"] == ARION_HASH

    fallback = await conn.fetchval(get_query("get_chunk_arion_hash"), "arion", ids["object_id"], 1, 1, 0)
    assert fallback == ARION_HASH
    # The downloader still addresses the chunk by HCFS's file_id.
    identifier = await conn.fetchval(get_query("get_chunk_backend_identifier"), "arion", ids["object_id"], 1, 1, 0)
    assert identifier == FILE_ID


async def test_multi_chunk_object_has_no_single_arion_hash(seeded: tuple[asyncpg.Connection, dict]) -> None:
    conn, ids = seeded
    part_id = await _add_part(conn, ids, 1, 2)
    await _store_chunk(conn, part_id, 0, "f0" * 32, "a0" * 32)
    await _store_chunk(conn, part_id, 1, "f1" * 32, "a1" * 32)
    await _rollup(conn, ids)
    assert await _version_arion_hash(conn, ids) is None


async def test_first_part_of_a_multipart_upload_does_not_stamp_the_object(
    seeded: tuple[asyncpg.Connection, dict],
) -> None:
    """The rollup runs after each part lands; part 1 finishing alone must not claim the object."""
    conn, ids = seeded
    part1 = await _add_part(conn, ids, 1, 1)
    await _add_part(conn, ids, 2, 1)
    await _store_chunk(conn, part1, 0, FILE_ID, ARION_HASH)
    await _rollup(conn, ids)
    assert await _version_arion_hash(conn, ids) is None


async def test_a_retry_without_a_hash_keeps_the_stored_one(seeded: tuple[asyncpg.Connection, dict]) -> None:
    conn, ids = seeded
    part_id = await _add_part(conn, ids, 1, 1)
    await _store_chunk(conn, part_id, 0, FILE_ID, ARION_HASH)
    await _store_chunk(conn, part_id, 0, FILE_ID, None)
    stored = await conn.fetchval(
        "SELECT cb.arion_hash FROM chunk_backend cb JOIN part_chunks pc ON pc.id = cb.chunk_id WHERE pc.part_id = $1",
        part_id,
    )
    assert stored == ARION_HASH
