"""One digest column, every surface that shows it — against real Postgres.

The Arion-hash digest is read by three independent surfaces (S3 ListObjects, the flagged SQL
rollup variant of it, and the console/user listings). They drifted once already: the console
queries were left pointing at `ipfs_cid`, which nothing has written since the Arion cutover, so
they returned NULL for every modern object while ListObjects showed a value. These tests pin all
of them to the same column so the next divergence fails here instead of in the console.

They also pin the reason the digest is NOT in `ipfs_cid`/`cid_id`: the purge+unpin scripts read
`COALESCE(c.cid, ov.ipfs_cid)` filtered only against NULL/''/'pending', and a 64-hex BLAKE3
digest passes all three — it would enter the unpin worklist as though it were a pin.
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

DIGEST = "6437b3ac38465133ffb63b75273a8db548c558465d79db03fd359c6cd5bd9d85"

# Every listing query that must carry the digest, and the column alias it exposes it under.
DIGEST_QUERIES = [
    "list_objects",
    "list_objects_delimited",
    "console_list_objects",
    "get_recent_uploads_for_account",
]


@pytest_asyncio.fixture
async def seeded() -> AsyncGenerator[tuple[asyncpg.Connection, dict], None]:
    try:
        conn = await asyncpg.connect(_DB_URL)
    except Exception as exc:  # noqa: BLE001 - integration tier skips when PG is absent
        pytest.skip(f"postgres unavailable: {exc}")

    tx = conn.transaction()
    await tx.start()

    account = f"5TEST{uuid.uuid4().hex[:12]}"
    bucket_id = uuid.uuid4()
    object_id = uuid.uuid4()
    bucket_name = f"digest-{uuid.uuid4().hex[:10]}"

    await conn.execute("INSERT INTO users (main_account_id, created_at) VALUES ($1, now())", account)
    await conn.execute(
        "INSERT INTO buckets (bucket_id, bucket_name, main_account_id, created_at) VALUES ($1, $2, $3, now())",
        bucket_id,
        bucket_name,
        account,
    )
    await conn.execute(
        "INSERT INTO objects (object_id, bucket_id, object_key, current_object_version, created_at) "
        "VALUES ($1, $2, 'k/digest.bin', 1, now())",
        object_id,
        bucket_id,
    )
    await conn.execute(
        "INSERT INTO object_versions "
        "(object_id, object_version, storage_version, size_bytes, md5_hash, content_type, body_blake3, status) "
        "VALUES ($1, 1, 5, 123, 'deadbeef', 'application/octet-stream', $2, 'uploaded')",
        object_id,
        DIGEST,
    )

    try:
        yield conn, {"bucket_id": bucket_id, "object_id": object_id, "account": account, "name": bucket_name}
    finally:
        await tx.rollback()
        await conn.close()


@pytest.mark.parametrize("query_name", DIGEST_QUERIES)
async def test_every_listing_query_selects_the_digest_column(query_name: str) -> None:
    """A query that stops projecting body_blake3 silently blanks its surface — catch it here."""
    assert "body_blake3" in get_query(query_name), (
        f"{query_name}.sql must project body_blake3; without it the surface it feeds shows no digest"
    )


async def test_list_objects_returns_the_digest(seeded: tuple[asyncpg.Connection, dict]) -> None:
    conn, ids = seeded
    rows = await conn.fetch(get_query("list_objects"), ids["bucket_id"], None, None, 10, None)
    assert [r["body_blake3"] for r in rows] == [DIGEST]


async def test_console_listing_returns_the_digest(seeded: tuple[asyncpg.Connection, dict]) -> None:
    """The console surface that used to return NULL for every post-Arion object."""
    conn, ids = seeded
    rows = await conn.fetch(get_query("console_list_objects"), ids["bucket_id"], None, 10, 0)
    assert [r["body_blake3"] for r in rows] == [DIGEST]
    assert rows[0]["ipfs_cid"] is None, "the legacy column must stay untouched"


async def test_recent_uploads_returns_the_digest(seeded: tuple[asyncpg.Connection, dict]) -> None:
    conn, ids = seeded
    rows = await conn.fetch(get_query("get_recent_uploads_for_account"), ids["account"])
    assert [r["body_blake3"] for r in rows] == [DIGEST]
    assert rows[0]["ipfs_cid"] is None


async def test_the_digest_is_invisible_to_the_unpin_worklist(seeded: tuple[asyncpg.Connection, dict]) -> None:
    """The whole reason for a dedicated column.

    This is the shape `nuke_user.py`, `purge_buckets.py`, `purge_source_versions.py` and
    `cleanup_migration_versions.py` use to collect CIDs to unpin. It must find nothing for an
    object whose only hash is a plaintext BLAKE3 digest.
    """
    conn, ids = seeded
    cids = await conn.fetch(
        """
        SELECT DISTINCT COALESCE(c.cid, ov.ipfs_cid) AS cid
        FROM object_versions ov
        LEFT JOIN cids c ON ov.cid_id = c.id
        WHERE ov.object_id = $1
          AND COALESCE(c.cid, ov.ipfs_cid) IS NOT NULL
          AND COALESCE(c.cid, ov.ipfs_cid) != ''
          AND COALESCE(c.cid, ov.ipfs_cid) != 'pending'
        """,
        ids["object_id"],
    )
    assert cids == [], "a plaintext digest must never be collectable as a CID to unpin"
