"""Truth table for `get_object_for_download_with_permissions_by_version`.

Two independently-correct rules meet in this one query, and they pull in opposite directions:

* Reserved multipart placeholders (`size_bytes = 0`, `md5_hash = ''`) must NOT be reachable by
  explicit versionId — serving one yields a 0-byte body instead of NoSuchVersion. Every in-flight
  MPU has one, and an aborted MPU leaves one behind permanently.
* A delete marker is byte-for-byte the same shape — zero size, no md5 — but MUST stay reachable,
  because addressing one by version is how a client gets the 405 + `x-amz-delete-marker` that says
  "this version is a marker", rather than a misleading 404.

Those rules landed on separate branches. Each passed its own CI; merged, the placeholder filter
silently swallowed delete markers and versioned GET/HEAD on a marker started returning
NoSuchVersion. Nothing in either branch's tests could catch that, because neither branch had both
row shapes in the same database.

This pins all three shapes at once, against the real query and a real Postgres.
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

_BUCKET = "resolver-matrix"
_KEY = "doc.txt"


@pytest_asyncio.fixture
async def conn() -> AsyncGenerator[asyncpg.Connection, None]:
    """TEMP tables shadowing everything the query touches. pg_temp precedes `public` in the search
    path, so the shipped SQL resolves to these; they vanish with the connection."""
    try:
        c = await asyncpg.connect(_DB_URL)
    except (OSError, asyncpg.PostgresError) as e:
        pytest.skip(f"integration Postgres unavailable ({e}); run `docker compose up -d postgres`")

    await c.execute("""
        CREATE TEMP TABLE buckets(
            bucket_id uuid, bucket_name text, main_account_id text, is_public bool DEFAULT false,
            deleted_at timestamptz
        ) ON COMMIT PRESERVE ROWS;
        CREATE TEMP TABLE objects(
            object_id uuid, bucket_id uuid, object_key text, current_object_version bigint,
            created_at timestamptz DEFAULT now(), deleted_at timestamptz
        ) ON COMMIT PRESERVE ROWS;
        CREATE TEMP TABLE object_versions(
            object_id uuid, object_version bigint, size_bytes bigint, md5_hash text,
            content_type text DEFAULT 'application/octet-stream', metadata jsonb,
            multipart bool DEFAULT false, status text DEFAULT 'publishing',
            append_version int DEFAULT 0, storage_version int2 DEFAULT 5, ipfs_cid text,
            cid_id uuid, encryption_version int2, enc_suite_id text, enc_chunk_size_bytes int,
            kek_id uuid, wrapped_dek bytea, is_delete_marker bool NOT NULL DEFAULT false,
            deleted_at timestamptz, last_modified timestamptz DEFAULT now(),
            created_at timestamptz DEFAULT now(), body_blake3 text
        ) ON COMMIT PRESERVE ROWS;
        CREATE TEMP TABLE cids(id uuid, cid text) ON COMMIT PRESERVE ROWS;
        CREATE TEMP TABLE parts(
            part_id uuid, object_id uuid, object_version bigint, part_number int,
            size_bytes bigint, ipfs_cid text, cid_id uuid
        ) ON COMMIT PRESERVE ROWS;
        CREATE TEMP TABLE multipart_uploads(
            upload_id uuid, object_id uuid, initiated_at timestamptz DEFAULT now()
        ) ON COMMIT PRESERVE ROWS;
    """)

    object_id = uuid.uuid4()
    bucket_id = uuid.uuid4()
    await c.execute(
        "INSERT INTO buckets(bucket_id, bucket_name, main_account_id) VALUES($1,$2,'acct')",
        bucket_id,
        _BUCKET,
    )
    await c.execute(
        "INSERT INTO objects(object_id, bucket_id, object_key, current_object_version) VALUES($1,$2,$3,3)",
        object_id,
        bucket_id,
        _KEY,
    )
    await c.executemany(
        "INSERT INTO object_versions(object_id, object_version, size_bytes, md5_hash, multipart,"
        " is_delete_marker) VALUES($1,$2,$3,$4,$5,$6)",
        [
            # A completed version: real bytes, real md5.
            (object_id, 1, 11, "d41d8cd98f00b204e9800998ecf8427e", False, False),
            # A reserved MPU placeholder: InitiateMultipartUpload ran, Complete never did.
            (object_id, 2, 0, "", True, False),
            # A delete marker: same zero/empty shape as the placeholder, opposite meaning.
            (object_id, 3, 0, None, False, True),
        ],
    )
    try:
        yield c
    finally:
        await c.close()


async def _resolve(conn: asyncpg.Connection, version: int):
    return await conn.fetchrow(get_query("get_object_for_download_with_permissions_by_version"), _BUCKET, _KEY, version)


async def test_completed_version_resolves(conn: asyncpg.Connection) -> None:
    row = await _resolve(conn, 1)
    assert row is not None
    assert row["is_delete_marker"] is False


async def test_reserved_multipart_placeholder_is_unreachable(conn: asyncpg.Connection) -> None:
    """Serving one would return a 0-byte body where the client expects NoSuchVersion."""
    assert await _resolve(conn, 2) is None


async def test_delete_marker_resolves_so_the_handler_can_reject_it(conn: asyncpg.Connection) -> None:
    """THE regression guard.

    A marker has the same zero-size/no-md5 shape as a reserved placeholder, so a completeness
    filter written with only placeholders in mind swallows it — and versioned GET/HEAD then answers
    NoSuchVersion (404) instead of MethodNotAllowed (405) + x-amz-delete-marker. The query must
    return the row; rejecting it is the endpoint's job, not the filter's.
    """
    row = await _resolve(conn, 3)
    assert row is not None, "a delete marker must resolve by explicit versionId"
    assert row["is_delete_marker"] is True


async def test_soft_deleted_version_is_unreachable(conn: asyncpg.Connection) -> None:
    """A version removed by a versioned DELETE is gone to readers even while its row lingers."""
    await conn.execute(
        "UPDATE object_versions SET deleted_at = now() WHERE object_version = 3 AND object_id ="
        " (SELECT object_id FROM objects WHERE object_key = $1)",
        _KEY,
    )
    assert await _resolve(conn, 3) is None


# ---------------------------------------------------------------------------------------------
# The UNVERSIONED resolvers, over the same three row shapes.
#
# These pick the NEWEST admitted version rather than an explicit one, and they are the hot path:
# get_object_for_download_with_permissions serves every GET that does not name a versionId.
#
# They carry the identical marker-admission predicate, and until now nothing executed it. Verified
# by mutation on 2026-08-26: deleting `v.is_delete_marker OR` from that query left all 295
# integration tests passing, while the query silently resolved past the marker to version 1 and
# served content the client had deleted. That is the single worst outcome in this whole feature,
# and it was invisible.
#
# The fixture's current_object_version is 3 (the marker), so "resolves to the marker" is the
# correct answer for all three. Falling through to version 1 is the bug.
# ---------------------------------------------------------------------------------------------


_BY_NAME_QUERIES = [
    "get_object_for_download_with_permissions",
    "get_object_head_by_path",
]


@pytest.mark.parametrize("query_name", _BY_NAME_QUERIES)
async def test_unversioned_resolver_stops_at_the_delete_marker(conn: asyncpg.Connection, query_name: str) -> None:
    """Must resolve TO the marker, never past it.

    Resolving past it returns version 1's bytes for a key the client deleted — a silent read of
    deleted data on the hottest query in the system.
    """
    row = await conn.fetchrow(get_query(query_name), _BUCKET, _KEY)
    assert row is not None, "the marker itself must resolve; rejecting it is the endpoint's job"
    assert row["is_delete_marker"] is True, "resolved PAST the delete marker — serving deleted data"
    assert (row["size_bytes"] or 0) == 0


async def test_unversioned_get_object_by_path_stops_at_the_delete_marker(conn: asyncpg.Connection) -> None:
    """Same rule, but this one is keyed on bucket_id rather than bucket_name."""
    bucket_id = await conn.fetchval("SELECT bucket_id FROM buckets WHERE bucket_name = $1", _BUCKET)
    row = await conn.fetchrow(get_query("get_object_by_path"), bucket_id, _KEY)
    assert row is not None
    assert row["is_delete_marker"] is True, "resolved PAST the delete marker — serving deleted data"


@pytest.mark.parametrize("query_name", _BY_NAME_QUERIES)
async def test_unversioned_resolver_skips_a_reserved_placeholder(conn: asyncpg.Connection, query_name: str) -> None:
    """With the marker soft-deleted, version 2 is a reserved MPU placeholder and must be skipped —
    resolution falls to version 1, the newest COMPLETED version, not to the 0-byte placeholder."""
    await conn.execute(
        "UPDATE object_versions SET deleted_at = now() WHERE object_version = 3 AND object_id ="
        " (SELECT object_id FROM objects WHERE object_key = $1)",
        _KEY,
    )
    row = await conn.fetchrow(get_query(query_name), _BUCKET, _KEY)
    assert row is not None
    assert row["object_version"] == 1, "a reserved MPU placeholder must never be served"
    assert row["is_delete_marker"] is False


@pytest.mark.parametrize("query_name", _BY_NAME_QUERIES)
async def test_unversioned_resolver_ignores_soft_deleted_versions(conn: asyncpg.Connection, query_name: str) -> None:
    """Every version tombstoned: the key resolves to nothing at all, rather than to a dead row."""
    await conn.execute(
        "UPDATE object_versions SET deleted_at = now() WHERE object_id ="
        " (SELECT object_id FROM objects WHERE object_key = $1)",
        _KEY,
    )
    assert await conn.fetchrow(get_query(query_name), _BUCKET, _KEY) is None
