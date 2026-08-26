"""Every query that resolves an S3 key must see copied keys, against real Postgres.

Same-bucket CopyObject cannot mint a new object_id — v5 AAD binds bucket_id+object_id — so it
attaches an extra name in `object_names` and both keys share one object_id. A query that matches
`objects.object_key` directly therefore sees the original and misses every copy.

That is not hypothetical: `get_object_by_path_and_version.sql` matched the key directly while its
unversioned sibling `get_object_by_path.sql` resolved properly, and `multipart.py` picks between
the two on whether CopySource carried a ?versionId. Copying from a copied key worked without a
version id and returned NoSuchVersion with one.

It survived review because these are NEW files on a feature branch: a three-way merge has no base
to conflict against, so they merge silently, and nothing exercised them against a database that
had an alias in it. This file is that database.
"""

from __future__ import annotations

import os
import uuid
from typing import AsyncGenerator

import asyncpg
import pytest
import pytest_asyncio

from hippius_s3.utils import get_query


_DB_URL = os.getenv("DATABASE_URL", "postgresql://postgres:postgres@localhost:5432/hippius?sslmode=disable")

PRIMARY_KEY = "orig/data.bin"
ALIAS_KEY = "copies/data-copy.bin"


@pytest_asyncio.fixture
async def aliased() -> AsyncGenerator[tuple[asyncpg.Connection, dict], None]:
    """One object reachable under two keys — the shape same-bucket CopyObject leaves behind."""
    try:
        conn = await asyncpg.connect(_DB_URL)
    except OSError as exc:  # only an unreachable server is a legitimate skip
        pytest.skip(f"postgres unavailable: {exc}")

    if not await conn.fetchval("SELECT to_regprocedure('resolve_object_id(uuid,text)') IS NOT NULL"):
        await conn.close()
        pytest.fail("resolve_object_id() is missing — run `python -m hippius_s3.scripts.migrate`")

    tx = conn.transaction()
    await tx.start()

    account = f"5T{uuid.uuid4().hex[:12]}"
    bucket_id, object_id = uuid.uuid4(), uuid.uuid4()
    bucket = f"alias-{uuid.uuid4().hex[:10]}"

    await conn.execute("INSERT INTO users (main_account_id) VALUES ($1)", account)
    await conn.execute(
        "INSERT INTO buckets (bucket_id, bucket_name, main_account_id, created_at) VALUES ($1,$2,$3, now())",
        bucket_id,
        bucket,
        account,
    )
    await conn.execute(
        "INSERT INTO objects (object_id, bucket_id, object_key, current_object_version, created_at) "
        "VALUES ($1,$2,$3,2, now())",
        object_id,
        bucket_id,
        PRIMARY_KEY,
    )
    for version in (1, 2):
        await conn.execute(
            "INSERT INTO object_versions "
            "(object_id, object_version, storage_version, size_bytes, md5_hash, content_type, status) "
            "VALUES ($1,$2,5,128,'deadbeef','application/octet-stream','uploaded')",
            object_id,
            version,
        )
    # the copy: a second name on the SAME object_id
    await conn.execute(
        "INSERT INTO object_names (bucket_id, object_key, object_id) VALUES ($1,$2,$3)",
        bucket_id,
        ALIAS_KEY,
        object_id,
    )

    try:
        yield conn, {"bucket_id": bucket_id, "object_id": object_id, "bucket": bucket}
    finally:
        await tx.rollback()
        await conn.close()


@pytest.mark.asyncio
async def test_resolve_object_id_prefers_the_primary_but_finds_the_alias(
    aliased: tuple[asyncpg.Connection, dict],
) -> None:
    conn, ids = aliased
    for key in (PRIMARY_KEY, ALIAS_KEY):
        got = await conn.fetchval("SELECT resolve_object_id($1::uuid, $2)", ids["bucket_id"], key)
        assert got == ids["object_id"], f"{key} must resolve to the shared object_id"
    assert await conn.fetchval("SELECT resolve_object_id($1::uuid, 'no/such.bin')", ids["bucket_id"]) is None


@pytest.mark.asyncio
async def test_versioned_copy_source_resolves_a_copied_key(aliased: tuple[asyncpg.Connection, dict]) -> None:
    """The regression. CopySource with ?versionId= on a copied key must not 404."""
    conn, ids = aliased
    row = await conn.fetchrow(get_query("get_object_by_path_and_version"), ids["bucket_id"], ALIAS_KEY, 1)
    assert row is not None, "CopySource ?versionId= on a copied key returned nothing — NoSuchVersion"
    assert row["object_id"] == ids["object_id"]
    assert row["object_version"] == 1


@pytest.mark.asyncio
async def test_both_copy_source_branches_agree(aliased: tuple[asyncpg.Connection, dict]) -> None:
    """multipart.py picks between these two on whether ?versionId= was supplied.

    They must resolve the same key to the same object, or CopySource succeeds or fails depending
    on a parameter that has nothing to do with which object is being named.
    """
    conn, ids = aliased
    unversioned = await conn.fetchrow(get_query("get_object_by_path"), ids["bucket_id"], ALIAS_KEY)
    versioned = await conn.fetchrow(get_query("get_object_by_path_and_version"), ids["bucket_id"], ALIAS_KEY, 2)
    assert unversioned is not None and versioned is not None
    assert unversioned["object_id"] == versioned["object_id"] == ids["object_id"]


@pytest.mark.parametrize(
    "query_name",
    [
        "get_object_by_path",
        "get_object_by_path_and_version",
        "get_object_head_by_path",
        "get_object_for_download_with_permissions",
        "get_object_for_download_with_permissions_by_version",
        "lock_object_and_get_version",
    ],
)
def test_key_resolving_queries_go_through_resolve_object_id(query_name: str) -> None:
    """A new key-resolving query that matches object_key directly silently drops copied keys."""
    sql = get_query(query_name)
    assert "resolve_object_id" in sql, (
        f"{query_name}.sql resolves an S3 key without resolve_object_id, so it cannot see keys "
        "created by same-bucket CopyObject"
    )


@pytest.mark.asyncio
async def test_delete_marker_insert_deliberately_does_not_resolve_aliases(
    aliased: tuple[asyncpg.Connection, dict],
) -> None:
    """The counter-case, pinned so nobody 'fixes' it into a bug.

    insert_delete_marker matches objects.object_key on purpose. A marker lives on the shared
    object_version, so minting one for a copied key would hide the ORIGINAL key too. Both delete
    paths call drop_s3_name first and only fall through on "last" — i.e. when the key is the sole
    remaining name — so by the time this query runs the direct match is the correct one.
    """
    conn, ids = aliased
    assert "resolve_object_id" not in get_query("insert_delete_marker")

    minted = await conn.fetchrow(get_query("insert_delete_marker"), ids["bucket_id"], ALIAS_KEY)
    assert minted is None, "a copied key must not mint a delete marker on the shared object"

    minted = await conn.fetchrow(get_query("insert_delete_marker"), ids["bucket_id"], PRIMARY_KEY)
    assert minted is not None and minted["object_version"] == 3
