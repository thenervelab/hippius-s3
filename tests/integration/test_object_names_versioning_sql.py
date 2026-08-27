"""Where object aliases meet object versioning, at the SQL layer.

A same-bucket CopyObject attaches a SECOND S3 key to one `object_id` — the v5 AAD binds the id, so
a copy cannot mint a new one (`object_names`, `resolve_object_id`). Versions therefore hang off the
object, not off the name. Two features that are each correct alone then meet head-on:

* A delete marker lives on the shared `object_id`, so it hides EVERY name at once. That is right
  when the object is being deleted and wrong when the client only asked to drop one key — which is
  why the endpoint drops the name first and only reaches the marker when that was the last name.
* A versioned DELETE would destroy a version reachable under the other key, so the endpoint refuses
  when the object carries an alias. It can only refuse if the resolver actually finds the object
  through the alias, and if it can see how many names there are.

These are the SQL-level facts those decisions rest on. The unit tests mock the database away, so
nothing else exercises them against real Postgres.
"""

from __future__ import annotations

import os
import uuid
from typing import Any
from typing import AsyncGenerator

import asyncpg
import pytest
import pytest_asyncio

from hippius_s3.utils import get_query


pytestmark = pytest.mark.asyncio

# HIPPIUS_E2E_DB_DSN first, matching test_list_objects_sql_rollup_diff.py: the integration
# conftest loads .env.test-local with override=True, so DATABASE_URL alone cannot be pointed
# at a scratch database from the command line.
_DB_URL = (
    os.getenv("HIPPIUS_E2E_DB_DSN")
    or os.getenv("DATABASE_URL")
    or "postgresql://postgres:postgres@localhost:5432/hippius?sslmode=disable"
)

PRIMARY = "primary.txt"
ALIAS = "alias.txt"


@pytest_asyncio.fixture
async def db() -> AsyncGenerator[dict[str, Any], None]:
    """One bucket, one object with two live versions, published under two names."""
    try:
        c = await asyncpg.connect(_DB_URL)
    except (OSError, asyncpg.PostgresError) as e:
        pytest.skip(f"integration Postgres unavailable ({e}); run `docker compose up -d postgres`")

    bucket_id = uuid.uuid4()
    object_id = uuid.uuid4()
    bucket_name = f"aliasver-{uuid.uuid4().hex[:8]}"
    account = f"acct-{uuid.uuid4().hex[:8]}"

    tx = c.transaction()
    await tx.start()
    await c.execute("INSERT INTO users(main_account_id) VALUES($1) ON CONFLICT DO NOTHING", account)
    await c.execute(
        "INSERT INTO buckets(bucket_id, bucket_name, created_at, is_public, main_account_id)"
        " VALUES($1,$2,now(),false,$3)",
        bucket_id,
        bucket_name,
        account,
    )
    await c.execute(
        "INSERT INTO objects(object_id, bucket_id, object_key, created_at, current_object_version)"
        " VALUES($1,$2,$3,now(),2)",
        object_id,
        bucket_id,
        PRIMARY,
    )
    await c.executemany(
        "INSERT INTO object_versions(object_id, object_version, storage_version, size_bytes,"
        " content_type, md5_hash) VALUES($1,$2,5,$3,'text/plain',$4)",
        [(object_id, 1, 8, "aaa"), (object_id, 2, 7, "bbb")],
    )
    await c.execute(
        "INSERT INTO object_names(bucket_id, object_key, object_id) VALUES($1,$2,$3)",
        bucket_id,
        ALIAS,
        object_id,
    )

    try:
        yield {"conn": c, "bucket_id": bucket_id, "object_id": object_id, "bucket_name": bucket_name}
    finally:
        await tx.rollback()
        await c.close()


async def _lock(db: dict[str, Any], key: str, version: int) -> Any:
    return await db["conn"].fetchrow(get_query("lock_object_and_get_version"), db["bucket_id"], key, version)


async def _listed_keys(db: dict[str, Any]) -> list[str]:
    rows = await db["conn"].fetch(get_query("list_objects"), db["bucket_id"], None, None, 100, None)
    return sorted(r["object_key"] for r in rows)


async def _mark_deleted(db: dict[str, Any]) -> None:
    """Insert a delete marker as the new current version, as the endpoint does."""
    await db["conn"].fetchrow(get_query("insert_delete_marker"), db["bucket_id"], PRIMARY)


# --- resolution through an alias -----------------------------------------------------------


async def test_the_primary_name_resolves(db: dict[str, Any]) -> None:
    row = await _lock(db, PRIMARY, 1)
    assert row is not None
    assert row["object_id"] == db["object_id"]
    assert row["object_version"] == 1


async def test_an_alias_resolves_to_the_same_object(db: dict[str, Any]) -> None:
    """Without resolve_object_id the alias matched nothing and a versioned DELETE on it answered a
    silent, misleading 204."""
    row = await _lock(db, ALIAS, 1)
    assert row is not None
    assert row["object_id"] == db["object_id"]
    assert row["object_version"] == 1


async def test_alias_count_is_visible_to_the_delete_guard(db: dict[str, Any]) -> None:
    """The endpoint refuses a versioned DELETE on this count, inside the same row lock."""
    assert (await _lock(db, PRIMARY, 1))["alias_count"] == 1
    assert (await _lock(db, ALIAS, 1))["alias_count"] == 1


async def test_alias_count_is_zero_for_a_single_named_object(db: dict[str, Any]) -> None:
    await db["conn"].execute("DELETE FROM object_names WHERE object_id = $1", db["object_id"])
    assert (await _lock(db, PRIMARY, 1))["alias_count"] == 0


async def test_an_unknown_key_resolves_to_nothing(db: dict[str, Any]) -> None:
    assert await _lock(db, "no-such-key.txt", 1) is None


# --- listing: a marker hides every name ----------------------------------------------------


async def test_both_names_are_listed_while_the_object_lives(db: dict[str, Any]) -> None:
    assert await _listed_keys(db) == sorted([PRIMARY, ALIAS])


async def test_a_delete_marker_hides_every_name(db: dict[str, Any]) -> None:
    """A marker is the OBJECT being deleted, and the object is what both names point at.

    This is exactly why the endpoint drops the name first: reaching this state when the client only
    asked to delete one key would silently take the other key's content out of the listing too.
    """
    await _mark_deleted(db)
    assert await _listed_keys(db) == []


async def test_removing_the_marker_brings_every_name_back(db: dict[str, Any]) -> None:
    await _mark_deleted(db)
    await db["conn"].execute(
        "UPDATE object_versions SET deleted_at = now() WHERE object_id = $1 AND is_delete_marker",
        db["object_id"],
    )
    await db["conn"].execute("UPDATE objects SET current_object_version = 2 WHERE object_id = $1", db["object_id"])
    assert await _listed_keys(db) == sorted([PRIMARY, ALIAS])


async def test_dropping_the_alias_row_leaves_the_primary_listed(db: dict[str, Any]) -> None:
    """The "alias" branch of drop_s3_name: one name goes, the object and its other name stay."""
    await db["conn"].execute(
        "DELETE FROM object_names WHERE bucket_id = $1 AND object_key = $2", db["bucket_id"], ALIAS
    )
    assert await _listed_keys(db) == [PRIMARY]
