"""The two statements a versioned DELETE runs, against real Postgres.

Both were covered only by FakeDb assertions, which verify that a query was called with certain
arguments — never what the SQL then does. Each has one clause carrying the whole invariant:

  repoint_current_version_after_delete.sql  `ov.object_version < $2`
      Picks the successor when the current version is deleted. Without the bound it becomes an
      unbounded MAX(), and `create_migration_version` inserts rows ABOVE current_object_version
      without bumping it — so the successor can be an incomplete migration placeholder, promoted to
      current and served as the object's live content.

  soft_delete_object_version.sql            `AND deleted_at IS NULL`
      Makes the soft-delete a compare-and-swap. Without it a repeated DELETE of the same version id
      re-stamps deleted_at and RETURNS a row again, and the caller enqueues a second unpin for
      backend copies the first unpin already destroyed.

Seeded through the real schema in a rolled-back transaction, so the FKs and defaults are the
shipped ones.
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

KEY = "obj/under-test.bin"


@pytest_asyncio.fixture
async def db() -> AsyncGenerator[asyncpg.Connection, None]:
    try:
        conn = await asyncpg.connect(_DB_URL)
    except OSError as exc:  # only an unreachable server is a legitimate skip
        pytest.skip(f"postgres unavailable: {exc}")
    tx = conn.transaction()
    await tx.start()
    try:
        yield conn
    finally:
        await tx.rollback()
        await conn.close()


async def _seed(
    conn: asyncpg.Connection,
    *,
    current: int,
    versions: dict[int, bool],
) -> uuid.UUID:
    """`versions` maps object_version -> live (True) / soft-deleted (False)."""
    account = f"5T{uuid.uuid4().hex[:12]}"
    bucket_id, object_id = uuid.uuid4(), uuid.uuid4()
    await conn.execute("INSERT INTO users (main_account_id) VALUES ($1)", account)
    await conn.execute(
        "INSERT INTO buckets (bucket_id, bucket_name, main_account_id, created_at) VALUES ($1,$2,$3, now())",
        bucket_id,
        f"vdp-{uuid.uuid4().hex[:10]}",
        account,
    )
    await conn.execute(
        "INSERT INTO objects (object_id, bucket_id, object_key, current_object_version, created_at) "
        "VALUES ($1,$2,$3,$4, now())",
        object_id,
        bucket_id,
        KEY,
        current,
    )
    for version in sorted(versions):
        await conn.execute(
            "INSERT INTO object_versions "
            "(object_id, object_version, storage_version, size_bytes, md5_hash, content_type, status) "
            "VALUES ($1,$2,5,128,'deadbeef','application/octet-stream','uploaded')",
            object_id,
            version,
        )
    if any(not live for live in versions.values()):
        await conn.execute(
            "UPDATE object_versions SET deleted_at = now() WHERE object_id = $1 AND object_version = ANY($2::bigint[])",
            object_id,
            [v for v, live in versions.items() if not live],
        )
    return object_id


async def _repoint(conn: asyncpg.Connection, object_id: uuid.UUID, deleted_version: int) -> int | None:
    return await conn.fetchval(get_query("repoint_current_version_after_delete"), object_id, deleted_version)


# --- repoint_current_version_after_delete -------------------------------------------------


async def test_repoint_picks_the_newest_live_version_below(db: asyncpg.Connection) -> None:
    object_id = await _seed(db, current=3, versions={1: True, 2: True, 3: True})

    assert await _repoint(db, object_id, 3) == 2


async def test_repoint_skips_a_soft_deleted_successor(db: asyncpg.Connection) -> None:
    """Version 2 is already tombstoned, so the successor must be 1 — promoting 2 would point
    current_object_version at a dead row: invisible to reads, and permanently un-unpinnable."""
    object_id = await _seed(db, current=3, versions={1: True, 2: False, 3: True})

    assert await _repoint(db, object_id, 3) == 1


async def test_repoint_never_promotes_a_version_above_the_deleted_one(db: asyncpg.Connection) -> None:
    """The clause under test.

    Version 9 is the `create_migration_version` shape: inserted ABOVE current_object_version without
    bumping it. An unbounded MAX() promotes that placeholder to current; `object_version < $2`
    confines the search to real predecessors.
    """
    object_id = await _seed(db, current=3, versions={1: True, 2: True, 3: True, 9: True})

    assert await _repoint(db, object_id, 3) == 2, "a version above the deleted one must never win"
    assert await db.fetchval("SELECT current_object_version FROM objects WHERE object_id = $1", object_id) == 2


async def test_repoint_returns_nothing_when_there_is_no_predecessor(db: asyncpg.Connection) -> None:
    """Caller's signal to soft-delete the whole object. The row must be left untouched."""
    object_id = await _seed(db, current=1, versions={1: True})

    assert await _repoint(db, object_id, 1) is None
    assert await db.fetchval("SELECT current_object_version FROM objects WHERE object_id = $1", object_id) == 1


async def test_repoint_is_a_noop_when_current_has_already_moved(db: asyncpg.Connection) -> None:
    """`AND o.current_object_version = $2` is the CAS: a concurrent delete that already repointed
    must not be undone by this one."""
    object_id = await _seed(db, current=2, versions={1: True, 2: True, 3: True})

    assert await _repoint(db, object_id, 3) is None
    assert await db.fetchval("SELECT current_object_version FROM objects WHERE object_id = $1", object_id) == 2


# --- soft_delete_object_version -----------------------------------------------------------


async def test_soft_delete_returns_the_row_once(db: asyncpg.Connection) -> None:
    object_id = await _seed(db, current=2, versions={1: True, 2: True})

    row = await db.fetchrow(get_query("soft_delete_object_version"), object_id, 1)

    assert row is not None
    assert (row["object_id"], row["object_version"]) == (object_id, 1)


async def test_soft_delete_is_a_compare_and_swap_not_a_blind_update(db: asyncpg.Connection) -> None:
    """The clause under test: a second delete of the same version must return NO row.

    The caller enqueues an unpin for whatever this returns. Returning a row twice enqueues a second
    unpin against backend copies the first one already destroyed.
    """
    object_id = await _seed(db, current=2, versions={1: True, 2: True})

    first = await db.fetchrow(get_query("soft_delete_object_version"), object_id, 1)
    second = await db.fetchrow(get_query("soft_delete_object_version"), object_id, 1)

    assert first is not None
    assert second is None, "a repeated versioned DELETE must not re-enqueue an unpin"


async def test_soft_delete_does_not_restamp_deleted_at(db: asyncpg.Connection) -> None:
    """Corollary: the tombstone timestamp is what the reaper's grace period is measured from, so
    re-stamping it would push the reap window out on every retry."""
    object_id = await _seed(db, current=2, versions={1: True, 2: True})

    await db.fetchrow(get_query("soft_delete_object_version"), object_id, 1)
    stamped = await db.fetchval(
        "SELECT deleted_at FROM object_versions WHERE object_id = $1 AND object_version = 1", object_id
    )
    await db.fetchrow(get_query("soft_delete_object_version"), object_id, 1)
    again = await db.fetchval(
        "SELECT deleted_at FROM object_versions WHERE object_id = $1 AND object_version = 1", object_id
    )

    assert stamped == again


async def test_soft_delete_reports_whether_it_removed_a_delete_marker(db: asyncpg.Connection) -> None:
    """The caller branches on `is_delete_marker` to decide whether deleting this version undeletes
    the object, so the projection has to be real rather than assumed."""
    object_id = await _seed(db, current=2, versions={1: True, 2: True})
    await db.execute(
        "UPDATE object_versions SET is_delete_marker = TRUE WHERE object_id = $1 AND object_version = 2",
        object_id,
    )

    row = await db.fetchrow(get_query("soft_delete_object_version"), object_id, 2)

    assert row is not None and row["is_delete_marker"] is True
