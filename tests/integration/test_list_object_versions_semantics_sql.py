"""Two predicates in `list_object_versions.sql` that nothing executed against real Postgres.

Both are one-line filters whose loss is silent — the query still runs, still returns rows, and every
existing test still passes. What changes is WHICH rows, and in both directions the extra rows are
ones the caller has already been told do not exist:

  `$7 current_only`      — buckets that never enabled versioning must answer with one entry per key,
                           the way AWS describes an unversioned bucket. Storage here has always been
                           versioned, so dropping this predicate does not return "nothing"; it
                           returns the entire retained overwrite history of every key in every
                           unversioned bucket. That is ~25 TB of superseded data across prod, exposed
                           through a listing whose callers use it to decide what to prune.
  `ov.deleted_at IS NULL` — a tombstoned version is gone as far as every read path is concerned. If
                           the listing still enumerates it, a client that lists-then-fetches gets a
                           version id that 404s, and a client pruning by listing tries to delete a
                           version that is already deleted.

The unit coverage asserts the flag reaches a FakePool, which cannot see either of these.
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

_DB_URL = os.getenv("DATABASE_URL", "postgresql://postgres:postgres@localhost:5432/hippius?sslmode=disable")

KEY_A = "a/first.bin"
KEY_B = "b/second.bin"


@pytest_asyncio.fixture
async def bucket_with_history() -> AsyncGenerator[tuple[asyncpg.Connection, dict], None]:
    """Two keys, each overwritten twice, so every key has versions 1..3 with 3 current.

    This is the shape EVERY existing unversioned bucket already has in prod: storage has been
    versioned all along, the API just never exposed it.
    """
    try:
        conn = await asyncpg.connect(_DB_URL)
    except OSError as exc:  # only an unreachable server is a legitimate skip
        pytest.skip(f"postgres unavailable: {exc}")

    tx = conn.transaction()
    await tx.start()

    account = f"5T{uuid.uuid4().hex[:12]}"
    bucket_id = uuid.uuid4()
    bucket = f"lov-{uuid.uuid4().hex[:10]}"
    ids: dict[str, uuid.UUID] = {}

    await conn.execute("INSERT INTO users (main_account_id) VALUES ($1)", account)
    await conn.execute(
        "INSERT INTO buckets (bucket_id, bucket_name, main_account_id, created_at) VALUES ($1,$2,$3, now())",
        bucket_id,
        bucket,
        account,
    )
    for key in (KEY_A, KEY_B):
        object_id = uuid.uuid4()
        ids[key] = object_id
        await conn.execute(
            "INSERT INTO objects (object_id, bucket_id, object_key, current_object_version, created_at) "
            "VALUES ($1,$2,$3,3, now())",
            object_id,
            bucket_id,
            key,
        )
        for version in (1, 2, 3):
            await conn.execute(
                "INSERT INTO object_versions "
                "(object_id, object_version, storage_version, size_bytes, md5_hash, content_type, status) "
                "VALUES ($1,$2,5,128,'deadbeef','application/octet-stream','uploaded')",
                object_id,
                version,
            )

    try:
        yield conn, {"bucket_id": bucket_id, "bucket": bucket, "ids": ids}
    finally:
        await tx.rollback()
        await conn.close()


async def _list(conn: asyncpg.Connection, bucket_id: uuid.UUID, *, current_only: bool) -> list[Any]:
    return await conn.fetch(
        get_query("list_object_versions"),
        bucket_id,
        None,  # $2 prefix
        None,  # $3 key marker
        None,  # $4 version marker
        1000,  # $5 limit
        None,  # $6 prefix upper bound
        current_only,  # $7
    )


async def test_current_only_returns_one_entry_per_key(bucket_with_history: tuple[asyncpg.Connection, dict]) -> None:
    """The unversioned-bucket shape: one row per key, at current_object_version."""
    conn, ctx = bucket_with_history
    rows = await _list(conn, ctx["bucket_id"], current_only=True)

    assert [(r["object_key"], r["object_version"]) for r in rows] == [(KEY_A, 3), (KEY_B, 3)]


async def test_current_only_false_returns_the_full_history(
    bucket_with_history: tuple[asyncpg.Connection, dict],
) -> None:
    """The control. Without it, a `current_only` test that returned 2 rows because the seed only
    HAD 2 rows would pass while proving nothing."""
    conn, ctx = bucket_with_history
    rows = await _list(conn, ctx["bucket_id"], current_only=False)

    assert [(r["object_key"], r["object_version"]) for r in rows] == [
        (KEY_A, 3),
        (KEY_A, 2),
        (KEY_A, 1),
        (KEY_B, 3),
        (KEY_B, 2),
        (KEY_B, 1),
    ]


async def test_current_only_hides_history_that_exists(bucket_with_history: tuple[asyncpg.Connection, dict]) -> None:
    """Pins the two against each other, which is the assertion that actually fails on mutation.

    Dropping `AND (NOT $7 OR ov.object_version = o.current_object_version)` makes both calls return
    the same 6 rows — the retained history of an unversioned bucket, published.
    """
    conn, ctx = bucket_with_history
    scoped = await _list(conn, ctx["bucket_id"], current_only=True)
    everything = await _list(conn, ctx["bucket_id"], current_only=False)

    assert len(scoped) == 2
    assert len(everything) == 6
    assert len(scoped) < len(everything)


async def test_a_tombstoned_version_is_not_listed(bucket_with_history: tuple[asyncpg.Connection, dict]) -> None:
    """Soft-delete one middle version; it must vanish from the listing while its siblings remain."""
    conn, ctx = bucket_with_history
    await conn.execute(
        "UPDATE object_versions SET deleted_at = now() WHERE object_id = $1 AND object_version = 2",
        ctx["ids"][KEY_A],
    )

    rows = await _list(conn, ctx["bucket_id"], current_only=False)
    versions_of_a = [r["object_version"] for r in rows if r["object_key"] == KEY_A]

    assert versions_of_a == [3, 1], "the tombstoned version 2 must not be enumerated"
    # the other key is untouched — proves the filter is row-scoped, not a blanket suppression
    assert [r["object_version"] for r in rows if r["object_key"] == KEY_B] == [3, 2, 1]


async def test_a_tombstoned_current_version_is_not_listed_either(
    bucket_with_history: tuple[asyncpg.Connection, dict],
) -> None:
    """The current version is the one a client is most likely to act on, so tombstoning it must
    remove it too — `object_version <= current_object_version` alone would still admit it."""
    conn, ctx = bucket_with_history
    await conn.execute(
        "UPDATE object_versions SET deleted_at = now() WHERE object_id = $1 AND object_version = 3",
        ctx["ids"][KEY_A],
    )

    rows = await _list(conn, ctx["bucket_id"], current_only=False)

    assert [r["object_version"] for r in rows if r["object_key"] == KEY_A] == [2, 1]
