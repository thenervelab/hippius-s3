"""If-None-Match: * write-once semantics, against real Postgres.

Covers the three queries the conditional path runs (reserve-time state, finalize-time conflict,
multipart completion conflict), the lock they rely on, and a step-by-step replay of two create-only
writers racing on a brand-new key using the real reserve query (upsert_object_basic).
"""

from __future__ import annotations

import os
import uuid
from datetime import datetime
from datetime import timezone
from typing import AsyncGenerator

import asyncpg
import pytest
import pytest_asyncio

from hippius_s3.utils import get_query
from hippius_s3.writer.db import upsert_object_basic


pytestmark = pytest.mark.asyncio

_DB_URL = os.getenv("DATABASE_URL", "postgresql://postgres:postgres@localhost:5432/hippius?sslmode=disable")

LIVE, MARKER, PLACEHOLDER = "live", "marker", "placeholder"


async def _connect() -> asyncpg.Connection:
    try:
        return await asyncpg.connect(_DB_URL)
    except OSError as exc:  # only an unreachable server is a legitimate skip
        pytest.skip(f"postgres unavailable: {exc}")


async def _seed_bucket(conn: asyncpg.Connection) -> uuid.UUID:
    account = f"5INM{uuid.uuid4().hex[:12]}"
    bucket_id = uuid.uuid4()
    await conn.execute("INSERT INTO users (main_account_id, created_at) VALUES ($1, now())", account)
    await conn.execute(
        "INSERT INTO buckets (bucket_id, bucket_name, main_account_id, created_at) VALUES ($1, $2, $3, now())",
        bucket_id,
        f"inm-{uuid.uuid4().hex[:10]}",
        account,
    )
    return bucket_id


@pytest_asyncio.fixture
async def db() -> AsyncGenerator[tuple[asyncpg.Connection, uuid.UUID], None]:
    conn = await _connect()
    tx = conn.transaction()
    await tx.start()
    try:
        yield conn, await _seed_bucket(conn)
    finally:
        await tx.rollback()
        await conn.close()


async def _object(
    conn: asyncpg.Connection,
    bucket_id: uuid.UUID,
    key: str,
    versions: list[str],
    *,
    soft_deleted: bool = False,
) -> uuid.UUID:
    """An object whose versions 1..N have the given shapes; current_object_version = N."""
    object_id = uuid.uuid4()
    await conn.execute(
        "INSERT INTO objects (object_id, bucket_id, object_key, current_object_version, created_at, deleted_at) "
        "VALUES ($1, $2, $3, $4, now(), CASE WHEN $5 THEN now() END)",
        object_id,
        bucket_id,
        key,
        len(versions),
        soft_deleted,
    )
    for n, shape in enumerate(versions, start=1):
        await conn.execute(
            "INSERT INTO object_versions (object_id, object_version, storage_version, size_bytes, md5_hash, "
            "content_type, status, is_delete_marker) VALUES ($1, $2, 5, $3, $4, 'text/plain', 'uploaded', $5)",
            object_id,
            n,
            5 if shape == LIVE else 0,
            "d41d8cd98f00b204e9800998ecf8427e" if shape == LIVE else "",
            shape == MARKER,
        )
    return object_id


async def _state(conn: asyncpg.Connection, bucket_id: uuid.UUID, key: str) -> tuple[bool, int]:
    row = await conn.fetchrow(get_query("conditional_write_state"), str(bucket_id), key)
    return bool(row["exists_live"]), int(row["baseline"])


# --- reserve-time state ---------------------------------------------------------------------------


async def test_state_of_a_key_that_never_existed(db: tuple[asyncpg.Connection, uuid.UUID]) -> None:
    conn, bucket_id = db
    assert await _state(conn, bucket_id, "nope") == (False, 0)


async def test_state_of_a_live_key(db: tuple[asyncpg.Connection, uuid.UUID]) -> None:
    conn, bucket_id = db
    await _object(conn, bucket_id, "k", [LIVE])
    assert await _state(conn, bucket_id, "k") == (True, 1)


async def test_a_delete_marker_on_top_means_the_key_does_not_exist(db: tuple[asyncpg.Connection, uuid.UUID]) -> None:
    conn, bucket_id = db
    await _object(conn, bucket_id, "k", [LIVE, MARKER])
    assert await _state(conn, bucket_id, "k") == (False, 2)


async def test_a_soft_deleted_key_does_not_exist_and_its_old_content_is_baselined(
    db: tuple[asyncpg.Connection, uuid.UUID],
) -> None:
    conn, bucket_id = db
    await _object(conn, bucket_id, "k", [LIVE], soft_deleted=True)
    assert await _state(conn, bucket_id, "k") == (False, 1)


async def test_an_in_flight_upload_does_not_hide_the_version_being_served(
    db: tuple[asyncpg.Connection, uuid.UUID],
) -> None:
    conn, bucket_id = db
    await _object(conn, bucket_id, "k", [LIVE, PLACEHOLDER])
    assert await _state(conn, bucket_id, "k") == (True, 1)


async def test_an_in_flight_first_upload_is_not_yet_an_object(db: tuple[asyncpg.Connection, uuid.UUID]) -> None:
    conn, bucket_id = db
    await _object(conn, bucket_id, "k", [PLACEHOLDER])
    assert await _state(conn, bucket_id, "k") == (False, 0)


async def test_a_copy_alias_counts_as_an_existing_key(db: tuple[asyncpg.Connection, uuid.UUID]) -> None:
    conn, bucket_id = db
    target = await _object(conn, bucket_id, "original", [LIVE])
    await conn.execute(
        "INSERT INTO object_names (bucket_id, object_key, object_id) VALUES ($1, 'alias', $2)", bucket_id, target
    )
    assert (await _state(conn, bucket_id, "alias"))[0] is True


# --- finalize-time conflict -----------------------------------------------------------------------


@pytest.mark.parametrize(
    ("versions", "expected"),
    [
        ([MARKER, LIVE, PLACEHOLDER], True),  # v2 finalized live after our reserve (baseline 1)
        ([MARKER, PLACEHOLDER, PLACEHOLDER], False),  # v2 still in flight
        ([MARKER, MARKER, PLACEHOLDER], False),  # v2 is a delete: the key still does not exist
        ([LIVE, PLACEHOLDER, PLACEHOLDER], False),  # v1 is at/below the baseline: judged at reserve
    ],
)
async def test_finalize_conflict(db: tuple[asyncpg.Connection, uuid.UUID], versions: list[str], expected: bool) -> None:
    conn, bucket_id = db
    object_id = await _object(conn, bucket_id, "k", versions)
    assert await conn.fetchval(get_query("conditional_write_conflict"), object_id, 3, 1) is expected


async def test_finalize_conflict_ignores_our_own_version(db: tuple[asyncpg.Connection, uuid.UUID]) -> None:
    conn, bucket_id = db
    object_id = await _object(conn, bucket_id, "k", [LIVE])
    assert await conn.fetchval(get_query("conditional_write_conflict"), object_id, 1, 0) is False


# --- multipart completion -------------------------------------------------------------------------


@pytest.mark.parametrize(
    ("versions", "ours", "expected"),
    [
        # Only versions ABOVE ours are judged here. What the key held when the upload was initiated
        # is decided at initiate (multipart_uploads.key_existed_at_initiate), because by completion
        # time the initiate has already cleared any soft delete on the objects row.
        ([PLACEHOLDER, LIVE], 1, True),  # someone created the key while our upload was open
        ([PLACEHOLDER, LIVE, MARKER], 1, False),  # ...and deleted it again: newest above is a marker
        ([PLACEHOLDER, PLACEHOLDER], 1, False),  # the other writer is still in flight
        ([LIVE, PLACEHOLDER], 2, False),  # v1 predates our upload: judged at initiate, not here
        ([LIVE, MARKER, PLACEHOLDER], 3, False),  # nothing above ours at all
        ([PLACEHOLDER], 1, False),  # only our own version
    ],
)
async def test_multipart_completion_conflict(
    db: tuple[asyncpg.Connection, uuid.UUID], versions: list[str], ours: int, expected: bool
) -> None:
    conn, bucket_id = db
    object_id = await _object(conn, bucket_id, "k", versions)
    assert await conn.fetchval(get_query("mpu_conditional_conflict"), object_id, ours) is expected


# --- what the multipart reserve records about the key it is about to un-delete --------------------


async def _initiate(conn: asyncpg.Connection, bucket_id: uuid.UUID, key: str) -> bool:
    """upsert_object_multipart, as InitiateMultipartUpload runs it; returns its existed_live verdict."""
    row = await conn.fetchrow(
        get_query("upsert_object_multipart"),
        uuid.uuid4(),
        bucket_id,
        key,
        "application/octet-stream",
        "{}",
        "",
        0,
        datetime.now(timezone.utc),
        5,
        ["arion"],
    )
    assert row is not None
    return bool(row["existed_live"])


@pytest.mark.parametrize(
    ("versions", "soft_deleted", "expected"),
    [
        ([LIVE], False, True),  # a live key: a create-only completion must be refused
        ([LIVE], True, False),  # soft-deleted: the key does not exist, however live the row looks after
        ([MARKER], False, False),  # newest version is a delete marker
        ([PLACEHOLDER], False, False),  # nothing serveable yet
    ],
)
async def test_multipart_reserve_records_existence_before_it_clears_the_soft_delete(
    db: tuple[asyncpg.Connection, uuid.UUID], versions: list[str], soft_deleted: bool, expected: bool
) -> None:
    conn, bucket_id = db
    await _object(conn, bucket_id, "k", versions, soft_deleted=soft_deleted)

    assert await _initiate(conn, bucket_id, "k") is expected

    # The same read AFTER the upsert can no longer tell: the initiate has cleared deleted_at, which is
    # exactly why the verdict is captured in the statement that clears it.
    state = await conn.fetchrow(get_query("conditional_write_state"), bucket_id, "k")
    assert state is not None
    if soft_deleted:
        assert state["exists_live"] is True, "post-initiate the key looks live — the recorded flag is the only truth"


async def test_multipart_reserve_on_a_brand_new_key_reports_absent(
    db: tuple[asyncpg.Connection, uuid.UUID],
) -> None:
    conn, bucket_id = db
    assert await _initiate(conn, bucket_id, "never-seen") is False


# --- the race, replayed step by step with the real reserve query ---------------------------------


async def _reserve(conn: asyncpg.Connection, bucket_id: uuid.UUID, key: str) -> tuple[bool, int, uuid.UUID, int]:
    """What put_simple_stream_full's conditional reserve transaction does, minus the envelope."""
    async with conn.transaction():
        await conn.execute(get_query("lock_object_by_key_for_update"), str(bucket_id), key)
        exists_live, baseline = await _state(conn, bucket_id, key)
        if exists_live:
            return True, baseline, uuid.UUID(int=0), 0
        row = await upsert_object_basic(
            conn,
            object_id=str(uuid.uuid4()),
            bucket_id=str(bucket_id),
            object_key=key,
            content_type="text/plain",
            metadata={},
            md5_hash="",
            size_bytes=0,
            storage_version=5,
        )
        return False, baseline, row["object_id"], int(row["current_object_version"])


async def _finalize(conn: asyncpg.Connection, object_id: uuid.UUID, version: int, baseline: int) -> bool:
    """The conditional tail: lock, re-check, and only then make the version serveable. True = landed."""
    async with conn.transaction():
        await conn.execute(get_query("lock_object_row_for_update"), object_id)
        if await conn.fetchval(get_query("conditional_write_conflict"), object_id, version, baseline):
            return False
        await conn.execute(
            "UPDATE object_versions SET size_bytes = 7, md5_hash = 'abc' WHERE object_id = $1 AND object_version = $2",
            object_id,
            version,
        )
        return True


async def test_two_create_only_writers_on_a_new_key_have_exactly_one_winner(
    db: tuple[asyncpg.Connection, uuid.UUID],
) -> None:
    conn, bucket_id = db

    # Both reserve before either has finalized: neither sees the other, both get a version.
    refused_a, base_a, obj_a, ver_a = await _reserve(conn, bucket_id, "race")
    refused_b, base_b, obj_b, ver_b = await _reserve(conn, bucket_id, "race")
    assert not refused_a and not refused_b
    assert obj_a == obj_b and ver_b == ver_a + 1

    # B finishes streaming first and lands; A's re-check then sees B's live version and refuses.
    assert await _finalize(conn, obj_b, ver_b, base_b) is True
    assert await _finalize(conn, obj_a, ver_a, base_a) is False

    # A third create-only writer is turned away at reserve, before it would read its body.
    refused_c, *_ = await _reserve(conn, bucket_id, "race")
    assert refused_c is True


# --- the lock -------------------------------------------------------------------------------------


async def test_the_conditional_tail_lock_excludes_other_finalizers() -> None:
    """lock_object_row_for_update must block the KEY SHARE every PUT tail takes, or the re-check could
    interleave with a concurrent finalize. Needs committed rows visible to a second session."""
    a, b = await _connect(), await _connect()
    # objects_current_version_fk is checked at commit, so the object and its version go in together.
    async with a.transaction():
        bucket_id = await _seed_bucket(a)
        object_id = await _object(a, bucket_id, "locked", [LIVE])
    try:
        async with a.transaction():
            await a.execute(get_query("lock_object_row_for_update"), object_id)
            async with b.transaction():
                await b.execute("SET LOCAL lock_timeout = '300ms'")
                with pytest.raises(asyncpg.exceptions.LockNotAvailableError):
                    await b.execute(get_query("lock_object_row_by_id"), object_id)

        # Released: an ordinary tail proceeds again.
        async with b.transaction():
            await b.execute("SET LOCAL lock_timeout = '300ms'")
            await b.execute(get_query("lock_object_row_by_id"), object_id)
    finally:
        async with a.transaction():
            owner = await a.fetchval("SELECT main_account_id FROM buckets WHERE bucket_id = $1", bucket_id)
            # objects first: its current-version FK forbids removing a version it still points at.
            await a.execute("DELETE FROM objects WHERE object_id = $1", object_id)
            await a.execute("DELETE FROM object_versions WHERE object_id = $1", object_id)
            await a.execute("DELETE FROM buckets WHERE bucket_id = $1", bucket_id)
            await a.execute("DELETE FROM users WHERE main_account_id = $1", owner)
        await a.close()
        await b.close()
