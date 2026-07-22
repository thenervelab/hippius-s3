"""Truth table for the MPU reaper's abandoned-upload query, against real Postgres.

`list_abandoned_versions.sql` decides which multipart uploads the reaper terminally
fails (marks the drain rows `failed` + aborts the MPU). Getting it wrong means either
churning forever on live uploads or terminating an in-flight one. The unit tests drive
`reap_abandoned_uploads` with a fake db, so the SQL predicate itself (the address-NULL +
age + not-completed gate, the DISTINCT-per-version dedup) is only exercised here.

We run the real `get_query("list_abandoned_versions")` against TEMP tables shadowing
`multipart_uploads`/`parts`/`object_versions` for the session — so the exact production
query is tested without the full schema or FKs.
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
_STALE = 3600  # a row is reapable once older than this


@pytest_asyncio.fixture
async def conn() -> AsyncGenerator[asyncpg.Connection, None]:
    try:
        c = await asyncpg.connect(_DB_URL)
    except (OSError, asyncpg.PostgresError) as e:
        pytest.skip(f"integration Postgres unavailable ({e}); run `docker compose up -d postgres`")

    await c.execute(
        """
        CREATE TEMP TABLE multipart_uploads (
            upload_id    uuid                     NOT NULL,
            is_completed boolean,
            initiated_at timestamptz              NOT NULL
        ) ON COMMIT PRESERVE ROWS;

        CREATE TEMP TABLE parts (
            upload_id      uuid        NOT NULL,
            object_id      uuid        NOT NULL,
            object_version bigint      NOT NULL,
            uploaded_at    timestamptz NOT NULL
        ) ON COMMIT PRESERVE ROWS;

        CREATE TEMP TABLE object_versions (
            object_id      uuid   NOT NULL,
            object_version bigint NOT NULL,
            address        text
        ) ON COMMIT PRESERVE ROWS;
        """
    )
    try:
        yield c
    finally:
        await c.close()


async def _mpu(conn: asyncpg.Connection, upload_id: str, *, completed: bool | None, age_seconds: int) -> None:
    await conn.execute(
        "INSERT INTO multipart_uploads (upload_id, is_completed, initiated_at) "
        "VALUES ($1::uuid, $2, now() - make_interval(secs => $3))",
        upload_id,
        completed,
        age_seconds,
    )


async def _part(
    conn: asyncpg.Connection, upload_id: str, object_id: str, version: int, *, uploaded_age_seconds: int = 7200
) -> None:
    # uploaded_age_seconds defaults to 2h ago (stale, since the tests use _STALE=3600) so
    # existing cases reap as before; pass a small value to model a still-active upload.
    await conn.execute(
        "INSERT INTO parts (upload_id, object_id, object_version, uploaded_at) "
        "VALUES ($1::uuid, $2::uuid, $3, now() - make_interval(secs => $4))",
        upload_id,
        object_id,
        version,
        uploaded_age_seconds,
    )


async def _ov(conn: asyncpg.Connection, object_id: str, version: int, *, address: str | None) -> None:
    await conn.execute(
        "INSERT INTO object_versions (object_id, object_version, address) VALUES ($1::uuid, $2, $3)",
        object_id,
        version,
        address,
    )


async def _abandoned(conn: asyncpg.Connection) -> set[tuple]:
    rows = await conn.fetch(get_query("list_abandoned_versions"), _STALE)
    return {(str(r["upload_id"]), str(r["object_id"]), r["object_version"]) for r in rows}


async def test_abandoned_upload_is_listed(conn):
    u, o = str(uuid.uuid4()), str(uuid.uuid4())
    await _mpu(conn, u, completed=False, age_seconds=7200)
    await _part(conn, u, o, 1)
    await _ov(conn, o, 1, address=None)
    assert (u, o, 1) in await _abandoned(conn)


async def test_missing_object_version_row_is_listed(conn):
    # parts landed but the version row is gone → ov.object_id IS NULL branch.
    u, o = str(uuid.uuid4()), str(uuid.uuid4())
    await _mpu(conn, u, completed=False, age_seconds=7200)
    await _part(conn, u, o, 1)
    assert (u, o, 1) in await _abandoned(conn)


async def test_completed_upload_with_address_is_not_listed(conn):
    u, o = str(uuid.uuid4()), str(uuid.uuid4())
    await _mpu(conn, u, completed=False, age_seconds=7200)
    await _part(conn, u, o, 1)
    await _ov(conn, o, 1, address="5Faddr")  # finalized → servable → never reaped
    assert await _abandoned(conn) == set()


async def test_is_completed_flag_excludes(conn):
    u, o = str(uuid.uuid4()), str(uuid.uuid4())
    await _mpu(conn, u, completed=True, age_seconds=7200)
    await _part(conn, u, o, 1)
    await _ov(conn, o, 1, address=None)
    assert await _abandoned(conn) == set()


async def test_fresh_upload_is_not_listed(conn):
    u, o = str(uuid.uuid4()), str(uuid.uuid4())
    await _mpu(conn, u, completed=False, age_seconds=0)  # just initiated
    await _part(conn, u, o, 1)
    await _ov(conn, o, 1, address=None)
    assert await _abandoned(conn) == set()


async def test_old_upload_with_recent_part_activity_is_not_listed(conn):
    # The resume-after-a-pause safety case: initiated long ago, but a part was uploaded
    # within the stale window → still active → must NOT be reaped.
    u, o = str(uuid.uuid4()), str(uuid.uuid4())
    await _mpu(conn, u, completed=False, age_seconds=7200)
    await _part(conn, u, o, 1, uploaded_age_seconds=60)  # part uploaded a minute ago
    await _ov(conn, o, 1, address=None)
    assert await _abandoned(conn) == set()


async def test_old_upload_with_only_stale_parts_is_listed(conn):
    # Initiated long ago AND no part touched within the stale window → genuinely abandoned.
    u, o = str(uuid.uuid4()), str(uuid.uuid4())
    await _mpu(conn, u, completed=False, age_seconds=7200)
    await _part(conn, u, o, 1, uploaded_age_seconds=7200)
    await _ov(conn, o, 1, address=None)
    assert (u, o, 1) in await _abandoned(conn)


async def test_one_recent_part_protects_an_upload_with_other_stale_parts(conn):
    # The core of the activity gate: NOT EXISTS is per-upload, so a single recent part
    # (a resumed upload adding part N) protects the whole upload even though earlier parts
    # are stale. Without this, a slow/resumed MPU would be reaped mid-flight.
    u, o = str(uuid.uuid4()), str(uuid.uuid4())
    await _mpu(conn, u, completed=False, age_seconds=7200)
    await _part(conn, u, o, 1, uploaded_age_seconds=7200)  # early part, stale
    await _part(conn, u, o, 1, uploaded_age_seconds=60)  # just-added part, recent
    await _ov(conn, o, 1, address=None)
    assert await _abandoned(conn) == set()


async def test_multipart_upload_dedups_to_one_row_per_version(conn):
    # An MPU with N parts of the same (object_id, version) yields ONE row (DISTINCT).
    u, o = str(uuid.uuid4()), str(uuid.uuid4())
    await _mpu(conn, u, completed=False, age_seconds=7200)
    await _part(conn, u, o, 1)
    await _part(conn, u, o, 1)
    await _part(conn, u, o, 1)
    await _ov(conn, o, 1, address=None)
    result = await _abandoned(conn)
    assert result == {(u, o, 1)}


async def test_full_mix(conn):
    # One pass with every case interleaved → only the two genuinely-abandoned uploads.
    abandoned_u, abandoned_o = str(uuid.uuid4()), str(uuid.uuid4())
    orphan_u, orphan_o = str(uuid.uuid4()), str(uuid.uuid4())
    done_u, done_o = str(uuid.uuid4()), str(uuid.uuid4())
    fresh_u, fresh_o = str(uuid.uuid4()), str(uuid.uuid4())

    await _mpu(conn, abandoned_u, completed=False, age_seconds=7200)
    await _part(conn, abandoned_u, abandoned_o, 1)
    await _ov(conn, abandoned_o, 1, address=None)

    await _mpu(conn, orphan_u, completed=False, age_seconds=7200)
    await _part(conn, orphan_u, orphan_o, 1)  # no ov row

    await _mpu(conn, done_u, completed=False, age_seconds=7200)
    await _part(conn, done_u, done_o, 1)
    await _ov(conn, done_o, 1, address="5Faddr")

    await _mpu(conn, fresh_u, completed=False, age_seconds=0)
    await _part(conn, fresh_u, fresh_o, 1)
    await _ov(conn, fresh_o, 1, address=None)

    assert await _abandoned(conn) == {(abandoned_u, abandoned_o, 1), (orphan_u, orphan_o, 1)}
