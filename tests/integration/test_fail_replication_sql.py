"""Truth table for `fail_replication_status_for_version.sql`, against real Postgres.

The reaper calls this to mark an abandoned upload's replication rows terminal ('failed')
so the per-node drain stops re-copying/re-deferring them. Getting the version predicate
wrong means either wedging on a NULL-version row (the staging regression: legacy parts
carry a NULL object_version) or failing rows for the wrong version. The unit tests drive
`fail_version_replication` with a fake db, so the SQL predicate itself — the
`$2 IS NULL → all versions` branch and the `status IN (...)` gate — is only exercised here.

Runs the real `get_query("fail_replication_status_for_version")` against a TEMP table
shadowing `cephor_replication_status` for the session.
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


@pytest_asyncio.fixture
async def conn() -> AsyncGenerator[asyncpg.Connection, None]:
    try:
        c = await asyncpg.connect(_DB_URL)
    except (OSError, asyncpg.PostgresError) as e:
        pytest.skip(f"integration Postgres unavailable ({e}); run `docker compose up -d postgres`")

    await c.execute(
        """
        CREATE TEMP TABLE cephor_replication_status (
            object_id  text        NOT NULL,
            version    bigint,
            status     text        NOT NULL,
            claimed_at timestamptz,
            updated_at timestamptz
        ) ON COMMIT PRESERVE ROWS;
        """
    )
    try:
        yield c
    finally:
        await c.close()


async def _row(conn: asyncpg.Connection, object_id: str, version: int | None, status: str) -> None:
    await conn.execute(
        "INSERT INTO cephor_replication_status (object_id, version, status, claimed_at, updated_at) "
        "VALUES ($1, $2, $3, now(), now())",
        object_id,
        version,
        status,
    )


async def _fail(conn: asyncpg.Connection, object_id: str, version: int | None) -> None:
    await conn.execute(get_query("fail_replication_status_for_version"), object_id, version)


async def _statuses(conn: asyncpg.Connection, object_id: str) -> list[tuple]:
    rows = await conn.fetch(
        "SELECT version, status FROM cephor_replication_status WHERE object_id = $1 ORDER BY version NULLS FIRST",
        object_id,
    )
    return [(r["version"], r["status"]) for r in rows]


async def test_specific_version_fails_only_that_version(conn):
    o = str(uuid.uuid4())
    await _row(conn, o, 1, "pending")
    await _row(conn, o, 2, "draining")
    await _fail(conn, o, 1)
    assert set(await _statuses(conn, o)) == {(1, "failed"), (2, "draining")}


async def test_null_version_fails_every_version(conn):
    # The regression fix: NULL $2 must fail all still-active rows for the object,
    # including a genuinely NULL-version legacy row.
    o = str(uuid.uuid4())
    await _row(conn, o, 1, "pending")
    await _row(conn, o, 2, "draining")
    await _row(conn, o, None, "pending")
    await _fail(conn, o, None)
    assert set(await _statuses(conn, o)) == {(1, "failed"), (2, "failed"), (None, "failed")}


async def test_only_pending_and_draining_are_touched(conn):
    o = str(uuid.uuid4())
    await _row(conn, o, 1, "replicated")
    await _row(conn, o, 2, "failed")
    await _row(conn, o, 3, "pending")
    await _fail(conn, o, None)
    assert set(await _statuses(conn, o)) == {(1, "replicated"), (2, "failed"), (3, "failed")}


async def test_other_objects_are_untouched(conn):
    o, other = str(uuid.uuid4()), str(uuid.uuid4())
    await _row(conn, o, 1, "pending")
    await _row(conn, other, 1, "pending")
    await _fail(conn, o, None)
    assert await _statuses(conn, o) == [(1, "failed")]
    assert await _statuses(conn, other) == [(1, "pending")]
