"""Truth table for the G2 replication-gate sentinel query, against real Postgres.

`find_underreplicated_live_chunks.sql` is the read-only durability alarm: it returns
chunks of LIVE, serveable object versions that lack full-union backend coverage — the
exact population the janitor's replication gate must never let be reclaimed. Getting it
wrong means either false alarms (flagging safe chunks) or, far worse, silence over a real
data-loss gap (e.g. the C10 case: a configured backup backend the enqueuer never pushed
to). The required-backend logic must mirror `is_replicated_on_all_backends` exactly, so
the SQL semantics are pinned here against real Postgres via TEMP tables.
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
_DEFAULT_UPLOAD = ["arion"]  # config.upload_backends fallback for legacy (NULL) rows


@pytest_asyncio.fixture
async def conn() -> AsyncGenerator[asyncpg.Connection, None]:
    """A real PG connection with TEMP tables shadowing the five tables the sentinel reads
    (objects, object_versions, parts, part_chunks, chunk_backend). Column shapes mirror
    production only where the query touches them; vanish on close (no cross-test bleed)."""
    try:
        c = await asyncpg.connect(_DB_URL)
    except (OSError, asyncpg.PostgresError) as e:
        pytest.skip(f"integration Postgres unavailable ({e}); run `docker compose up -d postgres`")

    await c.execute(
        """
        CREATE TEMP TABLE objects (
            object_id  uuid        NOT NULL PRIMARY KEY,
            deleted_at timestamptz
        ) ON COMMIT PRESERVE ROWS;

        CREATE TEMP TABLE object_versions (
            object_id       uuid   NOT NULL,
            object_version  bigint NOT NULL,
            address         text,
            version_type    text,
            upload_backends  text[],
            PRIMARY KEY (object_id, object_version)
        ) ON COMMIT PRESERVE ROWS;

        CREATE TEMP TABLE parts (
            part_id        uuid   NOT NULL PRIMARY KEY,
            object_id      uuid   NOT NULL,
            object_version bigint NOT NULL,
            part_number    bigint NOT NULL
        ) ON COMMIT PRESERVE ROWS;

        CREATE TEMP TABLE part_chunks (
            id      bigserial NOT NULL PRIMARY KEY,
            part_id uuid      NOT NULL
        ) ON COMMIT PRESERVE ROWS;

        CREATE TEMP TABLE chunk_backend (
            chunk_id bigint  NOT NULL,
            backend  text    NOT NULL,
            deleted  boolean NOT NULL DEFAULT false,
            PRIMARY KEY (chunk_id, backend)
        ) ON COMMIT PRESERVE ROWS;
        """
    )
    try:
        yield c
    finally:
        await c.close()


def _oid() -> str:
    return str(uuid.uuid4())


async def _chunk(
    conn: asyncpg.Connection,
    *,
    live_backends: list[str],
    deleted_backends: list[str] | None = None,
    object_deleted: bool = False,
    address: str | None = "5Faddr",
    version_type: str | None = None,
    upload_backends: list[str] | None = None,
) -> tuple[str, int, int]:
    """Seeds one object→version→part→chunk with the given backend rows and returns its
    (object_id, object_version, chunk_id). Defaults model a live, serveable chunk."""
    oid = _oid()
    deleted_at_sql = "now()" if object_deleted else "NULL"
    await conn.execute(
        f"INSERT INTO objects (object_id, deleted_at) VALUES ($1::uuid, {deleted_at_sql})",
        oid,
    )
    await conn.execute(
        "INSERT INTO object_versions (object_id, object_version, address, version_type, upload_backends) "
        "VALUES ($1::uuid, 1, $2, $3, $4)",
        oid,
        address,
        version_type,
        upload_backends,
    )
    part_id = _oid()
    await conn.execute(
        "INSERT INTO parts (part_id, object_id, object_version, part_number) VALUES ($1::uuid, $2::uuid, 1, 1)",
        part_id,
        oid,
    )
    chunk_id: int = await conn.fetchval(
        "INSERT INTO part_chunks (part_id) VALUES ($1::uuid) RETURNING id",
        part_id,
    )
    for backend in live_backends:
        await conn.execute(
            "INSERT INTO chunk_backend (chunk_id, backend, deleted) VALUES ($1, $2, false)",
            chunk_id,
            backend,
        )
    for backend in deleted_backends or []:
        await conn.execute(
            "INSERT INTO chunk_backend (chunk_id, backend, deleted) VALUES ($1, $2, true)",
            chunk_id,
            backend,
        )
    return oid, 1, chunk_id


async def _violations(
    conn: asyncpg.Connection,
    *,
    backup: list[str],
    default_upload: list[str] | None = None,
    limit: int = 500,
) -> set[tuple[str, int, int]]:
    rows = await conn.fetch(
        get_query("find_underreplicated_live_chunks"),
        backup,
        default_upload if default_upload is not None else _DEFAULT_UPLOAD,
        limit,
    )
    return {(str(r["object_id"]), r["object_version"], r["chunk_id"]) for r in rows}


# ===================================================== NOT flagged (safe)


async def test_fully_covered_upload_only_is_not_flagged(conn):
    await _chunk(conn, live_backends=["arion"], upload_backends=["arion"])
    assert await _violations(conn, backup=[]) == set()


async def test_fully_covered_upload_plus_backup_is_not_flagged(conn):
    await _chunk(conn, live_backends=["arion", "ipfs"], upload_backends=["arion"])
    assert await _violations(conn, backup=["ipfs"]) == set()


async def test_deleted_object_is_not_flagged_even_if_uncovered(conn):
    # A soft-deleted object's chunks are meant to lose coverage (the unpinner clears them).
    await _chunk(conn, live_backends=[], upload_backends=["arion"], object_deleted=True)
    assert await _violations(conn, backup=[]) == set()


async def test_unserveable_version_is_not_flagged(conn):
    # address NULL = mid-upload/aborted → the reaper/orphan-GC's concern, not a gap.
    await _chunk(conn, live_backends=[], upload_backends=["arion"], address=None)
    assert await _violations(conn, backup=[]) == set()


# ===================================================== flagged (data-loss risk)


async def test_missing_a_required_upload_backend_is_flagged(conn):
    oid, ver, chunk = await _chunk(conn, live_backends=[], upload_backends=["arion"])
    assert await _violations(conn, backup=[]) == {(oid, ver, chunk)}


async def test_missing_a_required_backup_backend_is_flagged(conn):
    # The C10 case: backup is required by the gate but the chunk was never pushed there.
    oid, ver, chunk = await _chunk(conn, live_backends=["arion"], upload_backends=["arion"])
    assert await _violations(conn, backup=["ipfs"]) == {(oid, ver, chunk)}


async def test_a_soft_deleted_backend_row_does_not_count_as_coverage(conn):
    # arion was unpinned (deleted=true) → the chunk lacks live arion coverage → flagged.
    oid, ver, chunk = await _chunk(conn, live_backends=[], deleted_backends=["arion"], upload_backends=["arion"])
    assert await _violations(conn, backup=[]) == {(oid, ver, chunk)}


# ===================================================== per-version required set


async def test_migration_version_requires_ipfs(conn):
    # version_type='migration' forces required=['ipfs'] regardless of upload_backends.
    covered = await _chunk(conn, live_backends=["ipfs"], version_type="migration", upload_backends=["arion"])
    missing_oid, ver, chunk = await _chunk(
        conn, live_backends=["arion"], version_type="migration", upload_backends=["arion"]
    )
    result = await _violations(conn, backup=[])
    assert (missing_oid, ver, chunk) in result, "a migration chunk without ipfs is flagged"
    assert (str(covered[0]), covered[1], covered[2]) not in result, "a migration chunk with ipfs is safe"


async def test_per_version_upload_backends_drive_the_requirement(conn):
    # required comes from the version's own upload_backends, not the config default.
    oid, ver, chunk = await _chunk(conn, live_backends=["arion"], upload_backends=["arion", "ovh"])
    assert await _violations(conn, backup=[]) == {(oid, ver, chunk)}, "missing ovh (a per-version backend) is flagged"


async def test_legacy_null_upload_backends_fall_back_to_config_default(conn):
    # upload_backends NULL (legacy) → required = $2 config default (['arion']) ∪ backup.
    oid, ver, chunk = await _chunk(conn, live_backends=[], upload_backends=None)
    assert await _violations(conn, backup=[], default_upload=["arion"]) == {(oid, ver, chunk)}


# ===================================================== bounding + mix


async def test_the_limit_bounds_the_result(conn):
    for _ in range(3):
        await _chunk(conn, live_backends=[], upload_backends=["arion"])
    assert len(await _violations(conn, backup=[], limit=2)) == 2, "at most `limit` violations returned"


async def test_full_mix(conn):
    # Both safe under backup=["ipfs"]: they carry live arion AND ipfs (upload ∪ backup).
    safe1 = await _chunk(conn, live_backends=["arion", "ipfs"], upload_backends=["arion", "ipfs"])
    safe2 = await _chunk(conn, live_backends=["arion", "ipfs"], upload_backends=["arion"])  # backup covered
    await _chunk(conn, live_backends=[], upload_backends=["arion"], object_deleted=True)  # deleted
    await _chunk(conn, live_backends=[], upload_backends=["arion"], address=None)  # unserveable
    gap_upload = await _chunk(conn, live_backends=[], upload_backends=["arion"])  # missing arion
    gap_backup = await _chunk(conn, live_backends=["arion"], upload_backends=["arion"])  # missing ipfs backup

    result = await _violations(conn, backup=["ipfs"])
    assert result == {
        (gap_upload[0], gap_upload[1], gap_upload[2]),
        (gap_backup[0], gap_backup[1], gap_backup[2]),
    }
    assert (safe1[0], safe1[1], safe1[2]) not in result
    assert (safe2[0], safe2[1], safe2[2]) not in result
