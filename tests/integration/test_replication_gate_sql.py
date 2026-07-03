"""The janitor reclaim gate `is_replicated_on_all_backends`, against real Postgres.

This is the gate the drain enqueuer must satisfy before the janitor will free a part's
SSD copy. It computes the required backend set PER VERSION (['ipfs'] for a migration
version, else the row's persisted upload_backends, else config default) unioned with
config.backup_backends, then checks every chunk has a live chunk_backend row for all of
them (via count_chunk_backends.sql).

These tests pin the C10 residual the review surfaced: the drain enqueuer is version-blind
(it pushes to the agent's global config.enqueue_backends), so a MIGRATION version — whose
gate requirement is ['ipfs'] regardless of config — or a version whose persisted
upload_backends drifted from current config is required-but-under-enqueued and therefore
NOT reclaimable (a permanent SSD leak, surfaced by the G2 sentinel; not data loss).
"""

from __future__ import annotations

import os
import uuid
from typing import AsyncGenerator
from unittest.mock import patch

import asyncpg
import pytest
import pytest_asyncio

from workers import run_janitor_in_loop as janitor


pytestmark = pytest.mark.asyncio

_DB_URL = os.getenv("DATABASE_URL", "postgresql://postgres:postgres@localhost:5432/hippius?sslmode=disable")


@pytest_asyncio.fixture
async def conn() -> AsyncGenerator[asyncpg.Connection, None]:
    """A real PG connection with TEMP tables shadowing the tables the gate + its
    count_chunk_backends.sql read (object_versions, parts, part_chunks, chunk_backend)."""
    try:
        c = await asyncpg.connect(_DB_URL)
    except (OSError, asyncpg.PostgresError) as e:
        pytest.skip(f"integration Postgres unavailable ({e}); run `docker compose up -d postgres`")

    await c.execute(
        """
        CREATE TEMP TABLE object_versions (
            object_id      uuid   NOT NULL,
            object_version bigint NOT NULL,
            version_type   text,
            upload_backends text[],
            PRIMARY KEY (object_id, object_version)
        ) ON COMMIT PRESERVE ROWS;

        CREATE TEMP TABLE parts (
            part_id          uuid   NOT NULL PRIMARY KEY,
            object_id        uuid   NOT NULL,
            object_version   bigint NOT NULL,
            part_number      bigint NOT NULL,
            size_bytes       bigint NOT NULL,
            chunk_size_bytes int
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


async def _one_chunk_part(
    conn: asyncpg.Connection,
    *,
    live_backends: list[str],
    version_type: str | None = None,
    upload_backends: list[str] | None = None,
) -> str:
    """Seeds a version + a single-chunk part with the given backend rows; returns object_id.
    size_bytes == chunk_size_bytes so count_chunk_backends expects exactly one chunk."""
    oid = _oid()
    await conn.execute(
        "INSERT INTO object_versions (object_id, object_version, version_type, upload_backends) VALUES ($1::uuid, 1, $2, $3)",
        oid,
        version_type,
        upload_backends,
    )
    part_id = _oid()
    await conn.execute(
        "INSERT INTO parts (part_id, object_id, object_version, part_number, size_bytes, chunk_size_bytes) "
        "VALUES ($1::uuid, $2::uuid, 1, 1, 100, 100)",
        part_id,
        oid,
    )
    chunk_id = await conn.fetchval("INSERT INTO part_chunks (part_id) VALUES ($1::uuid) RETURNING id", part_id)
    for backend in live_backends:
        await conn.execute(
            "INSERT INTO chunk_backend (chunk_id, backend, deleted) VALUES ($1, $2, false)",
            chunk_id,
            backend,
        )
    return oid


async def _replicated(conn: asyncpg.Connection, oid: str) -> bool:
    # config.upload_backends is only the fallback for legacy (empty) rows; pin it + no backup.
    with (
        patch.object(janitor.config, "upload_backends", ["arion"]),
        patch.object(janitor.config, "backup_backends", [], create=True),
    ):
        return await janitor.is_replicated_on_all_backends(conn, oid, 1, 1)


async def test_migration_version_on_arion_only_is_not_reclaimable(conn):
    # The C10 residual: a migration version's gate requirement is ['ipfs'], but the
    # version-blind enqueuer only pushed to arion → the gate never passes → SSD leak.
    oid = await _one_chunk_part(conn, live_backends=["arion"], version_type="migration")
    assert await _replicated(conn, oid) is False, "migration version requires ipfs; arion-only is not reclaimable"


async def test_migration_version_with_ipfs_is_reclaimable(conn):
    oid = await _one_chunk_part(conn, live_backends=["ipfs"], version_type="migration")
    assert await _replicated(conn, oid) is True, "migration version with its required ipfs copy is reclaimable"


async def test_drifted_per_version_upload_backends_are_not_reclaimable(conn):
    # Version persisted upload_backends=['arion','ovh'] (a superset of current config
    # ['arion']); the version-blind enqueuer only pushed arion → gate requires ovh → leak.
    oid = await _one_chunk_part(conn, live_backends=["arion"], upload_backends=["arion", "ovh"])
    assert await _replicated(conn, oid) is False, "a version needing ovh is not reclaimable when only arion landed"


async def test_matching_per_version_backends_are_reclaimable(conn):
    oid = await _one_chunk_part(conn, live_backends=["arion", "ovh"], upload_backends=["arion", "ovh"])
    assert await _replicated(conn, oid) is True, "full per-version coverage is reclaimable"
