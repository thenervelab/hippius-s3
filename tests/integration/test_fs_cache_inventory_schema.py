"""Schema checks for the janitor SQL-eviction tables, against real (migrated) Postgres.

Migration 20260725000000_fs_cache_inventory.sql adds `fs_cache_inventory` (advisory
index of parts materialized on the cache FS) and `janitor_state` (the single-instance
janitor's durable keyset cursor / shard counter). These tests pin the shapes the
eviction path depends on: the composite PK the pipelines upsert against, the
(cached_at, ...) index the oldest-first candidate scan walks, and JSONB upsert
semantics for the cursor state.

Mutation tests run inside `pg_tx` (always rolled back), so no rows persist.
"""

from __future__ import annotations

import json

import asyncpg
import pytest


pytestmark = pytest.mark.asyncio

_EXPECTED_COLUMNS = {
    "fs_cache_inventory": {
        "object_id": ("text", "NO"),
        "object_version": ("bigint", "NO"),
        "part_number": ("bigint", "NO"),
        "cached_at": ("timestamp with time zone", "NO"),
        # Added by 20260725120000_fs_cache_inventory_last_access.sql; nullable because rows
        # predating the read-recency tracker have never been read through it.
        "last_access_at": ("timestamp with time zone", "YES"),
    },
    "janitor_state": {
        "key": ("text", "NO"),
        "value": ("jsonb", "NO"),
        "updated_at": ("timestamp with time zone", "NO"),
    },
}


@pytest.mark.parametrize("table", sorted(_EXPECTED_COLUMNS))
async def test_table_exists_with_expected_columns(pg_conn: asyncpg.Connection, table: str) -> None:
    rows = await pg_conn.fetch(
        """
        SELECT column_name, data_type, is_nullable
        FROM information_schema.columns
        WHERE table_schema = 'public' AND table_name = $1
        """,
        table,
    )
    assert rows, f"table {table} missing — run `python -m hippius_s3.scripts.migrate`"
    actual = {r["column_name"]: (r["data_type"], r["is_nullable"]) for r in rows}
    assert actual == _EXPECTED_COLUMNS[table]


async def test_cached_at_index_exists(pg_conn: asyncpg.Connection) -> None:
    indexdef = await pg_conn.fetchval(
        "SELECT indexdef FROM pg_indexes WHERE schemaname = 'public' AND indexname = 'fs_cache_inventory_cached_at'"
    )
    assert indexdef is not None
    # The oldest-first scan and the keyset cursor both rely on this exact column order.
    assert "(cached_at, object_id, object_version, part_number)" in indexdef


async def test_inventory_insert_upsert_delete(pg_tx: asyncpg.Connection) -> None:
    # Seed cached_at an hour in the past: now() is frozen to the transaction start, so an
    # upsert of now() over a DEFAULT-now() row would be a no-op and prove nothing.
    old = await pg_tx.fetchval(
        """
        INSERT INTO fs_cache_inventory (object_id, object_version, part_number, cached_at)
        VALUES ('obj-schema-test', 3, 1, now() - interval '1 hour')
        RETURNING cached_at
        """
    )

    bumped = await pg_tx.fetchval(
        """
        INSERT INTO fs_cache_inventory (object_id, object_version, part_number)
        VALUES ('obj-schema-test', 3, 1)
        ON CONFLICT (object_id, object_version, part_number) DO UPDATE SET cached_at = now()
        RETURNING cached_at
        """
    )
    assert bumped > old

    count = await pg_tx.fetchval("SELECT count(*) FROM fs_cache_inventory WHERE object_id = 'obj-schema-test'")
    assert count == 1

    await pg_tx.execute("DELETE FROM fs_cache_inventory WHERE object_id = 'obj-schema-test'")
    remaining = await pg_tx.fetchval("SELECT count(*) FROM fs_cache_inventory WHERE object_id = 'obj-schema-test'")
    assert remaining == 0


async def test_janitor_state_upsert_keeps_latest_value(pg_tx: asyncpg.Connection) -> None:
    upsert = """
        INSERT INTO janitor_state (key, value)
        VALUES ($1, $2::jsonb)
        ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value, updated_at = now()
    """
    await pg_tx.execute(upsert, "eviction_cursor", json.dumps({"cached_at": "2026-07-01T00:00:00Z", "shard": 1}))
    await pg_tx.execute(upsert, "eviction_cursor", json.dumps({"cached_at": "2026-07-02T00:00:00Z", "shard": 7}))

    raw = await pg_tx.fetchval("SELECT value FROM janitor_state WHERE key = 'eviction_cursor'")
    assert json.loads(raw) == {"cached_at": "2026-07-02T00:00:00Z", "shard": 7}
