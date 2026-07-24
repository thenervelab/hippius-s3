"""Behavioral tests for the fs_cache_inventory repository helpers, against real Postgres.

Mutation tests run inside `pg_tx` (always rolled back). now() is frozen to the
transaction start, so to prove a cached_at/updated_at bump we seed the earlier
timestamp explicitly in the past (see test_fs_cache_inventory_schema.py).
"""

from __future__ import annotations

import logging
import uuid
from typing import Any

import asyncpg
import pytest

from hippius_s3.repositories import fs_cache_inventory


pytestmark = pytest.mark.asyncio


class _BrokenConn:
    """A conn stand-in whose execute always raises — proves the best-effort swallow."""

    def __init__(self) -> None:
        self.calls = 0

    async def execute(self, *_args: Any, **_kwargs: Any) -> None:
        self.calls += 1
        raise asyncpg.PostgresError("boom")


class _RecordingConn:
    """A conn stand-in that records execute calls without touching a database."""

    def __init__(self) -> None:
        self.calls: list[tuple[Any, ...]] = []

    async def execute(self, *args: Any, **_kwargs: Any) -> None:
        self.calls.append(args)


async def test_record_cached_round_trip(pg_tx: asyncpg.Connection) -> None:
    await fs_cache_inventory.record_cached(pg_tx, "obj-repo-rt", 3, 1)

    row = await pg_tx.fetchrow(
        "SELECT object_id, object_version, part_number FROM fs_cache_inventory WHERE object_id = 'obj-repo-rt'"
    )
    assert row is not None
    assert (row["object_id"], row["object_version"], row["part_number"]) == ("obj-repo-rt", 3, 1)


async def test_record_cached_twice_bumps_cached_at(pg_tx: asyncpg.Connection) -> None:
    await fs_cache_inventory.record_cached(pg_tx, "obj-repo-bump", 1, 0)
    # Push cached_at into the past so the second upsert's now() (frozen to tx start) is strictly newer.
    old = await pg_tx.fetchval(
        """
        UPDATE fs_cache_inventory SET cached_at = now() - interval '1 hour'
        WHERE object_id = 'obj-repo-bump' RETURNING cached_at
        """
    )

    await fs_cache_inventory.record_cached(pg_tx, "obj-repo-bump", 1, 0)

    bumped = await pg_tx.fetchval("SELECT cached_at FROM fs_cache_inventory WHERE object_id = 'obj-repo-bump'")
    assert bumped > old
    count = await pg_tx.fetchval("SELECT count(*) FROM fs_cache_inventory WHERE object_id = 'obj-repo-bump'")
    assert count == 1


async def test_record_cached_uuid_input_stored_as_str(pg_tx: asyncpg.Connection) -> None:
    oid = uuid.uuid4()
    await fs_cache_inventory.record_cached(pg_tx, oid, 2, 5)

    stored = await pg_tx.fetchval(
        "SELECT object_id FROM fs_cache_inventory WHERE object_version = 2 AND part_number = 5"
    )
    assert stored == str(oid)


async def test_record_cached_swallows_broken_conn(caplog: pytest.LogCaptureFixture) -> None:
    conn = _BrokenConn()
    with caplog.at_level(logging.WARNING):
        await fs_cache_inventory.record_cached(conn, "obj-repo-broken", 1, 0)

    assert conn.calls == 1
    assert any("fs_cache_inventory" in r.message for r in caplog.records)


async def test_clear_cached_removes_row(pg_tx: asyncpg.Connection) -> None:
    await fs_cache_inventory.record_cached(pg_tx, "obj-repo-clear", 4, 2)

    await fs_cache_inventory.clear_cached(pg_tx, "obj-repo-clear", 4, 2)

    remaining = await pg_tx.fetchval("SELECT count(*) FROM fs_cache_inventory WHERE object_id = 'obj-repo-clear'")
    assert remaining == 0


async def test_clear_cached_absent_row_is_noop(pg_tx: asyncpg.Connection) -> None:
    # No matching row: DELETE 0, no raise.
    await fs_cache_inventory.clear_cached(pg_tx, "obj-repo-absent", 9, 9)


async def test_record_cached_batch_inserts_all(pg_tx: asyncpg.Connection) -> None:
    rows = [("obj-batch-a", 1, 0), ("obj-batch-a", 1, 1), ("obj-batch-b", 2, 0)]
    await fs_cache_inventory.record_cached_batch(pg_tx, rows)

    count = await pg_tx.fetchval(
        "SELECT count(*) FROM fs_cache_inventory WHERE object_id IN ('obj-batch-a', 'obj-batch-b')"
    )
    assert count == 3


async def test_record_cached_batch_overlap_upserts(pg_tx: asyncpg.Connection) -> None:
    await fs_cache_inventory.record_cached_batch(pg_tx, [("obj-batch-ov", 1, 0)])
    # Overlapping row (1,0) is upserted; (1,1) is new.
    await fs_cache_inventory.record_cached_batch(pg_tx, [("obj-batch-ov", 1, 0), ("obj-batch-ov", 1, 1)])

    count = await pg_tx.fetchval("SELECT count(*) FROM fs_cache_inventory WHERE object_id = 'obj-batch-ov'")
    assert count == 2


async def test_record_cached_batch_empty_sends_no_sql() -> None:
    conn = _RecordingConn()
    await fs_cache_inventory.record_cached_batch(conn, [])
    assert conn.calls == []


async def test_record_cached_batch_coerces_uuid(pg_tx: asyncpg.Connection) -> None:
    oid = uuid.uuid4()
    await fs_cache_inventory.record_cached_batch(pg_tx, [(oid, 7, 3)])

    stored = await pg_tx.fetchval(
        "SELECT object_id FROM fs_cache_inventory WHERE object_version = 7 AND part_number = 3"
    )
    assert stored == str(oid)


async def test_record_cached_batch_swallows_broken_conn(caplog: pytest.LogCaptureFixture) -> None:
    conn = _BrokenConn()
    with caplog.at_level(logging.WARNING):
        await fs_cache_inventory.record_cached_batch(conn, [("obj-batch-broken", 1, 0)])

    assert conn.calls == 1
    assert any("fs_cache_inventory" in r.message for r in caplog.records)


async def test_janitor_state_round_trip_preserves_nested(pg_tx: asyncpg.Connection) -> None:
    value = {"cached_at": "2026-07-01T00:00:00Z", "shard": 3, "nested": {"cursor": [1, 2, 3]}}
    await fs_cache_inventory.set_janitor_state(pg_tx, "eviction_cursor", value)

    got = await fs_cache_inventory.get_janitor_state(pg_tx, "eviction_cursor")
    assert got == value


async def test_janitor_state_second_set_replaces_and_bumps(pg_tx: asyncpg.Connection) -> None:
    await fs_cache_inventory.set_janitor_state(pg_tx, "eviction_cursor", {"shard": 1})
    old = await pg_tx.fetchval(
        """
        UPDATE janitor_state SET updated_at = now() - interval '1 hour'
        WHERE key = 'eviction_cursor' RETURNING updated_at
        """
    )

    await fs_cache_inventory.set_janitor_state(pg_tx, "eviction_cursor", {"shard": 9})

    got = await fs_cache_inventory.get_janitor_state(pg_tx, "eviction_cursor")
    assert got == {"shard": 9}
    bumped = await pg_tx.fetchval("SELECT updated_at FROM janitor_state WHERE key = 'eviction_cursor'")
    assert bumped > old


async def test_janitor_state_missing_key_returns_none(pg_tx: asyncpg.Connection) -> None:
    got = await fs_cache_inventory.get_janitor_state(pg_tx, "does-not-exist")
    assert got is None
