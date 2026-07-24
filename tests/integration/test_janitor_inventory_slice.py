"""Keyset-slice contract for the janitor's SQL eviction cursor, against real Postgres.

`janitor_inventory_slice.sql` is the CURSOR-ADVANCING scan window: a pure keyset page of
fs_cache_inventory in (cached_at, object_id, object_version, part_number) order, served index-only
and bounded by LIMIT — independent of coverage/age (that lives in the separate filter). These tests
pin the two properties the stall fix depends on: the keyset walk is ordered / gap-free / complete so
advancing by the last returned row can never skip or repeat a part, and the scan is served by
fs_cache_inventory_cached_at rather than a seq scan so a bounded page stays bounded at any sparseness.

Seed rows are plain inventory rows (no object/part/chunk graph needed — the slice never joins them),
written via the `pg_tx` fixture (always rolled back). Each test wipes fs_cache_inventory first so the
walk sees exactly the seeded rows.
"""

from __future__ import annotations

import datetime
import json
import uuid
from typing import Any

import asyncpg
import pytest

from hippius_s3.utils import get_query


pytestmark = pytest.mark.asyncio

_UTC = datetime.timezone.utc
# Cold-start sentinel: strictly below every real row (cursor columns are NOT NULL, so a NULL element
# would make the row-value comparison NULL and drop every row).
_CURSOR_START = (datetime.datetime(1970, 1, 1, tzinfo=_UTC), "", 0, 0)


async def _fresh_inventory(conn: asyncpg.Connection) -> None:
    await conn.execute("DELETE FROM fs_cache_inventory")


async def _seed_inventory(conn: asyncpg.Connection, cached_at: datetime.datetime) -> tuple[str, int, int]:
    oid = str(uuid.uuid4())
    await conn.execute(
        "INSERT INTO fs_cache_inventory(object_id, object_version, part_number, cached_at) VALUES($1, 1, 1, $2)",
        oid,
        cached_at,
    )
    return (oid, 1, 1)


async def _slice(
    conn: asyncpg.Connection,
    *,
    limit: int,
    cursor: tuple[datetime.datetime, str, int, int] = _CURSOR_START,
) -> list[asyncpg.Record]:
    return await conn.fetch(get_query("janitor_inventory_slice"), limit, cursor[0], cursor[1], cursor[2], cursor[3])


# ===================================================== keyset walk


async def test_keyset_walk_is_ordered_disjoint_and_complete(pg_tx: asyncpg.Connection) -> None:
    await _fresh_inventory(pg_tx)
    base = datetime.datetime(2026, 1, 1, tzinfo=_UTC)
    seeded: set[tuple[str, int, int]] = set()
    for i in range(5):
        seeded.add(await _seed_inventory(pg_tx, base + datetime.timedelta(seconds=i)))

    collected: list[tuple[str, int, int]] = []
    ordered_cached_at: list[datetime.datetime] = []
    cursor = _CURSOR_START
    pages = 0
    while True:
        rows = await _slice(pg_tx, limit=2, cursor=cursor)
        pages += 1
        if not rows:
            break
        for r in rows:
            collected.append((r["object_id"], r["object_version"], r["part_number"]))
            ordered_cached_at.append(r["cached_at"])
        last = rows[-1]
        cursor = (last["cached_at"], last["object_id"], last["object_version"], last["part_number"])
        if len(rows) < 2:
            break

    assert ordered_cached_at == sorted(ordered_cached_at), "pages walk oldest-first"
    assert len(collected) == len(set(collected)) == 5, "every row seen exactly once (gap-free, dup-free)"
    assert set(collected) == seeded
    assert pages == 3, "5 rows at LIMIT 2 => pages of 2, 2, 1 (the short last page terminates)"


async def test_short_final_page_terminates(pg_tx: asyncpg.Connection) -> None:
    await _fresh_inventory(pg_tx)
    base = datetime.datetime(2026, 2, 1, tzinfo=_UTC)
    for i in range(2):
        await _seed_inventory(pg_tx, base + datetime.timedelta(seconds=i))

    page1 = await _slice(pg_tx, limit=5)
    assert len(page1) == 2  # short page (< limit) => end of ring, no second page needed
    last = page1[-1]
    cursor = (last["cached_at"], last["object_id"], last["object_version"], last["part_number"])
    assert await _slice(pg_tx, limit=5, cursor=cursor) == []


async def test_cursor_row_is_excluded_strictly(pg_tx: asyncpg.Connection) -> None:
    # The row-value comparison is strict (>), so resuming AT a row's own tuple returns rows strictly
    # after it — the just-processed row is never re-served.
    await _fresh_inventory(pg_tx)
    base = datetime.datetime(2026, 3, 1, tzinfo=_UTC)
    first = await _seed_inventory(pg_tx, base)
    second = await _seed_inventory(pg_tx, base + datetime.timedelta(seconds=1))

    first_cached_at = await pg_tx.fetchval("SELECT cached_at FROM fs_cache_inventory WHERE object_id = $1", first[0])
    rows = await _slice(pg_tx, limit=10, cursor=(first_cached_at, first[0], first[1], first[2]))
    keys = {(r["object_id"], r["object_version"], r["part_number"]) for r in rows}
    assert first not in keys, "the cursor row itself is excluded"
    assert second in keys


# ===================================================== plan shape (index-only walk)


def _walk_plan(node: dict[str, Any]) -> list[dict[str, Any]]:
    nodes = [node]
    for child in node.get("Plans", []):
        nodes.extend(_walk_plan(child))
    return nodes


async def test_explain_slice_uses_cached_at_index_no_seq_scan(pg_tx: asyncpg.Connection) -> None:
    await _fresh_inventory(pg_tx)
    base = datetime.datetime(2026, 4, 1, tzinfo=_UTC)
    for i in range(3):
        await _seed_inventory(pg_tx, base + datetime.timedelta(seconds=i))

    # The test DB is tiny, so force seqscan off to prove the slice CAN be served purely by the keyset
    # index — the property that keeps a bounded page bounded no matter how sparse the ring is.
    await pg_tx.execute("SET LOCAL enable_seqscan = off")
    rows = await pg_tx.fetch(
        "EXPLAIN (FORMAT JSON) " + get_query("janitor_inventory_slice"),
        100,
        _CURSOR_START[0],
        _CURSOR_START[1],
        _CURSOR_START[2],
        _CURSOR_START[3],
    )
    raw = rows[0][0]
    plan = json.loads(raw) if isinstance(raw, str) else raw
    nodes = _walk_plan(plan[0]["Plan"])

    seq_scans = {n.get("Relation Name") for n in nodes if n.get("Node Type") == "Seq Scan"}
    assert "fs_cache_inventory" not in seq_scans, f"inventory seq-scanned: {nodes}"

    used_indexes = {n.get("Index Name") for n in nodes if n.get("Index Name")}
    assert "fs_cache_inventory_cached_at" in used_indexes, f"cached_at index not used: {nodes}"
