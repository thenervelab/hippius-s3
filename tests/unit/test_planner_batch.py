"""Tests for batched DB query in build_chunk_plan (planner) and read_parts_plain_and_chunk_sizes_batch."""

from __future__ import annotations

from unittest.mock import AsyncMock

import pytest

from hippius_s3.reader.db_meta import read_parts_plain_and_chunk_sizes_batch
from hippius_s3.reader.planner import build_chunk_plan


class FakeDB:
    """Simulates asyncpg connection with configurable fetch results."""

    def __init__(self, rows: list[dict] | None = None) -> None:
        self._rows = rows or []
        self.fetch = AsyncMock(return_value=self._rows)
        self.fetchrow = AsyncMock(return_value=None)


# --- read_parts_plain_and_chunk_sizes_batch tests ---


@pytest.mark.asyncio
async def test_batch_read_empty_part_numbers():
    db = FakeDB()
    result = await read_parts_plain_and_chunk_sizes_batch(db, "obj1", [], 1)
    assert result == {}
    db.fetch.assert_not_awaited()


@pytest.mark.asyncio
async def test_batch_read_single_part():
    db = FakeDB([{"part_number": 1, "size_bytes": 4096, "chunk_size_bytes": 4096}])
    result = await read_parts_plain_and_chunk_sizes_batch(db, "obj1", [1], 1)

    assert result == {1: (4096, 4096)}
    db.fetch.assert_awaited_once()


@pytest.mark.asyncio
async def test_batch_read_multiple_parts():
    db = FakeDB(
        [
            {"part_number": 1, "size_bytes": 8000, "chunk_size_bytes": 4000},
            {"part_number": 2, "size_bytes": 6000, "chunk_size_bytes": 4000},
            {"part_number": 3, "size_bytes": 2000, "chunk_size_bytes": 4000},
        ]
    )
    result = await read_parts_plain_and_chunk_sizes_batch(db, "obj1", [1, 2, 3], 1)

    assert result == {1: (8000, 4000), 2: (6000, 4000), 3: (2000, 4000)}
    db.fetch.assert_awaited_once()


@pytest.mark.asyncio
async def test_batch_read_missing_part():
    """If a part_number isn't found in DB, it won't be in the result dict."""
    db = FakeDB([{"part_number": 1, "size_bytes": 4096, "chunk_size_bytes": 4096}])
    result = await read_parts_plain_and_chunk_sizes_batch(db, "obj1", [1, 2], 1)

    assert 1 in result
    assert 2 not in result


@pytest.mark.asyncio
async def test_batch_read_null_chunk_size():
    db = FakeDB([{"part_number": 1, "size_bytes": 4096, "chunk_size_bytes": None}])
    result = await read_parts_plain_and_chunk_sizes_batch(db, "obj1", [1], 1)

    assert result == {1: (4096, 0)}


# --- build_chunk_plan tests ---


@pytest.mark.asyncio
async def test_plan_single_part_no_range():
    db = FakeDB([{"part_number": 1, "size_bytes": 8000, "chunk_size_bytes": 4000}])
    parts = [{"part_number": 1}]

    plan = await build_chunk_plan(db, "obj1", parts, None, object_version=1)

    assert len(plan) == 2
    assert plan[0].part_number == 1
    assert plan[0].chunk_index == 0
    assert plan[1].part_number == 1
    assert plan[1].chunk_index == 1


@pytest.mark.asyncio
async def test_plan_multiple_parts_no_range():
    db = FakeDB(
        [
            {"part_number": 1, "size_bytes": 4000, "chunk_size_bytes": 4000},
            {"part_number": 2, "size_bytes": 4000, "chunk_size_bytes": 4000},
        ]
    )
    parts = [{"part_number": 1}, {"part_number": 2}]

    plan = await build_chunk_plan(db, "obj1", parts, None, object_version=1)

    assert len(plan) == 2
    assert plan[0].part_number == 1
    assert plan[0].chunk_index == 0
    assert plan[1].part_number == 2
    assert plan[1].chunk_index == 0


@pytest.mark.asyncio
async def test_plan_skips_zero_size_parts():
    db = FakeDB(
        [
            {"part_number": 1, "size_bytes": 4000, "chunk_size_bytes": 4000},
        ]
    )
    parts = [{"part_number": 1}, {"part_number": 2}]

    plan = await build_chunk_plan(db, "obj1", parts, None, object_version=1)

    # Part 2 has (0,0) from missing DB row, should be skipped
    assert len(plan) == 1
    assert plan[0].part_number == 1


@pytest.mark.asyncio
async def test_plan_empty_parts():
    db = FakeDB([])
    plan = await build_chunk_plan(db, "obj1", [], None, object_version=1)
    assert plan == []


@pytest.mark.asyncio
async def test_plan_with_range_spanning_single_chunk():
    from hippius_s3.reader.types import RangeRequest

    db = FakeDB([{"part_number": 1, "size_bytes": 8000, "chunk_size_bytes": 4000}])
    parts = [{"part_number": 1}]
    rng = RangeRequest(start=0, end=3999)

    plan = await build_chunk_plan(db, "obj1", parts, rng, object_version=1)

    assert len(plan) == 1
    assert plan[0].part_number == 1
    assert plan[0].chunk_index == 0


@pytest.mark.asyncio
async def test_plan_uses_single_batch_query():
    """Verify that build_chunk_plan calls fetch exactly once (batched), not N times."""
    db = FakeDB(
        [
            {"part_number": 1, "size_bytes": 4000, "chunk_size_bytes": 4000},
            {"part_number": 2, "size_bytes": 4000, "chunk_size_bytes": 4000},
            {"part_number": 3, "size_bytes": 4000, "chunk_size_bytes": 4000},
        ]
    )
    parts = [{"part_number": 1}, {"part_number": 2}, {"part_number": 3}]

    await build_chunk_plan(db, "obj1", parts, None, object_version=1)

    assert db.fetch.await_count == 1
