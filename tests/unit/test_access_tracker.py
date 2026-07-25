"""Unit tests for the sampled read-recency tracker (fs_cache_inventory.last_access_at)."""

from __future__ import annotations

import pytest

import hippius_s3.cache.access_tracker as at
from hippius_s3.cache.access_tracker import AccessTracker
from hippius_s3.cache.access_tracker import get_access_tracker
from hippius_s3.cache.access_tracker import initialize_access_tracker


class FakePool:
    def __init__(self, fail: bool = False) -> None:
        self.execute_calls: list[tuple[str, tuple]] = []
        self.fail = fail

    async def execute(self, sql: str, *args):
        if self.fail:
            raise ConnectionError("db down")
        self.execute_calls.append((sql, args))
        return "UPDATE 1"


OID = "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"


@pytest.fixture(autouse=True)
def _reset_singleton():
    at._tracker = None
    yield
    at._tracker = None


@pytest.mark.asyncio
async def test_note_read_batches_and_flushes():
    pool = FakePool()
    tracker = AccessTracker(pool, hot_window_seconds=14400)
    tracker.note_read(OID, 1, 1)
    tracker.note_read(OID, 1, 2)

    flushed = await tracker.flush_once()

    assert flushed == 2
    assert len(pool.execute_calls) == 1
    sql, args = pool.execute_calls[0]
    assert "last_access_at = now()" in sql
    assert sorted(zip(args[0], args[1], args[2], strict=False)) == [(OID, 1, 1), (OID, 1, 2)]


@pytest.mark.asyncio
async def test_sampling_suppresses_repeat_notes_within_window():
    pool = FakePool()
    tracker = AccessTracker(pool, hot_window_seconds=14400)
    for _ in range(100):
        tracker.note_read(OID, 1, 1)

    assert await tracker.flush_once() == 1
    # Still inside the sample window: nothing new to record.
    tracker.note_read(OID, 1, 1)
    assert await tracker.flush_once() == 0
    assert len(pool.execute_calls) == 1


@pytest.mark.asyncio
async def test_flush_failure_drops_batch_without_raising():
    pool = FakePool(fail=True)
    tracker = AccessTracker(pool, hot_window_seconds=14400)
    tracker.note_read(OID, 1, 1)

    assert await tracker.flush_once() == 1  # attempted
    pool.fail = False
    # Batch was dropped, not requeued: nothing to flush now.
    assert await tracker.flush_once() == 0


@pytest.mark.asyncio
async def test_empty_flush_is_noop():
    pool = FakePool()
    tracker = AccessTracker(pool, hot_window_seconds=14400)
    assert await tracker.flush_once() == 0
    assert pool.execute_calls == []


def test_singleton_wiring():
    assert get_access_tracker() is None
    tracker = initialize_access_tracker(FakePool(), hot_window_seconds=3600)
    assert get_access_tracker() is tracker


@pytest.mark.asyncio
async def test_object_parts_get_chunk_notes_read_on_hit():
    from hippius_s3.cache.object_parts import RedisObjectPartsCache

    tracker = initialize_access_tracker(FakePool(), hot_window_seconds=3600)

    class FakeFs:
        async def get_chunk(self, object_id, object_version, part_number, chunk_index):
            return b"data" if chunk_index == 0 else None

    cache = RedisObjectPartsCache.__new__(RedisObjectPartsCache)
    cache._fs = FakeFs()

    assert await cache.get_chunk(OID, 1, 3, 0) == b"data"
    assert await cache.get_chunk(OID, 1, 4, 1) is None  # miss: no note

    assert (OID, 1, 3) in tracker._pending
    assert (OID, 1, 4) not in tracker._pending
