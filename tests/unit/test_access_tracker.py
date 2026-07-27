"""Unit tests for the sampled read-recency tracker (fs_cache_inventory.last_access_at)."""

from __future__ import annotations

import logging

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
async def test_flush_failure_requeues_and_unsuppresses():
    pool = FakePool(fail=True)
    tracker = AccessTracker(pool, hot_window_seconds=14400)
    tracker.note_read(OID, 1, 1)

    assert await tracker.flush_once() == 1  # attempted
    # A transient error must not blind a continuously-read part: the key is
    # requeued and its sampling suppression cleared so recency is not lost.
    assert (OID, 1, 1) in tracker._pending
    assert (OID, 1, 1) not in tracker._last_noted
    pool.fail = False
    assert await tracker.flush_once() == 1  # retried and recorded


def test_pending_is_bounded_under_key_diverse_storm():
    """`_pending` drains only on flush; a key-diverse storm during a stalled
    flush must not grow it past the bound."""
    tracker = AccessTracker(FakePool(), hot_window_seconds=3600)
    for i in range(at.MAX_TRACKED_KEYS + 5000):
        tracker.note_read(OID, 1, i)

    assert len(tracker._pending) <= at.MAX_TRACKED_KEYS


@pytest.mark.asyncio
async def test_flush_failure_then_continued_read_re_notes():
    pool = FakePool(fail=True)
    tracker = AccessTracker(pool, hot_window_seconds=14400)
    tracker.note_read(OID, 1, 1)
    assert await tracker.flush_once() == 1  # attempted, failed, unsuppressed

    # Suppression was dropped, so a read still inside the sample window re-notes.
    tracker._pending.clear()
    tracker.note_read(OID, 1, 1)
    assert (OID, 1, 1) in tracker._pending


@pytest.mark.asyncio
async def test_zero_row_flush_is_surfaced(caplog):
    class ZeroPool:
        async def execute(self, sql, *args):
            return "UPDATE 0"

    tracker = AccessTracker(ZeroPool(), hot_window_seconds=14400)
    tracker.note_read(OID, 1, 1)

    with caplog.at_level(logging.WARNING, logger="hippius_s3.cache.access_tracker"):
        assert await tracker.flush_once() == 1
    assert any("0/1" in r.getMessage() for r in caplog.records)


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
async def test_fs_store_get_chunk_notes_read_on_hit(tmp_path):
    """The hook must live in FileSystemPartsStore.get_chunk itself: the
    streamer passes fetch_fn = fs.get_chunk and bypasses every wrapper, so a
    wrapper-level hook would be inert for all streamed GET traffic (the
    original review blocker)."""
    from hippius_s3.cache.fs_store import FileSystemPartsStore

    tracker = initialize_access_tracker(FakePool(), hot_window_seconds=3600)
    store = FileSystemPartsStore(str(tmp_path))
    await store.set_chunk(OID, 1, 3, 0, b"data")
    await store.set_meta(OID, 1, 3, chunk_size=4, num_chunks=1, size_bytes=4)

    assert await store.get_chunk(OID, 1, 3, 0) == b"data"
    assert await store.get_chunk(OID, 1, 4, 0) is None  # miss: no note

    assert (OID, 1, 3) in tracker._pending
    assert (OID, 1, 4) not in tracker._pending


def test_note_read_enforces_hard_key_bound():
    """A key-diverse read storm inside the sample window must not grow the
    sampling map unboundedly — the window prune alone can't shrink it."""
    import hippius_s3.cache.access_tracker as mod

    tracker = AccessTracker(FakePool(), hot_window_seconds=3600)
    for i in range(mod.MAX_TRACKED_KEYS + 1000):
        tracker.note_read(OID, 1, i)

    assert len(tracker._last_noted) <= mod.MAX_TRACKED_KEYS
