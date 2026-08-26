"""Tests for FS-backed chunks_exist_batch via RedisObjectPartsCache."""

from __future__ import annotations

import time
from pathlib import Path

import pytest

from hippius_s3.cache.fs_store import FileSystemPartsStore
from hippius_s3.cache.object_parts import RedisObjectPartsCache


OBJ = "11111111-2222-3333-4444-555555555555"


def _make_cache(tmp_path: Path) -> tuple[RedisObjectPartsCache, FileSystemPartsStore]:
    fs = FileSystemPartsStore(str(tmp_path))
    cache = RedisObjectPartsCache(redis_client=None, queues_client=None, fs_store=fs)
    return cache, fs


async def _prepare_part(fs: FileSystemPartsStore, part_number: int, num_chunks: int = 3) -> None:
    for i in range(num_chunks):
        await fs.set_chunk(OBJ, 1, part_number, i, b"chunk")
    await fs.set_meta(OBJ, 1, part_number, chunk_size=5, num_chunks=num_chunks, size_bytes=5 * num_chunks)


@pytest.mark.asyncio
async def test_batch_empty_checks(tmp_path):
    cache, _ = _make_cache(tmp_path)
    assert await cache.chunks_exist_batch(OBJ, 1, []) == []


@pytest.mark.asyncio
async def test_batch_all_present(tmp_path):
    cache, fs = _make_cache(tmp_path)
    await _prepare_part(fs, part_number=1, num_chunks=3)

    result = await cache.chunks_exist_batch(OBJ, 1, [(1, 0), (1, 1), (1, 2)])
    assert result == [True, True, True]


@pytest.mark.asyncio
async def test_batch_none_present(tmp_path):
    cache, _ = _make_cache(tmp_path)
    result = await cache.chunks_exist_batch(OBJ, 1, [(1, 0), (1, 1), (2, 0)])
    assert result == [False, False, False]


@pytest.mark.asyncio
async def test_batch_partial_present(tmp_path):
    cache, fs = _make_cache(tmp_path)
    await _prepare_part(fs, part_number=1, num_chunks=2)
    # part 2 has no meta

    result = await cache.chunks_exist_batch(OBJ, 1, [(1, 0), (1, 1), (2, 0), (2, 1)])
    assert result == [True, True, False, False]


@pytest.mark.asyncio
async def test_batch_requires_meta(tmp_path):
    """If meta.json is missing, chunk files alone don't count as present."""
    cache, fs = _make_cache(tmp_path)
    # Write a chunk file but NOT meta — simulates partial write / crashed worker
    await fs.set_chunk(OBJ, 1, 7, 0, b"data")

    result = await cache.chunks_exist_batch(OBJ, 1, [(7, 0)])
    assert result == [False]


@pytest.mark.asyncio
async def test_batch_cross_part(tmp_path):
    """Chunks from different parts are resolved in one call."""
    cache, fs = _make_cache(tmp_path)
    await _prepare_part(fs, part_number=1, num_chunks=1)
    await _prepare_part(fs, part_number=3, num_chunks=1)

    result = await cache.chunks_exist_batch(OBJ, 1, [(1, 0), (2, 0), (3, 0)])
    assert result == [True, False, True]


@pytest.mark.asyncio
async def test_batch_missing_chunk_file_with_meta_present(tmp_path):
    """Meta present, but a specific chunk file is missing (partial range fill)."""
    cache, fs = _make_cache(tmp_path)
    # Write meta eagerly (num_chunks=5) but only chunks 0 and 2
    await fs.set_meta(OBJ, 1, 1, chunk_size=4, num_chunks=5, size_bytes=20)
    await fs.set_chunk(OBJ, 1, 1, 0, b"aaaa")
    await fs.set_chunk(OBJ, 1, 1, 2, b"cccc")

    result = await cache.chunks_exist_batch(OBJ, 1, [(1, 0), (1, 1), (1, 2), (1, 3), (1, 4)])
    assert result == [True, False, True, False, False]


@pytest.mark.asyncio
async def test_batch_ignores_staged_and_tmp_files(tmp_path):
    """The scan must count only published `chunk_<i>.bin`, never .tmp/.staged siblings.

    The write path leaves `chunk_<i>.bin.staged.<attempt>` and `<f>.tmp.<uuid>` files around;
    a directory scan sees them, so the index parser must reject anything that isn't exactly
    `chunk_<int>.bin` — otherwise a half-written chunk would read as present.
    """
    cache, fs = _make_cache(tmp_path)
    await fs.set_meta(OBJ, 1, 1, chunk_size=4, num_chunks=3, size_bytes=12)
    await fs.set_chunk(OBJ, 1, 1, 0, b"aaaa")
    part_dir = Path(fs.part_path(OBJ, 1, 1))
    # Sibling files that must NOT be parsed as chunk 1 or chunk 2.
    (part_dir / "chunk_1.bin.staged.3").write_bytes(b"bbbb")
    (part_dir / "chunk_2.bin.tmp.abcd").write_bytes(b"cccc")

    result = await cache.chunks_exist_batch(OBJ, 1, [(1, 0), (1, 1), (1, 2)])
    assert result == [True, False, False]


@pytest.mark.asyncio
async def test_batch_large_single_part_all_present(tmp_path):
    """A single part with many chunks resolves in one scan — the TTFB-anomaly shape.

    A 5 GB single-part object is ~1250 chunks in one part dir; this is the case the old
    per-chunk stat loop turned into ~1250 serial stats. One scandir must return them all.
    """
    cache, fs = _make_cache(tmp_path)
    n = 400
    for i in range(n):
        await fs.set_chunk(OBJ, 1, 1, i, b"x")
    await fs.set_meta(OBJ, 1, 1, chunk_size=1, num_chunks=n, size_bytes=n)

    checks = [(1, i) for i in range(n)]
    result = await cache.chunks_exist_batch(OBJ, 1, checks)
    assert result == [True] * n


@pytest.mark.asyncio
async def test_batch_scans_each_part_once(tmp_path, monkeypatch):
    """One scandir per distinct part, regardless of how many chunks are checked in it."""
    import hippius_s3.cache.fs_store as fs_mod

    cache, fs = _make_cache(tmp_path)
    await _prepare_part(fs, part_number=1, num_chunks=5)
    await _prepare_part(fs, part_number=2, num_chunks=5)

    calls: list[str] = []
    real_scandir = fs_mod.os.scandir

    def counting_scandir(path):
        calls.append(str(path))
        return real_scandir(path)

    monkeypatch.setattr(fs_mod.os, "scandir", counting_scandir)

    checks = [(1, i) for i in range(5)] + [(2, i) for i in range(5)]
    result = await cache.chunks_exist_batch(OBJ, 1, checks)
    assert result == [True] * 10
    # 10 chunk checks across 2 parts must cost exactly 2 directory scans, not 10.
    assert len(calls) == 2, f"expected 2 scandir calls (one per part), got {len(calls)}"


class TestParallelPartScans:
    """Parts are scanned concurrently, but the result must stay positionally exact.

    The scans are fanned out, so they complete out of order. Everything below pins the
    properties that fan-out can break: ordering, the per-part scan count, the bound on
    concurrency, and the single-part fast path.
    """

    @pytest.mark.asyncio
    async def test_result_order_survives_out_of_order_completion(self, tmp_path, monkeypatch):
        """Interleaved parts + deliberately inverted scan latency must not reorder results."""
        import hippius_s3.cache.fs_store as fs_mod

        cache, fs = _make_cache(tmp_path)
        # part 1 present, part 2 absent, part 3 present -> a distinctive expected pattern
        await _prepare_part(fs, part_number=1, num_chunks=2)
        await _prepare_part(fs, part_number=3, num_chunks=2)

        real_scandir = fs_mod.os.scandir

        def slow_for_low_parts(path):
            # Make the FIRST-listed part the SLOWEST, so completion order inverts scan order.
            if str(path).endswith("part_1"):
                time.sleep(0.05)
            return real_scandir(path)

        monkeypatch.setattr(fs_mod.os, "scandir", slow_for_low_parts)

        checks = [(1, 0), (2, 0), (3, 0), (1, 1), (3, 1), (2, 1)]
        result = await cache.chunks_exist_batch(OBJ, 1, checks)
        assert result == [True, False, True, True, True, False]

@pytest.mark.parametrize(
    "bogus",
    ["chunk_007.bin", "chunk_+1.bin", "chunk_ 1.bin", "chunk_١.bin", "chunk_-1.bin"],
)
@pytest.mark.asyncio
async def test_noncanonical_chunk_names_are_not_treated_as_chunks(tmp_path, bogus):
    """Presence must be defined by the exact filename, not by what `int()` tolerates.

    The old code stat'd the exact path `chunk_<i>.bin`, so an oddly-spelled sibling could never
    register. `int()` accepts leading zeros, signs, whitespace and non-ASCII digits, so without a
    round-trip check `chunk_007.bin` would read as index 7. The writer never emits these, but the
    equivalence should hold by construction rather than by argument.
    """
    cache, fs = _make_cache(tmp_path)
    await fs.set_meta(OBJ, 1, 1, chunk_size=4, num_chunks=8, size_bytes=32)
    part_dir = Path(fs.part_path(OBJ, 1, 1))
    (part_dir / bogus).write_bytes(b"xxxx")

    # Index 1 and 7 must both still read as absent — only the canonical name counts.
    assert await cache.chunks_exist_batch(OBJ, 1, [(1, 1), (1, 7)]) == [False, False]


class TestScanPoolFairness:
    """One request must not be able to monopolise the shared scan pool.

    The pool's max_workers bounds what the PROCESS puts on the MDS. It does not bound what one
    request may QUEUE: `gather` submits every part at once and the executor's queue is unbounded
    FIFO with no fairness, so without a per-request cap a many-part read parks every one of its
    jobs ahead of a small read that arrives a moment later. Measured at ~143ms of added latency
    for the small read on a local FS; on the pool tier, where a readdir is ~6ms, a 2000-part
    object is worth ~0.8s and a 10,000-part one ~4s — the exact latency this method exists to
    remove, moved onto whoever reads alongside a large object.

    The pool is pinned small here so the difference is unambiguous rather than timing-luck: with
    a cap the small read waits on a couple of scans, without one it waits on parts/workers of them.
    """

    @pytest.mark.asyncio
    async def test_a_large_scan_does_not_stall_a_small_concurrent_one(self, tmp_path, monkeypatch):
        import asyncio
        from concurrent.futures import ThreadPoolExecutor

        import hippius_s3.cache.fs_store as fs_mod

        workers, big_parts, per_scan = 4, 200, 0.002
        cache, fs = _make_cache(tmp_path)
        for p in range(1, big_parts + 1):
            await _prepare_part(fs, part_number=p, num_chunks=1)
        small_part = big_parts + 1
        await _prepare_part(fs, part_number=small_part, num_chunks=1)

        real_scandir = fs_mod.os.scandir

        def slow(path):
            time.sleep(per_scan)
            return real_scandir(path)

        monkeypatch.setattr(fs_mod.os, "scandir", slow)
        fs_mod._reset_scan_pool_for_tests()
        fs_mod._SCAN_POOL = ThreadPoolExecutor(max_workers=workers, thread_name_prefix="fs-scan-test")
        fs_mod._SCAN_POOL_SIZE = workers
        try:

            async def big():
                return await cache.chunks_exist_batch(OBJ, 1, [(p, 0) for p in range(1, big_parts + 1)])

            async def small():
                await asyncio.sleep(0.01)  # start once the big scan is well underway
                t0 = time.perf_counter()
                r = await cache.chunks_exist_batch(OBJ, 1, [(small_part, 0)])
                return r, time.perf_counter() - t0

            big_task = asyncio.create_task(big())
            small_result, small_elapsed = await small()
            await big_task
        finally:
            fs_mod._reset_scan_pool_for_tests()

        assert small_result == [True]
        # Unbounded, the small read waits out big_parts/workers scans = 200/4 * 2ms = ~100ms.
        # Capped, it waits on a couple of in-flight scans. 25ms sits well clear of both.
        assert small_elapsed < 0.025, f"small read waited {small_elapsed * 1000:.0f}ms behind the large one"


class TestScanPoolBounds:
    @pytest.mark.asyncio
    async def test_in_flight_scans_never_exceed_the_configured_limit(self, tmp_path, monkeypatch):
        """Both bounds hold: concurrency is real, and capped by the configured value."""
        import threading

        import hippius_s3.cache.fs_store as fs_mod

        limit = fs_mod._scan_pool_size()
        cache, fs = _make_cache(tmp_path)
        nparts = limit + 8
        for p in range(1, nparts + 1):
            await _prepare_part(fs, part_number=p, num_chunks=1)

        lock = threading.Lock()
        state = {"live": 0, "peak": 0}
        real_scandir = fs_mod.os.scandir

        def tracking(path):
            with lock:
                state["live"] += 1
                state["peak"] = max(state["peak"], state["live"])
            try:
                time.sleep(0.02)
                return real_scandir(path)
            finally:
                with lock:
                    state["live"] -= 1

        monkeypatch.setattr(fs_mod.os, "scandir", tracking)

        result = await cache.chunks_exist_batch(OBJ, 1, [(p, 0) for p in range(1, nparts + 1)])
        assert result == [True] * nparts
        assert state["peak"] > 1, "scans did not run concurrently"
        assert state["peak"] <= limit, f"concurrency {state['peak']} exceeded limit {limit}"

    @pytest.mark.asyncio
    async def test_single_part_costs_exactly_one_scan(self, tmp_path, monkeypatch):
        import hippius_s3.cache.fs_store as fs_mod

        cache, fs = _make_cache(tmp_path)
        await _prepare_part(fs, part_number=1, num_chunks=50)

        calls: list[str] = []
        real_scandir = fs_mod.os.scandir

        def counting(path):
            calls.append(str(path))
            return real_scandir(path)

        monkeypatch.setattr(fs_mod.os, "scandir", counting)
        assert await cache.chunks_exist_batch(OBJ, 1, [(1, i) for i in range(50)]) == [True] * 50
        assert len(calls) == 1
