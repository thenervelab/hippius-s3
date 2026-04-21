"""Tests for FS-backed chunks_exist_batch via RedisObjectPartsCache."""

from __future__ import annotations

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
