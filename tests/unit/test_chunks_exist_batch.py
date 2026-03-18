"""Tests for RedisObjectPartsCache.chunks_exist_batch and NullObjectPartsCache.chunks_exist_batch."""

from __future__ import annotations

from unittest.mock import MagicMock

import pytest

from hippius_s3.cache.object_parts import NullObjectPartsCache
from hippius_s3.cache.object_parts import RedisObjectPartsCache


class FakePipeline:
    """Simulates redis.asyncio pipeline with recorded commands."""

    def __init__(self, results: list) -> None:
        self._results = results
        self._commands: list[str] = []

    def exists(self, key: str) -> None:
        self._commands.append(key)

    async def execute(self) -> list:
        return self._results

    async def __aenter__(self):
        return self

    async def __aexit__(self, *args):
        pass


def _make_cache(pipeline_results: list) -> RedisObjectPartsCache:
    pipe = FakePipeline(pipeline_results)
    redis_mock = MagicMock()
    redis_mock.pipeline = MagicMock(return_value=pipe)
    return RedisObjectPartsCache(redis_mock), pipe


@pytest.mark.asyncio
async def test_batch_empty_checks():
    cache, _ = _make_cache([])
    result = await cache.chunks_exist_batch("obj1", 1, [])
    assert result == []


@pytest.mark.asyncio
async def test_batch_all_cached():
    cache, pipe = _make_cache([1, 1, 1])
    checks = [(1, 0), (1, 1), (1, 2)]
    result = await cache.chunks_exist_batch("obj1", 1, checks)

    assert result == [True, True, True]
    assert len(pipe._commands) == 3


@pytest.mark.asyncio
async def test_batch_none_cached():
    cache, pipe = _make_cache([0, 0, 0])
    checks = [(1, 0), (1, 1), (2, 0)]
    result = await cache.chunks_exist_batch("obj1", 1, checks)

    assert result == [False, False, False]
    assert len(pipe._commands) == 3


@pytest.mark.asyncio
async def test_batch_partial_cached():
    cache, pipe = _make_cache([1, 0, 1, 0])
    checks = [(1, 0), (1, 1), (2, 0), (2, 1)]
    result = await cache.chunks_exist_batch("obj1", 1, checks)

    assert result == [True, False, True, False]


@pytest.mark.asyncio
async def test_batch_single_chunk():
    cache, _ = _make_cache([1])
    result = await cache.chunks_exist_batch("obj1", 1, [(1, 0)])
    assert result == [True]


@pytest.mark.asyncio
async def test_batch_key_format():
    """Verify correct Redis key construction for each check."""
    cache, pipe = _make_cache([0, 0])
    checks = [(3, 7), (5, 2)]
    await cache.chunks_exist_batch("myobj", 42, checks)

    assert pipe._commands[0] == "obj:myobj:v:42:part:3:chunk:7"
    assert pipe._commands[1] == "obj:myobj:v:42:part:5:chunk:2"


@pytest.mark.asyncio
async def test_batch_cross_part_chunks():
    """Chunks from different parts are batched in a single pipeline."""
    cache, pipe = _make_cache([1, 0, 1])
    checks = [(1, 0), (2, 0), (3, 0)]
    result = await cache.chunks_exist_batch("obj1", 1, checks)

    assert result == [True, False, True]
    assert len(pipe._commands) == 3


@pytest.mark.asyncio
async def test_null_cache_batch_returns_all_false():
    cache = NullObjectPartsCache()
    result = await cache.chunks_exist_batch("obj1", 1, [(1, 0), (1, 1), (2, 0)])
    assert result == [False, False, False]


@pytest.mark.asyncio
async def test_null_cache_batch_empty():
    cache = NullObjectPartsCache()
    result = await cache.chunks_exist_batch("obj1", 1, [])
    assert result == []
