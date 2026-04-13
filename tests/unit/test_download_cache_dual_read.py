"""Tests for RedisObjectPartsCache dual-read (main Redis + download cache).

Validates that get_chunk checks main Redis first, then falls back to the
download cache. Also tests set_download_chunk and delete_download_chunks.
"""

from __future__ import annotations

from unittest.mock import AsyncMock
from unittest.mock import MagicMock

import pytest

from hippius_s3.cache.object_parts import RedisObjectPartsCache


def _make_redis_mock(get_return=None):
    mock = MagicMock()
    mock.get = AsyncMock(return_value=get_return)
    mock.setex = AsyncMock()
    mock.delete = AsyncMock()
    return mock


@pytest.mark.asyncio
async def test_get_chunk_main_hit_skips_download_cache():
    """Main Redis has data -> returned immediately, download cache not checked."""
    main = _make_redis_mock(get_return=b"main-data")
    dl = _make_redis_mock()
    cache = RedisObjectPartsCache(main, download_cache_client=dl)

    result = await cache.get_chunk("obj1", 1, 1, 0)

    assert result == b"main-data"
    main.get.assert_awaited_once()
    dl.get.assert_not_awaited()


@pytest.mark.asyncio
async def test_get_chunk_main_miss_download_cache_hit():
    """Main miss -> falls back to download cache -> returns data."""
    main = _make_redis_mock(get_return=None)
    dl = _make_redis_mock(get_return=b"dl-data")
    cache = RedisObjectPartsCache(main, download_cache_client=dl)

    result = await cache.get_chunk("obj1", 1, 1, 0)

    assert result == b"dl-data"
    main.get.assert_awaited_once()
    dl.get.assert_awaited_once()


@pytest.mark.asyncio
async def test_get_chunk_both_miss():
    """Both miss -> returns None."""
    main = _make_redis_mock(get_return=None)
    dl = _make_redis_mock(get_return=None)
    cache = RedisObjectPartsCache(main, download_cache_client=dl)

    result = await cache.get_chunk("obj1", 1, 1, 0)

    assert result is None


@pytest.mark.asyncio
async def test_get_chunk_no_download_cache_returns_none_on_miss():
    """download_cache_client=None, main miss -> returns None."""
    main = _make_redis_mock(get_return=None)
    cache = RedisObjectPartsCache(main)

    result = await cache.get_chunk("obj1", 1, 1, 0)

    assert result is None


@pytest.mark.asyncio
async def test_set_download_chunk_writes_to_download_cache():
    """set_download_chunk writes to download cache, NOT main Redis."""
    main = _make_redis_mock()
    dl = _make_redis_mock()
    cache = RedisObjectPartsCache(main, download_cache_client=dl)

    await cache.set_download_chunk("obj1", 1, 1, 0, b"data", ttl=300)

    dl.setex.assert_awaited_once()
    call_args = dl.setex.call_args
    assert call_args[0][0] == "obj:obj1:v:1:part:1:chunk:0"
    assert call_args[0][1] == 300
    assert call_args[0][2] == b"data"
    main.setex.assert_not_awaited()


@pytest.mark.asyncio
async def test_set_download_chunk_noop_without_client():
    """When download_cache_client=None, set_download_chunk is a no-op."""
    main = _make_redis_mock()
    cache = RedisObjectPartsCache(main)

    await cache.set_download_chunk("obj1", 1, 1, 0, b"data", ttl=300)

    # No write to main Redis — download cache is not configured
    main.setex.assert_not_awaited()


@pytest.mark.asyncio
async def test_set_download_chunk_uses_short_ttl():
    """Verify default 300s TTL."""
    dl = _make_redis_mock()
    cache = RedisObjectPartsCache(_make_redis_mock(), download_cache_client=dl)

    await cache.set_download_chunk("obj1", 1, 1, 0, b"data")

    call_args = dl.setex.call_args
    assert call_args[0][1] == 300


@pytest.mark.asyncio
async def test_delete_download_chunks_removes_keys():
    """Explicit cleanup deletes keys from download cache."""
    from hippius_s3.reader.types import ChunkPlanItem

    dl = _make_redis_mock()
    cache = RedisObjectPartsCache(_make_redis_mock(), download_cache_client=dl)

    items = [ChunkPlanItem(part_number=1, chunk_index=0), ChunkPlanItem(part_number=1, chunk_index=1)]
    await cache.delete_download_chunks("obj1", 1, items)

    dl.delete.assert_awaited_once()
    call_args = dl.delete.call_args[0]
    assert "obj:obj1:v:1:part:1:chunk:0" in call_args
    assert "obj:obj1:v:1:part:1:chunk:1" in call_args


@pytest.mark.asyncio
async def test_delete_download_chunks_noop_without_client():
    """download_cache_client=None -> no error."""
    from hippius_s3.reader.types import ChunkPlanItem

    cache = RedisObjectPartsCache(_make_redis_mock())
    items = [ChunkPlanItem(part_number=1, chunk_index=0)]

    await cache.delete_download_chunks("obj1", 1, items)
    # No exception raised
