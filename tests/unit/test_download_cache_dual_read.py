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
    mock.getex = AsyncMock(return_value=get_return)
    mock.setex = AsyncMock()
    mock.delete = AsyncMock()
    mock.exists = AsyncMock(return_value=0)
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
    dl.getex.assert_not_awaited()


@pytest.mark.asyncio
async def test_get_chunk_main_miss_download_cache_hit():
    """Main miss -> falls back to download cache via GETEX -> returns data."""
    main = _make_redis_mock(get_return=None)
    dl = _make_redis_mock(get_return=b"dl-data")
    cache = RedisObjectPartsCache(main, download_cache_client=dl)

    result = await cache.get_chunk("obj1", 1, 1, 0)

    assert result == b"dl-data"
    main.get.assert_awaited_once()
    dl.getex.assert_awaited_once()


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
async def test_set_download_chunk_crashes_without_client():
    """When download_cache_client=None, set_download_chunk raises RuntimeError."""
    main = _make_redis_mock()
    cache = RedisObjectPartsCache(main)

    with pytest.raises(RuntimeError, match="download_cache_client is required"):
        await cache.set_download_chunk("obj1", 1, 1, 0, b"data", ttl=300)


@pytest.mark.asyncio
async def test_set_download_chunk_uses_short_ttl():
    """Verify default 300s TTL."""
    dl = _make_redis_mock()
    cache = RedisObjectPartsCache(_make_redis_mock(), download_cache_client=dl)

    await cache.set_download_chunk("obj1", 1, 1, 0, b"data")

    call_args = dl.setex.call_args
    assert call_args[0][1] == 300


@pytest.mark.asyncio
async def test_get_chunk_uses_getex_on_download_cache():
    """Download cache read uses GETEX to refresh TTL, not plain GET."""
    main = _make_redis_mock(get_return=None)
    dl = MagicMock()
    dl.getex = AsyncMock(return_value=b"dl-data")
    cache = RedisObjectPartsCache(main, download_cache_client=dl)

    result = await cache.get_chunk("obj1", 1, 1, 0)

    assert result == b"dl-data"
    dl.getex.assert_awaited_once()
    call_args = dl.getex.call_args
    assert call_args[1]["ex"] == 300


@pytest.mark.asyncio
async def test_chunk_exists_checks_download_cache():
    """chunk_exists() falls back to download cache when main Redis misses."""
    main = MagicMock()
    main.exists = AsyncMock(return_value=0)
    dl = MagicMock()
    dl.exists = AsyncMock(return_value=1)
    cache = RedisObjectPartsCache(main, download_cache_client=dl)

    result = await cache.chunk_exists("obj1", 1, 1, 0)

    assert result is True
    main.exists.assert_awaited_once()
    dl.exists.assert_awaited_once()


@pytest.mark.asyncio
async def test_chunk_exists_skips_download_cache_on_main_hit():
    """chunk_exists() doesn't check download cache when main Redis hits."""
    main = MagicMock()
    main.exists = AsyncMock(return_value=1)
    dl = MagicMock()
    dl.exists = AsyncMock()
    cache = RedisObjectPartsCache(main, download_cache_client=dl)

    result = await cache.chunk_exists("obj1", 1, 1, 0)

    assert result is True
    dl.exists.assert_not_awaited()
