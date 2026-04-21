# --- FS cache fallback tests ---


def _make_fs_store_mock(get_return=None):
    mock = MagicMock()
    mock.get_chunk = AsyncMock(return_value=get_return)
    return mock


@pytest.mark.asyncio
async def test_get_chunk_fs_hit_skips_redis():
    """FS cache hit -> Redis not checked (FS is checked first)."""
    main = _make_redis_mock(get_return=b"main-data")
    dl = _make_redis_mock(get_return=b"dl-data")
    fs = _make_fs_store_mock(get_return=b"fs-data")
    cache = RedisObjectPartsCache(main, download_cache_client=dl, fs_store=fs)

    result = await cache.get_chunk("obj1", 1, 1, 0)

    assert result == b"fs-data"
    fs.get_chunk.assert_awaited_once_with("obj1", 1, 1, 0)
    main.get.assert_not_awaited()
    dl.getex.assert_not_awaited()


@pytest.mark.asyncio
async def test_get_chunk_fs_miss_falls_to_main_redis():
    """FS miss -> checks main Redis."""
    main = _make_redis_mock(get_return=b"main-data")
    fs = _make_fs_store_mock(get_return=None)
    cache = RedisObjectPartsCache(main, fs_store=fs)

    result = await cache.get_chunk("obj1", 1, 1, 0)

    assert result == b"main-data"
    fs.get_chunk.assert_awaited_once()
    main.get.assert_awaited_once()


@pytest.mark.asyncio
async def test_get_chunk_fs_miss_redis_miss_falls_to_download_cache():
    """FS miss, main Redis miss -> checks download cache."""
    main = _make_redis_mock(get_return=None)
    dl = _make_redis_mock(get_return=b"dl-data")
    fs = _make_fs_store_mock(get_return=None)
    cache = RedisObjectPartsCache(main, download_cache_client=dl, fs_store=fs)

    result = await cache.get_chunk("obj1", 1, 1, 0)

    assert result == b"dl-data"
    fs.get_chunk.assert_awaited_once()
    main.get.assert_awaited_once()
    dl.getex.assert_awaited_once()


@pytest.mark.asyncio
async def test_get_chunk_all_three_miss():
    """All caches miss -> returns None."""
    main = _make_redis_mock(get_return=None)
    dl = _make_redis_mock(get_return=None)
    fs = _make_fs_store_mock(get_return=None)
    cache = RedisObjectPartsCache(main, download_cache_client=dl, fs_store=fs)

    result = await cache.get_chunk("obj1", 1, 1, 0)

    assert result is None
    fs.get_chunk.assert_awaited_once()
    main.get.assert_awaited_once()
    dl.getex.assert_awaited_once()


@pytest.mark.asyncio
async def test_get_chunk_no_fs_store_checks_redis_directly():
    """fs_store=None -> skips FS, checks Redis directly."""
    main = _make_redis_mock(get_return=b"main-data")
    cache = RedisObjectPartsCache(main)

    result = await cache.get_chunk("obj1", 1, 1, 0)

    assert result == b"main-data"
    main.get.assert_awaited_once()


@pytest.mark.asyncio
async def test_get_chunk_no_fs_no_download_cache_miss():
    """No FS, no download cache, main miss -> returns None."""
    main = _make_redis_mock(get_return=None)
    cache = RedisObjectPartsCache(main)

    result = await cache.get_chunk("obj1", 1, 1, 0)

    assert result is None
