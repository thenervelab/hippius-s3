"""Tests for bounded batch processing in process_download_request.

Verifies that process_download_request processes parts in bounded batches
instead of creating all tasks upfront, preventing OOM with large objects.
"""
from __future__ import annotations

import asyncio
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from hippius_s3.queue import DownloadChainRequest, PartChunkSpec, PartToDownload
from hippius_s3.workers.downloader import process_download_request


def _make_request(num_parts: int, chunks_per_part: int = 2) -> DownloadChainRequest:
    """Build a DownloadChainRequest with the specified number of parts and chunks."""
    parts = []
    for pn in range(1, num_parts + 1):
        specs = [PartChunkSpec(index=ci, cid=f"cid-{pn}-{ci}") for ci in range(chunks_per_part)]
        parts.append(PartToDownload(part_number=pn, chunks=specs))
    return DownloadChainRequest(
        object_id="aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee",
        object_version=1,
        object_key="test/object.bin",
        bucket_name="test-bucket",
        object_storage_version=5,
        address="5TestAddr",
        subaccount="5TestAddr",
        subaccount_seed_phrase="",
        substrate_url="http://test",
        size=num_parts * chunks_per_part * 4 * 1024 * 1024,
        multipart=num_parts > 1,
        chunks=parts,
    )


def _make_mock_config(semaphore: int = 4, retries: int = 1):
    config = MagicMock()
    config.downloader_semaphore = semaphore
    config.downloader_chunk_retries = retries
    config.downloader_retry_base_seconds = 0.0
    config.downloader_retry_jitter_seconds = 0.0
    config.download_cache_ttl_seconds = 300
    return config


def _make_mock_db_pool(identifier: str = "arion-id-123"):
    """Create a mock DB pool that returns a backend identifier for all chunks."""
    pool = MagicMock()
    conn = AsyncMock()
    conn.fetchrow = AsyncMock(return_value={"backend_identifier": identifier})
    pool.acquire = MagicMock(
        return_value=MagicMock(
            __aenter__=AsyncMock(return_value=conn),
            __aexit__=AsyncMock(return_value=False),
        )
    )
    return pool


def _make_mock_db_pool_no_identifier():
    """Create a mock DB pool that returns no backend identifier (chunk not on this backend)."""
    pool = MagicMock()
    conn = AsyncMock()
    conn.fetchrow = AsyncMock(return_value={"backend_identifier": None})
    pool.acquire = MagicMock(
        return_value=MagicMock(
            __aenter__=AsyncMock(return_value=conn),
            __aexit__=AsyncMock(return_value=False),
        )
    )
    return pool


def _make_mock_obj_cache(cached_keys: set[str] | None = None):
    """Create a mock obj_cache matching RedisObjectPartsCache's surface used here."""
    cache = MagicMock()
    cached = cached_keys or set()

    async def chunks_exist_batch(object_id, object_version, checks):
        return [
            f"obj:{object_id}:v:{object_version}:part:{pn}:chunk:{ci}" in cached
            for pn, ci in checks
        ]

    cache.chunks_exist_batch = AsyncMock(side_effect=chunks_exist_batch)
    cache.set_download_chunk = AsyncMock()
    cache.notify_chunk = AsyncMock()
    cache.redis = MagicMock()
    cache.redis.delete = AsyncMock()
    return cache


@pytest.mark.asyncio
@patch("hippius_s3.workers.downloader.get_config")
@patch("hippius_s3.workers.downloader.get_metrics_collector")
async def test_all_parts_processed_with_batching(mock_metrics, mock_config):
    """All parts/chunks are processed correctly with bounded batching."""
    num_parts = 20
    config = _make_mock_config(semaphore=4)
    mock_config.return_value = config
    mock_metrics.return_value = MagicMock()

    fetch_fn = AsyncMock(return_value=b"x" * 4096)
    db_pool = _make_mock_db_pool()
    obj_cache = _make_mock_obj_cache()
    request = _make_request(num_parts)

    result = await process_download_request(
        request,
        backend_name="arion",
        fetch_fn=fetch_fn,
        db_pool=db_pool,
        obj_cache=obj_cache,
    )

    assert result is True
    assert fetch_fn.await_count == num_parts * 2
    assert obj_cache.set_download_chunk.await_count == num_parts * 2
    assert obj_cache.notify_chunk.await_count == num_parts * 2


@pytest.mark.asyncio
@patch("hippius_s3.workers.downloader.get_config")
@patch("hippius_s3.workers.downloader.get_metrics_collector")
async def test_single_part_request(mock_metrics, mock_config):
    """Single-part request works correctly."""
    config = _make_mock_config(semaphore=4)
    mock_config.return_value = config
    mock_metrics.return_value = MagicMock()

    fetch_fn = AsyncMock(return_value=b"x" * 4096)
    db_pool = _make_mock_db_pool()
    obj_cache = _make_mock_obj_cache()
    request = _make_request(1, chunks_per_part=1)

    result = await process_download_request(
        request,
        backend_name="arion",
        fetch_fn=fetch_fn,
        db_pool=db_pool,
        obj_cache=obj_cache,
    )

    assert result is True
    assert fetch_fn.await_count == 1


@pytest.mark.asyncio
@patch("hippius_s3.workers.downloader.get_config")
@patch("hippius_s3.workers.downloader.get_metrics_collector")
async def test_large_object_with_many_parts(mock_metrics, mock_config):
    """Large object (100+ parts) is processed correctly in batches."""
    num_parts = 100
    config = _make_mock_config(semaphore=8)
    mock_config.return_value = config
    mock_metrics.return_value = MagicMock()

    fetch_fn = AsyncMock(return_value=b"x" * 4096)
    db_pool = _make_mock_db_pool()
    obj_cache = _make_mock_obj_cache()
    request = _make_request(num_parts, chunks_per_part=1)

    result = await process_download_request(
        request,
        backend_name="arion",
        fetch_fn=fetch_fn,
        db_pool=db_pool,
        obj_cache=obj_cache,
    )

    assert result is True
    assert fetch_fn.await_count == num_parts


@pytest.mark.asyncio
@patch("hippius_s3.workers.downloader.get_config")
@patch("hippius_s3.workers.downloader.get_metrics_collector")
async def test_cached_chunks_are_skipped(mock_metrics, mock_config):
    """Chunks already in cache are not re-fetched."""
    config = _make_mock_config(semaphore=4)
    mock_config.return_value = config
    mock_metrics.return_value = MagicMock()

    fetch_fn = AsyncMock(return_value=b"x" * 4096)
    db_pool = _make_mock_db_pool()
    # Pre-cache parts 1-5
    cached = set()
    for pn in range(1, 6):
        for ci in range(2):
            cached.add(f"obj:aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee:v:1:part:{pn}:chunk:{ci}")
    obj_cache = _make_mock_obj_cache(cached_keys=cached)
    request = _make_request(10)

    result = await process_download_request(
        request,
        backend_name="arion",
        fetch_fn=fetch_fn,
        db_pool=db_pool,
        obj_cache=obj_cache,
    )

    assert result is True
    # Only parts 6-10 should be fetched (5 parts × 2 chunks)
    assert fetch_fn.await_count == 5 * 2


@pytest.mark.asyncio
@patch("hippius_s3.workers.downloader.get_config")
@patch("hippius_s3.workers.downloader.get_metrics_collector")
async def test_no_backend_identifier_skips_chunk(mock_metrics, mock_config):
    """Chunks without a backend identifier are skipped (backend doesn't hold them)."""
    config = _make_mock_config(semaphore=4)
    mock_config.return_value = config
    mock_metrics.return_value = MagicMock()

    fetch_fn = AsyncMock(return_value=b"x" * 4096)
    db_pool = _make_mock_db_pool_no_identifier()
    obj_cache = _make_mock_obj_cache()
    request = _make_request(5)

    result = await process_download_request(
        request,
        backend_name="arion",
        fetch_fn=fetch_fn,
        db_pool=db_pool,
        obj_cache=obj_cache,
    )

    assert result is True
    fetch_fn.assert_not_awaited()


@pytest.mark.asyncio
@patch("hippius_s3.workers.downloader.get_config")
@patch("hippius_s3.workers.downloader.get_metrics_collector")
async def test_fetch_failure_returns_false(mock_metrics, mock_config):
    """When a chunk fetch fails after all retries, the request returns False."""
    config = _make_mock_config(semaphore=4, retries=2)
    mock_config.return_value = config
    mock_metrics.return_value = MagicMock()

    fetch_fn = AsyncMock(side_effect=Exception("backend down"))
    db_pool = _make_mock_db_pool()
    obj_cache = _make_mock_obj_cache()
    request = _make_request(1, chunks_per_part=1)

    result = await process_download_request(
        request,
        backend_name="arion",
        fetch_fn=fetch_fn,
        db_pool=db_pool,
        obj_cache=obj_cache,
    )

    assert result is False
    # Should have retried
    assert fetch_fn.await_count == 2


@pytest.mark.asyncio
@patch("hippius_s3.workers.downloader.get_config")
@patch("hippius_s3.workers.downloader.get_metrics_collector")
async def test_partial_failure_returns_false(mock_metrics, mock_config):
    """If some parts succeed and some fail, result is False."""
    config = _make_mock_config(semaphore=4, retries=1)
    mock_config.return_value = config
    mock_metrics.return_value = MagicMock()

    call_count = 0

    async def flaky_fetch(identifier, account):
        nonlocal call_count
        call_count += 1
        if call_count <= 2:
            return b"x" * 4096
        raise Exception("backend down")

    fetch_fn = flaky_fetch
    db_pool = _make_mock_db_pool()
    obj_cache = _make_mock_obj_cache()
    request = _make_request(3, chunks_per_part=1)

    result = await process_download_request(
        request,
        backend_name="arion",
        fetch_fn=fetch_fn,
        db_pool=db_pool,
        obj_cache=obj_cache,
    )

    assert result is False


@pytest.mark.asyncio
@patch("hippius_s3.workers.downloader.get_config")
@patch("hippius_s3.workers.downloader.get_metrics_collector")
async def test_empty_request_succeeds(mock_metrics, mock_config):
    """Request with no parts completes successfully."""
    config = _make_mock_config(semaphore=4)
    mock_config.return_value = config
    mock_metrics.return_value = MagicMock()

    fetch_fn = AsyncMock(return_value=b"x" * 4096)
    db_pool = _make_mock_db_pool()
    obj_cache = _make_mock_obj_cache()
    request = _make_request(0)

    result = await process_download_request(
        request,
        backend_name="arion",
        fetch_fn=fetch_fn,
        db_pool=db_pool,
        obj_cache=obj_cache,
    )

    assert result is True
    fetch_fn.assert_not_awaited()


@pytest.mark.asyncio
@patch("hippius_s3.workers.downloader.get_config")
@patch("hippius_s3.workers.downloader.get_metrics_collector")
async def test_notifications_sent_for_each_chunk(mock_metrics, mock_config):
    """Each successfully fetched chunk triggers a pub/sub notification."""
    num_parts = 5
    config = _make_mock_config(semaphore=4)
    mock_config.return_value = config
    mock_metrics.return_value = MagicMock()

    fetch_fn = AsyncMock(return_value=b"x" * 4096)
    db_pool = _make_mock_db_pool()
    obj_cache = _make_mock_obj_cache()
    request = _make_request(num_parts, chunks_per_part=1)

    result = await process_download_request(
        request,
        backend_name="arion",
        fetch_fn=fetch_fn,
        db_pool=db_pool,
        obj_cache=obj_cache,
    )

    assert result is True
    assert obj_cache.notify_chunk.await_count == num_parts


@pytest.mark.asyncio
@patch("hippius_s3.workers.downloader.get_config")
@patch("hippius_s3.workers.downloader.get_metrics_collector")
async def test_semaphore_of_1_still_processes_all(mock_metrics, mock_config):
    """With semaphore=1, all parts are still processed (just slower)."""
    num_parts = 10
    config = _make_mock_config(semaphore=1)
    mock_config.return_value = config
    mock_metrics.return_value = MagicMock()

    fetch_fn = AsyncMock(return_value=b"x" * 4096)
    db_pool = _make_mock_db_pool()
    obj_cache = _make_mock_obj_cache()
    request = _make_request(num_parts, chunks_per_part=1)

    result = await process_download_request(
        request,
        backend_name="arion",
        fetch_fn=fetch_fn,
        db_pool=db_pool,
        obj_cache=obj_cache,
    )

    assert result is True
    assert fetch_fn.await_count == num_parts


@pytest.mark.asyncio
@patch("hippius_s3.workers.downloader.get_config")
@patch("hippius_s3.workers.downloader.get_metrics_collector")
async def test_peak_concurrency_bounded_by_semaphore(mock_metrics, mock_config):
    """fetch_fn is never invoked more than `config.downloader_semaphore` times concurrently."""
    semaphore = 4
    num_parts = 20
    config = _make_mock_config(semaphore=semaphore)
    mock_config.return_value = config
    mock_metrics.return_value = MagicMock()

    current = 0
    peak = 0
    lock = asyncio.Lock()

    async def tracking_fetch(identifier, account):
        nonlocal current, peak
        async with lock:
            current += 1
            if current > peak:
                peak = current
        try:
            await asyncio.sleep(0.005)
            return b"x" * 4096
        finally:
            async with lock:
                current -= 1

    db_pool = _make_mock_db_pool()
    obj_cache = _make_mock_obj_cache()
    request = _make_request(num_parts, chunks_per_part=1)

    result = await process_download_request(
        request,
        backend_name="arion",
        fetch_fn=tracking_fetch,
        db_pool=db_pool,
        obj_cache=obj_cache,
    )

    assert result is True
    assert peak <= semaphore
    assert peak >= 1


@pytest.mark.asyncio
@patch("hippius_s3.workers.downloader.get_config")
@patch("hippius_s3.workers.downloader.get_metrics_collector")
async def test_chunks_exist_batch_called_once(mock_metrics, mock_config):
    """Cache existence is checked via one pipelined call, not per-chunk."""
    config = _make_mock_config(semaphore=4)
    mock_config.return_value = config
    mock_metrics.return_value = MagicMock()

    fetch_fn = AsyncMock(return_value=b"x" * 4096)
    db_pool = _make_mock_db_pool()
    obj_cache = _make_mock_obj_cache()
    request = _make_request(50, chunks_per_part=2)

    await process_download_request(
        request,
        backend_name="arion",
        fetch_fn=fetch_fn,
        db_pool=db_pool,
        obj_cache=obj_cache,
    )

    assert obj_cache.chunks_exist_batch.await_count == 1


@pytest.mark.asyncio
@patch("hippius_s3.workers.downloader.get_config")
@patch("hippius_s3.workers.downloader.get_metrics_collector")
async def test_in_progress_flag_cleared_per_part(mock_metrics, mock_config):
    """The download_in_progress flag is cleared for each part after processing."""
    num_parts = 3
    config = _make_mock_config(semaphore=4)
    mock_config.return_value = config
    mock_metrics.return_value = MagicMock()

    fetch_fn = AsyncMock(return_value=b"x" * 4096)
    db_pool = _make_mock_db_pool()
    obj_cache = _make_mock_obj_cache()
    request = _make_request(num_parts, chunks_per_part=1)

    await process_download_request(
        request,
        backend_name="arion",
        fetch_fn=fetch_fn,
        db_pool=db_pool,
        obj_cache=obj_cache,
    )

    assert obj_cache.redis.delete.await_count == num_parts
