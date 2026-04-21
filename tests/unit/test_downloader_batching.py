"""Tests for bounded batch processing in process_download_request.

Verifies the downloader:
- processes parts in bounded batches (prevents OOM with large objects),
- writes chunks to the FS store (not Redis),
- writes eager meta.json per part so reads can resolve per-chunk,
- publishes pub/sub notifications via obj_cache.notify_chunk.
"""

from __future__ import annotations

from pathlib import Path
from unittest.mock import AsyncMock
from unittest.mock import MagicMock
from unittest.mock import patch

import pytest

from hippius_s3.cache.fs_store import FileSystemPartsStore
from hippius_s3.queue import DownloadChainRequest
from hippius_s3.queue import PartChunkSpec
from hippius_s3.queue import PartToDownload
from hippius_s3.workers.downloader import process_download_request


OBJ = "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"


def _make_request(num_parts: int, chunks_per_part: int = 2) -> DownloadChainRequest:
    """Build a DownloadChainRequest with the specified number of parts and chunks."""
    parts = []
    for pn in range(1, num_parts + 1):
        specs = [PartChunkSpec(index=ci, cid=f"cid-{pn}-{ci}") for ci in range(chunks_per_part)]
        parts.append(PartToDownload(part_number=pn, chunks=specs))
    return DownloadChainRequest(
        object_id=OBJ,
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
    return config


def _make_mock_db_pool(
    identifier: str | None = "arion-id-123", *, size_bytes: int = 4096, chunk_size: int = 4 * 1024 * 1024
):
    """Create a mock DB pool whose queries return:
    - `get_chunk_backend_identifier`: a backend_identifier row
    - `SELECT size_bytes, chunk_size_bytes FROM parts ...`: the part's meta row
    """
    pool = MagicMock()
    conn = AsyncMock()

    async def _fetchrow(sql: str, *args):
        if "backend_identifier" in sql or "chunk_backend" in sql:
            return {"backend_identifier": identifier}
        if "size_bytes" in sql:
            return {"size_bytes": size_bytes, "chunk_size_bytes": chunk_size}
        return None

    conn.fetchrow = AsyncMock(side_effect=_fetchrow)
    pool.acquire = MagicMock(
        return_value=MagicMock(
            __aenter__=AsyncMock(return_value=conn),
            __aexit__=AsyncMock(return_value=False),
        )
    )
    return pool


def _make_mock_obj_cache():
    """Cache stub that only needs notify_chunk + redis.delete (legacy flag clear)."""
    cache = MagicMock()
    cache.notify_chunk = AsyncMock()
    cache.redis = MagicMock()
    cache.redis.delete = AsyncMock()
    return cache


@pytest.fixture
def fs_store(tmp_path: Path) -> FileSystemPartsStore:
    return FileSystemPartsStore(str(tmp_path))


@pytest.mark.asyncio
@patch("hippius_s3.workers.downloader.get_config")
@patch("hippius_s3.workers.downloader.get_metrics_collector")
async def test_all_parts_processed_with_batching(mock_metrics, mock_config, fs_store):
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
        fs_store=fs_store,
    )

    assert result is True
    assert fetch_fn.await_count == num_parts * 2
    assert obj_cache.notify_chunk.await_count == num_parts * 2
    # Every fetched chunk lands on FS
    for pn in range(1, num_parts + 1):
        for ci in range(2):
            assert await fs_store.chunk_exists(OBJ, 1, pn, ci)


@pytest.mark.asyncio
@patch("hippius_s3.workers.downloader.get_config")
@patch("hippius_s3.workers.downloader.get_metrics_collector")
async def test_eager_meta_written_per_part(mock_metrics, mock_config, fs_store):
    """Meta.json is written eagerly before chunks so partial fills are readable."""
    config = _make_mock_config(semaphore=4)
    mock_config.return_value = config
    mock_metrics.return_value = MagicMock()

    fetch_fn = AsyncMock(return_value=b"x" * 4096)
    db_pool = _make_mock_db_pool()
    obj_cache = _make_mock_obj_cache()
    request = _make_request(3, chunks_per_part=2)

    await process_download_request(
        request,
        backend_name="arion",
        fetch_fn=fetch_fn,
        db_pool=db_pool,
        obj_cache=obj_cache,
        fs_store=fs_store,
    )

    for pn in range(1, 4):
        meta = await fs_store.get_meta(OBJ, 1, pn)
        assert meta is not None
        assert meta["size_bytes"] == 4096


@pytest.mark.asyncio
@patch("hippius_s3.workers.downloader.get_config")
@patch("hippius_s3.workers.downloader.get_metrics_collector")
async def test_single_part_request(mock_metrics, mock_config, fs_store):
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
        fs_store=fs_store,
    )

    assert result is True
    assert fetch_fn.await_count == 1


@pytest.mark.asyncio
@patch("hippius_s3.workers.downloader.get_config")
@patch("hippius_s3.workers.downloader.get_metrics_collector")
async def test_large_object_with_many_parts(mock_metrics, mock_config, fs_store):
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
        fs_store=fs_store,
    )

    assert result is True
    assert fetch_fn.await_count == num_parts


@pytest.mark.asyncio
@patch("hippius_s3.workers.downloader.get_config")
@patch("hippius_s3.workers.downloader.get_metrics_collector")
async def test_cached_chunks_are_skipped(mock_metrics, mock_config, fs_store):
    """Chunks already on FS are not re-fetched."""
    config = _make_mock_config(semaphore=4)
    mock_config.return_value = config
    mock_metrics.return_value = MagicMock()

    fetch_fn = AsyncMock(return_value=b"x" * 4096)
    db_pool = _make_mock_db_pool()
    obj_cache = _make_mock_obj_cache()
    # Pre-cache parts 1-5 on FS
    for pn in range(1, 6):
        await fs_store.set_meta(OBJ, 1, pn, chunk_size=4096, num_chunks=2, size_bytes=8192)
        for ci in range(2):
            await fs_store.set_chunk(OBJ, 1, pn, ci, b"preexisting")

    request = _make_request(10)

    result = await process_download_request(
        request,
        backend_name="arion",
        fetch_fn=fetch_fn,
        db_pool=db_pool,
        obj_cache=obj_cache,
        fs_store=fs_store,
    )

    assert result is True
    # Only parts 6-10 should be fetched (5 parts × 2 chunks)
    assert fetch_fn.await_count == 5 * 2


@pytest.mark.asyncio
@patch("hippius_s3.workers.downloader.get_config")
@patch("hippius_s3.workers.downloader.get_metrics_collector")
async def test_no_backend_identifier_skips_chunk(mock_metrics, mock_config, fs_store):
    """Chunks without a backend identifier are skipped (backend doesn't hold them)."""
    config = _make_mock_config(semaphore=4)
    mock_config.return_value = config
    mock_metrics.return_value = MagicMock()

    fetch_fn = AsyncMock(return_value=b"x" * 4096)
    db_pool = _make_mock_db_pool(identifier=None)
    obj_cache = _make_mock_obj_cache()
    request = _make_request(5)

    result = await process_download_request(
        request,
        backend_name="arion",
        fetch_fn=fetch_fn,
        db_pool=db_pool,
        obj_cache=obj_cache,
        fs_store=fs_store,
    )

    assert result is True
    fetch_fn.assert_not_awaited()


@pytest.mark.asyncio
@patch("hippius_s3.workers.downloader.get_config")
@patch("hippius_s3.workers.downloader.get_metrics_collector")
async def test_fetch_failure_returns_false(mock_metrics, mock_config, fs_store):
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
        fs_store=fs_store,
    )

    assert result is False
    # Should have retried
    assert fetch_fn.await_count == 2


@pytest.mark.asyncio
@patch("hippius_s3.workers.downloader.get_config")
@patch("hippius_s3.workers.downloader.get_metrics_collector")
async def test_partial_failure_returns_false(mock_metrics, mock_config, fs_store):
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
        fs_store=fs_store,
    )

    assert result is False


@pytest.mark.asyncio
@patch("hippius_s3.workers.downloader.get_config")
@patch("hippius_s3.workers.downloader.get_metrics_collector")
async def test_empty_request_succeeds(mock_metrics, mock_config, fs_store):
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
        fs_store=fs_store,
    )

    assert result is True
    fetch_fn.assert_not_awaited()


@pytest.mark.asyncio
@patch("hippius_s3.workers.downloader.get_config")
@patch("hippius_s3.workers.downloader.get_metrics_collector")
async def test_notifications_sent_for_each_chunk(mock_metrics, mock_config, fs_store):
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
        fs_store=fs_store,
    )

    assert result is True
    assert obj_cache.notify_chunk.await_count == num_parts


@pytest.mark.asyncio
@patch("hippius_s3.workers.downloader.get_config")
@patch("hippius_s3.workers.downloader.get_metrics_collector")
async def test_semaphore_of_1_still_processes_all(mock_metrics, mock_config, fs_store):
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
        fs_store=fs_store,
    )

    assert result is True
    assert fetch_fn.await_count == num_parts


@pytest.mark.asyncio
@patch("hippius_s3.workers.downloader.get_config")
@patch("hippius_s3.workers.downloader.get_metrics_collector")
async def test_in_progress_flag_cleared_per_part(mock_metrics, mock_config, fs_store):
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
        fs_store=fs_store,
    )

    assert obj_cache.redis.delete.await_count == num_parts
