"""RD-5 / RD-1: the downloader batches per-chunk FS and DB lookups per part/request.

RD-5: existence is checked once per part via chunks_exist_batch (off-loop) instead of a synchronous
per-chunk fs_store.chunk_exists on the event loop.
RD-1: the backend identifier is resolved in one query per DownloadChainRequest into a map, instead of
a pool-acquire + 3-table-join fetchrow per chunk.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any
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


def _request(num_parts: int, chunks_per_part: int) -> DownloadChainRequest:
    parts = [
        PartToDownload(part_number=pn, chunks=[PartChunkSpec(index=ci, cid=None) for ci in range(chunks_per_part)])
        for pn in range(1, num_parts + 1)
    ]
    return DownloadChainRequest(
        object_id=OBJ,
        object_version=1,
        object_key="test/object.bin",
        bucket_name="test-bucket",
        object_storage_version=5,
        address="5Addr",
        subaccount="5Addr",
        substrate_url="http://test",
        size=num_parts * chunks_per_part * 4 * 1024 * 1024,
        multipart=num_parts > 1,
        chunks=parts,
    )


def _config(semaphore: int = 4) -> Any:
    c = MagicMock()
    c.downloader_semaphore = semaphore
    c.downloader_chunk_retries = 1
    c.downloader_retry_base_seconds = 0.0
    c.downloader_retry_jitter_seconds = 0.0
    return c


def _db_pool(identifier: str = "arion-id") -> Any:
    """DB pool that answers the per-part meta query (fetchrow), the per-request identifier batch
    (fetch), and the singular per-chunk identifier fallback (fetchrow)."""
    conn = AsyncMock()

    async def _fetchrow(sql: str, *args: Any) -> Any:
        if "size_bytes" in sql:
            return {"size_bytes": 4096, "chunk_size_bytes": 4 * 1024 * 1024}
        if "backend_identifier" in sql or "chunk_backend" in sql:
            return {"backend_identifier": identifier}
        return None

    async def _fetch(sql: str, *args: Any) -> Any:
        # RD-1 batch identifier query returns rows keyed by (part_number, chunk_index).
        if "backend_identifier" in sql or "chunk_backend" in sql:
            return []  # empty on purpose so the singular fallback is exercised where present
        return []

    conn.fetchrow = AsyncMock(side_effect=_fetchrow)
    conn.fetch = AsyncMock(side_effect=_fetch)
    pool = MagicMock()
    pool.acquire = MagicMock(
        return_value=MagicMock(__aenter__=AsyncMock(return_value=conn), __aexit__=AsyncMock(return_value=False))
    )
    return pool, conn


def _obj_cache() -> Any:
    cache = MagicMock()
    cache.notify_chunk = AsyncMock()
    cache.redis = MagicMock()
    cache.redis.eval = AsyncMock(return_value=1)
    return cache


@pytest.fixture
def fs_store(tmp_path: Path) -> FileSystemPartsStore:
    return FileSystemPartsStore(str(tmp_path))


@pytest.mark.asyncio
@patch("hippius_s3.workers.downloader.get_config")
@patch("hippius_s3.workers.downloader.get_metrics_collector")
async def test_rd5_batches_existence_and_skips_cached(mock_metrics: Any, mock_config: Any, fs_store: Any) -> None:
    mock_config.return_value = _config()
    mock_metrics.return_value = MagicMock()

    # Pre-cache part 1 / chunk 0 (meta + chunk) so it must be skipped.
    await fs_store.set_meta(OBJ, 1, 1, chunk_size=4 * 1024 * 1024, num_chunks=2, size_bytes=4096)
    await fs_store.set_chunk(OBJ, 1, 1, 0, b"cached-bytes")

    per_chunk_exist_calls = {"n": 0}
    orig_exists = fs_store.chunk_exists

    async def _counting_exists(*a: Any, **k: Any) -> Any:
        per_chunk_exist_calls["n"] += 1
        return await orig_exists(*a, **k)

    fs_store.chunk_exists = _counting_exists  # type: ignore[method-assign]

    fetch_fn = AsyncMock(return_value=b"y" * 4096)
    pool, _conn = _db_pool()
    result = await process_download_request(
        _request(1, 2),
        backend_name="arion",
        fetch_fn=fetch_fn,
        db_pool=pool,
        obj_cache=_obj_cache(),
        fs_store=fs_store,
    )

    assert result is True
    # RD-5: existence is resolved by the per-part batch, not a per-chunk chunk_exists on the loop.
    assert per_chunk_exist_calls["n"] == 0, "downloader must not call per-chunk chunk_exists during fetch"
    # Only the missing chunk (index 1) is fetched; the pre-cached one is skipped.
    assert fetch_fn.await_count == 1
