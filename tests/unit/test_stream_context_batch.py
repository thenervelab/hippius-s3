"""Tests for build_stream_context batched chunk existence checks.

Validates that the refactored build_stream_context uses a single batched
chunks_exist_batch call instead of sequential per-chunk checks.
"""

from __future__ import annotations

from unittest.mock import AsyncMock
from unittest.mock import MagicMock
from unittest.mock import patch

import pytest


class FakeObjCache:
    """Minimal cache mock for build_stream_context tests."""

    def __init__(self, exist_results: list[bool]) -> None:
        self._exist_results = exist_results
        self.chunks_exist_batch = AsyncMock(return_value=exist_results)
        self.chunk_exists = AsyncMock()

    def build_chunk_key(self, object_id: str, object_version: int, part_number: int, chunk_index: int) -> str:
        return f"obj:{object_id}:v:{object_version}:part:{part_number}:chunk:{chunk_index}"


class FakeDB:
    def __init__(self) -> None:
        self.fetch = AsyncMock(return_value=[])
        self.fetchrow = AsyncMock(return_value=None)


def _make_info(object_id: str = "obj-1", object_version: int = 1) -> dict:
    return {
        "object_id": object_id,
        "object_version": object_version,
        "current_object_version": object_version,
        "storage_version": 5,
        "bucket_id": "bucket-1",
        "upload_id": "upload-1",
        "object_key": "test.bin",
        "bucket_name": "test-bucket",
        "size_bytes": 8000,
        "multipart": False,
        "enc_suite_id": "hip-enc/aes256gcm",
        "kek_id": "kek-1",
        "wrapped_dek": b"\x00" * 32,
    }


def _make_plan_items(count: int) -> list:
    from hippius_s3.reader.types import ChunkPlanItem

    return [ChunkPlanItem(part_number=1, chunk_index=i) for i in range(count)]


@pytest.mark.asyncio
@patch("hippius_s3.services.object_reader.build_chunk_plan")
@patch("hippius_s3.services.object_reader.read_parts_list")
@patch("hippius_s3.services.object_reader.require_supported_storage_version", return_value=5)
@patch("hippius_s3.services.object_reader.get_config")
@patch("hippius_s3.services.object_reader.unwrap_dek", return_value=b"\x01" * 32)
@patch("hippius_s3.services.object_reader.get_bucket_kek_bytes", new_callable=AsyncMock, return_value=b"\x02" * 32)
@patch("hippius_s3.services.object_reader.CryptoService")
async def test_all_cached_uses_batch_and_sets_source_cache(
    mock_crypto, mock_kek, mock_unwrap, mock_config, mock_storage, mock_read_parts, mock_plan
):
    plan_items = _make_plan_items(3)
    mock_plan.return_value = plan_items
    mock_read_parts.return_value = [{"part_number": 1, "cid": "cid1"}]
    mock_crypto.is_supported_suite_id.return_value = True
    mock_config.return_value = MagicMock()

    obj_cache = FakeObjCache([True, True, True])
    db = FakeDB()

    from hippius_s3.services.object_reader import build_stream_context

    ctx = await build_stream_context(db, None, obj_cache, _make_info(), rng=None, address="addr1")

    assert ctx.source == "cache"
    obj_cache.chunks_exist_batch.assert_awaited_once()
    # Should NOT have fallen back to individual chunk_exists
    obj_cache.chunk_exists.assert_not_awaited()


@pytest.mark.asyncio
@patch("hippius_s3.services.object_reader.enqueue_download_request", new_callable=AsyncMock)
@patch("hippius_s3.services.object_reader.resolve_object_backends", new_callable=AsyncMock, return_value=["ipfs"])
@patch("hippius_s3.services.object_reader.build_chunk_plan")
@patch("hippius_s3.services.object_reader.read_parts_list")
@patch("hippius_s3.services.object_reader.require_supported_storage_version", return_value=5)
@patch("hippius_s3.services.object_reader.get_config")
@patch("hippius_s3.services.object_reader.unwrap_dek", return_value=b"\x01" * 32)
@patch("hippius_s3.services.object_reader.get_bucket_kek_bytes", new_callable=AsyncMock, return_value=b"\x02" * 32)
@patch("hippius_s3.services.object_reader.CryptoService")
async def test_partial_cached_sets_source_pipeline(
    mock_crypto,
    mock_kek,
    mock_unwrap,
    mock_config,
    mock_storage,
    mock_read_parts,
    mock_plan,
    mock_resolve,
    mock_enqueue,
):
    plan_items = _make_plan_items(3)
    mock_plan.return_value = plan_items
    mock_read_parts.return_value = [{"part_number": 1, "cid": "cid1"}]
    mock_crypto.is_supported_suite_id.return_value = True
    mock_config.return_value = MagicMock(substrate_url="ws://localhost")

    # First and third cached, second missing
    obj_cache = FakeObjCache([True, False, True])
    db = FakeDB()

    from hippius_s3.services.object_reader import build_stream_context

    ctx = await build_stream_context(db, None, obj_cache, _make_info(), rng=None, address="addr1")

    assert ctx.source == "pipeline"
    obj_cache.chunks_exist_batch.assert_awaited_once()
    # Download should have been enqueued for the missing chunk
    mock_enqueue.assert_awaited_once()


@pytest.mark.asyncio
@patch("hippius_s3.services.object_reader.enqueue_download_request", new_callable=AsyncMock)
@patch("hippius_s3.services.object_reader.resolve_object_backends", new_callable=AsyncMock, return_value=["ipfs"])
@patch("hippius_s3.services.object_reader.build_chunk_plan")
@patch("hippius_s3.services.object_reader.read_parts_list")
@patch("hippius_s3.services.object_reader.require_supported_storage_version", return_value=5)
@patch("hippius_s3.services.object_reader.get_config")
@patch("hippius_s3.services.object_reader.unwrap_dek", return_value=b"\x01" * 32)
@patch("hippius_s3.services.object_reader.get_bucket_kek_bytes", new_callable=AsyncMock, return_value=b"\x02" * 32)
@patch("hippius_s3.services.object_reader.CryptoService")
async def test_none_cached_sets_source_pipeline(
    mock_crypto,
    mock_kek,
    mock_unwrap,
    mock_config,
    mock_storage,
    mock_read_parts,
    mock_plan,
    mock_resolve,
    mock_enqueue,
):
    plan_items = _make_plan_items(4)
    mock_plan.return_value = plan_items
    mock_read_parts.return_value = [{"part_number": 1, "cid": "cid1"}]
    mock_crypto.is_supported_suite_id.return_value = True
    mock_config.return_value = MagicMock(substrate_url="ws://localhost")

    obj_cache = FakeObjCache([False, False, False, False])
    db = FakeDB()

    from hippius_s3.services.object_reader import build_stream_context

    ctx = await build_stream_context(db, None, obj_cache, _make_info(), rng=None, address="addr1")

    assert ctx.source == "pipeline"
    mock_enqueue.assert_awaited_once()


@pytest.mark.asyncio
@patch("hippius_s3.services.object_reader.build_chunk_plan")
@patch("hippius_s3.services.object_reader.read_parts_list")
@patch("hippius_s3.services.object_reader.require_supported_storage_version", return_value=5)
@patch("hippius_s3.services.object_reader.get_config")
@patch("hippius_s3.services.object_reader.unwrap_dek", return_value=b"\x01" * 32)
@patch("hippius_s3.services.object_reader.get_bucket_kek_bytes", new_callable=AsyncMock, return_value=b"\x02" * 32)
@patch("hippius_s3.services.object_reader.CryptoService")
async def test_empty_plan_is_cache_source(
    mock_crypto, mock_kek, mock_unwrap, mock_config, mock_storage, mock_read_parts, mock_plan
):
    mock_plan.return_value = []
    mock_read_parts.return_value = []
    mock_crypto.is_supported_suite_id.return_value = True
    mock_config.return_value = MagicMock()

    obj_cache = FakeObjCache([])
    db = FakeDB()

    from hippius_s3.services.object_reader import build_stream_context

    ctx = await build_stream_context(db, None, obj_cache, _make_info(), rng=None, address="addr1")

    assert ctx.source == "cache"
    obj_cache.chunks_exist_batch.assert_awaited_once_with("obj-1", 1, [])
