"""Tests for concurrent-download coalescing in build_stream_context.

When multiple streamers hit a cache miss on the same part at the same time,
only ONE should actually enqueue a DownloadChainRequest. Others must wait
on pub/sub for the chunk to appear on the FS cache.

Implementation is a Redis `SET NX EX` lock per (object_id, version, part).
The downloader clears the lock after writing all chunks.
"""

from __future__ import annotations

from typing import Any
from unittest.mock import AsyncMock
from unittest.mock import MagicMock
from unittest.mock import patch

import pytest


OBJ = "11111111-2222-3333-4444-555555555555"


class _RedisStub:
    """In-memory stand-in for the subset of redis.asyncio we care about."""

    def __init__(self, held_keys: set[str] | None = None) -> None:
        self._held = set(held_keys or ())
        self.set_calls: list[tuple[str, Any, bool, int | None]] = []
        self.deleted: list[str] = []

    async def set(self, key: str, value: Any, *, nx: bool = False, ex: int | None = None) -> bool:
        self.set_calls.append((key, value, nx, ex))
        if nx and key in self._held:
            return False
        self._held.add(key)
        return True

    async def delete(self, key: str) -> int:
        self._held.discard(key)
        self.deleted.append(key)
        return 1


def _stub_config():
    cfg = MagicMock()
    cfg.substrate_url = ""
    cfg.download_coalesce_lock_ttl_seconds = 120
    return cfg


def _info() -> dict:
    return {
        "object_id": OBJ,
        "object_version": 1,
        "current_object_version": 1,
        "object_key": "key",
        "bucket_name": "b",
        "storage_version": 5,
        "size_bytes": 4096,
        "multipart": False,
        "ray_id": "ray-XYZ",
        "bucket_id": "deadbeef-0000-0000-0000-000000000000",
        "enc_suite_id": "hip-enc/aes256gcm",
        "kek_id": "kek-1",
        "wrapped_dek": b"w",
    }


def _mock_obj_cache(exist_results: list[bool]) -> MagicMock:
    cache = MagicMock()
    cache.chunks_exist_batch = AsyncMock(return_value=exist_results)
    return cache


def _mock_db_pool():
    db = MagicMock()
    db.fetch = AsyncMock(return_value=[])
    return db


def _mock_plan(num_chunks: int) -> list:
    from hippius_s3.reader.types import ChunkPlanItem

    return [ChunkPlanItem(part_number=1, chunk_index=i) for i in range(num_chunks)]


@pytest.mark.asyncio
@patch("hippius_s3.services.object_reader.get_bucket_kek_bytes", new=AsyncMock(return_value=b"k" * 32))
@patch("hippius_s3.services.object_reader.unwrap_dek", new=MagicMock(return_value=b"d" * 32))
@patch("hippius_s3.services.object_reader.CryptoService.is_supported_suite_id", new=MagicMock(return_value=True))
@patch("hippius_s3.services.object_reader.resolve_object_backends", new=AsyncMock(return_value=["arion"]))
@patch("hippius_s3.services.object_reader.enqueue_download_request", new_callable=AsyncMock)
@patch("hippius_s3.services.object_reader.build_chunk_plan", new_callable=AsyncMock)
@patch("hippius_s3.services.object_reader.read_parts_list", new_callable=AsyncMock)
@patch("hippius_s3.services.object_reader.get_config")
async def test_first_streamer_acquires_lock_and_enqueues(
    mock_cfg,
    mock_parts,
    mock_plan,
    mock_enqueue,
):
    from hippius_s3.services.object_reader import build_stream_context

    mock_cfg.return_value = _stub_config()
    mock_parts.return_value = [{"part_number": 1, "plain_size": 4096, "cid": None}]
    mock_plan.return_value = _mock_plan(2)

    redis = _RedisStub()
    obj_cache = _mock_obj_cache([False, False])

    ctx = await build_stream_context(
        db=_mock_db_pool(),
        redis=redis,
        obj_cache=obj_cache,
        info=_info(),
        rng=None,
        address="addr",
    )

    assert ctx.source == "pipeline"
    # Lock attempted with NX
    assert redis.set_calls, "expected coalescing lock attempt"
    key, value, nx, ex = redis.set_calls[0]
    assert key == f"download_in_progress:{OBJ}:v:1:part:1"
    assert value == "ray-XYZ"
    assert nx is True
    assert ex == 120
    # And the enqueue happened because we acquired the lock
    mock_enqueue.assert_awaited_once()


@pytest.mark.asyncio
@patch("hippius_s3.services.object_reader.get_bucket_kek_bytes", new=AsyncMock(return_value=b"k" * 32))
@patch("hippius_s3.services.object_reader.unwrap_dek", new=MagicMock(return_value=b"d" * 32))
@patch("hippius_s3.services.object_reader.CryptoService.is_supported_suite_id", new=MagicMock(return_value=True))
@patch("hippius_s3.services.object_reader.resolve_object_backends", new=AsyncMock(return_value=["arion"]))
@patch("hippius_s3.services.object_reader.enqueue_download_request", new_callable=AsyncMock)
@patch("hippius_s3.services.object_reader.build_chunk_plan", new_callable=AsyncMock)
@patch("hippius_s3.services.object_reader.read_parts_list", new_callable=AsyncMock)
@patch("hippius_s3.services.object_reader.get_config")
async def test_second_streamer_skips_enqueue_when_lock_held(
    mock_cfg,
    mock_parts,
    mock_plan,
    mock_enqueue,
):
    from hippius_s3.services.object_reader import build_stream_context

    mock_cfg.return_value = _stub_config()
    mock_parts.return_value = [{"part_number": 1, "plain_size": 4096, "cid": None}]
    mock_plan.return_value = _mock_plan(2)

    # Another streamer is already fetching part 1.
    held = {f"download_in_progress:{OBJ}:v:1:part:1"}
    redis = _RedisStub(held_keys=held)
    obj_cache = _mock_obj_cache([False, False])

    ctx = await build_stream_context(
        db=_mock_db_pool(),
        redis=redis,
        obj_cache=obj_cache,
        info=_info(),
        rng=None,
        address="addr",
    )

    assert ctx.source == "pipeline"
    # Lock attempted but NOT acquired → no enqueue
    assert len(redis.set_calls) == 1
    mock_enqueue.assert_not_awaited()


@pytest.mark.asyncio
@patch("hippius_s3.services.object_reader.get_bucket_kek_bytes", new=AsyncMock(return_value=b"k" * 32))
@patch("hippius_s3.services.object_reader.unwrap_dek", new=MagicMock(return_value=b"d" * 32))
@patch("hippius_s3.services.object_reader.CryptoService.is_supported_suite_id", new=MagicMock(return_value=True))
@patch("hippius_s3.services.object_reader.resolve_object_backends", new=AsyncMock(return_value=["arion"]))
@patch("hippius_s3.services.object_reader.enqueue_download_request", new_callable=AsyncMock)
@patch("hippius_s3.services.object_reader.build_chunk_plan", new_callable=AsyncMock)
@patch("hippius_s3.services.object_reader.read_parts_list", new_callable=AsyncMock)
@patch("hippius_s3.services.object_reader.get_config")
async def test_cache_hit_skips_lock_entirely(
    mock_cfg,
    mock_parts,
    mock_plan,
    mock_enqueue,
):
    """source="cache" must neither lock nor enqueue."""
    from hippius_s3.services.object_reader import build_stream_context

    mock_cfg.return_value = _stub_config()
    mock_parts.return_value = [{"part_number": 1, "plain_size": 4096, "cid": None}]
    mock_plan.return_value = _mock_plan(2)

    redis = _RedisStub()
    obj_cache = _mock_obj_cache([True, True])

    ctx = await build_stream_context(
        db=_mock_db_pool(),
        redis=redis,
        obj_cache=obj_cache,
        info=_info(),
        rng=None,
        address="addr",
    )

    assert ctx.source == "cache"
    assert redis.set_calls == []
    mock_enqueue.assert_not_awaited()
