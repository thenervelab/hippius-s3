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

    def pipeline(self, transaction: bool = False) -> "_RedisPipelineStub":
        # RD-6: the coalesce lock is now acquired via a pipeline. The stub records each queued set
        # into set_calls (so existing assertions hold) and applies NX on execute().
        return _RedisPipelineStub(self)


class _RedisPipelineStub:
    def __init__(self, parent: "_RedisStub") -> None:
        self._parent = parent
        self._ops: list[tuple[str, Any, bool, int | None]] = []

    def set(self, key: str, value: Any, *, nx: bool = False, ex: int | None = None) -> "_RedisPipelineStub":
        self._ops.append((key, value, nx, ex))
        return self

    async def execute(self) -> list[Any]:
        results: list[Any] = []
        for key, value, nx, ex in self._ops:
            self._parent.set_calls.append((key, value, nx, ex))
            if nx and key in self._parent._held:
                results.append(None)
            else:
                self._parent._held.add(key)
                results.append(True)
        return results


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


@pytest.mark.asyncio
@patch("hippius_s3.services.object_reader.get_bucket_kek_bytes", new=AsyncMock(return_value=b"k" * 32))
@patch("hippius_s3.services.object_reader.unwrap_dek", new=MagicMock(return_value=b"d" * 32))
@patch("hippius_s3.services.object_reader.CryptoService.is_supported_suite_id", new=MagicMock(return_value=True))
@patch("hippius_s3.services.object_reader.resolve_object_backends", new=AsyncMock(return_value=["arion"]))
@patch("hippius_s3.services.object_reader.enqueue_download_request", new_callable=AsyncMock)
@patch("hippius_s3.services.object_reader.build_chunk_plan", new_callable=AsyncMock)
@patch("hippius_s3.services.object_reader.read_parts_list", new_callable=AsyncMock)
@patch("hippius_s3.services.object_reader.get_config")
async def test_arion_object_skips_per_part_cid_query(
    mock_cfg,
    mock_parts,
    mock_plan,
    mock_enqueue,
):
    """RQ-3: for a deterministically-addressed (Arion) object the downloader never reads spec.cid, so
    the per-part get_part_chunks_by_object_and_number query is skipped and specs are indices-only."""
    from hippius_s3.services.object_reader import build_stream_context

    mock_cfg.return_value = _stub_config()
    mock_parts.return_value = [{"part_number": 1, "plain_size": 4096, "cid": None}]
    mock_plan.return_value = _mock_plan(2)

    redis = _RedisStub()
    obj_cache = _mock_obj_cache([False, False])
    db = _mock_db_pool()  # db.fetch would serve the CID query; assert it is never awaited.

    ctx = await build_stream_context(
        db=db,
        redis=redis,
        obj_cache=obj_cache,
        info=_info(),
        rng=None,
        address="addr",
    )

    assert ctx.source == "pipeline"
    mock_enqueue.assert_awaited_once()
    db.fetch.assert_not_awaited()

    dcr = mock_enqueue.call_args.args[0]
    all_specs = [spec for part in dcr.chunks for spec in part.chunks]
    assert all_specs, "expected chunk specs on the enqueued request"
    assert all(spec.cid is None for spec in all_specs), "Arion specs must be indices-only (cid=None)"


class _LocatingFetcher:
    """A peer resolver that reports every part as held by `owner`."""

    def __init__(self, owner: str | None, unreplicated: bool) -> None:
        self._owner = owner
        self._unreplicated = unreplicated

    async def __call__(self, object_id: str, version: int, part_number: int, chunk_index: int) -> bytes | None:
        return None

    async def locate(self, object_id: str, version: int, part_number: int) -> tuple[str | None, bool]:
        return self._owner, self._unreplicated


def _tiered_obj_cache(tmp_path, fetcher: _LocatingFetcher, exist_results: list[bool]) -> MagicMock:
    from hippius_s3.cache.dual_fs_store import DualFileSystemPartsStore

    cache = _mock_obj_cache(exist_results)
    cache.fs = DualFileSystemPartsStore(str(tmp_path / "ssd"), str(tmp_path / "pool"), peer_fetch=fetcher)
    return cache


@pytest.mark.asyncio
@patch("hippius_s3.services.object_reader.get_bucket_kek_bytes", new=AsyncMock(return_value=b"k" * 32))
@patch("hippius_s3.services.object_reader.unwrap_dek", new=MagicMock(return_value=b"d" * 32))
@patch("hippius_s3.services.object_reader.CryptoService.is_supported_suite_id", new=MagicMock(return_value=True))
@patch("hippius_s3.services.object_reader.resolve_object_backends", new=AsyncMock(return_value=["arion"]))
@patch("hippius_s3.services.object_reader.enqueue_download_request", new_callable=AsyncMock)
@patch("hippius_s3.services.object_reader.build_chunk_plan", new_callable=AsyncMock)
@patch("hippius_s3.services.object_reader.read_parts_list", new_callable=AsyncMock)
@patch("hippius_s3.services.object_reader.get_config")
async def test_a_part_a_peer_holds_unreplicated_is_not_sent_to_the_downloader(
    mock_cfg,
    mock_parts,
    mock_plan,
    mock_enqueue,
    tmp_path,
):
    """A fresh part is on its ingest node's SSD alone; Arion gets it only after the pool does.

    The DownloadChainRequest would come back empty ~1s later. The peer tier serves the part on
    the way through wait_for_chunk, so the read stays `pipeline` but enqueues nothing — and
    takes no coalesce lock, so a later reader can still enqueue once the part is somewhere.
    """
    from hippius_s3.services.object_reader import build_stream_context

    mock_cfg.return_value = _stub_config()
    mock_parts.return_value = [{"part_number": 1, "plain_size": 4096, "cid": None}]
    mock_plan.return_value = _mock_plan(2)

    redis = _RedisStub()
    obj_cache = _tiered_obj_cache(tmp_path, _LocatingFetcher("node-b", True), [False, False])

    ctx = await build_stream_context(
        db=_mock_db_pool(),
        redis=redis,
        obj_cache=obj_cache,
        info=_info(),
        rng=None,
        address="addr",
    )

    assert ctx.source == "pipeline"
    mock_enqueue.assert_not_awaited()
    assert redis.set_calls == []


@pytest.mark.asyncio
@patch("hippius_s3.services.object_reader.get_bucket_kek_bytes", new=AsyncMock(return_value=b"k" * 32))
@patch("hippius_s3.services.object_reader.unwrap_dek", new=MagicMock(return_value=b"d" * 32))
@patch("hippius_s3.services.object_reader.CryptoService.is_supported_suite_id", new=MagicMock(return_value=True))
@patch("hippius_s3.services.object_reader.resolve_object_backends", new=AsyncMock(return_value=["arion"]))
@patch("hippius_s3.services.object_reader.enqueue_download_request", new_callable=AsyncMock)
@patch("hippius_s3.services.object_reader.build_chunk_plan", new_callable=AsyncMock)
@patch("hippius_s3.services.object_reader.read_parts_list", new_callable=AsyncMock)
@patch("hippius_s3.services.object_reader.get_config")
async def test_a_part_a_peer_holds_replicated_is_still_sent_to_the_downloader(
    mock_cfg,
    mock_parts,
    mock_plan,
    mock_enqueue,
    tmp_path,
):
    """Replicated means the pool (and so Arion) has it: the download is the normal repair path."""
    from hippius_s3.services.object_reader import build_stream_context

    mock_cfg.return_value = _stub_config()
    mock_parts.return_value = [{"part_number": 1, "plain_size": 4096, "cid": None}]
    mock_plan.return_value = _mock_plan(2)

    obj_cache = _tiered_obj_cache(tmp_path, _LocatingFetcher("node-b", False), [False, False])

    ctx = await build_stream_context(
        db=_mock_db_pool(),
        redis=_RedisStub(),
        obj_cache=obj_cache,
        info=_info(),
        rng=None,
        address="addr",
    )

    assert ctx.source == "pipeline"
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
async def test_an_unreplicated_flag_without_an_owner_never_suppresses_the_downloader(
    mock_cfg,
    mock_parts,
    mock_plan,
    mock_enqueue,
    tmp_path,
):
    """No reachable owner means nobody serves the part on the way through; the download stays."""
    from hippius_s3.services.object_reader import build_stream_context

    mock_cfg.return_value = _stub_config()
    mock_parts.return_value = [{"part_number": 1, "plain_size": 4096, "cid": None}]
    mock_plan.return_value = _mock_plan(2)

    obj_cache = _tiered_obj_cache(tmp_path, _LocatingFetcher(None, True), [False, False])

    ctx = await build_stream_context(
        db=_mock_db_pool(),
        redis=_RedisStub(),
        obj_cache=obj_cache,
        info=_info(),
        rng=None,
        address="addr",
    )

    assert ctx.source == "pipeline"
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
async def test_a_store_without_a_peer_tier_enqueues_exactly_as_before(
    mock_cfg,
    mock_parts,
    mock_plan,
    mock_enqueue,
):
    """HIPPIUS_PEER_FETCH_ENABLED=false: the facade has no tiered store, and nothing is skipped."""
    from hippius_s3.services.object_reader import build_stream_context

    mock_cfg.return_value = _stub_config()
    mock_parts.return_value = [{"part_number": 1, "plain_size": 4096, "cid": None}]
    mock_plan.return_value = _mock_plan(2)

    ctx = await build_stream_context(
        db=_mock_db_pool(),
        redis=_RedisStub(),
        obj_cache=_mock_obj_cache([False, False]),
        info=_info(),
        rng=None,
        address="addr",
    )

    assert ctx.source == "pipeline"
    mock_enqueue.assert_awaited_once()
