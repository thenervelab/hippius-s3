"""F1: `ensure_part_download_enqueued` is the streamer's idempotent per-part re-enqueue helper.

It shares the cold-read path's coalesce lock, so the FIRST call for a part acquires the
`download_in_progress:{oid}:v:{v}:part:{pn}` lock and enqueues ONE DownloadChainRequest covering the
part's needed chunks; a SECOND call while the lock is held is a no-op (another producer is already
fetching). This is what makes a mid-stream miss safe to hit repeatedly without stacking downloads.
"""

from __future__ import annotations

from typing import Any
from unittest.mock import AsyncMock
from unittest.mock import MagicMock
from unittest.mock import patch

import pytest


OBJ = "11111111-2222-3333-4444-555555555555"


class _RedisStub:
    def __init__(self, held_keys: set[str] | None = None) -> None:
        self._held = set(held_keys or ())
        self.set_calls: list[tuple[str, Any, bool, int | None]] = []

    def pipeline(self, transaction: bool = False) -> "_RedisPipelineStub":
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


def _cfg() -> MagicMock:
    cfg = MagicMock()
    cfg.substrate_url = ""
    cfg.download_coalesce_lock_ttl_seconds = 120
    cfg.cache_ttl_seconds = 3600
    return cfg


def _info() -> dict:
    return {
        "object_id": OBJ,
        "object_key": "key",
        "bucket_name": "b",
        "size_bytes": 8192,
        "multipart": True,
        "ray_id": "ray-XYZ",
    }


@pytest.mark.asyncio
@patch("hippius_s3.services.object_reader.resolve_object_backends", new=AsyncMock(return_value=["arion"]))
@patch("hippius_s3.services.object_reader.enqueue_download_request", new_callable=AsyncMock)
async def test_first_call_locks_and_enqueues_whole_part(mock_enqueue: AsyncMock) -> None:
    from hippius_s3.services.object_reader import ensure_part_download_enqueued

    redis = _RedisStub()
    await ensure_part_download_enqueued(
        db=MagicMock(),
        redis=redis,
        info=_info(),
        object_version=1,
        storage_version=5,
        part_number=2,
        chunk_indices={0, 1},
        address="addr",
        cfg=_cfg(),
    )

    key, value, nx, ex = redis.set_calls[0]
    assert key == f"download_in_progress:{OBJ}:v:1:part:2"
    assert value == "ray-XYZ"
    assert nx is True and ex == 120
    mock_enqueue.assert_awaited_once()

    dcr = mock_enqueue.call_args.args[0]
    assert [p.part_number for p in dcr.chunks] == [2]
    assert sorted(spec.index for spec in dcr.chunks[0].chunks) == [0, 1], "the whole part's chunks are enqueued"


@pytest.mark.asyncio
@patch("hippius_s3.services.object_reader.resolve_object_backends", new=AsyncMock(return_value=["arion"]))
@patch("hippius_s3.services.object_reader.enqueue_download_request", new_callable=AsyncMock)
async def test_second_call_with_lock_held_is_noop(mock_enqueue: AsyncMock) -> None:
    from hippius_s3.services.object_reader import ensure_part_download_enqueued

    redis = _RedisStub(held_keys={f"download_in_progress:{OBJ}:v:1:part:2"})
    await ensure_part_download_enqueued(
        db=MagicMock(),
        redis=redis,
        info=_info(),
        object_version=1,
        storage_version=5,
        part_number=2,
        chunk_indices={0, 1},
        address="addr",
        cfg=_cfg(),
    )

    assert len(redis.set_calls) == 1, "lock attempted once"
    # Lock held by another producer -> no duplicate enqueue.
    mock_enqueue.assert_not_awaited()


@pytest.mark.asyncio
@patch("hippius_s3.services.object_reader.resolve_object_backends", new=AsyncMock(return_value=["arion"]))
@patch("hippius_s3.services.object_reader.enqueue_download_request", new_callable=AsyncMock)
async def test_empty_chunk_indices_is_noop(mock_enqueue: AsyncMock) -> None:
    from hippius_s3.services.object_reader import ensure_part_download_enqueued

    redis = _RedisStub()
    await ensure_part_download_enqueued(
        db=MagicMock(),
        redis=redis,
        info=_info(),
        object_version=1,
        storage_version=5,
        part_number=2,
        chunk_indices=set(),
        address="addr",
        cfg=_cfg(),
    )

    assert redis.set_calls == []
    mock_enqueue.assert_not_awaited()
