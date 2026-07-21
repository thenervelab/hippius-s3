"""B1: a terminal chunk miss on the first-chunk peek must map to 503, not 500.

When the downloader gives up fast (a chunk 404s on the backend for an undrained object), it
publishes a chunk-ready notification anyway. The streamer's bounded first-chunk peek wakes, re-reads,
finds nothing, and the notifier raises the terminal-miss `ChunkNotReadyError`. That must surface as
`DownloadNotReadyError` (→ 503 SlowDown, retryable) — NOT escape as an unknown exception the global
handler turns into a 500 InternalError. This is the exact regression vs the old 90s-wait→503 path.
"""

from __future__ import annotations

import contextlib
from types import SimpleNamespace
from typing import Any
from typing import Iterator
from unittest.mock import AsyncMock
from unittest.mock import patch

import pytest

from hippius_s3.cache.notifier import ChunkNotReadyError
from hippius_s3.services import object_reader


def _ctx() -> object_reader.StreamContext:
    return object_reader.StreamContext(
        plan=[object()],
        object_version=1,
        storage_version=5,
        source="pipeline",
        key_bytes=None,
        suite_id="hip-enc/aes256gcm",
        bucket_id="bkt",
        upload_id="",
    )


def _info() -> dict:
    return {
        "object_id": "11111111-2222-3333-4444-555555555555",
        "bucket_name": "bkt",
        "content_type": "application/octet-stream",
        "size_bytes": 10,
        "metadata": {},
    }


@contextlib.contextmanager
def _patched(cfg: Any, stream_plan_factory: Any) -> Iterator[None]:
    with (
        patch.object(object_reader, "build_stream_context", new=AsyncMock(return_value=_ctx())),
        patch.object(object_reader, "get_config", return_value=cfg),
        patch.object(object_reader, "build_headers", return_value={}),
        patch.object(object_reader, "stream_plan", new=lambda **kw: stream_plan_factory()),
    ):
        yield


@pytest.mark.asyncio
async def test_read_response_terminal_miss_raises_download_not_ready() -> None:
    """Terminal miss on the first-chunk peek → DownloadNotReadyError (503), not a bare 500."""

    async def _terminal_miss():
        # Mirrors ChunkNotifier.wait_for_chunk's terminal branch: notified, re-fetched, still gone.
        raise ChunkNotReadyError("Chunk missing after pub/sub notification: obj:x:v:1:part:0:chunk:0")
        yield b"unreachable"  # pragma: no cover — makes this an async generator

    cfg = SimpleNamespace(
        stream_first_chunk_timeout_seconds=5, stream_chunk_timeout_seconds=300, http_stream_prefetch_chunks=0
    )
    with _patched(cfg, _terminal_miss):
        with pytest.raises(object_reader.DownloadNotReadyError):
            await object_reader.read_response(
                db=None, redis=None, obj_cache=None, info=_info(), read_mode="auto", rng=None, address="a"
            )


@pytest.mark.asyncio
async def test_stream_object_bound_peek_terminal_miss_raises_download_not_ready() -> None:
    """The stream_object bound-first-chunk peek (streaming CopyObject source) maps the miss the same."""

    async def _terminal_miss():
        raise ChunkNotReadyError("Chunk missing after pub/sub notification: obj:x:v:1:part:0:chunk:0")
        yield b"unreachable"  # pragma: no cover

    cfg = SimpleNamespace(
        stream_first_chunk_timeout_seconds=5, stream_chunk_timeout_seconds=300, http_stream_prefetch_chunks=0
    )
    with _patched(cfg, _terminal_miss):
        with pytest.raises(object_reader.DownloadNotReadyError):
            await object_reader.stream_object(
                db=None,
                redis=None,
                obj_cache=None,
                info=_info(),
                rng=None,
                address="a",
                bound_first_chunk=True,
            )


def test_download_not_ready_maps_to_503_slowdown() -> None:
    """End-of-chain: the DownloadNotReadyError the peek raises maps to a 503 SlowDown response."""
    from hippius_s3.api.s3.errors import map_read_path_exception

    resp = map_read_path_exception(object_reader.DownloadNotReadyError("Parts not ready"))
    assert resp is not None
    assert resp.status_code == 503
