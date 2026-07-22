"""A2: read_response bounds the wait for the FIRST chunk.

An un-drained object whose part is on no backend yet must fail fast with DownloadNotReadyError
(→ 503 at the endpoint) instead of hanging up to cache_ttl_seconds (~1h). Warm reads still stream
normally, and the first chunk is preserved (not dropped by the peek).
"""

from __future__ import annotations

import asyncio
import contextlib
from types import SimpleNamespace
from typing import Any
from typing import Iterator
from unittest.mock import AsyncMock
from unittest.mock import patch

import pytest

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
    """Patch read_response's collaborators: a fixed ctx, the given config, no-op headers, and a
    controllable stream_plan (each call returns a fresh generator from `stream_plan_factory`)."""
    with (
        patch.object(object_reader, "build_stream_context", new=AsyncMock(return_value=_ctx())),
        patch.object(object_reader, "get_config", return_value=cfg),
        patch.object(object_reader, "build_headers", return_value={}),
        patch.object(object_reader, "stream_plan", new=lambda **kw: stream_plan_factory()),
    ):
        yield


async def _collect(resp: Any) -> bytes:
    return b"".join([chunk async for chunk in resp.body_iterator])


@pytest.mark.asyncio
async def test_first_chunk_timeout_raises_download_not_ready() -> None:
    """First chunk never arrives within the (tiny) bound → DownloadNotReadyError, fast (not ~1h)."""

    async def _hanging():
        await asyncio.sleep(30)  # far longer than the 0.15s bound
        yield b"unreachable"

    cfg = SimpleNamespace(
        stream_first_chunk_timeout_seconds=0.15, stream_chunk_timeout_seconds=300, http_stream_prefetch_chunks=0
    )
    with _patched(cfg, _hanging):
        loop = asyncio.get_event_loop()
        t0 = loop.time()
        with pytest.raises(object_reader.DownloadNotReadyError):
            await object_reader.read_response(
                db=None, redis=None, obj_cache=None, info=_info(), read_mode="auto", rng=None, address="a"
            )
        assert loop.time() - t0 < 5.0, "must fail fast on the bound, not hang"


@pytest.mark.asyncio
async def test_warm_read_streams_all_chunks_including_the_peeked_first() -> None:
    """The peeked first chunk must be re-yielded, not swallowed."""

    async def _two():
        yield b"hello"
        yield b"world"

    cfg = SimpleNamespace(
        stream_first_chunk_timeout_seconds=5, stream_chunk_timeout_seconds=300, http_stream_prefetch_chunks=0
    )
    with _patched(cfg, _two):
        resp = await object_reader.read_response(
            db=None, redis=None, obj_cache=None, info=_info(), read_mode="auto", rng=None, address="a"
        )
        assert resp.status_code == 200
        assert await _collect(resp) == b"helloworld"


@pytest.mark.asyncio
async def test_zero_byte_object_streams_empty_body() -> None:
    """StopAsyncIteration on the first peek (empty object) → a clean empty 200, not an error."""

    async def _empty():
        return
        yield b""  # pragma: no cover — makes this an async generator

    cfg = SimpleNamespace(
        stream_first_chunk_timeout_seconds=5, stream_chunk_timeout_seconds=300, http_stream_prefetch_chunks=0
    )
    with _patched(cfg, _empty):
        resp = await object_reader.read_response(
            db=None, redis=None, obj_cache=None, info=_info(), read_mode="auto", rng=None, address="a"
        )
        assert resp.status_code == 200
        assert await _collect(resp) == b""
