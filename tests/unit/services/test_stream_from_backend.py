"""The read path with the backend as its lowest tier.

A GET serves each chunk from the first tier that has it — this node's SSD, a peer's, the pool —
and otherwise pulls the ciphertext from the backend into memory, decrypts it and yields it. The
bytes are never written to any cache on the way, no downloader is enqueued, and nothing waits on
pub/sub. These tests drive the real `stream_plan` + `read_response` with a fake cache and a fake
backend fetcher.
"""

from __future__ import annotations

import asyncio
from types import SimpleNamespace
from typing import Any
from unittest.mock import patch

import pytest

from hippius_s3.reader import backend_fetch
from hippius_s3.reader import streamer
from hippius_s3.reader.backend_fetch import BackendChunkFetcher
from hippius_s3.reader.types import ChunkPlanItem
from hippius_s3.services import object_reader


OBJ = "11111111-2222-3333-4444-555555555555"


class _Cache:
    """Local tiers: whatever is in `held`; records every write so the test can assert there are none."""

    def __init__(self, held: dict[tuple[int, int], bytes]) -> None:
        self.held = held
        self.reads: list[tuple[int, int]] = []
        self.writes: list[tuple[int, int]] = []

    async def get_chunk(self, oid: str, v: int, pn: int, ci: int) -> bytes | None:
        self.reads.append((int(pn), int(ci)))
        return self.held.get((int(pn), int(ci)))

    async def set_chunk(self, *args: Any, **kwargs: Any) -> None:
        self.writes.append((int(args[2]), int(args[3])))


def _backend(served: dict[str, bytes]) -> tuple[BackendChunkFetcher, list[str]]:
    fetched: list[str] = []

    async def arion(identifier: str, address: str) -> bytes:
        fetched.append(identifier)
        return served[identifier]

    return BackendChunkFetcher({"arion": arion}, concurrency=4, attempts=1, base_sleep=0, jitter=0), fetched


def _ctx(plan: list[ChunkPlanItem], locations: dict) -> object_reader.StreamContext:
    return object_reader.StreamContext(
        plan=plan,
        object_version=1,
        storage_version=5,
        source="pipeline",
        key_bytes=None,
        suite_id="hip-enc/aes256gcm",
        bucket_id="bkt",
        upload_id="",
        locations=locations,
    )


async def _identity_decrypt(c: bytes, **_kw: Any) -> bytes:
    return c


def _cfg(**overrides: Any) -> SimpleNamespace:
    base = {
        "stream_first_chunk_timeout_seconds": 5,
        "stream_chunk_timeout_seconds": 300,
        "http_stream_prefetch_chunks": 2,
        "read_missing_chunk_wait_seconds": 0.0,
    }
    base.update(overrides)
    return SimpleNamespace(**base)


@pytest.mark.asyncio
async def test_a_cold_chunk_is_fetched_from_the_backend_decrypted_and_never_cached() -> None:
    plan = [ChunkPlanItem(part_number=1, chunk_index=0), ChunkPlanItem(part_number=1, chunk_index=1)]
    cache = _Cache({(1, 0): b"local-0"})
    fetcher, fetched = _backend({"id-1": b"backend-1"})
    backend_fetch.set_backend_fetcher(fetcher)
    try:
        with (
            patch.object(streamer, "decrypt_chunk_if_needed", new=_identity_decrypt),
            patch.object(object_reader, "get_config", return_value=_cfg()),
        ):
            ctx = _ctx(plan, {(1, 1): (("arion", "id-1"),)})
            gen = object_reader._stream(ctx, cache, {"object_id": OBJ, "bucket_name": "b"}, address="addr")
            out = b"".join([c async for c in gen])
    finally:
        backend_fetch.set_backend_fetcher(None)

    assert out == b"local-0backend-1"
    assert fetched == ["id-1"], "only the chunk the local tiers missed is fetched from the backend"
    assert cache.writes == [], "a backend-served chunk is decrypted from memory, never written to a cache"


@pytest.mark.asyncio
async def test_read_response_maps_a_chunk_on_no_tier_to_a_retryable_503() -> None:
    # The upload-window shape: the part is on another node's SSD, no backend row exists yet, and
    # the peer tier missed. After the bounded local re-poll the first-chunk peek raises the
    # retryable DownloadNotReadyError (503 + Retry-After at the endpoint), never a 500.
    plan = [ChunkPlanItem(part_number=1, chunk_index=0)]
    cache = _Cache({})
    fetcher, fetched = _backend({})
    backend_fetch.set_backend_fetcher(fetcher)
    try:
        with (
            patch.object(streamer, "decrypt_chunk_if_needed", new=_identity_decrypt),
            patch.object(object_reader, "get_config", return_value=_cfg(read_missing_chunk_wait_seconds=0.0)),
            patch.object(object_reader, "build_headers", return_value={}),
            patch.object(asyncio, "sleep", new=_no_sleep),
        ):
            with pytest.raises(object_reader.DownloadNotReadyError):
                await object_reader.read_response(
                    ctx=_ctx(plan, {}),
                    redis=None,
                    obj_cache=cache,
                    info={"object_id": OBJ, "bucket_name": "b", "content_type": "x", "size_bytes": 1, "metadata": {}},
                    read_mode="auto",
                    rng=None,
                    address="addr",
                )
    finally:
        backend_fetch.set_backend_fetcher(None)

    assert fetched == [], "nothing to fetch: no backend holds the chunk"
    assert (1, 0) in cache.reads, "the local tiers were re-polled before giving up"


async def _no_sleep(_seconds: float) -> None:
    return None


@pytest.mark.asyncio
async def test_a_chunk_that_lands_locally_during_the_re_poll_is_served() -> None:
    # A peer that shed the first fetch (saturated) serves on the re-poll: the read recovers
    # without the client retrying.
    plan = [ChunkPlanItem(part_number=1, chunk_index=0)]
    cache = _Cache({})

    async def _lands_on_second_read(oid: str, v: int, pn: int, ci: int) -> bytes | None:
        cache.reads.append((pn, ci))
        return b"from-peer" if len(cache.reads) >= 2 else None

    cache.get_chunk = _lands_on_second_read  # type: ignore[method-assign]
    fetcher, _ = _backend({})
    backend_fetch.set_backend_fetcher(fetcher)
    try:
        with (
            patch.object(streamer, "decrypt_chunk_if_needed", new=_identity_decrypt),
            patch.object(object_reader, "get_config", return_value=_cfg(read_missing_chunk_wait_seconds=30.0)),
            patch.object(asyncio, "sleep", new=_no_sleep),
        ):
            gen = object_reader._stream(_ctx(plan, {}), cache, {"object_id": OBJ, "bucket_name": "b"}, address="addr")
            out = b"".join([c async for c in gen])
    finally:
        backend_fetch.set_backend_fetcher(None)
    assert out == b"from-peer"


@pytest.mark.asyncio
async def test_backend_fetches_overlap_under_prefetch() -> None:
    # The prefetch window is the per-request backend parallelism on a cold read: with prefetch=3,
    # four chunks are in flight at once instead of one round trip each.
    plan = [ChunkPlanItem(part_number=1, chunk_index=i) for i in range(6)]
    in_flight = 0
    peak = 0

    async def arion(identifier: str, address: str) -> bytes:
        nonlocal in_flight, peak
        in_flight += 1
        peak = max(peak, in_flight)
        await asyncio.sleep(0.01)
        in_flight -= 1
        return identifier.encode()

    fetcher = BackendChunkFetcher({"arion": arion}, concurrency=16, attempts=1, base_sleep=0, jitter=0)
    backend_fetch.set_backend_fetcher(fetcher)
    try:
        with (
            patch.object(streamer, "decrypt_chunk_if_needed", new=_identity_decrypt),
            patch.object(object_reader, "get_config", return_value=_cfg(http_stream_prefetch_chunks=3)),
        ):
            ctx = _ctx(plan, {(1, i): (("arion", f"c{i}"),) for i in range(6)})
            gen = object_reader._stream(ctx, _Cache({}), {"object_id": OBJ, "bucket_name": "b"}, address="addr")
            out = b"".join([c async for c in gen])
    finally:
        backend_fetch.set_backend_fetcher(None)

    assert out == b"c0c1c2c3c4c5", "chunks are yielded in plan order regardless of fetch completion order"
    assert peak >= 2, f"backend fetches overlap under prefetch (peak {peak})"
