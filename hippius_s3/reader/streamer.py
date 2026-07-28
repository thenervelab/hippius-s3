from __future__ import annotations

import asyncio
import logging
from collections import deque
from typing import Any
from typing import AsyncGenerator
from typing import Awaitable
from typing import Callable
from typing import Coroutine
from typing import Iterable

from .decrypter import decrypt_chunk_if_needed
from .decrypter import maybe_slice
from .types import ChunkPlanItem


logger = logging.getLogger(__name__)

# `wait_fn` results are scheduled via `asyncio.create_task`, which requires a coroutine (not a bare
# Awaitable). Both call sites are `async def`, so this is exact.
WaitFn = Callable[[ChunkPlanItem], Coroutine[Any, Any, bytes]]
DecryptFn = Callable[[bytes, ChunkPlanItem], Awaitable[bytes]]


async def _emit(
    *,
    plan: Iterable[ChunkPlanItem],
    wait_fn: WaitFn,
    decrypt_fn: DecryptFn,
    object_id: str,
    object_version: int,
    prefetch: int,
) -> AsyncGenerator[bytes, None]:
    # Correctness: prefetch=0 must preserve the original sequential behavior.
    # (The pipelined scheduler below requires at least one "refill" per iteration.)
    if prefetch == 0:
        for item in plan:
            c = await wait_fn(item)
            pt = await decrypt_fn(c, item)
            yield maybe_slice(pt, item.slice_start, item.slice_end_excl)
        return

    it = iter(plan)

    # A small lookahead window to overlap chunk fetch with decrypt + response IO.
    pending: deque[tuple[ChunkPlanItem, asyncio.Task[bytes]]] = deque()

    def _schedule_one() -> bool:
        try:
            nxt = next(it)
        except StopIteration:
            return False
        pending.append((nxt, asyncio.create_task(wait_fn(nxt))))
        return True

    # Always schedule at least one, and then up to prefetch extra.
    if not _schedule_one():
        return
    for _ in range(prefetch):
        if not _schedule_one():
            break

    try:
        while pending:
            item, task = pending.popleft()
            try:
                c = await task
            except Exception:
                logger.exception(
                    "STREAM fetch failed object_id=%s v=%s part=%s chunk=%s",
                    object_id,
                    int(object_version),
                    int(item.part_number),
                    int(item.chunk_index),
                )
                raise

            # Keep the pipeline full.
            _schedule_one()

            pt = await decrypt_fn(c, item)
            yield maybe_slice(pt, item.slice_start, item.slice_end_excl)
    finally:
        # Ensure any pending tasks are cancelled if the client disconnects mid-stream.
        if pending:
            tasks = [t for _, t in pending]
            pending.clear()
            for t in tasks:
                t.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)


async def stream_plan(
    *,
    obj_cache: Any,
    object_id: str,
    object_version: int,
    plan: Iterable[ChunkPlanItem],
    storage_version: int,
    key_bytes: bytes | None,
    suite_id: str | None,
    bucket_id: str,
    upload_id: str,
    address: str = "",
    bucket_name: str = "",
    # Fallback only; object_reader passes the wired default HTTP_STREAM_PREFETCH_CHUNKS (16 in prod).
    prefetch_chunks: int = 0,
    chunk_timeout: float | None = None,
    # F1: called with a part_number when a chunk is missing from FS at stream time, to (idempotently)
    # re-enqueue that part's download. Without it a mid-stream eviction on a cache-source read waits on
    # a `notify:` no producer will ever publish and times out inside the response body (silent
    # truncation). None (HEAD/copy/migrate callers) keeps the pure wait-on-pub/sub behavior.
    ensure_part_fn: Callable[[int], Awaitable[None]] | None = None,
) -> AsyncGenerator[bytes, None]:
    prefetch = max(0, int(prefetch_chunks))

    # Parts we've already re-enqueued this stream. ensure_part_fn covers the WHOLE part's needed
    # chunks in one call, so a part is only ensured once even if several of its chunks miss.
    ensured_parts: set[int] = set()

    async def _maybe_ensure(item: ChunkPlanItem) -> None:
        # Only a genuine FS miss triggers the enqueue — a present chunk (the common case) pays a
        # single stat and no Redis/DB work, keeping the cache-source fast path fast.
        if ensure_part_fn is None:
            return
        pn = int(item.part_number)
        if pn in ensured_parts:
            return
        if await obj_cache.chunk_exists(object_id, int(object_version), pn, int(item.chunk_index)):
            return
        ensured_parts.add(pn)
        await ensure_part_fn(pn)

    async def _decrypt(c: bytes, item: ChunkPlanItem) -> bytes:
        return await decrypt_chunk_if_needed(
            c,
            object_id=object_id,
            part_number=int(item.part_number),
            chunk_index=int(item.chunk_index),
            storage_version=int(storage_version),
            key_bytes=key_bytes,
            suite_id=suite_id,
            bucket_id=bucket_id,
            upload_id=upload_id,
            address=address,
            bucket_name=bucket_name,
        )

    # RQ-1: one pub/sub subscription for the whole stream, demuxed per chunk, instead of a fresh
    # subscribe/unsubscribe per cold chunk. Opt-in; the default per-chunk path is unchanged.
    if _single_subscription_enabled() and hasattr(obj_cache, "stream_subscription"):
        sub_timeout = float(chunk_timeout) if chunk_timeout is not None else _default_chunk_timeout()
        async with obj_cache.stream_subscription(object_id, int(object_version)) as sub:

            async def _wait_sub(item: ChunkPlanItem) -> bytes:
                await _maybe_ensure(item)
                return await sub.wait_for_chunk(int(item.part_number), int(item.chunk_index), timeout=sub_timeout)

            async for out in _emit(
                plan=plan,
                wait_fn=_wait_sub,
                decrypt_fn=_decrypt,
                object_id=object_id,
                object_version=int(object_version),
                prefetch=prefetch,
            ):
                yield out
        return

    async def _wait(item: ChunkPlanItem) -> bytes:
        await _maybe_ensure(item)
        return await obj_cache.wait_for_chunk(
            object_id,
            int(object_version),
            int(item.part_number),
            int(item.chunk_index),
            timeout=chunk_timeout,
        )

    async for out in _emit(
        plan=plan,
        wait_fn=_wait,
        decrypt_fn=_decrypt,
        object_id=object_id,
        object_version=int(object_version),
        prefetch=prefetch,
    ):
        yield out


def _single_subscription_enabled() -> bool:
    try:
        from hippius_s3.config import get_config

        return bool(get_config().stream_single_subscription)
    except Exception:
        return False


def _default_chunk_timeout() -> float:
    try:
        from hippius_s3.config import get_config

        return float(get_config().cache_ttl_seconds)
    except Exception:
        return 3600.0
