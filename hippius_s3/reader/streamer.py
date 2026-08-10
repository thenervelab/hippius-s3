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

from hippius_s3.monitoring import AeadFailureOutcome
from hippius_s3.monitoring import AeadFailureTier

from .decrypter import CIPHERTEXT_UNUSABLE
from .decrypter import decrypt_chunk_if_needed
from .decrypter import maybe_slice
from .types import ChunkPlanItem


logger = logging.getLogger(__name__)

# `wait_fn` results are scheduled via `asyncio.create_task`, which requires a coroutine (not a bare
# Awaitable). Both call sites are `async def`, so this is exact.
WaitFn = Callable[[ChunkPlanItem], Coroutine[Any, Any, bytes]]
DecryptFn = Callable[[bytes, ChunkPlanItem], Awaitable[bytes]]
# Drops this node's cached copy of a chunk, returning whether one was removed. False means there
# was nothing local to invalidate, so re-fetching would only return the same bytes.
InvalidateFn = Callable[[ChunkPlanItem], Awaitable[bool]]


def _record_aead_failure(tier: AeadFailureTier, outcome: AeadFailureOutcome) -> None:
    """Count a chunk that failed to authenticate. Never let observability mask the failure."""
    try:
        from hippius_s3.monitoring import get_metrics_collector

        collector = get_metrics_collector()
        if collector is not None:
            collector.record_aead_failure(tier, outcome)
    except Exception:  # noqa: BLE001 - a metrics failure must not replace the real error
        pass


async def _decrypt_reloading_once(
    *,
    item: ChunkPlanItem,
    cbytes: bytes,
    wait_fn: WaitFn,
    decrypt_fn: DecryptFn,
    invalidate_fn: InvalidateFn,
    object_id: str,
    object_version: int,
) -> bytes:
    """Decrypt a chunk; on an authentication failure, drop the local copy and try the next tier once.

    This is the seam where the fix has to live: the decrypter sees the failure but not which tier
    served the bytes, and the store knows the tier but does not decrypt. Here both are in hand.

    The retry is straight-line and happens EXACTLY once — deliberately not a loop. A DEK-level
    fault (wrong wrapped key, wrong AAD) fails every chunk of every object, and an unbounded
    invalidate-and-retry would turn that single fault into a fleet-wide cache wipe that also
    hammers the pool. Note what the bound implies per request: a retry that fails too ends the
    stream, so a request invalidates once and stops, while a request that keeps recovering is by
    definition healing isolated corruption one chunk at a time.
    """
    try:
        return await decrypt_fn(cbytes, item)
    except CIPHERTEXT_UNUSABLE:
        if not await invalidate_fn(item):
            # Nothing local held these bytes (a peer or the pool served them, or this deployment
            # has no lower tier) — re-fetching would return the same bytes, and the pool copy is
            # authoritative, so a fault there is a real error. The store also answers False when
            # a redrive has marked the pool copy suspect: the retry would read superseded bytes
            # that still authenticate, so failing here is the only safe outcome.
            _record_aead_failure("remote", "unrecovered")
            raise
        logger.warning(
            "chunk failed authentication; local copy dropped, retrying from the next tier "
            "object_id=%s v=%s part=%s chunk=%s",
            object_id,
            int(object_version),
            int(item.part_number),
            int(item.chunk_index),
        )

    reloaded = await wait_fn(item)
    try:
        plaintext = await decrypt_fn(reloaded, item)
    except CIPHERTEXT_UNUSABLE:
        # Surviving a tier change means the bytes were never the problem — a wrong DEK, or an
        # object that is genuinely unreadable. Either way there is nothing left to invalidate.
        logger.error(
            "chunk still failed authentication after invalidation — key fault, not local corruption "
            "object_id=%s v=%s part=%s chunk=%s",
            object_id,
            int(object_version),
            int(item.part_number),
            int(item.chunk_index),
        )
        _record_aead_failure("local", "unrecovered")
        raise
    _record_aead_failure("local", "recovered")
    return plaintext


async def _emit(
    *,
    plan: Iterable[ChunkPlanItem],
    wait_fn: WaitFn,
    decrypt_fn: DecryptFn,
    invalidate_fn: InvalidateFn,
    object_id: str,
    object_version: int,
    prefetch: int,
) -> AsyncGenerator[bytes, None]:
    async def _decrypt_one(item: ChunkPlanItem, cbytes: bytes) -> bytes:
        return await _decrypt_reloading_once(
            item=item,
            cbytes=cbytes,
            wait_fn=wait_fn,
            decrypt_fn=decrypt_fn,
            invalidate_fn=invalidate_fn,
            object_id=object_id,
            object_version=object_version,
        )

    # Correctness: prefetch=0 must preserve the original sequential behavior.
    # (The pipelined scheduler below requires at least one "refill" per iteration.)
    if prefetch == 0:
        for item in plan:
            c = await wait_fn(item)
            pt = await _decrypt_one(item, c)
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

            pt = await _decrypt_one(item, c)
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
) -> AsyncGenerator[bytes, None]:
    prefetch = max(0, int(prefetch_chunks))

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

    # WI-10: resolved from the cache's store rather than passed by every call site, because only
    # the store can know whether there is a tier BELOW the copy being dropped. A store with no
    # fallback does not expose this — its one copy is the authoritative one — and then a chunk that
    # fails to authenticate stays an error, which is the correct outcome for an unattributable fault.
    invalidate_local = getattr(getattr(obj_cache, "fs", None), "invalidate_local_chunk", None)

    async def _invalidate(item: ChunkPlanItem) -> bool:
        if invalidate_local is None:
            return False
        return bool(
            await invalidate_local(object_id, int(object_version), int(item.part_number), int(item.chunk_index))
        )

    # RQ-1: one pub/sub subscription for the whole stream, demuxed per chunk, instead of a fresh
    # subscribe/unsubscribe per cold chunk. Opt-in; the default per-chunk path is unchanged.
    if _single_subscription_enabled() and hasattr(obj_cache, "stream_subscription"):
        sub_timeout = float(chunk_timeout) if chunk_timeout is not None else _default_chunk_timeout()
        async with obj_cache.stream_subscription(object_id, int(object_version)) as sub:

            async def _wait_sub(item: ChunkPlanItem) -> bytes:
                return await sub.wait_for_chunk(int(item.part_number), int(item.chunk_index), timeout=sub_timeout)

            async for out in _emit(
                plan=plan,
                wait_fn=_wait_sub,
                decrypt_fn=_decrypt,
                invalidate_fn=_invalidate,
                object_id=object_id,
                object_version=int(object_version),
                prefetch=prefetch,
            ):
                yield out
        return

    async def _wait(item: ChunkPlanItem) -> bytes:
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
        invalidate_fn=_invalidate,
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
