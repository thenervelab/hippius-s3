from __future__ import annotations

import asyncio
import logging
from collections import deque
from typing import Any
from typing import AsyncGenerator
from typing import Iterable

from .decrypter import decrypt_chunk_if_needed
from .decrypter import maybe_slice
from .fetcher import fetch_chunk_blocking
from .types import ChunkPlanItem


logger = logging.getLogger(__name__)


async def stream_plan(
    *,
    object_id: str,
    object_version: int,
    plan: Iterable[ChunkPlanItem],
    sleep_seconds: float,
    storage_version: int,
    key_bytes: bytes | None,
    suite_id: str | None,
    bucket_id: str,
    upload_id: str,
    address: str = "",
    bucket_name: str = "",
    prefetch_chunks: int = 0,
    fs_store: Any,
) -> AsyncGenerator[bytes, None]:
    prefetch = max(0, int(prefetch_chunks))

    # Correctness: prefetch=0 must preserve the original sequential behavior.
    # (The pipelined scheduler below requires at least one "refill" per iteration.)
    if prefetch == 0:
        for item in plan:
            c = await fetch_chunk_blocking(
                object_id,
                int(object_version),
                int(item.part_number),
                int(item.chunk_index),
                sleep_seconds=sleep_seconds,
                fs_store=fs_store,
            )
            pt = await decrypt_chunk_if_needed(
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
            yield maybe_slice(pt, item.slice_start, item.slice_end_excl)
        return

    it = iter(plan)

    async def _fetch(item: ChunkPlanItem) -> bytes:
        return await fetch_chunk_blocking(
            object_id,
            int(object_version),
            int(item.part_number),
            int(item.chunk_index),
            sleep_seconds=sleep_seconds,
            fs_store=fs_store,
        )

    # A small lookahead window to overlap FS fetch with decrypt + response IO.
    pending: deque[tuple[ChunkPlanItem, asyncio.Task[bytes]]] = deque()

    def _schedule_one() -> bool:
        try:
            nxt = next(it)
        except StopIteration:
            return False
        pending.append((nxt, asyncio.create_task(_fetch(nxt))))
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
                # Helpful context for failures mid-stream.
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

            try:
                clen = len(c) if isinstance(c, (bytes, bytearray)) else None
                head8 = c[:8].hex() if isinstance(c, (bytes, bytearray)) else None
                logger.debug(
                    "STREAM fetched key=obj:%s:v:%s:part:%s:chunk:%s len=%s head8=%s",
                    object_id,
                    int(object_version),
                    int(item.part_number),
                    int(item.chunk_index),
                    str(clen),
                    head8,
                )
            except Exception:
                pass

            pt = await decrypt_chunk_if_needed(
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
            yield maybe_slice(pt, item.slice_start, item.slice_end_excl)
    finally:
        # Ensure any pending tasks are cancelled if the client disconnects mid-stream.
        if pending:
            tasks = [t for _, t in pending]
            pending.clear()
            for t in tasks:
                t.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)
