from __future__ import annotations

from typing import Any
from typing import AsyncGenerator
from typing import Iterable

from .decrypter import decrypt_chunk_if_needed
from .decrypter import maybe_slice
from .fetcher import fetch_chunk_blocking
from .types import ChunkPlanItem


async def stream_plan(
    *,
    obj_cache: Any,
    object_id: str,
    object_version: int,
    plan: Iterable[ChunkPlanItem],
    should_decrypt: bool,
    sleep_seconds: float,
    address: str = "",
    bucket_name: str = "",
    storage_version: int = 2,
) -> AsyncGenerator[bytes, None]:
    for item in plan:
        c = await fetch_chunk_blocking(
            obj_cache,
            object_id,
            int(object_version),
            int(item.part_number),
            int(item.chunk_index),
            sleep_seconds=sleep_seconds,
        )
        pt = await decrypt_chunk_if_needed(
            should_decrypt,
            c,
            object_id=object_id,
            part_number=int(item.part_number),
            chunk_index=int(item.chunk_index),
            address=address,
            bucket_name=bucket_name,
            storage_version=int(storage_version),
        )
        yield maybe_slice(pt, item.slice_start, item.slice_end_excl)
