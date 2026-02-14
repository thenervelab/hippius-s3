from __future__ import annotations

from typing import Any
from typing import cast


async def fetch_chunk_blocking(
    obj_cache: Any, object_id: str, object_version: int, part_number: int, chunk_index: int, *, sleep_seconds: float
) -> bytes:
    while True:
        c = await obj_cache.get_chunk(object_id, int(object_version), int(part_number), int(chunk_index))
        if c is not None:
            return cast(bytes, c)
        import asyncio as _asyncio

        await _asyncio.sleep(float(sleep_seconds))
