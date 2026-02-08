from __future__ import annotations

from typing import Any
from typing import cast


async def fetch_chunk_blocking(
    object_id: str,
    object_version: int,
    part_number: int,
    chunk_index: int,
    *,
    sleep_seconds: float,
    fs_store: Any,
) -> bytes:
    while True:
        fs_data = await fs_store.get_chunk(
            object_id,
            int(object_version),
            int(part_number),
            int(chunk_index),
        )
        if fs_data is not None:
            return cast(bytes, fs_data)
        import asyncio as _asyncio

        await _asyncio.sleep(float(sleep_seconds))
