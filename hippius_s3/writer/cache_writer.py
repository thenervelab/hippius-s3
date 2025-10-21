from __future__ import annotations

from typing import Any


class CacheWriter:
    def __init__(self, obj_cache: Any, *, ttl_seconds: int) -> None:
        self.obj_cache = obj_cache
        self.ttl_seconds = int(ttl_seconds)

    async def write_meta(
        self,
        object_id: str,
        object_version: int,
        part_number: int,
        *,
        chunk_size: int,
        num_chunks: int,
        plain_size: int,
    ) -> None:
        await self.obj_cache.set_meta(
            object_id,
            int(object_version),
            int(part_number),
            chunk_size=int(chunk_size),
            num_chunks=int(num_chunks),
            size_bytes=int(plain_size),
            ttl=self.ttl_seconds,
        )

    async def write_chunks(self, object_id: str, object_version: int, part_number: int, chunks: list[bytes]) -> None:
        for i, ct in enumerate(chunks):
            await self.obj_cache.set_chunk(
                object_id, int(object_version), int(part_number), int(i), ct, ttl=self.ttl_seconds
            )
