from __future__ import annotations

from typing import Any


async def write_cache_meta(
    obj_cache: Any,
    object_id: str,
    object_version: int,
    part_number: int,
    *,
    chunk_size: int,
    num_chunks: int,
    plain_size: int,
    ttl: int | None = None,
) -> None:
    """Write normalized cache meta for a part.

    Schema normalization:
    - chunk_size: plaintext bytes per chunk (used for range math for both public and private)
    - num_chunks: number of chunks in cache for this part
    - plain_size: authoritative plaintext size of this part

    Back-compat:
    - We store plain_size in the legacy size_bytes field to avoid changing cache schema now.
    """

    kwargs = {
        "chunk_size": int(chunk_size),
        "num_chunks": int(num_chunks),
        # Back-compat: size_bytes carries plaintext size
        "size_bytes": int(plain_size),
    }
    if ttl is not None:
        await obj_cache.set_meta(object_id, int(object_version), int(part_number), **kwargs, ttl=int(ttl))  # type: ignore[arg-type]
    else:
        await obj_cache.set_meta(object_id, int(object_version), int(part_number), **kwargs)  # type: ignore[arg-type]
