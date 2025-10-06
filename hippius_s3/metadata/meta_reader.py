"""Explicit meta readers for DB and cache.

Separates authoritative DB reads from cache-only readiness checks.
"""

from __future__ import annotations

from typing import Any
from typing import TypedDict

from asyncpg import Connection


class DBMeta(TypedDict):
    plain_size: int
    chunk_size_bytes: int | None
    num_chunks_db: int


class CacheMeta(TypedDict):
    chunk_size: int
    num_chunks: int
    size_bytes: int


async def read_db_meta(
    db: Connection,
    object_id: str,
    part_number: int,
) -> DBMeta | None:
    """Read authoritative part metadata from DB.

    Args:
        db: asyncpg connection.
        object_id: Object UUID.
        part_number: Part number (1-based).

    Returns:
        {plain_size, chunk_size_bytes, num_chunks_db} or None if part doesn't exist.
    """
    # Get part metadata
    part_row = await db.fetchrow(
        """
        SELECT part_id, size_bytes, chunk_size_bytes
        FROM parts
        WHERE object_id = $1 AND part_number = $2
        """,
        object_id,
        part_number,
    )
    if not part_row:
        return None

    part_id = part_row["part_id"]
    plain_size = int(part_row["size_bytes"])
    chunk_size_bytes = part_row["chunk_size_bytes"]

    # Count chunks from part_chunks table
    num_chunks_row = await db.fetchrow(
        """
        SELECT COUNT(*) as cnt
        FROM part_chunks
        WHERE part_id = $1
        """,
        part_id,
    )
    num_chunks_db = int(num_chunks_row["cnt"]) if num_chunks_row else 0

    return {
        "plain_size": plain_size,
        "chunk_size_bytes": chunk_size_bytes,
        "num_chunks_db": num_chunks_db,
    }


async def read_cache_meta(
    obj_cache: Any,
    object_id: str,
    part_number: int,
) -> CacheMeta | None:
    """Read cache meta if present; for readiness checks only.

    Args:
        obj_cache: RedisObjectPartsCache instance.
        object_id: Object UUID.
        part_number: Part number (1-based).

    Returns:
        {chunk_size, num_chunks, size_bytes} or None if not cached.
    """
    try:
        raw = await obj_cache.get_meta(object_id, part_number)
        if isinstance(raw, dict):
            return {
                "chunk_size": int(raw.get("chunk_size", 4 * 1024 * 1024)),
                "num_chunks": int(raw.get("num_chunks", 0)),
                "size_bytes": int(raw.get("size_bytes", 0)),
            }
    except Exception:
        pass
    return None
