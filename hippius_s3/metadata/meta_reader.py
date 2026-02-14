"""Explicit meta readers for DB and cache.

Separates authoritative DB reads from cache-only readiness checks.
"""

from __future__ import annotations

import math
from typing import Any
from typing import TypedDict


def _default_chunk_size() -> int:
    from hippius_s3.config import get_config

    return get_config().object_chunk_size_bytes


class DBMeta(TypedDict):
    plain_size: int
    chunk_size_bytes: int | None
    num_chunks_db: int


class CacheMeta(TypedDict):
    chunk_size: int
    num_chunks: int
    plain_size: int


async def read_db_meta(
    db: Any,
    object_id: str,
    part_number: int,
    object_version: int,
) -> DBMeta | None:
    """Read authoritative part metadata from DB for a specific version.

    Args:
        db: asyncpg connection.
        object_id: Object UUID.
        part_number: Part number (1-based).

    Returns:
        {plain_size, chunk_size_bytes, num_chunks_db} or None if part doesn't exist.
    """
    # Get part metadata for the specified object version
    part_row = await db.fetchrow(
        """
        SELECT part_id, size_bytes, chunk_size_bytes
        FROM parts
        WHERE object_id = $1 AND part_number = $2 AND object_version = $3
        """,
        object_id,
        part_number,
        int(object_version),
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
    object_version: int,
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
        raw = await obj_cache.get_meta(object_id, int(object_version), part_number)
        if isinstance(raw, dict):
            # Back-compat: use legacy size_bytes as plain_size
            chunk_size = int(raw.get("chunk_size", _default_chunk_size()))
            num_chunks = int(raw.get("num_chunks", 0))
            size_bytes = int(raw.get("size_bytes", 0))
            plain_size = size_bytes
            return {
                "chunk_size": chunk_size,
                "num_chunks": num_chunks,
                "plain_size": plain_size,
            }
    except Exception:
        pass
    return None


class NormalizedMeta(TypedDict):
    chunk_size: int
    num_chunks: int
    plain_size: int


async def ensure_cache_meta(
    db: Any,
    obj_cache: Any,
    object_id: str,
    object_version: int,
    part_number: int,
) -> NormalizedMeta:
    """Return authoritative meta, hydrating cache from DB when missing/invalid.

    - Tries Redis cache first; if valid, returns it.
    - Otherwise reads DB meta, writes normalized cache meta, and returns it.
    - If neither available, returns zeros.
    """
    try:
        c = await read_cache_meta(obj_cache, object_id, int(object_version), part_number)
        if (
            c
            and int(c.get("chunk_size", 0)) > 0
            and int(c.get("num_chunks", 0)) > 0
            and int(c.get("plain_size", 0)) > 0
        ):
            return {
                "chunk_size": int(c.get("chunk_size", 0)),
                "num_chunks": int(c.get("num_chunks", 0)),
                "plain_size": int(c.get("plain_size", 0)),
            }
    except Exception:
        pass

    # Fallback to DB
    try:
        dbm = await read_db_meta(db, object_id, part_number, int(object_version))
    except Exception:
        dbm = None

    if dbm:
        cs = int(dbm.get("chunk_size_bytes") or 0)
        nc = int(dbm.get("num_chunks_db") or 0)
        ps = int(dbm.get("plain_size") or 0)
        # If chunk size is known but DB has no chunk rows yet, infer num_chunks from plaintext size
        if cs > 0 and ps > 0 and nc <= 0:
            nc = int(math.ceil(ps / cs))
        if cs > 0 and nc > 0 and ps > 0:
            try:
                from hippius_s3.metadata.meta_writer import write_cache_meta  # local import to avoid cycles

                await write_cache_meta(
                    obj_cache,
                    object_id,
                    int(object_version),
                    int(part_number),
                    chunk_size=cs,
                    num_chunks=nc,
                    plain_size=ps,
                )
            except Exception:
                pass
            return {"chunk_size": cs, "num_chunks": nc, "plain_size": ps}

    return {"chunk_size": 0, "num_chunks": 0, "plain_size": 0}
