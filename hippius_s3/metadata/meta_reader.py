"""Explicit meta readers for DB.

Reads authoritative metadata from PostgreSQL.
"""

from __future__ import annotations

from typing import Any
from typing import TypedDict


class DBMeta(TypedDict):
    plain_size: int
    chunk_size_bytes: int | None
    num_chunks_db: int


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
