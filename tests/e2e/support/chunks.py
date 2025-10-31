from __future__ import annotations

import os
from dataclasses import dataclass
from typing import List
from typing import Optional

import psycopg  # type: ignore[import-untyped]


@dataclass
class ChunkRow:
    chunk_index: int
    cid: str
    cipher_size_bytes: int
    plain_size_bytes: int


def get_part_chunks(
    bucket_name: str,
    object_key: str,
    part_number: int = 1,
    *,
    dsn: Optional[str] = None,
) -> List[ChunkRow]:
    """Return chunk rows for a given object's part, ordered by chunk_index."""
    sql = (
        "SELECT pc.chunk_index, pc.cid, pc.cipher_size_bytes, pc.plain_size_bytes\n"
        "FROM objects o\n"
        "JOIN buckets b ON b.bucket_id = o.bucket_id\n"
        "JOIN parts p ON p.object_id = o.object_id AND p.part_number = %s\n"
        "JOIN part_chunks pc ON pc.part_id = p.part_id\n"
        "WHERE b.bucket_name = %s AND o.object_key = %s\n"
        "ORDER BY pc.chunk_index ASC"
    )
    env = os.environ.get("DATABASE_URL")
    resolved_dsn: str = (
        dsn
        if dsn is not None
        else (env if env is not None else "postgresql://postgres:postgres@localhost:5432/hippius")
    )
    with psycopg.connect(resolved_dsn) as conn, conn.cursor() as cur:
        cur.execute(sql, (int(part_number), bucket_name, object_key))
        rows = cur.fetchall()
        result: List[ChunkRow] = [
            ChunkRow(
                chunk_index=int(r[0]),
                cid=str(r[1]),
                cipher_size_bytes=int(r[2]) if r[2] is not None else 0,
                plain_size_bytes=int(r[3]) if r[3] is not None else 0,
            )
            for r in rows or []
        ]
        return result


def get_first_chunk_cid(
    bucket_name: str,
    object_key: str,
    *,
    dsn: Optional[str] = None,
) -> Optional[str]:
    """Return the first chunk CID for part 1, or None if not available."""
    rows = get_part_chunks(bucket_name, object_key, part_number=1, dsn=dsn)
    if not rows:
        return None
    return rows[0].cid
