from __future__ import annotations

import time
from typing import Iterable
from typing import Optional

import psycopg  # type: ignore[import-untyped]
import redis  # type: ignore[import-untyped]

from hippius_s3.cache import RedisObjectPartsCache


def get_object_id_and_version(
    bucket_name: str, object_key: str, *, dsn: str = "postgresql://postgres:postgres@localhost:5432/hippius"
) -> tuple[str, int]:
    """Fetch (object_id, current_object_version) for a (bucket_name, object_key)."""
    with psycopg.connect(dsn) as conn, conn.cursor() as cur:
        cur.execute(
            """
                SELECT o.object_id, o.current_object_version
                FROM objects o
                JOIN buckets b ON b.bucket_id = o.bucket_id
                WHERE b.bucket_name = %s AND o.object_key = %s
                ORDER BY o.created_at DESC
                LIMIT 1
                """,
            (bucket_name, object_key),
        )
        row = cur.fetchone()
        if not row:
            raise RuntimeError("object_not_found")
        return str(row[0]), int(row[1] or 1)


def get_object_id(
    bucket_name: str, object_key: str, *, dsn: str = "postgresql://postgres:postgres@localhost:5432/hippius"
) -> str:
    """Back-compat helper: return only object_id."""
    oid, _ver = get_object_id_and_version(bucket_name, object_key, dsn=dsn)
    return oid


def get_object_cids(
    bucket_name: str,
    object_key: str,
    *,
    dsn: str = "postgresql://postgres:postgres@localhost:5432/hippius",
) -> tuple[str, int, str, list[str], Optional[str]]:
    """Get all CIDs for an object including parts, chunks, and object-level CID.

    Returns: (object_id, object_version, main_account_id, part_cids, object_cid)
    """
    with psycopg.connect(dsn) as conn, conn.cursor() as cur:
        cur.execute(
            """
            SELECT o.object_id, o.current_object_version, b.main_account_id
            FROM objects o
            JOIN buckets b ON b.bucket_id = o.bucket_id
            WHERE b.bucket_name = %s AND o.object_key = %s
            """,
            (bucket_name, object_key),
        )
        row = cur.fetchone()
        if not row:
            raise RuntimeError("object_not_found")
        object_id, object_version, main_account_id = str(row[0]), int(row[1] or 1), str(row[2])

        cur.execute(
            """
            SELECT COALESCE(c.cid, p.ipfs_cid) as cid
            FROM parts p
            LEFT JOIN cids c ON p.cid_id = c.id
            WHERE p.object_id = %s AND p.object_version = %s
            AND COALESCE(c.cid, p.ipfs_cid) IS NOT NULL
            AND COALESCE(c.cid, p.ipfs_cid) != 'pending'
            ORDER BY p.part_number
            """,
            (object_id, object_version),
        )
        part_cids = [str(row[0]) for row in cur.fetchall()]

        cur.execute(
            """
            SELECT pc.cid
            FROM parts p
            JOIN part_chunks pc ON pc.part_id = p.part_id
            WHERE p.object_id = %s AND p.object_version = %s
            AND pc.cid IS NOT NULL
            AND pc.cid != 'pending'
            ORDER BY p.part_number, pc.chunk_index
            """,
            (object_id, object_version),
        )
        chunk_cids = [str(row[0]) for row in cur.fetchall()]

        cur.execute(
            """
            SELECT COALESCE(c.cid, ov.ipfs_cid) as cid
            FROM object_versions ov
            LEFT JOIN cids c ON ov.cid_id = c.id
            WHERE ov.object_id = %s AND ov.object_version = %s
            AND COALESCE(c.cid, ov.ipfs_cid) IS NOT NULL
            AND COALESCE(c.cid, ov.ipfs_cid) != 'pending'
            """,
            (object_id, object_version),
        )
        obj_cid_row = cur.fetchone()
        object_cid = str(obj_cid_row[0]) if obj_cid_row else None

        return object_id, object_version, main_account_id, part_cids + chunk_cids, object_cid


essentially_all_parts = range(0, 256)


def clear_object_cache(
    object_id: str,
    parts: Iterable[int] | None = None,
    *,
    redis_url: str = "redis://localhost:6379/0",
    dsn: str = "postgresql://postgres:postgres@localhost:5432/hippius",
) -> None:
    """Delete chunked obj:{object_id}:part:{n}:chunk:* and meta keys in Redis for the given parts.

    If parts is None, queries DB to find actual parts for this object (much faster than scanning 256).
    """
    import psycopg

    r = redis.Redis.from_url(redis_url)

    # If no parts specified, query DB for actual part numbers to avoid scanning 256 empty parts
    if parts is None:
        with psycopg.connect(dsn) as conn, conn.cursor() as cur:
            cur.execute(
                """
                SELECT DISTINCT part_number
                FROM parts
                WHERE object_id = %s
                ORDER BY part_number
                """,
                (object_id,),
            )
            parts_list = [row[0] for row in cur.fetchall()]
            # Always include part 1 (the base object part)
            if 1 not in parts_list:
                parts_list.insert(0, 1)
            parts = parts_list

    # Determine current object_version for namespacing
    with psycopg.connect(dsn) as conn, conn.cursor() as cur:
        cur.execute(
            """
            SELECT o.current_object_version
            FROM objects o
            WHERE o.object_id = %s
            LIMIT 1
            """,
            (object_id,),
        )
        row = cur.fetchone()
        object_version = int(row[0]) if row and row[0] is not None else 1

    roc = RedisObjectPartsCache(r)
    for pn in parts:
        # Delete meta first, then all chunk keys (versioned namespace)
        meta_key = roc.build_meta_key(object_id, int(object_version), int(pn))
        r.delete(meta_key)
        # Scan and delete chunk keys using cache key prefix
        base_key = roc.build_key(object_id, int(object_version), int(pn))
        for k in r.scan_iter(match=f"{base_key}:chunk:*"):
            r.delete(k)
    # Sanity: assert no meta remains for requested parts
    for pn in parts:
        meta_key = roc.build_meta_key(object_id, int(object_version), int(pn))
        assert not r.exists(meta_key), f"Part {pn} cache not cleared"


def read_part_from_cache(
    object_id: str,
    part_number: int,
    *,
    redis_url: str = "redis://localhost:6379/0",
    dsn: str = "postgresql://postgres:postgres@localhost:5432/hippius",
) -> Optional[bytes]:
    """Assemble part bytes from chunked cache if present; returns None if missing."""
    r = redis.Redis.from_url(redis_url)
    # Fetch current object_version
    with psycopg.connect(dsn) as conn, conn.cursor() as cur:
        cur.execute(
            """
            SELECT o.current_object_version
            FROM objects o
            WHERE o.object_id = %s
            LIMIT 1
            """,
            (object_id,),
        )
        row = cur.fetchone()
        object_version = int(row[0]) if row and row[0] is not None else 1

    roc = RedisObjectPartsCache(r)
    meta_key = roc.build_meta_key(object_id, int(object_version), int(part_number))
    meta = r.get(meta_key)
    if not meta:
        return None
    import json as _json

    try:
        m = _json.loads(meta)
    except Exception:
        return None
    num_chunks = int(m.get("num_chunks", 0))
    if num_chunks <= 0:
        return b""
    chunks: list[bytes] = []
    for i in range(num_chunks):
        chunk_key = roc.build_chunk_key(object_id, int(object_version), int(part_number), i)
        c = r.get(chunk_key)
        if not isinstance(c, (bytes, bytearray)):
            return None
        chunks.append(bytes(c))
    return b"".join(chunks)


def wait_for_parts_cids(
    bucket_name: str,
    object_key: str,
    *,
    min_count: int,
    backend: str = "ipfs",
    timeout_seconds: float = 20.0,
    dsn: str = "postgresql://postgres:postgres@localhost:5432/hippius",
) -> bool:
    """Wait until at least min_count parts for the object have backend identifiers in chunk_backend.

    In the multi-backend design, upload workers only write to chunk_backend. This helper
    therefore treats an object as "pipeline-ready" when enough parts have *all* their
    chunk_backend rows present for the given backend.

    Returns True if ready within timeout, False otherwise.
    """
    print(
        f"DEBUG: wait_for_parts_cids called for {bucket_name}/{object_key}, "
        f"backend={backend} expecting min_count={min_count}"
    )
    deadline = time.time() + timeout_seconds
    with psycopg.connect(dsn) as conn, conn.cursor() as cur:
        while time.time() < deadline:
            cur.execute(
                """
                WITH chunks AS (
                    SELECT
                        o.object_id,
                        o.current_object_version AS object_version,
                        p.part_number,
                        pc.id AS chunk_id
                    FROM objects o
                    JOIN buckets b ON b.bucket_id = o.bucket_id
                    JOIN parts p ON p.object_id = o.object_id AND p.object_version = o.current_object_version
                    JOIN part_chunks pc ON pc.part_id = p.part_id
                    WHERE b.bucket_name = %s
                      AND o.object_key = %s
                      AND o.deleted_at IS NULL
                ),
                per_part AS (
                    SELECT
                        c.part_number,
                        COUNT(*) AS total_chunks,
                        COUNT(cb.chunk_id) FILTER (
                            WHERE cb.backend = %s
                              AND NOT cb.deleted
                              AND cb.backend_identifier IS NOT NULL
                        ) AS backend_chunks
                    FROM chunks c
                    LEFT JOIN chunk_backend cb ON cb.chunk_id = c.chunk_id
                    GROUP BY c.part_number
                )
                SELECT COUNT(*)
                FROM per_part
                WHERE total_chunks > 0
                  AND backend_chunks = total_chunks
                """,
                (bucket_name, object_key, backend),
            )
            row = cur.fetchone()
            count = int(row[0]) if row else 0
            print(f"DEBUG: Fully-backed parts count: {count} (need {min_count})")
            if count >= min_count:
                print(f"DEBUG: wait_for_parts_cids SUCCESS - found {count} fully-backed parts")
                return True
            print(f"DEBUG: wait_for_parts_cids still waiting... ({count}/{min_count})")
            time.sleep(0.3)
    print(f"DEBUG: wait_for_parts_cids TIMEOUT - only found {count} non-pending parts after {timeout_seconds}s")
    return False


def make_all_object_parts_pending(
    bucket_name: str,
    object_key: str,
    *,
    dsn: str = "postgresql://postgres:postgres@localhost:5432/hippius",
) -> str:
    """Simulate "not yet processed by workers" by clearing backend registrations.

    Returns the object_id of the affected object.
    """
    with psycopg.connect(dsn) as conn, conn.cursor() as cur:
        # First get the object_id
        cur.execute(
            """
            SELECT o.object_id
            FROM objects o
            JOIN buckets b ON b.bucket_id = o.bucket_id
            WHERE b.bucket_name = %s AND o.object_key = %s
            """,
            (bucket_name, object_key),
        )
        row = cur.fetchone()
        if not row:
            raise RuntimeError(f"Object {bucket_name}/{object_key} not found")
        object_id = str(row[0])

        # Mark all backend registrations as deleted so pipeline hydration cannot use them.
        # This models a state where the content is still readable from cache, but workers
        # have not yet registered backend identifiers.
        cur.execute(
            """
            UPDATE chunk_backend cb
               SET deleted = true,
                   deleted_at = now()
              FROM part_chunks pc
              JOIN parts p ON p.part_id = pc.part_id
             WHERE cb.chunk_id = pc.id
               AND p.object_id = %s
               AND NOT cb.deleted
            """,
            (object_id,),
        )

        conn.commit()

        return object_id
