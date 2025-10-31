from __future__ import annotations

import os
import time
from typing import Iterable
from typing import Optional

import psycopg  # type: ignore[import-untyped]
import redis  # type: ignore[import-untyped]

from hippius_s3.cache import RedisObjectPartsCache


def get_object_id_and_version(bucket_name: str, object_key: str, *, dsn: Optional[str] = None) -> tuple[str, int]:
    """Fetch (object_id, current_object_version) for a (bucket_name, object_key)."""
    resolved_dsn = dsn or os.environ.get("DATABASE_URL", "postgresql://postgres:postgres@localhost:5432/hippius")
    with psycopg.connect(resolved_dsn) as conn, conn.cursor() as cur:
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


def get_object_id(bucket_name: str, object_key: str, *, dsn: Optional[str] = None) -> str:
    """Back-compat helper: return only object_id."""
    oid, _ver = get_object_id_and_version(bucket_name, object_key, dsn=dsn)
    return oid


def get_object_cids(
    bucket_name: str,
    object_key: str,
    *,
    dsn: Optional[str] = None,
) -> tuple[str, int, str, list[str], Optional[str]]:
    """Get all CIDs for an object including parts, chunks, and manifest.

    Returns: (object_id, object_version, main_account_id, part_cids, manifest_cid)
    """
    resolved_dsn = dsn or os.environ.get("DATABASE_URL", "postgresql://postgres:postgres@localhost:5432/hippius")
    with psycopg.connect(resolved_dsn) as conn, conn.cursor() as cur:
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
        manifest_row = cur.fetchone()
        manifest_cid = str(manifest_row[0]) if manifest_row else None

        return object_id, object_version, main_account_id, part_cids + chunk_cids, manifest_cid


essentially_all_parts = range(0, 256)


def clear_object_cache(
    object_id: str,
    parts: Iterable[int] | None = None,
    *,
    redis_url: str = "redis://localhost:6379/0",
    dsn: Optional[str] = None,
) -> None:
    """Delete chunked obj:{object_id}:part:{n}:chunk:* and meta keys in Redis for the given parts.

    If parts is None, queries DB to find actual parts for this object (much faster than scanning 256).
    """
    import psycopg

    r = redis.Redis.from_url(redis_url)

    # If no parts specified, query DB for actual part numbers to avoid scanning 256 empty parts
    resolved_dsn = dsn or os.environ.get("DATABASE_URL", "postgresql://postgres:postgres@localhost:5432/hippius")
    if parts is None:
        with psycopg.connect(resolved_dsn) as conn, conn.cursor() as cur:
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
    with psycopg.connect(resolved_dsn) as conn, conn.cursor() as cur:
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
    dsn: Optional[str] = None,
) -> Optional[bytes]:
    """Assemble part bytes from chunked cache if present; returns None if missing."""
    r = redis.Redis.from_url(redis_url)
    # Fetch current object_version
    resolved_dsn = dsn or os.environ.get("DATABASE_URL", "postgresql://postgres:postgres@localhost:5432/hippius")
    with psycopg.connect(resolved_dsn) as conn, conn.cursor() as cur:
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
        m = _json.loads(meta.decode("utf-8") if isinstance(meta, (bytes, bytearray)) else str(meta))
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
    timeout_seconds: float = 20.0,
    dsn: Optional[str] = None,
) -> bool:
    """Wait until at least min_count parts for the object have non-pending ipfs_cid values.

    This includes both the base part (from objects table) and appended parts (from parts table).

    Returns True if ready within timeout, False otherwise.
    """
    print(f"DEBUG: wait_for_parts_cids called for {bucket_name}/{object_key}, expecting min_count={min_count}")
    deadline = time.time() + timeout_seconds
    resolved_dsn = dsn or os.environ.get("DATABASE_URL", "postgresql://postgres:postgres@localhost:5432/hippius")
    with psycopg.connect(resolved_dsn) as conn, conn.cursor() as cur:
        while time.time() < deadline:
            # First, get object_id for debugging
            cur.execute(
                """
                SELECT o.object_id, ov.ipfs_cid, c.cid as object_cid
                FROM objects o
                JOIN object_versions ov ON ov.object_id = o.object_id AND ov.object_version = o.current_object_version
                LEFT JOIN cids c ON ov.cid_id = c.id
                JOIN buckets b ON b.bucket_id = o.bucket_id
                WHERE b.bucket_name = %s AND o.object_key = %s
                """,
                (bucket_name, object_key),
            )
            obj_row = cur.fetchone()
            print(f"DEBUG: Object row: {obj_row}")

            # Get parts for debugging
            cur.execute(
                """
                SELECT p.part_number, p.ipfs_cid, p.size_bytes, p.etag, c.cid as part_cid
                FROM parts p
                LEFT JOIN cids c ON p.cid_id = c.id
                JOIN object_versions ov ON p.object_version = ov.object_version AND ov.object_id = p.object_id
                JOIN objects o ON o.object_id = ov.object_id
                JOIN buckets b ON b.bucket_id = o.bucket_id
                WHERE b.bucket_name = %s AND o.object_key = %s
                ORDER BY p.part_number
                """,
                (bucket_name, object_key),
            )
            parts_rows = cur.fetchall()
            print(f"DEBUG: Parts rows: {parts_rows}")

            cur.execute(
                """
                    SELECT COUNT(*)
                    FROM (
                        SELECT COALESCE(c.cid, ov.ipfs_cid) as cid
                        FROM objects o
                        JOIN object_versions ov ON ov.object_id = o.object_id AND ov.object_version = o.current_object_version
                        LEFT JOIN cids c ON ov.cid_id = c.id
                        JOIN buckets b ON b.bucket_id = o.bucket_id
                        WHERE b.bucket_name = %s AND o.object_key = %s

                        UNION ALL

                        SELECT COALESCE(c.cid, p.ipfs_cid) as cid
                        FROM parts p
                        LEFT JOIN cids c ON p.cid_id = c.id
                        JOIN objects o ON o.object_id = p.object_id
                        JOIN buckets b ON b.bucket_id = o.bucket_id
                        WHERE b.bucket_name = %s AND o.object_key = %s
                    ) AS all_parts
                    WHERE COALESCE(NULLIF(TRIM(cid), ''), 'pending') <> 'pending'
                    """,
                (bucket_name, object_key, bucket_name, object_key),
            )
            row = cur.fetchone()
            count = int(row[0]) if row else 0
            print(f"DEBUG: Non-pending parts count: {count} (need {min_count})")
            if count >= min_count:
                print(f"DEBUG: wait_for_parts_cids SUCCESS - found {count} non-pending parts")
                return True
            print(f"DEBUG: wait_for_parts_cids still waiting... ({count}/{min_count})")
            time.sleep(0.3)
    print(f"DEBUG: wait_for_parts_cids TIMEOUT - only found {count} non-pending parts after {timeout_seconds}s")
    return False


def wait_for_chunk_cids(
    bucket_name: str,
    object_key: str,
    *,
    timeout_seconds: float = 30.0,
    dsn: Optional[str] = None,
) -> bool:
    """Wait until all expected chunk CIDs (in part_chunks) are non-pending for the object.

    Computes expected total chunks as ceil(size_bytes / chunk_size_bytes) per part
    and polls part_chunks until that many non-pending CIDs exist.
    """
    import math as _math

    print(f"DEBUG: wait_for_chunk_cids called for {bucket_name}/{object_key}")
    deadline = time.time() + timeout_seconds
    resolved_dsn = dsn or os.environ.get("DATABASE_URL", "postgresql://postgres:postgres@localhost:5432/hippius")
    with psycopg.connect(resolved_dsn) as conn, conn.cursor() as cur:
        expected_total = 0
        while time.time() < deadline:
            # Load per-part sizes and chunk sizes
            cur.execute(
                """
                SELECT p.part_id, p.part_number, p.size_bytes, p.chunk_size_bytes
                FROM parts p
                JOIN objects o ON o.object_id = p.object_id
                JOIN buckets b ON b.bucket_id = o.bucket_id
                WHERE b.bucket_name = %s AND o.object_key = %s
                ORDER BY p.part_number
                """,
                (bucket_name, object_key),
            )
            parts = cur.fetchall() or []
            expected_total = 0
            for _part_id, _pn, size_bytes, chunk_size_bytes in parts:
                try:
                    sb = int(size_bytes or 0)
                    cs = int(chunk_size_bytes or 0)
                    if sb > 0 and cs > 0:
                        expected_total += int(_math.ceil(sb / float(cs)))
                except Exception:
                    continue

            # Count realized chunk CIDs
            cur.execute(
                """
                SELECT COUNT(*)
                FROM part_chunks pc
                JOIN parts p ON p.part_id = pc.part_id
                JOIN objects o ON o.object_id = p.object_id
                JOIN buckets b ON b.bucket_id = o.bucket_id
                WHERE b.bucket_name = %s AND o.object_key = %s
                  AND pc.cid IS NOT NULL AND pc.cid <> 'pending'
                """,
                (bucket_name, object_key),
            )
            row = cur.fetchone()
            have = int(row[0]) if row else 0
            print(f"DEBUG: non-pending chunk_cids: {have} / expected {expected_total}")
            if expected_total > 0 and have >= expected_total:
                print("DEBUG: wait_for_chunk_cids SUCCESS")
                return True
            time.sleep(0.3)
    print(
        f"DEBUG: wait_for_chunk_cids TIMEOUT - only found {have} of {expected_total} non-pending chunks after {timeout_seconds}s"
    )
    return False


def make_all_object_parts_pending(
    bucket_name: str,
    object_key: str,
    *,
    dsn: Optional[str] = None,
) -> str:
    """Set all parts for an object to pending state (both base and appended parts).

    Returns the object_id of the affected object.
    """
    resolved_dsn = dsn or os.environ.get("DATABASE_URL", "postgresql://postgres:postgres@localhost:5432/hippius")
    with psycopg.connect(resolved_dsn) as conn, conn.cursor() as cur:
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

        # Set all parts to pending (both base and appended)
        cur.execute(
            """
            UPDATE parts
            SET cid_id = NULL, ipfs_cid = 'pending'
            WHERE object_id = %s
            """,
            (object_id,),
        )

        # Also set object-level CID to pending (in object_versions table) for current version
        cur.execute(
            """
            UPDATE object_versions ov
               SET cid_id = NULL, ipfs_cid = 'pending'
            FROM objects o
            WHERE ov.object_id = o.object_id
              AND ov.object_id = %s
              AND o.object_id = %s
              AND ov.object_version = o.current_object_version
            """,
            (object_id, object_id),
        )

        conn.commit()

        return object_id
