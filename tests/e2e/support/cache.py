from __future__ import annotations

import time
from typing import Iterable

import psycopg  # type: ignore[import-untyped]
import redis  # type: ignore[import-untyped]


def get_object_id(
    bucket_name: str, object_key: str, *, dsn: str = "postgresql://postgres:postgres@localhost:5432/hippius"
) -> str:
    """Fetch object_id for a (bucket_name, object_key) pair from Postgres."""
    with psycopg.connect(dsn) as conn, conn.cursor() as cur:
        cur.execute(
            """
                SELECT o.object_id
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
        return str(row[0])


essentially_all_parts = range(0, 256)


def clear_object_cache(
    object_id: str, parts: Iterable[int] | None = None, *, redis_url: str = "redis://localhost:6379/0"
) -> None:
    """Delete obj:{object_id}:part:{n} keys in Redis for the given parts.

    If parts is None, clears a reasonable range (0..255) by default.
    """
    r = redis.Redis.from_url(redis_url)
    for pn in parts or essentially_all_parts:
        r.delete(f"obj:{object_id}:part:{int(pn)}")


def wait_for_parts_cids(
    bucket_name: str,
    object_key: str,
    *,
    min_count: int,
    timeout_seconds: float = 20.0,
    dsn: str = "postgresql://postgres:postgres@localhost:5432/hippius",
) -> bool:
    """Wait until at least min_count parts for the object have non-pending ipfs_cid values.

    This includes both the base part (from objects table) and appended parts (from parts table).

    Returns True if ready within timeout, False otherwise.
    """
    deadline = time.time() + timeout_seconds
    with psycopg.connect(dsn) as conn, conn.cursor() as cur:
        while time.time() < deadline:
            cur.execute(
                """
                    SELECT COUNT(*)
                    FROM (
                        SELECT COALESCE(c.cid, o.ipfs_cid) as cid
                        FROM objects o
                        LEFT JOIN cids c ON o.cid_id = c.id
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
            if count >= min_count:
                return True
            time.sleep(0.3)
    return False


def make_all_object_parts_pending(
    bucket_name: str,
    object_key: str,
    *,
    dsn: str = "postgresql://postgres:postgres@localhost:5432/hippius",
) -> str:
    """Set all parts for an object to pending state (both base and appended parts).

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

        # Set all parts to pending (both base and appended)
        cur.execute(
            """
            UPDATE parts
            SET cid_id = NULL, ipfs_cid = 'pending'
            WHERE object_id = %s
            """,
            (object_id,),
        )

        # Also set object-level CID to pending
        cur.execute(
            """
            UPDATE objects
            SET cid_id = NULL, ipfs_cid = 'pending'
            WHERE object_id = %s
            """,
            (object_id,),
        )

        conn.commit()

        return object_id
