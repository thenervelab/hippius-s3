"""Database helpers for E2E tests."""

from __future__ import annotations

import os
from typing import Any

import psycopg  # type: ignore[import-untyped]


def get_keystore_dsn() -> str:
    """Get the keystore database DSN from environment."""
    return os.environ.get(
        "HIPPIUS_KEYSTORE_DATABASE_URL",
        "postgresql://postgres:postgres@localhost:5432/hippius",
    )


def get_main_dsn() -> str:
    """Get the main database DSN from environment."""
    return os.environ.get(
        "DATABASE_URL",
        "postgresql://postgres:postgres@localhost:5432/hippius",
    )


def get_bucket_id(bucket_name: str, *, dsn: str | None = None) -> str:
    """Get bucket_id for a bucket name."""
    dsn = dsn or get_main_dsn()
    with psycopg.connect(dsn) as conn, conn.cursor() as cur:
        cur.execute(
            "SELECT bucket_id FROM buckets WHERE bucket_name = %s",
            (bucket_name,),
        )
        row = cur.fetchone()
        if not row:
            raise RuntimeError(f"Bucket {bucket_name} not found")
        return str(row[0])


def query_bucket_keks(bucket_name: str, *, keystore_dsn: str | None = None, main_dsn: str | None = None) -> dict[str, Any]:
    """Query keystore DB for bucket's active KEK row.

    Returns dict with keys:
      - kek_id: UUID of the KEK
      - wrapped_kek_bytes: The wrapped (encrypted) KEK bytes
      - kms_key_id: The KMS key ID used to wrap
      - status: KEK status (should be 'active')
    """
    keystore_dsn = keystore_dsn or get_keystore_dsn()
    main_dsn = main_dsn or get_main_dsn()

    # First get the bucket_id from main DB
    bucket_id = get_bucket_id(bucket_name, dsn=main_dsn)

    with psycopg.connect(keystore_dsn) as conn, conn.cursor() as cur:
        cur.execute(
            """
            SELECT kek_id, wrapped_kek_bytes, kms_key_id, status
            FROM bucket_keks
            WHERE bucket_id = %s AND status = 'active'
            ORDER BY created_at DESC
            LIMIT 1
            """,
            (bucket_id,),
        )
        row = cur.fetchone()
        if not row:
            raise RuntimeError(f"No active KEK found for bucket {bucket_name}")

        return {
            "kek_id": str(row[0]),
            "wrapped_kek_bytes": bytes(row[1]) if row[1] else None,
            "kms_key_id": row[2],
            "status": row[3],
        }


def clear_kek_cache() -> None:
    """Clear KEK cache.

    Note: The KEK cache is in-memory within the API/worker processes.
    For E2E tests, we need to restart services or use a Redis-based cache
    to clear it. This function is a placeholder that documents the limitation.

    For now, we rely on the 300s TTL expiry or service restart.
    """
    # In-memory cache cannot be cleared from outside the process.
    # To force cache miss in E2E tests:
    # 1. Wait for TTL (300s by default)
    # 2. Restart the service
    # 3. Use a very short TTL in test config
    pass


def get_object_versioning_info(bucket_name: str, object_key: str, *, dsn: str | None = None) -> dict[str, Any]:
    """Get object versioning information for testing.

    Returns dict with keys:
      - object_id: UUID of the object
      - current_object_version: Current version number
      - versions: List of tuples (object_version, size_bytes, md5_hash)
      - part_counts: List of tuples (object_version, part_count)
    """
    dsn = dsn or get_main_dsn()
    with psycopg.connect(dsn) as conn, conn.cursor() as cur:
        # Get object_id and current version
        cur.execute(
            """
            SELECT object_id, current_object_version
            FROM objects
            WHERE bucket_id = (SELECT bucket_id FROM buckets WHERE bucket_name = %s)
              AND object_key = %s
            """,
            (bucket_name, object_key),
        )
        obj_row = cur.fetchone()
        if not obj_row:
            raise RuntimeError(f"Object {bucket_name}/{object_key} not found")

        object_id, current_version = obj_row

        # Get all versions
        cur.execute(
            "SELECT object_version, size_bytes, md5_hash FROM object_versions WHERE object_id = %s ORDER BY object_version",
            (object_id,),
        )
        versions = cur.fetchall()

        # Get part counts per version
        cur.execute(
            "SELECT object_version, COUNT(*) FROM parts WHERE object_id = %s GROUP BY object_version ORDER BY object_version",
            (object_id,),
        )
        part_counts = cur.fetchall()

        return {
            "object_id": str(object_id),
            "current_object_version": int(current_version),
            "versions": versions,
            "part_counts": part_counts,
        }
