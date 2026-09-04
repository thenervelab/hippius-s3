"""Database helpers for E2E tests."""

from __future__ import annotations

import os
from typing import Any

import psycopg  # type: ignore[import-untyped]

from .dsn import DEFAULT_DSN


def get_keystore_dsn() -> str:
    """Get the keystore database DSN from environment."""
    return os.environ.get(
        "HIPPIUS_KEYSTORE_DATABASE_URL",
        DEFAULT_DSN,
    )


def get_main_dsn() -> str:
    """Get the main database DSN from environment."""
    return os.environ.get(
        "DATABASE_URL",
        DEFAULT_DSN,
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


def query_bucket_keks(
    bucket_name: str, *, keystore_dsn: str | None = None, main_dsn: str | None = None
) -> dict[str, Any]:
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
            SELECT o.object_id, o.current_object_version
            FROM buckets b
            JOIN objects o ON o.object_id = resolve_object_id(b.bucket_id, %s)
            WHERE b.bucket_name = %s
            """,
            (object_key, bucket_name),
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


def get_multipart_upload_version(upload_id: str, *, dsn: str | None = None) -> tuple[str, int]:
    """`(object_id, object_version)` an in-flight MPU's parts live under, keyed by upload_id.

    Resolved from `parts` rather than `objects.current_object_version` for the same reason the
    abort handler does (get_multipart_version_by_upload.sql): a concurrent same-key upload
    advances the pointer. Must be called BEFORE abort — abort cascades the parts rows away.
    """
    dsn = dsn or get_main_dsn()
    with psycopg.connect(dsn) as conn, conn.cursor() as cur:
        cur.execute(
            "SELECT object_id, object_version FROM parts WHERE upload_id = %s ORDER BY object_version DESC LIMIT 1",
            (upload_id,),
        )
        row = cur.fetchone()
        if not row:
            raise RuntimeError(f"No parts found for upload {upload_id}")
        return str(row[0]), int(row[1])


def count_residency_rows(node_id: str, object_id: str, object_version: int, *, dsn: str | None = None) -> int:
    """Rows in the drain's per-node SSD ledger for one version on one node."""
    dsn = dsn or get_main_dsn()
    with psycopg.connect(dsn) as conn, conn.cursor() as cur:
        cur.execute(
            "SELECT COUNT(*) FROM cephor_ssd_residency WHERE node_id = %s AND object_id = %s AND version = %s",
            (node_id, object_id, object_version),
        )
        row = cur.fetchone()
        return int(row[0]) if row else 0
