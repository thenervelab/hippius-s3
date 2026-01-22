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
