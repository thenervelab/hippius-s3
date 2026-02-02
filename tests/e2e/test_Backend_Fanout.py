"""E2E test verifying upload fans out to both IPFS and Arion backends."""

from typing import Any
from typing import Callable

import psycopg  # type: ignore[import-untyped]
import pytest

from .support.cache import wait_for_all_backends_ready


@pytest.mark.local
def test_upload_fans_out_to_both_backends(
    docker_services: Any,
    boto3_client: Any,
    unique_bucket_name: Callable[[str], str],
    cleanup_buckets: Callable[[str], None],
) -> None:
    """Verify chunk_backend has rows for both ipfs and arion after upload."""
    bucket = unique_bucket_name("fanout-both")
    cleanup_buckets(bucket)
    boto3_client.create_bucket(Bucket=bucket)

    key = "fanout-test.txt"
    boto3_client.put_object(Bucket=bucket, Key=key, Body=b"fanout verification payload")

    assert wait_for_all_backends_ready(bucket, key, min_count=1, timeout_seconds=30.0)

    dsn = "postgresql://postgres:postgres@localhost:5432/hippius"
    with psycopg.connect(dsn) as conn, conn.cursor() as cur:
        cur.execute(
            """
            SELECT DISTINCT cb.backend
            FROM chunk_backend cb
            JOIN part_chunks pc ON pc.id = cb.chunk_id
            JOIN parts p ON p.part_id = pc.part_id
            JOIN objects o ON o.object_id = p.object_id AND o.current_object_version = p.object_version
            JOIN buckets b ON b.bucket_id = o.bucket_id
            WHERE b.bucket_name = %s
              AND o.object_key = %s
              AND NOT cb.deleted
              AND cb.backend_identifier IS NOT NULL
            """,
            (bucket, key),
        )
        backends = {row[0] for row in cur.fetchall()}

    assert {"ipfs", "arion"} <= backends, f"Expected both ipfs and arion in backends, got {backends}"
