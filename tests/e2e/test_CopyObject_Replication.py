"""E2E: a CopyObject destination must become drain-eligible and reach the backend (C1)."""

import time
from typing import Any
from typing import Callable

import psycopg  # type: ignore[import-untyped]
import pytest

from .support.cache import get_object_id_and_version
from .support.cache import wait_for_all_backends_ready
from .support.dsn import DEFAULT_DSN


DSN = DEFAULT_DSN


def _address_for(object_id: str, object_version: int) -> str | None:
    with psycopg.connect(DSN) as conn, conn.cursor() as cur:
        cur.execute(
            "SELECT address FROM object_versions WHERE object_id = %s AND object_version = %s",
            (object_id, object_version),
        )
        row = cur.fetchone()
        return None if row is None else row[0]


def _wait_for_address(object_id: str, object_version: int, *, timeout_seconds: float = 15.0) -> str | None:
    deadline = time.time() + timeout_seconds
    while time.time() < deadline:
        address = _address_for(object_id, object_version)
        if address:
            return str(address)
        time.sleep(0.25)
    return None


def _backend_row_count(object_id: str, object_version: int) -> int:
    with psycopg.connect(DSN) as conn, conn.cursor() as cur:
        cur.execute(
            """
            SELECT COUNT(*)
            FROM chunk_backend cb
            JOIN part_chunks pc ON pc.id = cb.chunk_id
            JOIN parts p ON p.part_id = pc.part_id
            WHERE p.object_id = %s
              AND p.object_version = %s
              AND NOT cb.deleted
              AND cb.backend_identifier IS NOT NULL
            """,
            (object_id, object_version),
        )
        row = cur.fetchone()
        return 0 if row is None else int(row[0])


@pytest.mark.local
def test_copy_destination_replicates_to_backend(
    docker_services: Any,
    boto3_client: Any,
    unique_bucket_name: Callable[[str], str],
    cleanup_buckets: Callable[[str], None],
) -> None:
    """Same-bucket CopyObject aliases dest onto the source object_id.

    Dest is not a new drain target: it shares source parts and backends.
    Dest GET must still resolve and the shared object must stay backend-backed.
    """
    bucket = unique_bucket_name("copy-replication")
    cleanup_buckets(bucket)
    boto3_client.create_bucket(Bucket=bucket)

    src_key = "copy-src.txt"
    dst_key = "copy-dst.txt"
    body = b"copy replication payload"

    boto3_client.put_object(Bucket=bucket, Key=src_key, Body=body)
    assert wait_for_all_backends_ready(bucket, src_key, min_count=1, timeout_seconds=60.0)

    boto3_client.copy_object(Bucket=bucket, Key=dst_key, CopySource=f"/{bucket}/{src_key}")

    src_object_id, src_object_version = get_object_id_and_version(bucket, src_key)
    dst_object_id, dst_object_version = get_object_id_and_version(bucket, dst_key)
    assert dst_object_id == src_object_id
    assert dst_object_version == src_object_version

    # The address is what makes the (shared) object visible to the Rust drain's enqueue sweep.
    address = _wait_for_address(dst_object_id, dst_object_version)
    assert address, f"destination {dst_object_id} v{dst_object_version} has no object_versions.address"

    assert wait_for_all_backends_ready(bucket, dst_key, min_count=1, timeout_seconds=90.0), (
        f"destination {dst_object_id} v{dst_object_version} never got chunk_backend rows "
        f"(count={_backend_row_count(dst_object_id, dst_object_version)}) — data exists only in the FS cache"
    )

    assert _backend_row_count(dst_object_id, dst_object_version) > 0

    assert boto3_client.get_object(Bucket=bucket, Key=dst_key)["Body"].read() == body


@pytest.mark.local
def test_copy_overwrite_streams_and_replicates_to_backend(
    docker_services: Any,
    boto3_client: Any,
    unique_bucket_name: Callable[[str], str],
    cleanup_buckets: Callable[[str], None],
) -> None:
    """Copy onto a live dest primary still streams; the new dest version must drain.

    A streamed copy that only lands in the FS cache is silent data loss: the drain
    never enqueues it.
    """
    bucket = unique_bucket_name("copy-overwrite-repl")
    cleanup_buckets(bucket)
    boto3_client.create_bucket(Bucket=bucket)

    src_key = "copy-src.txt"
    dst_key = "copy-dst.txt"
    src_body = b"copy overwrite payload"
    old_body = b"old dest payload"

    boto3_client.put_object(Bucket=bucket, Key=src_key, Body=src_body)
    boto3_client.put_object(Bucket=bucket, Key=dst_key, Body=old_body)
    assert wait_for_all_backends_ready(bucket, src_key, min_count=1, timeout_seconds=60.0)
    assert wait_for_all_backends_ready(bucket, dst_key, min_count=1, timeout_seconds=60.0)

    src_object_id, _src_ver = get_object_id_and_version(bucket, src_key)
    dst_id_before, dst_ver_before = get_object_id_and_version(bucket, dst_key)
    assert dst_id_before != src_object_id

    boto3_client.copy_object(Bucket=bucket, Key=dst_key, CopySource=f"/{bucket}/{src_key}")

    dst_object_id, dst_object_version = get_object_id_and_version(bucket, dst_key)
    assert dst_object_id == dst_id_before
    assert dst_object_id != src_object_id
    assert dst_object_version > dst_ver_before

    address = _wait_for_address(dst_object_id, dst_object_version)
    assert address, f"destination {dst_object_id} v{dst_object_version} has no object_versions.address"

    assert wait_for_all_backends_ready(bucket, dst_key, min_count=1, timeout_seconds=90.0), (
        f"destination {dst_object_id} v{dst_object_version} never got chunk_backend rows "
        f"(count={_backend_row_count(dst_object_id, dst_object_version)}) — data exists only in the FS cache"
    )

    assert _backend_row_count(dst_object_id, dst_object_version) > 0
    assert boto3_client.get_object(Bucket=bucket, Key=dst_key)["Body"].read() == src_body
