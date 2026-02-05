"""E2E stress test for concurrent PutObject operations.

This test verifies that the database lock contention fix works by uploading
many files concurrently. Previously, the PUT endpoint held a transaction open
for the entire upload duration, causing lock contention under concurrent uploads.

Run with default settings (10 x 1MB files):
    pytest tests/e2e/test_PutObject_Concurrent.py -xvs

Run full stress test (30 x 100MB files):
    STRESS_FILE_COUNT=30 STRESS_FILE_SIZE_MB=100 pytest tests/e2e/test_PutObject_Concurrent.py -xvs
"""

import os
import secrets
import time
from concurrent.futures import ThreadPoolExecutor
from concurrent.futures import as_completed
from typing import Any
from typing import Callable

import pytest


@pytest.mark.local
def test_concurrent_uploads_no_lock_contention(
    docker_services: Any,
    boto3_client: Any,
    unique_bucket_name: Callable[[str], str],
    cleanup_buckets: Callable[[str], None],
) -> None:
    """Upload many files concurrently to verify no database lock contention.

    This test would fail before the fix because:
    - Each PUT held a transaction open for the entire upload duration
    - Concurrent uploads blocked waiting for row locks on the objects table
    - After ~30 seconds, blocked transactions would timeout with HTTP 500

    After the fix:
    - The upsert_object_basic query is atomic (uses CTEs)
    - No transaction wrapper means locks are held only for milliseconds
    - All concurrent uploads should succeed
    """
    file_count = int(os.getenv("STRESS_FILE_COUNT", "10"))
    file_size_mb = int(os.getenv("STRESS_FILE_SIZE_MB", "1"))
    max_workers = int(os.getenv("STRESS_MAX_WORKERS", "10"))

    file_size_bytes = file_size_mb * 1024 * 1024

    bucket_name = unique_bucket_name("concurrent-upload")
    cleanup_buckets(bucket_name)

    boto3_client.create_bucket(Bucket=bucket_name)

    test_files: list[tuple[str, bytes]] = []
    for i in range(file_count):
        key = f"file-{i:03d}.bin"
        content = secrets.token_bytes(file_size_bytes)
        test_files.append((key, content))

    results: dict[str, dict[str, Any]] = {}
    errors: list[str] = []

    def upload_file(key: str, content: bytes) -> dict[str, Any]:
        start = time.monotonic()
        try:
            response = boto3_client.put_object(
                Bucket=bucket_name,
                Key=key,
                Body=content,
                ContentType="application/octet-stream",
            )
            elapsed = time.monotonic() - start
            return {
                "key": key,
                "success": True,
                "etag": response.get("ETag"),
                "elapsed_seconds": elapsed,
            }
        except Exception as e:
            elapsed = time.monotonic() - start
            return {
                "key": key,
                "success": False,
                "error": str(e),
                "elapsed_seconds": elapsed,
            }

    print(f"\nUploading {file_count} x {file_size_mb}MB files with {max_workers} workers...")
    start_time = time.monotonic()

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {
            executor.submit(upload_file, key, content): key
            for key, content in test_files
        }

        for future in as_completed(futures):
            key = futures[future]
            result = future.result()
            results[key] = result

            if result["success"]:
                print(f"  {key}: OK ({result['elapsed_seconds']:.2f}s)")
            else:
                print(f"  {key}: FAILED - {result['error']}")
                errors.append(f"{key}: {result['error']}")

    total_time = time.monotonic() - start_time
    successful = sum(1 for r in results.values() if r["success"])
    failed = len(results) - successful

    print(f"\nResults: {successful}/{len(results)} succeeded in {total_time:.2f}s")
    if failed > 0:
        print(f"Failures: {failed}")
        for err in errors:
            print(f"  - {err}")

    avg_time = sum(r["elapsed_seconds"] for r in results.values()) / len(results)
    print(f"Average upload time: {avg_time:.2f}s")

    assert failed == 0, f"{failed} uploads failed: {errors}"

    print("\nVerifying all files are readable...")
    for key, original_content in test_files:
        response = boto3_client.get_object(Bucket=bucket_name, Key=key)
        downloaded = response["Body"].read()
        assert len(downloaded) == len(original_content), f"{key}: size mismatch"


@pytest.mark.local
def test_concurrent_uploads_same_key_overwrites(
    docker_services: Any,
    boto3_client: Any,
    unique_bucket_name: Callable[[str], str],
    cleanup_buckets: Callable[[str], None],
) -> None:
    """Upload to the same key concurrently to test version allocation under contention.

    This stresses the ON CONFLICT DO UPDATE path where multiple transactions
    compete to update the same object row.
    """
    overwrite_count = int(os.getenv("STRESS_OVERWRITE_COUNT", "20"))
    file_size_kb = int(os.getenv("STRESS_OVERWRITE_SIZE_KB", "100"))
    max_workers = int(os.getenv("STRESS_MAX_WORKERS", "10"))

    file_size_bytes = file_size_kb * 1024

    bucket_name = unique_bucket_name("concurrent-overwrite")
    cleanup_buckets(bucket_name)

    boto3_client.create_bucket(Bucket=bucket_name)

    target_key = "contested-file.bin"
    contents: list[bytes] = [secrets.token_bytes(file_size_bytes) for _ in range(overwrite_count)]

    results: list[dict[str, Any]] = []

    def upload_version(idx: int, content: bytes) -> dict[str, Any]:
        start = time.monotonic()
        try:
            response = boto3_client.put_object(
                Bucket=bucket_name,
                Key=target_key,
                Body=content,
                ContentType="application/octet-stream",
            )
            elapsed = time.monotonic() - start
            return {
                "idx": idx,
                "success": True,
                "etag": response.get("ETag"),
                "elapsed_seconds": elapsed,
            }
        except Exception as e:
            elapsed = time.monotonic() - start
            return {
                "idx": idx,
                "success": False,
                "error": str(e),
                "elapsed_seconds": elapsed,
            }

    print(f"\nUploading {overwrite_count} versions to same key with {max_workers} workers...")
    start_time = time.monotonic()

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = [
            executor.submit(upload_version, i, content)
            for i, content in enumerate(contents)
        ]

        for future in as_completed(futures):
            result = future.result()
            results.append(result)
            status = "OK" if result["success"] else f"FAILED: {result.get('error')}"
            print(f"  version {result['idx']}: {status} ({result['elapsed_seconds']:.2f}s)")

    total_time = time.monotonic() - start_time
    successful = sum(1 for r in results if r["success"])
    failed = len(results) - successful

    print(f"\nResults: {successful}/{len(results)} succeeded in {total_time:.2f}s")

    assert failed == 0, f"{failed} overwrites failed"

    response = boto3_client.get_object(Bucket=bucket_name, Key=target_key)
    final_content = response["Body"].read()
    assert len(final_content) == file_size_bytes, "Final content size mismatch"
    print(f"Final object readable, size: {len(final_content)} bytes")
