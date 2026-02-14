"""E2E tests for KMS-wrapped KEK encryption.

These tests verify that:
1. KEKs are wrapped (encrypted) before storage in the database
2. Objects can be written and read correctly with KMS-wrapped KEKs
3. The system fails closed when KMS is unavailable
4. Cached KEKs allow operations during brief KMS outages

Note: These tests require the mock-kms service running with proper certificates.
KMS is always enabled (mandatory).
"""

from __future__ import annotations

import os
from typing import Any
from typing import Callable

import pytest


@pytest.mark.local
def test_kek_stored_wrapped_not_plaintext(
    docker_services: Any,
    boto3_client: Any,
    unique_bucket_name: Callable[[str], str],
    cleanup_buckets: Callable[[str], None],
) -> None:
    """Verify KEK in database is wrapped (not plaintext 32 bytes).

    When KMS is enabled, the bucket_keks table should contain:
    - wrapped_kek_bytes: The KMS-encrypted KEK (not raw 32 bytes)
    - kms_key_id: The KMS key ID used for wrapping
    """
    from .support.cache import wait_for_all_backends_ready
    from .support.db import query_bucket_keks

    bucket = unique_bucket_name("kms-wrap")
    cleanup_buckets(bucket)

    boto3_client.create_bucket(Bucket=bucket)
    boto3_client.put_object(Bucket=bucket, Key="test.txt", Body=b"hello world")

    # Wait for async processing
    assert wait_for_all_backends_ready(bucket, "test.txt", min_count=1, timeout_seconds=30)

    # Query keystore DB directly
    kek_row = query_bucket_keks(bucket)

    # With KMS, we should have wrapped_kek_bytes
    assert "wrapped_kek_bytes" in kek_row, "Expected wrapped_kek_bytes column (KMS schema)"
    wrapped_bytes = kek_row["wrapped_kek_bytes"]

    # Wrapped bytes should exist and be non-empty
    assert wrapped_bytes is not None, "wrapped_kek_bytes should not be None"
    assert len(wrapped_bytes) > 0, "wrapped_kek_bytes should not be empty"

    # KMS key ID should be recorded
    assert "kms_key_id" in kek_row, "Expected kms_key_id column"
    assert kek_row["kms_key_id"], "kms_key_id should not be empty"


@pytest.mark.local
def test_object_readable_after_kek_wrapped(
    docker_services: Any,
    boto3_client: Any,
    unique_bucket_name: Callable[[str], str],
    cleanup_buckets: Callable[[str], None],
) -> None:
    """Full round-trip: write with wrapped KEK, read back correctly.

    This tests the entire flow:
    1. Create bucket → generates KEK → wraps via KMS → stores wrapped
    2. Write object → uses plaintext KEK (from unwrap) → encrypts DEK → encrypts data
    3. Read object → fetches wrapped KEK → unwraps via KMS → decrypts DEK → decrypts data
    """
    from .support.cache import wait_for_all_backends_ready

    bucket = unique_bucket_name("kms-roundtrip")
    cleanup_buckets(bucket)
    content = b"KMS encryption test content " * 1000  # ~29KB

    boto3_client.create_bucket(Bucket=bucket)
    boto3_client.put_object(Bucket=bucket, Key="data.bin", Body=content)

    assert wait_for_all_backends_ready(bucket, "data.bin", min_count=1, timeout_seconds=30)

    # Read should work (KMS unwraps KEK → unwrap DEK → decrypt)
    response = boto3_client.get_object(Bucket=bucket, Key="data.bin")
    retrieved = response["Body"].read()
    assert retrieved == content


@pytest.mark.local
def test_large_object_with_kms_wrapped_kek(
    docker_services: Any,
    boto3_client: Any,
    unique_bucket_name: Callable[[str], str],
    cleanup_buckets: Callable[[str], None],
) -> None:
    """Test larger object (multiple chunks) with KMS-wrapped KEK."""
    from .support.cache import wait_for_all_backends_ready

    bucket = unique_bucket_name("kms-large")
    cleanup_buckets(bucket)

    # Ensure body spans multiple chunks regardless of configured chunk size
    chunk_size = int(os.environ.get("HIPPIUS_CHUNK_SIZE_BYTES", 16 * 1024 * 1024))
    content = os.urandom(chunk_size * 2 + 123)

    boto3_client.create_bucket(Bucket=bucket)
    boto3_client.put_object(Bucket=bucket, Key="large.bin", Body=content)

    assert wait_for_all_backends_ready(bucket, "large.bin", min_count=1, timeout_seconds=60)

    # Read back and verify
    response = boto3_client.get_object(Bucket=bucket, Key="large.bin")
    retrieved = response["Body"].read()
    assert retrieved == content


@pytest.mark.local
def test_multiple_buckets_different_keks(
    docker_services: Any,
    boto3_client: Any,
    unique_bucket_name: Callable[[str], str],
    cleanup_buckets: Callable[[str], None],
) -> None:
    """Each bucket gets its own KEK, all wrapped by same KMS key."""
    from .support.cache import wait_for_all_backends_ready
    from .support.db import query_bucket_keks

    bucket1 = unique_bucket_name("kms-multi-1")
    bucket2 = unique_bucket_name("kms-multi-2")
    cleanup_buckets(bucket1)
    cleanup_buckets(bucket2)

    boto3_client.create_bucket(Bucket=bucket1)
    boto3_client.create_bucket(Bucket=bucket2)

    boto3_client.put_object(Bucket=bucket1, Key="file1.txt", Body=b"bucket1 content")
    boto3_client.put_object(Bucket=bucket2, Key="file2.txt", Body=b"bucket2 content")

    assert wait_for_all_backends_ready(bucket1, "file1.txt", min_count=1, timeout_seconds=30)
    assert wait_for_all_backends_ready(bucket2, "file2.txt", min_count=1, timeout_seconds=30)

    # Get KEKs for both buckets
    kek1 = query_bucket_keks(bucket1)
    kek2 = query_bucket_keks(bucket2)

    # Different KEK IDs
    assert kek1["kek_id"] != kek2["kek_id"], "Each bucket should have unique KEK"

    # Same KMS key ID (both wrapped by same master key)
    assert kek1["kms_key_id"] == kek2["kms_key_id"], "Both should use same KMS master key"

    # Both should be readable
    resp1 = boto3_client.get_object(Bucket=bucket1, Key="file1.txt")
    resp2 = boto3_client.get_object(Bucket=bucket2, Key="file2.txt")

    assert resp1["Body"].read() == b"bucket1 content"
    assert resp2["Body"].read() == b"bucket2 content"


@pytest.mark.local
def test_overwrite_object_reuses_kek(
    docker_services: Any,
    boto3_client: Any,
    unique_bucket_name: Callable[[str], str],
    cleanup_buckets: Callable[[str], None],
) -> None:
    """Overwriting an object in same bucket reuses the same KEK."""
    from .support.cache import wait_for_all_backends_ready
    from .support.db import query_bucket_keks

    bucket = unique_bucket_name("kms-overwrite")
    cleanup_buckets(bucket)

    boto3_client.create_bucket(Bucket=bucket)

    # First write
    boto3_client.put_object(Bucket=bucket, Key="data.txt", Body=b"version 1")
    assert wait_for_all_backends_ready(bucket, "data.txt", min_count=1, timeout_seconds=30)
    kek_before = query_bucket_keks(bucket)

    # Overwrite
    boto3_client.put_object(Bucket=bucket, Key="data.txt", Body=b"version 2")
    assert wait_for_all_backends_ready(bucket, "data.txt", min_count=1, timeout_seconds=30)
    kek_after = query_bucket_keks(bucket)

    # Same KEK should be used
    assert kek_before["kek_id"] == kek_after["kek_id"], "KEK should be reused for same bucket"

    # New version should be readable
    response = boto3_client.get_object(Bucket=bucket, Key="data.txt")
    assert response["Body"].read() == b"version 2"


# KMS fault injection tests using toxiproxy
# These tests verify fail-closed behavior and cache resilience during KMS outages.


@pytest.mark.local
def test_kms_outage_blocks_new_object_operations(
    docker_services: Any,
    boto3_client: Any,
    unique_bucket_name: Callable[[str], str],
    cleanup_buckets: Callable[[str], None],
) -> None:
    """When KMS is down, new object creation fails (fail-closed).

    Writing an object to a new bucket requires wrapping a new KEK, which needs KMS.
    This verifies the system doesn't silently fall back to unprotected keys.

    Note: Bucket creation itself doesn't require KMS - the KEK is only created
    when the first object is written to the bucket.
    """
    import time

    from botocore.exceptions import ClientError
    from botocore.exceptions import ReadTimeoutError

    from .support.compose import pause_service
    from .support.compose import unpause_service

    bucket = unique_bucket_name("kms-outage")
    cleanup_buckets(bucket)

    # Create bucket while KMS is up (bucket creation doesn't need KMS)
    boto3_client.create_bucket(Bucket=bucket)

    # Pause mock KMS via toxiproxy (deletes the proxy)
    pause_service("mock-kms")

    try:
        # Give time for proxy to fully close and connections to drop
        time.sleep(2.0)

        # Writing an object should fail - can't wrap new KEK without KMS
        # May fail with either:
        # - ClientError (500 Internal Server Error) if API responds with error
        # - ReadTimeoutError if KMS retries exhaust the request timeout
        # Both are valid "fail closed" behaviors
        with pytest.raises((ClientError, ReadTimeoutError)) as exc_info:
            boto3_client.put_object(Bucket=bucket, Key="test.txt", Body=b"test content")

        # If it's a ClientError, verify it's a server error (500)
        if isinstance(exc_info.value, ClientError):
            assert exc_info.value.response["Error"]["Code"] in (
                "InternalServerError",
                "InternalError",
                "500",
            ) or exc_info.value.response["ResponseMetadata"]["HTTPStatusCode"] == 500
        # ReadTimeoutError is also acceptable - means API couldn't complete due to KMS unavailability
    finally:
        unpause_service("mock-kms")


@pytest.mark.local
def test_cached_kek_survives_brief_kms_outage(
    docker_services: Any,
    boto3_client: Any,
    unique_bucket_name: Callable[[str], str],
    cleanup_buckets: Callable[[str], None],
) -> None:
    """Cached KEKs allow operations during brief KMS outage.

    Tests that:
    1. KEKs are cached after first use
    2. Operations continue working when KMS goes down (within cache TTL)
    3. This proves graceful degradation during transient outages
    """
    import time

    from .support.cache import wait_for_all_backends_ready
    from .support.compose import pause_service
    from .support.compose import unpause_service

    bucket = unique_bucket_name("kms-cache")
    cleanup_buckets(bucket)

    # Upload while KMS is up (this caches the KEK)
    boto3_client.create_bucket(Bucket=bucket)
    boto3_client.put_object(Bucket=bucket, Key="cached.txt", Body=b"cached content")
    assert wait_for_all_backends_ready(bucket, "cached.txt", min_count=1, timeout_seconds=30)

    # First read (ensures KEK is in cache)
    response = boto3_client.get_object(Bucket=bucket, Key="cached.txt")
    assert response["Body"].read() == b"cached content"

    # Now pause KMS
    pause_service("mock-kms")

    try:
        # Give a moment for the proxy to fully disable
        time.sleep(0.5)

        # Read should still work - KEK is cached (within 300s TTL)
        response = boto3_client.get_object(Bucket=bucket, Key="cached.txt")
        assert response["Body"].read() == b"cached content"

        # Can also write to the same bucket (reuses cached KEK)
        boto3_client.put_object(Bucket=bucket, Key="new-during-outage.txt", Body=b"new content")
    finally:
        unpause_service("mock-kms")
