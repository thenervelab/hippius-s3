"""E2E test for GetObject (GET /{bucket}/{key})."""

import os
import time
from typing import Any
from typing import Callable

import redis  # type: ignore[import-untyped]

from .conftest import assert_hippius_source
from .conftest import is_real_aws
from .support.cache import clear_object_cache
from .support.cache import get_object_id
from .support.cache import make_all_object_parts_pending
from .support.cache import wait_for_parts_cids


def test_get_object_downloads_and_matches_headers(
    docker_services: Any,
    boto3_client: Any,
    unique_bucket_name: Callable[[str], str],
    cleanup_buckets: Callable[[str], None],
) -> None:
    bucket_name = unique_bucket_name("get-object")
    cleanup_buckets(bucket_name)

    boto3_client.create_bucket(Bucket=bucket_name)

    key = "file.txt"
    content = b"hello get object"
    content_type = "text/plain"

    boto3_client.put_object(
        Bucket=bucket_name,
        Key=key,
        Body=content,
        ContentType=content_type,
        Metadata={"test-meta": "test-value"},
    )

    # First GET: expect success
    resp_cache = boto3_client.get_object(Bucket=bucket_name, Key=key)
    assert resp_cache["ResponseMetadata"]["HTTPStatusCode"] == 200
    headers = resp_cache["ResponseMetadata"]["HTTPHeaders"]
    assert_hippius_source(headers)
    assert resp_cache["Body"].read() == content
    # User metadata should be present via headers
    assert headers.get("x-amz-meta-test-meta") == "test-value"

    if not is_real_aws():
        # Wait until object has at least 1 part with CID before clearing cache (pipeline readiness)
        assert wait_for_parts_cids(bucket_name, key, min_count=1, timeout_seconds=20.0)

        # Simulate pipeline path by clearing obj: cache, then GET should still succeed
        object_id = get_object_id(bucket_name, key)
        clear_object_cache(object_id)

    resp = boto3_client.get_object(Bucket=bucket_name, Key=key)

    assert resp is not None
    assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200
    assert resp["Body"].read() == content
    assert resp["ResponseMetadata"]["HTTPHeaders"].get("x-amz-meta-test-meta") == "test-value"


def test_get_object_eventual_consistency(
    docker_services: Any,
    boto3_client: Any,
    unique_bucket_name: Callable[[str], str],
    cleanup_buckets: Callable[[str], None],
) -> None:
    """Test eventual consistency: GET should work when CIDs are pending but cache exists.

    This reproduces the real-world scenario where:
    1. Object is uploaded and cached in Redis
    2. Background workers haven't processed it yet (CIDs are still 'pending')
    3. GET request should still succeed by falling back to cache
    """
    # Skip if running against real AWS (this test requires Hippius internals)
    if is_real_aws():
        return

    bucket_name = unique_bucket_name("eventual-consistency")
    cleanup_buckets(bucket_name)

    boto3_client.create_bucket(Bucket=bucket_name)

    key = "multipart-test.bin"
    # Create 2MB object: 1MB initial + 1MB append
    part1_data = os.urandom(1024 * 1024)  # 1MB
    part2_data = os.urandom(1024 * 1024)  # 1MB
    expected_content = part1_data + part2_data

    # Step 1: Upload initial object
    boto3_client.put_object(
        Bucket=bucket_name,
        Key=key,
        Body=part1_data,
    )

    # Step 2: Append to make it multipart
    append_id = "test-append-" + str(int(time.time()))
    boto3_client.put_object(
        Bucket=bucket_name,
        Key=key,
        Body=part2_data,
        Metadata={"append": "true", "append-if-version": "0", "append-id": append_id},
    )

    # Verify object exists and has correct size
    head_resp = boto3_client.head_object(Bucket=bucket_name, Key=key)
    assert head_resp["ContentLength"] == 2 * 1024 * 1024

    # Wait for object to be processed and cached
    assert wait_for_parts_cids(bucket_name, key, min_count=2, timeout_seconds=20.0)

    # Step 3: Get object_id and verify cache exists
    object_id = get_object_id(bucket_name, key)

    # Verify Redis has cached parts (obj:{object_id}:part:{n})
    redis_client = redis.Redis.from_url("redis://localhost:6379/0")
    cache_keys = redis_client.keys(f"obj:{object_id}:part:*")
    assert len(cache_keys) >= 2, f"Expected at least 2 cached parts, found {len(cache_keys)}"

    # Step 4: Simulate eventual consistency issue - force all CIDs to 'pending'
    # This simulates the state where upload succeeded but background processing hasn't completed
    object_id = make_all_object_parts_pending(bucket_name, key)

    # With our cache fallback implementation, GET should now succeed
    # even when CIDs are pending, by falling back to Redis cache
    get_resp = boto3_client.get_object(Bucket=bucket_name, Key=key)
    actual_content = get_resp["Body"].read()
    assert actual_content == expected_content
