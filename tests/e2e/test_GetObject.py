"""E2E test for GetObject (GET /{bucket}/{key})."""

import time
from typing import Any
from typing import Callable

from .support.cache import clear_object_cache
from .support.cache import get_object_id
from .support.cache import wait_for_object_cid


def test_get_object_downloads_and_matches_headers(
    docker_services: Any,
    boto3_client: Any,
    unique_bucket_name: Callable[[str], str],
    cleanup_buckets: Callable[[str], None],
    signed_http_get: Any,
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

    # First GET: expect cache (write-through)
    resp_cache = signed_http_get(bucket_name, key)
    assert resp_cache.status_code == 200
    assert resp_cache.headers.get("x-hippius-source") == "cache"
    assert resp_cache.content == content
    # User metadata should be present via headers
    assert resp_cache.headers.get("x-amz-meta-test-meta") == "test-value"

    # Wait until object has CID before clearing cache (pipeline readiness)
    assert wait_for_object_cid(bucket_name, key, timeout_seconds=20.0)

    # Simulate pipeline path by clearing obj: cache, then GET should still succeed
    object_id = get_object_id(bucket_name, key)
    clear_object_cache(object_id)

    # Allow brief retry window for pipeline to fetch
    resp_after_clear = None
    for _ in range(30):
        resp_after_clear = signed_http_get(bucket_name, key)
        if resp_after_clear.status_code == 200 and resp_after_clear.content == content:
            break
        time.sleep(0.2)
    assert resp_after_clear is not None
    assert resp_after_clear.status_code == 200
    assert resp_after_clear.content == content
    assert resp_after_clear.headers.get("x-amz-meta-test-meta") == "test-value"
