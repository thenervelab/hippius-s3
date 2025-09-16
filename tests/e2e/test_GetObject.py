"""E2E test for GetObject (GET /{bucket}/{key})."""

import time
from typing import Any
from typing import Callable


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

    # First GET: expect cache
    resp_cache = signed_http_get(bucket_name, key)
    assert resp_cache.status_code == 200
    assert resp_cache.headers.get("x-hippius-source") == "cache"
    assert resp_cache.content == content
    # User metadata should be present via headers
    assert resp_cache.headers.get("x-amz-meta-test-meta") == "test-value"

    time.sleep(0.5)
    # Second GET with pipeline_only: expect pipeline
    resp_pipe = signed_http_get(bucket_name, key, {"x-hippius-read-mode": "pipeline_only"})
    assert resp_pipe.status_code == 200
    assert resp_pipe.headers.get("x-hippius-source") == "pipeline"
    assert resp_pipe.content == content
    assert resp_pipe.headers.get("x-amz-meta-test-meta") == "test-value"
