"""E2E test for PutObject (PUT /{bucket}/{key})."""

import time
from typing import Any
from typing import Callable

from .support.cache import clear_object_cache
from .support.cache import get_object_id
from .support.cache import wait_for_object_cid


def test_put_object_returns_etag(
    docker_services: Any,
    boto3_client: Any,
    unique_bucket_name: Callable[[str], str],
    cleanup_buckets: Callable[[str], None],
    signed_http_get: Any,
) -> None:
    bucket_name = unique_bucket_name("put-object")
    cleanup_buckets(bucket_name)

    boto3_client.create_bucket(Bucket=bucket_name)

    body = b"hello put object"
    response = boto3_client.put_object(
        Bucket=bucket_name,
        Key="hello.txt",
        Body=body,
        ContentType="text/plain",
        Metadata={"test-meta": "test-value"},
    )

    assert "ETag" in response
    assert isinstance(response["ETag"], str)

    # Validate x-hippius-source headers via GET
    resp_cache = signed_http_get(bucket_name, "hello.txt")
    assert resp_cache.status_code == 200
    assert resp_cache.headers.get("x-hippius-source") == "cache"
    assert resp_cache.content == body

    # Wait until object CID is available before clearing cache
    assert wait_for_object_cid(bucket_name, "hello.txt", timeout_seconds=20.0)

    # Clear cache and validate GET still returns content (now via pipeline)
    object_id = get_object_id(bucket_name, "hello.txt")
    clear_object_cache(object_id)

    resp_after_clear = None
    for _ in range(30):
        resp_after_clear = signed_http_get(bucket_name, "hello.txt")
        if resp_after_clear.status_code == 200 and resp_after_clear.content == body:
            break
        time.sleep(0.2)
    assert resp_after_clear is not None
    assert resp_after_clear.status_code == 200
    assert resp_after_clear.content == body
