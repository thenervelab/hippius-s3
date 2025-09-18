"""E2E test for GetObject (GET /{bucket}/{key})."""

from typing import Any
from typing import Callable

from .conftest import assert_hippius_source
from .conftest import is_real_aws
from .support.cache import clear_object_cache
from .support.cache import get_object_id
from .support.cache import wait_for_object_cid


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
        # Wait until object has CID before clearing cache (pipeline readiness)
        assert wait_for_object_cid(bucket_name, key, timeout_seconds=20.0)

        # Simulate pipeline path by clearing obj: cache, then GET should still succeed
        object_id = get_object_id(bucket_name, key)
        clear_object_cache(object_id)

    resp = boto3_client.get_object(Bucket=bucket_name, Key=key)

    assert resp is not None
    assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200
    assert resp["Body"].read() == content
    assert resp["ResponseMetadata"]["HTTPHeaders"].get("x-amz-meta-test-meta") == "test-value"
