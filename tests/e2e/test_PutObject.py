"""E2E test for PutObject (PUT /{bucket}/{key})."""

from typing import Any
from typing import Callable

from .conftest import assert_hippius_source
from .conftest import is_real_aws
from .support.cache import clear_object_cache
from .support.cache import get_object_id
from .support.cache import wait_for_parts_cids


def test_put_object_returns_etag(
    docker_services: Any,
    boto3_client: Any,
    unique_bucket_name: Callable[[str], str],
    cleanup_buckets: Callable[[str], None],
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

    # Validate headers via boto GET
    resp_cache = boto3_client.get_object(Bucket=bucket_name, Key="hello.txt")
    assert resp_cache["ResponseMetadata"]["HTTPStatusCode"] == 200
    headers = resp_cache["ResponseMetadata"]["HTTPHeaders"]
    assert_hippius_source(headers)
    assert resp_cache["Body"].read() == body

    if not is_real_aws():
        # Wait until object has at least 1 part with CID before clearing cache
        assert wait_for_parts_cids(bucket_name, "hello.txt", min_count=1, timeout_seconds=20.0)

        # Clear cache and validate GET still returns content (now via pipeline)
        object_id = get_object_id(bucket_name, "hello.txt")
        clear_object_cache(object_id)

    resp = boto3_client.get_object(Bucket=bucket_name, Key="hello.txt")
    assert resp is not None
    assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200
    assert resp["Body"].read() == body
