"""E2E test for PutObject (PUT /{bucket}/{key})."""

from typing import Any
from typing import Callable


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
