"""E2E test for GetObject (GET /{bucket}/{key})."""

from typing import Any
from typing import Callable


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

    # Objects are now immediately available from cache
    resp = boto3_client.get_object(Bucket=bucket_name, Key=key)

    assert resp["Body"].read() == content
    assert resp["ContentType"] == content_type
    assert resp["Metadata"]["test-meta"] == "test-value"
    assert "ETag" in resp
    assert "LastModified" in resp
