"""E2E test for HeadObject (HEAD /{bucket}/{key})."""

from typing import Any
from typing import Callable

from .conftest import assert_hippius_source


def test_head_object_returns_metadata(
    docker_services: Any,
    boto3_client: Any,
    unique_bucket_name: Callable[[str], str],
    cleanup_buckets: Callable[[str], None],
) -> None:
    bucket_name = unique_bucket_name("head-object")
    cleanup_buckets(bucket_name)

    boto3_client.create_bucket(Bucket=bucket_name)

    key = "file.txt"
    content = b"hello head object"
    content_type = "text/plain"

    boto3_client.put_object(
        Bucket=bucket_name,
        Key=key,
        Body=content,
        ContentType=content_type,
        Metadata={"test-meta": "test-value"},
    )

    resp = boto3_client.head_object(Bucket=bucket_name, Key=key)

    assert resp["ContentType"] == content_type
    assert resp["ContentLength"] == len(content)
    assert resp["Metadata"]["test-meta"] == "test-value"
    # Hippius-only header; no-op on AWS
    assert_hippius_source(resp["ResponseMetadata"]["HTTPHeaders"])  # type: ignore[arg-type]
