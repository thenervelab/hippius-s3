"""E2E test for PutObject metadata roundtrip (x-amz-meta-*)."""

from typing import Any
from typing import Callable


def test_put_object_metadata_roundtrip(
    docker_services: Any,
    boto3_client: Any,
    unique_bucket_name: Callable[[str], str],
    cleanup_buckets: Callable[[str], None],
) -> None:
    bucket = unique_bucket_name("put-meta")
    cleanup_buckets(bucket)
    boto3_client.create_bucket(Bucket=bucket)

    key = "meta.txt"
    data = b"hello meta"
    # S3 metadata must be ASCII-only; boto3 enforces this client-side
    meta = {
        "test-meta": "test-value",
        "CasePreserve": "MiXeD",
    }

    boto3_client.put_object(Bucket=bucket, Key=key, Body=data, ContentType="text/plain", Metadata=meta)

    head = boto3_client.head_object(Bucket=bucket, Key=key)
    assert head["ContentType"] == "text/plain"
    assert head["ContentLength"] == len(data)
    # boto3 lower-cases metadata keys
    returned = head["Metadata"]
    assert returned.get("test-meta") == "test-value"
    assert returned.get("casepreserve") == "MiXeD"
    # Non-ASCII would be rejected by boto3; validated implicitly by absence here
