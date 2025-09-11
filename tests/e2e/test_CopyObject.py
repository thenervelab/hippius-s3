"""E2E test for CopyObject (PUT with x-amz-copy-source)."""

from typing import Any
from typing import Callable


def test_copy_object_same_bucket(
    docker_services: Any,
    boto3_client: Any,
    unique_bucket_name: Callable[[str], str],
    cleanup_buckets: Callable[[str], None],
) -> None:
    bucket = unique_bucket_name("copy-object")
    cleanup_buckets(bucket)

    boto3_client.create_bucket(Bucket=bucket)

    src_key = "src.txt"
    dst_key = "dst.txt"
    body = b"copy me"
    content_type = "text/plain"

    put_resp = boto3_client.put_object(Bucket=bucket, Key=src_key, Body=body, ContentType=content_type)
    assert "ETag" in put_resp

    # Perform server-side copy within the same bucket
    copy_source = f"/{bucket}/{src_key}"
    resp = boto3_client.copy_object(Bucket=bucket, Key=dst_key, CopySource=copy_source)

    # AWS returns CopyObjectResult in XML; boto3 maps to dict
    assert "CopyObjectResult" in resp
    assert "ETag" in resp["CopyObjectResult"]

    # Verify destination exists and matches
    head = boto3_client.head_object(Bucket=bucket, Key=dst_key)
    assert head["ContentType"] == content_type
    assert head["ContentLength"] == len(body)
