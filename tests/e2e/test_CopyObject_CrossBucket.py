"""E2E test for CopyObject cross-bucket (re-encrypt path)."""

from typing import Any
from typing import Callable


def test_copy_object_cross_bucket(
    docker_services: Any,
    boto3_client: Any,
    unique_bucket_name: Callable[[str], str],
    cleanup_buckets: Callable[[str], None],
) -> None:
    src = unique_bucket_name("copy-src")
    dst = unique_bucket_name("copy-dst")
    cleanup_buckets(src)
    cleanup_buckets(dst)

    boto3_client.create_bucket(Bucket=src)
    boto3_client.create_bucket(Bucket=dst)

    key_src = "src.txt"
    key_dst = "dst.txt"
    body = b"hello cross-bucket"

    boto3_client.put_object(Bucket=src, Key=key_src, Body=body, ContentType="text/plain")

    # Copy using dict CopySource
    # Some S3-compatible stacks require a URL-encoded string; boto3 will serialize dict to string.
    # Ensure server supports both; we use the string form for maximum compatibility locally.
    resp = boto3_client.copy_object(Bucket=dst, Key=key_dst, CopySource=f"{src}/{key_src}")
    assert "CopyObjectResult" in resp

    head = boto3_client.head_object(Bucket=dst, Key=key_dst)
    assert head["ContentType"] == "text/plain"
    assert head["ContentLength"] == len(body)
