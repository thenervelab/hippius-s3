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


def test_copy_preserves_custom_metadata(
    docker_services: Any,
    boto3_client: Any,
    unique_bucket_name: Callable[[str], str],
    cleanup_buckets: Callable[[str], None],
) -> None:
    """Test that copying preserves custom metadata headers."""
    bucket = unique_bucket_name("copy-metadata")
    cleanup_buckets(bucket)

    boto3_client.create_bucket(Bucket=bucket)

    src_key = "src-with-meta.txt"
    dst_key = "dst-with-meta.txt"
    body = b"data with metadata"
    custom_metadata = {"custom-key": "custom-value", "another-key": "another-value"}

    boto3_client.put_object(Bucket=bucket, Key=src_key, Body=body, Metadata=custom_metadata)

    copy_source = f"/{bucket}/{src_key}"
    resp = boto3_client.copy_object(Bucket=bucket, Key=dst_key, CopySource=copy_source)

    assert "CopyObjectResult" in resp

    head = boto3_client.head_object(Bucket=bucket, Key=dst_key)
    assert head["Metadata"] == custom_metadata


def test_copy_preserves_content_type(
    docker_services: Any,
    boto3_client: Any,
    unique_bucket_name: Callable[[str], str],
    cleanup_buckets: Callable[[str], None],
) -> None:
    """Test that copying preserves ContentType."""
    bucket = unique_bucket_name("copy-ctype")
    cleanup_buckets(bucket)

    boto3_client.create_bucket(Bucket=bucket)

    src_key = "data.json"
    dst_key = "data-copy.json"
    body = b'{"key": "value"}'
    content_type = "application/json"

    boto3_client.put_object(Bucket=bucket, Key=src_key, Body=body, ContentType=content_type)

    copy_source = f"/{bucket}/{src_key}"
    resp = boto3_client.copy_object(Bucket=bucket, Key=dst_key, CopySource=copy_source)

    assert "CopyObjectResult" in resp

    head = boto3_client.head_object(Bucket=bucket, Key=dst_key)
    assert head["ContentType"] == content_type


def test_copy_with_empty_metadata(
    docker_services: Any,
    boto3_client: Any,
    unique_bucket_name: Callable[[str], str],
    cleanup_buckets: Callable[[str], None],
) -> None:
    """Test that copying objects with no metadata works correctly."""
    bucket = unique_bucket_name("copy-nometa")
    cleanup_buckets(bucket)

    boto3_client.create_bucket(Bucket=bucket)

    src_key = "plain.txt"
    dst_key = "plain-copy.txt"
    body = b"plain data"

    boto3_client.put_object(Bucket=bucket, Key=src_key, Body=body)

    copy_source = f"/{bucket}/{src_key}"
    resp = boto3_client.copy_object(Bucket=bucket, Key=dst_key, CopySource=copy_source)

    assert "CopyObjectResult" in resp

    head = boto3_client.head_object(Bucket=bucket, Key=dst_key)
    assert head["Metadata"] == {}
