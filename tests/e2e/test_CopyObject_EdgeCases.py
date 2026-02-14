"""E2E tests for CopyObject edge cases and error scenarios."""

import os
from typing import Any
from typing import Callable

import pytest
from botocore.exceptions import ClientError


def test_copy_nonexistent_source_bucket(
    docker_services: Any,
    boto3_client: Any,
    unique_bucket_name: Callable[[str], str],
    cleanup_buckets: Callable[[str], None],
) -> None:
    """Test copy from non-existent source bucket returns NoSuchBucket error."""
    src_bucket = "nonexistent-source-bucket-12345"
    dst_bucket = unique_bucket_name("copy-dst")
    cleanup_buckets(dst_bucket)

    boto3_client.create_bucket(Bucket=dst_bucket)

    copy_source = f"/{src_bucket}/test.txt"
    with pytest.raises(ClientError) as exc_info:
        boto3_client.copy_object(Bucket=dst_bucket, Key="dst.txt", CopySource=copy_source)

    error = exc_info.value.response["Error"]
    assert error["Code"] == "NoSuchBucket"


def test_copy_nonexistent_destination_bucket(
    docker_services: Any,
    boto3_client: Any,
    unique_bucket_name: Callable[[str], str],
    cleanup_buckets: Callable[[str], None],
) -> None:
    """Test copy to non-existent destination bucket returns NoSuchBucket error."""
    src_bucket = unique_bucket_name("copy-src")
    dst_bucket = "nonexistent-dest-bucket-12345"
    cleanup_buckets(src_bucket)

    boto3_client.create_bucket(Bucket=src_bucket)
    boto3_client.put_object(Bucket=src_bucket, Key="src.txt", Body=b"test data")

    copy_source = f"/{src_bucket}/src.txt"
    with pytest.raises(ClientError) as exc_info:
        boto3_client.copy_object(Bucket=dst_bucket, Key="dst.txt", CopySource=copy_source)

    error = exc_info.value.response["Error"]
    assert error["Code"] == "NoSuchBucket"


def test_copy_nonexistent_source_key(
    docker_services: Any,
    boto3_client: Any,
    unique_bucket_name: Callable[[str], str],
    cleanup_buckets: Callable[[str], None],
) -> None:
    """Test copy from non-existent source key returns NoSuchKey error."""
    bucket = unique_bucket_name("copy-nokey")
    cleanup_buckets(bucket)

    boto3_client.create_bucket(Bucket=bucket)

    copy_source = f"/{bucket}/nonexistent.txt"
    with pytest.raises(ClientError) as exc_info:
        boto3_client.copy_object(Bucket=bucket, Key="dst.txt", CopySource=copy_source)

    error = exc_info.value.response["Error"]
    assert error["Code"] == "NoSuchKey"


def test_copy_same_source_and_destination(
    docker_services: Any,
    boto3_client: Any,
    unique_bucket_name: Callable[[str], str],
    cleanup_buckets: Callable[[str], None],
) -> None:
    """Test copying object to itself is idempotent and succeeds."""
    bucket = unique_bucket_name("copy-same")
    cleanup_buckets(bucket)

    boto3_client.create_bucket(Bucket=bucket)

    key = "test.txt"
    body = b"test data"
    boto3_client.put_object(Bucket=bucket, Key=key, Body=body)

    copy_source = f"/{bucket}/{key}"
    resp = boto3_client.copy_object(Bucket=bucket, Key=key, CopySource=copy_source)

    assert "CopyObjectResult" in resp
    assert "ETag" in resp["CopyObjectResult"]

    head = boto3_client.head_object(Bucket=bucket, Key=key)
    assert head["ContentLength"] == len(body)


def test_copy_empty_object(
    docker_services: Any,
    boto3_client: Any,
    unique_bucket_name: Callable[[str], str],
    cleanup_buckets: Callable[[str], None],
) -> None:
    """Test copying an empty (0 bytes) object."""
    bucket = unique_bucket_name("copy-empty")
    cleanup_buckets(bucket)

    boto3_client.create_bucket(Bucket=bucket)

    src_key = "empty.txt"
    dst_key = "empty-copy.txt"
    boto3_client.put_object(Bucket=bucket, Key=src_key, Body=b"")

    copy_source = f"/{bucket}/{src_key}"
    resp = boto3_client.copy_object(Bucket=bucket, Key=dst_key, CopySource=copy_source)

    assert "CopyObjectResult" in resp
    assert "ETag" in resp["CopyObjectResult"]

    head = boto3_client.head_object(Bucket=bucket, Key=dst_key)
    assert head["ContentLength"] == 0

    get_resp = boto3_client.get_object(Bucket=bucket, Key=dst_key)
    assert get_resp["Body"].read() == b""


def test_copy_one_byte_object(
    docker_services: Any,
    boto3_client: Any,
    unique_bucket_name: Callable[[str], str],
    cleanup_buckets: Callable[[str], None],
) -> None:
    """Test copying a minimal (1 byte) object."""
    bucket = unique_bucket_name("copy-1byte")
    cleanup_buckets(bucket)

    boto3_client.create_bucket(Bucket=bucket)

    src_key = "one.txt"
    dst_key = "one-copy.txt"
    body = b"x"
    boto3_client.put_object(Bucket=bucket, Key=src_key, Body=body)

    copy_source = f"/{bucket}/{src_key}"
    resp = boto3_client.copy_object(Bucket=bucket, Key=dst_key, CopySource=copy_source)

    assert "CopyObjectResult" in resp

    head = boto3_client.head_object(Bucket=bucket, Key=dst_key)
    assert head["ContentLength"] == 1

    get_resp = boto3_client.get_object(Bucket=bucket, Key=dst_key)
    assert get_resp["Body"].read() == body


def test_copy_1mb_object(
    docker_services: Any,
    boto3_client: Any,
    unique_bucket_name: Callable[[str], str],
    cleanup_buckets: Callable[[str], None],
) -> None:
    """Test copying a 1 MB object."""
    bucket = unique_bucket_name("copy-1mb")
    cleanup_buckets(bucket)

    boto3_client.create_bucket(Bucket=bucket)

    src_key = "1mb.bin"
    dst_key = "1mb-copy.bin"
    body = b"A" * (1024 * 1024)
    boto3_client.put_object(Bucket=bucket, Key=src_key, Body=body)

    copy_source = f"/{bucket}/{src_key}"
    resp = boto3_client.copy_object(Bucket=bucket, Key=dst_key, CopySource=copy_source)

    assert "CopyObjectResult" in resp

    head = boto3_client.head_object(Bucket=bucket, Key=dst_key)
    assert head["ContentLength"] == len(body)

    get_resp = boto3_client.get_object(Bucket=bucket, Key=dst_key)
    copied_body = get_resp["Body"].read()
    assert len(copied_body) == len(body)
    assert copied_body == body


def test_copy_10mb_object(
    docker_services: Any,
    boto3_client: Any,
    unique_bucket_name: Callable[[str], str],
    cleanup_buckets: Callable[[str], None],
) -> None:
    """Test copying a multi-chunk object."""
    bucket = unique_bucket_name("copy-multichunk")
    cleanup_buckets(bucket)

    boto3_client.create_bucket(Bucket=bucket)

    # Ensure body spans multiple chunks regardless of configured chunk size
    chunk_size = int(os.environ.get("HIPPIUS_CHUNK_SIZE_BYTES", 16 * 1024 * 1024))
    src_key = "multichunk.bin"
    dst_key = "multichunk-copy.bin"
    body = b"B" * (chunk_size * 2 + 123)
    boto3_client.put_object(Bucket=bucket, Key=src_key, Body=body)

    copy_source = f"/{bucket}/{src_key}"
    resp = boto3_client.copy_object(Bucket=bucket, Key=dst_key, CopySource=copy_source)

    assert "CopyObjectResult" in resp

    head = boto3_client.head_object(Bucket=bucket, Key=dst_key)
    assert head["ContentLength"] == len(body)

    get_resp = boto3_client.get_object(Bucket=bucket, Key=dst_key)
    copied_body = get_resp["Body"].read()
    assert len(copied_body) == len(body)
    assert copied_body == body
