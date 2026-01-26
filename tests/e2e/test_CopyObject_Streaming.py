"""E2E tests for CopyObject streaming fallback scenarios."""

from typing import Any
from typing import Callable

import pytest


def test_copy_multipart_source_succeeds(
    docker_services: Any,
    boto3_client: Any,
    unique_bucket_name: Callable[[str], str],
    cleanup_buckets: Callable[[str], None],
) -> None:
    """Test that copying multipart objects succeeds."""
    bucket = unique_bucket_name("copy-mpu")
    cleanup_buckets(bucket)

    boto3_client.create_bucket(Bucket=bucket)

    src_key = "multipart.bin"
    dst_key = "multipart-copy.bin"

    upload_resp = boto3_client.create_multipart_upload(Bucket=bucket, Key=src_key)
    upload_id = upload_resp["UploadId"]

    part1_data = b"A" * (5 * 1024 * 1024)
    part1_resp = boto3_client.upload_part(Bucket=bucket, Key=src_key, PartNumber=1, UploadId=upload_id, Body=part1_data)

    boto3_client.complete_multipart_upload(
        Bucket=bucket,
        Key=src_key,
        UploadId=upload_id,
        MultipartUpload={"Parts": [{"PartNumber": 1, "ETag": part1_resp["ETag"]}]},
    )

    copy_source = f"/{bucket}/{src_key}"
    resp = boto3_client.copy_object(Bucket=bucket, Key=dst_key, CopySource=copy_source)

    assert "CopyObjectResult" in resp
    assert "ETag" in resp["CopyObjectResult"]

    head = boto3_client.head_object(Bucket=bucket, Key=dst_key)
    assert head["ContentLength"] == len(part1_data)


@pytest.mark.local
@pytest.mark.hippius_headers
def test_copy_to_existing_creates_version_via_streaming(
    docker_services: Any,
    boto3_client: Any,
    unique_bucket_name: Callable[[str], str],
    cleanup_buckets: Callable[[str], None],
) -> None:
    """Test that copying to an existing destination creates a new version via streaming.

    When the destination already exists, the fast path cannot be used because:
    1. The AAD (Additional Authenticated Data) for encryption includes the version number
    2. Creating a new version requires re-encrypting with new AAD
    3. This triggers the streaming fallback path

    Expected behavior:
    - Copy should succeed via streaming fallback
    - Destination should have 2 versions
    - Logs should show: "streaming fallback: destination exists (new version), AAD mismatch"
    """
    bucket = unique_bucket_name("copy-version")
    cleanup_buckets(bucket)

    boto3_client.create_bucket(Bucket=bucket)

    key = "test.txt"
    body1 = b"version 1 content"
    body2 = b"version 2 content to copy"

    boto3_client.put_object(Bucket=bucket, Key=key, Body=body1)

    src_key = "source.txt"
    boto3_client.put_object(Bucket=bucket, Key=src_key, Body=body2)

    copy_source = f"/{bucket}/{src_key}"
    resp = boto3_client.copy_object(Bucket=bucket, Key=key, CopySource=copy_source)

    assert "CopyObjectResult" in resp

    head = boto3_client.head_object(Bucket=bucket, Key=key)
    assert head["ContentLength"] == len(body2)
    assert int(head["ResponseMetadata"]["HTTPHeaders"].get("x-hippius-version", "0")) == 2

    get_resp = boto3_client.get_object(Bucket=bucket, Key=key)
    assert get_resp["Body"].read() == body2
