"""E2E tests for CopyObject streaming fallback scenarios."""

from typing import Any
from typing import Callable


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


def test_copy_to_existing_replaces_object(
    docker_services: Any,
    boto3_client: Any,
    unique_bucket_name: Callable[[str], str],
    cleanup_buckets: Callable[[str], None],
) -> None:
    """Test that copying to an existing destination key succeeds.

    When copying to an existing key, the system replaces the object
    with the source content via streaming fallback (required when
    destination exists due to AAD encryption constraints).

    Expected behavior:
    - Copy should succeed
    - Destination should have the source object's content
    - A new object version may be created depending on storage/versioning mode
    """
    bucket = unique_bucket_name("copy-replace")
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

    get_resp = boto3_client.get_object(Bucket=bucket, Key=key)
    assert get_resp["Body"].read() == body2
