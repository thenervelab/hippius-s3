"""E2E test for CreateMultipartUpload (POST ?uploads)."""

from typing import Any
from typing import Callable


def test_create_multipart_upload_returns_upload_id(
    docker_services: Any,
    boto3_client: Any,
    unique_bucket_name: Callable[[str], str],
    cleanup_buckets: Callable[[str], None],
) -> None:
    bucket = unique_bucket_name("mpu-create")
    cleanup_buckets(bucket)

    boto3_client.create_bucket(Bucket=bucket)
    key = "large.bin"

    resp = boto3_client.create_multipart_upload(Bucket=bucket, Key=key, ContentType="application/octet-stream")
    assert "UploadId" in resp
    assert isinstance(resp["UploadId"], str)
