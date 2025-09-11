"""E2E test for AbortMultipartUpload (DELETE with uploadId)."""

from typing import Any
from typing import Callable


def test_abort_multipart_upload_deletes_upload(
    docker_services: Any,
    boto3_client: Any,
    unique_bucket_name: Callable[[str], str],
    cleanup_buckets: Callable[[str], None],
) -> None:
    bucket = unique_bucket_name("mpu-abort")
    cleanup_buckets(bucket)
    boto3_client.create_bucket(Bucket=bucket)
    key = "large.bin"

    create = boto3_client.create_multipart_upload(Bucket=bucket, Key=key, ContentType="application/octet-stream")
    upload_id = create["UploadId"]

    # Upload a part to ensure existence
    boto3_client.upload_part(Bucket=bucket, Key=key, UploadId=upload_id, PartNumber=1, Body=b"a" * 1024)

    # Abort
    resp = boto3_client.abort_multipart_upload(Bucket=bucket, Key=key, UploadId=upload_id)
    assert resp["ResponseMetadata"]["HTTPStatusCode"] in (200, 204)
