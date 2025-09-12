"""E2E test for CompleteMultipartUpload (POST ?uploadId=...)."""

from typing import Any
from typing import Callable


def test_complete_multipart_upload_returns_combined_etag(
    docker_services: Any,
    boto3_client: Any,
    unique_bucket_name: Callable[[str], str],
    cleanup_buckets: Callable[[str], None],
) -> None:
    bucket = unique_bucket_name("mpu-complete")
    cleanup_buckets(bucket)
    boto3_client.create_bucket(Bucket=bucket)
    key = "large.bin"

    create = boto3_client.create_multipart_upload(Bucket=bucket, Key=key, ContentType="application/octet-stream")
    upload_id = create["UploadId"]

    # AWS requires each part except the last to be at least 5 MiB. Use 5 MiB for both parts.
    part_size = 5 * 1024 * 1024
    etag1 = boto3_client.upload_part(Bucket=bucket, Key=key, UploadId=upload_id, PartNumber=1, Body=b"a" * part_size)[
        "ETag"
    ]
    etag2 = boto3_client.upload_part(Bucket=bucket, Key=key, UploadId=upload_id, PartNumber=2, Body=b"b" * part_size)[
        "ETag"
    ]

    completed = boto3_client.complete_multipart_upload(
        Bucket=bucket,
        Key=key,
        UploadId=upload_id,
        MultipartUpload={
            "Parts": [
                {"ETag": etag1, "PartNumber": 1},
                {"ETag": etag2, "PartNumber": 2},
            ]
        },
    )
    # Combined ETag should include -2 suffix for two parts
    assert "ETag" in completed
    assert isinstance(completed["ETag"], str)
    etag_xml = completed["ETag"].strip('"')
    assert etag_xml.endswith("-2")

    # HEAD should expose the same ETag
    head = boto3_client.head_object(Bucket=bucket, Key=key)
    assert head["ETag"].strip('"') == etag_xml
