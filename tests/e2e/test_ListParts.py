from typing import Any
from typing import Callable


def test_list_parts(
    docker_services: Any,
    boto3_client: Any,
    unique_bucket_name: Callable[[str], str],
    cleanup_buckets: Callable[[str], None],
) -> None:
    bucket = unique_bucket_name("list-parts")
    cleanup_buckets(bucket)

    boto3_client.create_bucket(Bucket=bucket)
    key = "large.bin"

    create = boto3_client.create_multipart_upload(Bucket=bucket, Key=key, ContentType="application/octet-stream")
    upload_id = create["UploadId"]

    part_size = 5 * 1024 * 1024
    etag1 = boto3_client.upload_part(Bucket=bucket, Key=key, UploadId=upload_id, PartNumber=1, Body=b"a" * part_size)[
        "ETag"
    ]
    etag2 = boto3_client.upload_part(Bucket=bucket, Key=key, UploadId=upload_id, PartNumber=2, Body=b"b" * part_size)[
        "ETag"
    ]

    # List parts (no pagination)
    listed = boto3_client.list_parts(Bucket=bucket, Key=key, UploadId=upload_id)
    assert listed["ResponseMetadata"]["HTTPStatusCode"] == 200
    parts = listed.get("Parts", [])
    assert len(parts) == 2
    assert parts[0]["PartNumber"] == 1 and parts[0]["ETag"] == etag1 and parts[0]["Size"] == part_size
    assert parts[1]["PartNumber"] == 2 and parts[1]["ETag"] == etag2 and parts[1]["Size"] == part_size

    # Pagination: max-parts=1 then use NextPartNumberMarker
    page1 = boto3_client.list_parts(Bucket=bucket, Key=key, UploadId=upload_id, MaxParts=1)
    assert page1.get("IsTruncated") is True
    assert len(page1.get("Parts", [])) == 1
    marker = page1.get("NextPartNumberMarker")
    page2 = boto3_client.list_parts(Bucket=bucket, Key=key, UploadId=upload_id, PartNumberMarker=marker, MaxParts=1)
    assert page2.get("IsTruncated") is False
    assert len(page2.get("Parts", [])) == 1
