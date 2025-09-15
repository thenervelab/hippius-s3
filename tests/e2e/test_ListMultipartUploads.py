"""E2E test for ListMultipartUploads (GET ?uploads)."""

from typing import Any
from typing import Callable


def test_list_multipart_uploads_shows_initiated_upload(
    docker_services: Any,
    boto3_client: Any,
    unique_bucket_name: Callable[[str], str],
    cleanup_buckets: Callable[[str], None],
) -> None:
    bucket = unique_bucket_name("mpu-list")
    cleanup_buckets(bucket)
    boto3_client.create_bucket(Bucket=bucket)
    key = "large.bin"

    create = boto3_client.create_multipart_upload(Bucket=bucket, Key=key, ContentType="application/octet-stream")
    upload_id = create["UploadId"]
    assert upload_id

    # boto3 doesn't expose list_multipart_uploads directly; use list_multipart_uploads via client meta
    # The standard S3 API for boto3 is list_multipart_uploads
    listed = boto3_client.list_multipart_uploads(Bucket=bucket)
    uploads = listed.get("Uploads", [])
    assert any(u.get("Key") == key and u.get("UploadId") == upload_id for u in uploads)

    # Prefix filter should include our key
    listed_pref = boto3_client.list_multipart_uploads(Bucket=bucket, Prefix="large")
    uploads_pref = listed_pref.get("Uploads", [])
    assert any(u.get("Key") == key for u in uploads_pref)

    # Abort and ensure it disappears
    boto3_client.abort_multipart_upload(Bucket=bucket, Key=key, UploadId=upload_id)
    listed_after = boto3_client.list_multipart_uploads(Bucket=bucket)
    assert not any(u.get("UploadId") == upload_id for u in listed_after.get("Uploads", []))
