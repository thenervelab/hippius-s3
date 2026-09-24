"""E2E test for AbortMultipartUpload (DELETE with uploadId)."""

from typing import Any
from typing import Callable

import pytest
from botocore.exceptions import ClientError


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


def test_abort_of_a_completed_upload_is_no_such_upload_and_keeps_the_object(
    docker_services: Any,
    boto3_client: Any,
    unique_bucket_name: Callable[[str], str],
    cleanup_buckets: Callable[[str], None],
) -> None:
    """A completed upload's parts ARE the object. Aborting it used to delete the upload row and, by
    cascade, those parts — destroying a committed version through a permission that is not
    DeleteObject, past Object Lock. AWS answers NoSuchUpload."""
    bucket = unique_bucket_name("mpu-abort-done")
    cleanup_buckets(bucket)
    boto3_client.create_bucket(Bucket=bucket)
    key = "done.bin"
    body = b"z" * (5 * 1024 * 1024)

    upload_id = boto3_client.create_multipart_upload(Bucket=bucket, Key=key)["UploadId"]
    part = boto3_client.upload_part(Bucket=bucket, Key=key, UploadId=upload_id, PartNumber=1, Body=body)
    boto3_client.complete_multipart_upload(
        Bucket=bucket,
        Key=key,
        UploadId=upload_id,
        MultipartUpload={"Parts": [{"PartNumber": 1, "ETag": part["ETag"]}]},
    )

    with pytest.raises(ClientError) as exc:
        boto3_client.abort_multipart_upload(Bucket=bucket, Key=key, UploadId=upload_id)
    assert exc.value.response["ResponseMetadata"]["HTTPStatusCode"] == 404
    assert exc.value.response["Error"]["Code"] == "NoSuchUpload"

    assert boto3_client.get_object(Bucket=bucket, Key=key)["Body"].read() == body


def test_abort_through_another_key_is_no_such_upload(
    docker_services: Any,
    boto3_client: Any,
    unique_bucket_name: Callable[[str], str],
    cleanup_buckets: Callable[[str], None],
) -> None:
    """The ACL layer authorised the PATH; an upload id initiated on a different key is not the
    upload that path names."""
    bucket = unique_bucket_name("mpu-abort-key")
    cleanup_buckets(bucket)
    boto3_client.create_bucket(Bucket=bucket)

    upload_id = boto3_client.create_multipart_upload(Bucket=bucket, Key="real.bin")["UploadId"]
    with pytest.raises(ClientError) as exc:
        boto3_client.abort_multipart_upload(Bucket=bucket, Key="other.bin", UploadId=upload_id)
    assert exc.value.response["Error"]["Code"] == "NoSuchUpload"

    boto3_client.abort_multipart_upload(Bucket=bucket, Key="real.bin", UploadId=upload_id)


def test_first_part_after_the_key_was_overwritten_does_not_rewrite_the_new_object(
    docker_services: Any,
    boto3_client: Any,
    unique_bucket_name: Callable[[str], str],
    cleanup_buckets: Callable[[str], None],
) -> None:
    """UploadPart used to target the key's CURRENT version. A PUT between initiate and the first
    part made that the PUT's finished object, and the part was written into it in place."""
    bucket = unique_bucket_name("mpu-overwritten")
    cleanup_buckets(bucket)
    boto3_client.create_bucket(Bucket=bucket)
    key = "k.bin"

    upload_id = boto3_client.create_multipart_upload(Bucket=bucket, Key=key)["UploadId"]
    boto3_client.put_object(Bucket=bucket, Key=key, Body=b"the real object")

    with pytest.raises(ClientError) as exc:
        boto3_client.upload_part(Bucket=bucket, Key=key, UploadId=upload_id, PartNumber=1, Body=b"x" * 1024)
    assert exc.value.response["ResponseMetadata"]["HTTPStatusCode"] == 409
    assert boto3_client.get_object(Bucket=bucket, Key=key)["Body"].read() == b"the real object"
