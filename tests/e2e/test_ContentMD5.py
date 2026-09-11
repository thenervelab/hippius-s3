"""E2E: Content-MD5 is verified on PutObject and UploadPart.

S3 contract: a Content-MD5 that is not the base64 of a 16-byte digest is 400 InvalidDigest; a
well-formed one that does not match the body is 400 BadDigest, and nothing is stored. Valid on real
AWS too (RUN_REAL_AWS=1).
"""

import base64
import hashlib
from typing import Any
from typing import Callable

import pytest
from botocore.exceptions import ClientError


def _md5_b64(data: bytes) -> str:
    return base64.b64encode(hashlib.md5(data).digest()).decode()


def _error_code(exc: pytest.ExceptionInfo[ClientError]) -> str:
    return str(exc.value.response["Error"]["Code"])


def test_put_object_with_matching_content_md5_succeeds(
    docker_services: Any,
    boto3_client: Any,
    unique_bucket_name: Callable[[str], str],
    cleanup_buckets: Callable[[str], None],
) -> None:
    bucket = unique_bucket_name("md5-ok")
    cleanup_buckets(bucket)
    boto3_client.create_bucket(Bucket=bucket)
    body = b"write-once audit record"

    resp = boto3_client.put_object(Bucket=bucket, Key="k.txt", Body=body, ContentMD5=_md5_b64(body))

    assert resp["ETag"].strip('"') == hashlib.md5(body).hexdigest()
    assert boto3_client.get_object(Bucket=bucket, Key="k.txt")["Body"].read() == body


def test_put_object_with_wrong_content_md5_stores_nothing(
    docker_services: Any,
    boto3_client: Any,
    unique_bucket_name: Callable[[str], str],
    cleanup_buckets: Callable[[str], None],
) -> None:
    bucket = unique_bucket_name("md5-bad-new")
    cleanup_buckets(bucket)
    boto3_client.create_bucket(Bucket=bucket)

    with pytest.raises(ClientError) as exc:
        boto3_client.put_object(Bucket=bucket, Key="k.txt", Body=b"actual", ContentMD5=_md5_b64(b"claimed"))

    assert _error_code(exc) == "BadDigest"
    assert exc.value.response["ResponseMetadata"]["HTTPStatusCode"] == 400
    with pytest.raises(ClientError) as head:
        boto3_client.head_object(Bucket=bucket, Key="k.txt")
    assert head.value.response["ResponseMetadata"]["HTTPStatusCode"] == 404


def test_put_object_with_wrong_content_md5_keeps_the_previous_content(
    docker_services: Any,
    boto3_client: Any,
    unique_bucket_name: Callable[[str], str],
    cleanup_buckets: Callable[[str], None],
) -> None:
    bucket = unique_bucket_name("md5-bad-overwrite")
    cleanup_buckets(bucket)
    boto3_client.create_bucket(Bucket=bucket)
    boto3_client.put_object(Bucket=bucket, Key="k.txt", Body=b"original")

    with pytest.raises(ClientError) as exc:
        boto3_client.put_object(Bucket=bucket, Key="k.txt", Body=b"replacement", ContentMD5=_md5_b64(b"other"))

    assert _error_code(exc) == "BadDigest"
    assert boto3_client.get_object(Bucket=bucket, Key="k.txt")["Body"].read() == b"original"


def test_put_object_with_malformed_content_md5_is_invalid_digest(
    docker_services: Any,
    boto3_client: Any,
    unique_bucket_name: Callable[[str], str],
    cleanup_buckets: Callable[[str], None],
) -> None:
    bucket = unique_bucket_name("md5-malformed")
    cleanup_buckets(bucket)
    boto3_client.create_bucket(Bucket=bucket)

    with pytest.raises(ClientError) as exc:
        # The hex digest, not the base64 of the raw bytes: a common client mistake.
        boto3_client.put_object(Bucket=bucket, Key="k.txt", Body=b"x", ContentMD5=hashlib.md5(b"x").hexdigest())

    assert _error_code(exc) == "InvalidDigest"


def test_upload_part_with_wrong_content_md5_is_rejected(
    docker_services: Any,
    boto3_client: Any,
    unique_bucket_name: Callable[[str], str],
    cleanup_buckets: Callable[[str], None],
) -> None:
    bucket = unique_bucket_name("md5-part")
    cleanup_buckets(bucket)
    boto3_client.create_bucket(Bucket=bucket)
    upload_id = boto3_client.create_multipart_upload(Bucket=bucket, Key="big.bin")["UploadId"]
    part = b"p" * (5 * 1024 * 1024)

    with pytest.raises(ClientError) as exc:
        boto3_client.upload_part(
            Bucket=bucket, Key="big.bin", UploadId=upload_id, PartNumber=1, Body=part, ContentMD5=_md5_b64(b"nope")
        )
    assert _error_code(exc) == "BadDigest"

    # The rejected attempt left nothing behind: a correct retry of the same part completes normally.
    etag = boto3_client.upload_part(
        Bucket=bucket, Key="big.bin", UploadId=upload_id, PartNumber=1, Body=part, ContentMD5=_md5_b64(part)
    )["ETag"]
    boto3_client.complete_multipart_upload(
        Bucket=bucket,
        Key="big.bin",
        UploadId=upload_id,
        MultipartUpload={"Parts": [{"ETag": etag, "PartNumber": 1}]},
    )
    assert boto3_client.get_object(Bucket=bucket, Key="big.bin")["Body"].read() == part
