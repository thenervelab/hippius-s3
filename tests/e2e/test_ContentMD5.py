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


# --- Content-MD5 alongside the SDK's own checksum ------------------------------------------------
#
# botocore >= 1.36 adds its own CRC32 (a header, or an aws-chunked trailer) to PutObject/UploadPart
# by default. These pin what actually goes over the wire in both modes, and that the digest is
# checked against the decoded body either way — hashing the aws-chunked framing instead would turn
# every correct Content-MD5 into a BadDigest.

_SDK_CHECKSUM_HEADERS = ("x-amz-checksum-crc32", "x-amz-trailer", "x-amz-sdk-checksum-algorithm")


def _recording_client(access_key: str, secret: str, checksum_calculation: str, sent: list[dict[str, str]]) -> Any:
    import boto3
    from botocore.config import Config

    client = boto3.client(
        "s3",
        endpoint_url="http://localhost:8080",
        aws_access_key_id=access_key,
        aws_secret_access_key=secret,
        region_name="us-east-1",
        config=Config(
            s3={"addressing_style": "path"},
            signature_version="s3v4",
            connect_timeout=5,
            read_timeout=30,
            request_checksum_calculation=checksum_calculation,
        ),
    )

    def record(request: Any, **_kw: Any) -> None:
        # botocore keeps some header values as bytes; str() would record them as "b'...'".
        sent.append(
            {
                str(k).lower(): v.decode("latin-1") if isinstance(v, bytes) else str(v)
                for k, v in request.headers.items()
            }
        )

    client.meta.events.register("before-send.s3.PutObject", record)
    client.meta.events.register("before-send.s3.UploadPart", record)
    return client


def _local_only() -> None:
    import os

    if os.getenv("RUN_REAL_AWS") == "1" or os.getenv("AWS") == "1":
        pytest.skip("pins the local wire format; the plain tests above already run against AWS")


@pytest.mark.parametrize("digest_matches", [True, False])
def test_content_md5_is_checked_when_the_sdk_also_sends_crc32(
    docker_services: Any,
    boto3_client: Any,
    test_access_key: str,
    test_access_key_secret: str,
    unique_bucket_name: Callable[[str], str],
    cleanup_buckets: Callable[[str], None],
    digest_matches: bool,
) -> None:
    _local_only()
    bucket = unique_bucket_name("md5-crc")
    cleanup_buckets(bucket)
    boto3_client.create_bucket(Bucket=bucket)
    sent: list[dict[str, str]] = []
    client = _recording_client(test_access_key, test_access_key_secret, "when_supported", sent)
    body = b"audit entry " * 512

    claimed = body if digest_matches else b"not the body"
    call = lambda: client.put_object(  # noqa: E731
        Bucket=bucket, Key="k", Body=body, ContentMD5=_md5_b64(claimed), ChecksumAlgorithm="CRC32"
    )

    if digest_matches:
        resp = call()
        assert resp["ETag"].strip('"') == hashlib.md5(body).hexdigest()
        assert boto3_client.get_object(Bucket=bucket, Key="k")["Body"].read() == body
    else:
        with pytest.raises(ClientError) as exc:
            call()
        assert _error_code(exc) == "BadDigest"
        with pytest.raises(ClientError):
            boto3_client.head_object(Bucket=bucket, Key="k")

    headers = sent[-1]
    assert headers.get("content-md5") == _md5_b64(claimed), "Content-MD5 must reach the server unchanged"
    assert any(h in headers for h in _SDK_CHECKSUM_HEADERS), f"the SDK checksum was not sent: {sorted(headers)}"


@pytest.mark.parametrize("digest_matches", [True, False])
def test_content_md5_is_checked_when_it_is_the_only_checksum(
    docker_services: Any,
    boto3_client: Any,
    test_access_key: str,
    test_access_key_secret: str,
    unique_bucket_name: Callable[[str], str],
    cleanup_buckets: Callable[[str], None],
    digest_matches: bool,
) -> None:
    _local_only()
    bucket = unique_bucket_name("md5-only")
    cleanup_buckets(bucket)
    boto3_client.create_bucket(Bucket=bucket)
    sent: list[dict[str, str]] = []
    client = _recording_client(test_access_key, test_access_key_secret, "when_required", sent)
    body = b"only md5"

    claimed = body if digest_matches else b"something else"
    if digest_matches:
        client.put_object(Bucket=bucket, Key="k", Body=body, ContentMD5=_md5_b64(claimed))
        assert boto3_client.get_object(Bucket=bucket, Key="k")["Body"].read() == body
    else:
        with pytest.raises(ClientError) as exc:
            client.put_object(Bucket=bucket, Key="k", Body=body, ContentMD5=_md5_b64(claimed))
        assert _error_code(exc) == "BadDigest"

    headers = sent[-1]
    assert headers.get("content-md5") == _md5_b64(claimed)
    assert "x-amz-checksum-crc32" not in headers and "x-amz-trailer" not in headers


def test_upload_part_content_md5_alongside_sdk_crc32(
    docker_services: Any,
    boto3_client: Any,
    test_access_key: str,
    test_access_key_secret: str,
    unique_bucket_name: Callable[[str], str],
    cleanup_buckets: Callable[[str], None],
) -> None:
    _local_only()
    bucket = unique_bucket_name("md5-part-crc")
    cleanup_buckets(bucket)
    boto3_client.create_bucket(Bucket=bucket)
    sent: list[dict[str, str]] = []
    client = _recording_client(test_access_key, test_access_key_secret, "when_supported", sent)
    upload_id = client.create_multipart_upload(Bucket=bucket, Key="big.bin", ChecksumAlgorithm="CRC32")["UploadId"]
    part = b"z" * (5 * 1024 * 1024)

    with pytest.raises(ClientError) as exc:
        client.upload_part(
            Bucket=bucket,
            Key="big.bin",
            UploadId=upload_id,
            PartNumber=1,
            Body=part,
            ContentMD5=_md5_b64(b"wrong"),
            ChecksumAlgorithm="CRC32",
        )
    assert _error_code(exc) == "BadDigest"
    assert any(h in sent[-1] for h in _SDK_CHECKSUM_HEADERS)

    etag = client.upload_part(
        Bucket=bucket,
        Key="big.bin",
        UploadId=upload_id,
        PartNumber=1,
        Body=part,
        ContentMD5=_md5_b64(part),
        ChecksumAlgorithm="CRC32",
    )["ETag"]
    assert etag.strip('"') == hashlib.md5(part).hexdigest()
    boto3_client.abort_multipart_upload(Bucket=bucket, Key="big.bin", UploadId=upload_id)
