"""E2E: If-None-Match: * makes PutObject and CompleteMultipartUpload create-only (write-once).

S3 contract: the write succeeds only if the key does not currently exist; otherwise 412
PreconditionFailed and the existing object is untouched. A key whose newest version is a delete
marker does not exist. Valid on real AWS too (RUN_REAL_AWS=1); AWS may answer a lost concurrent race
with 409 ConditionalRequestConflict instead of 412, so the race test accepts either.
"""

from concurrent.futures import ThreadPoolExecutor
from typing import Any
from typing import Callable

import pytest
from botocore.exceptions import ClientError


def _status(exc: ClientError) -> int:
    return int(exc.response["ResponseMetadata"]["HTTPStatusCode"])


def test_second_create_only_put_is_rejected_and_keeps_the_first(
    docker_services: Any,
    boto3_client: Any,
    unique_bucket_name: Callable[[str], str],
    cleanup_buckets: Callable[[str], None],
) -> None:
    bucket = unique_bucket_name("inm-put")
    cleanup_buckets(bucket)
    boto3_client.create_bucket(Bucket=bucket)

    boto3_client.put_object(Bucket=bucket, Key="audit.log", Body=b"first", IfNoneMatch="*")
    with pytest.raises(ClientError) as exc:
        boto3_client.put_object(Bucket=bucket, Key="audit.log", Body=b"second", IfNoneMatch="*")

    assert exc.value.response["Error"]["Code"] == "PreconditionFailed"
    assert _status(exc.value) == 412
    assert boto3_client.get_object(Bucket=bucket, Key="audit.log")["Body"].read() == b"first"


def test_create_only_put_over_an_unconditional_object_is_rejected(
    docker_services: Any,
    boto3_client: Any,
    unique_bucket_name: Callable[[str], str],
    cleanup_buckets: Callable[[str], None],
) -> None:
    bucket = unique_bucket_name("inm-existing")
    cleanup_buckets(bucket)
    boto3_client.create_bucket(Bucket=bucket)
    boto3_client.put_object(Bucket=bucket, Key="k", Body=b"existing")

    with pytest.raises(ClientError) as exc:
        boto3_client.put_object(Bucket=bucket, Key="k", Body=b"new", IfNoneMatch="*")

    assert _status(exc.value) == 412
    assert boto3_client.get_object(Bucket=bucket, Key="k")["Body"].read() == b"existing"


def test_create_only_put_succeeds_after_the_key_is_deleted(
    docker_services: Any,
    boto3_client: Any,
    unique_bucket_name: Callable[[str], str],
    cleanup_buckets: Callable[[str], None],
) -> None:
    bucket = unique_bucket_name("inm-deleted")
    cleanup_buckets(bucket)
    boto3_client.create_bucket(Bucket=bucket)
    boto3_client.put_object(Bucket=bucket, Key="k", Body=b"old")
    boto3_client.delete_object(Bucket=bucket, Key="k")

    boto3_client.put_object(Bucket=bucket, Key="k", Body=b"new", IfNoneMatch="*")

    assert boto3_client.get_object(Bucket=bucket, Key="k")["Body"].read() == b"new"


def test_unconditional_put_still_overwrites(
    docker_services: Any,
    boto3_client: Any,
    unique_bucket_name: Callable[[str], str],
    cleanup_buckets: Callable[[str], None],
) -> None:
    bucket = unique_bucket_name("inm-plain")
    cleanup_buckets(bucket)
    boto3_client.create_bucket(Bucket=bucket)
    boto3_client.put_object(Bucket=bucket, Key="k", Body=b"one", IfNoneMatch="*")
    boto3_client.put_object(Bucket=bucket, Key="k", Body=b"two")
    assert boto3_client.get_object(Bucket=bucket, Key="k")["Body"].read() == b"two"


def test_concurrent_create_only_puts_have_exactly_one_winner(
    docker_services: Any,
    boto3_client: Any,
    unique_bucket_name: Callable[[str], str],
    cleanup_buckets: Callable[[str], None],
) -> None:
    bucket = unique_bucket_name("inm-race")
    cleanup_buckets(bucket)
    boto3_client.create_bucket(Bucket=bucket)
    writers = 8

    def put(i: int) -> tuple[int, bytes]:
        body = f"writer-{i}".encode() * 1024
        try:
            boto3_client.put_object(Bucket=bucket, Key="race", Body=body, IfNoneMatch="*")
            return 200, body
        except ClientError as exc:
            return _status(exc), body

    with ThreadPoolExecutor(max_workers=writers) as pool:
        results = list(pool.map(put, range(writers)))

    winners = [body for status, body in results if status == 200]
    losers = [status for status, _ in results if status != 200]
    assert len(winners) == 1, f"expected exactly one successful create, got {[s for s, _ in results]}"
    assert all(status in (409, 412) for status in losers)
    assert boto3_client.get_object(Bucket=bucket, Key="race")["Body"].read() == winners[0]


def test_create_only_complete_multipart_upload_is_rejected_when_the_key_exists(
    docker_services: Any,
    boto3_client: Any,
    unique_bucket_name: Callable[[str], str],
    cleanup_buckets: Callable[[str], None],
) -> None:
    bucket = unique_bucket_name("inm-mpu")
    cleanup_buckets(bucket)
    boto3_client.create_bucket(Bucket=bucket)
    boto3_client.put_object(Bucket=bucket, Key="big.bin", Body=b"existing")

    upload_id = boto3_client.create_multipart_upload(Bucket=bucket, Key="big.bin")["UploadId"]
    part = b"p" * (5 * 1024 * 1024)
    etag = boto3_client.upload_part(Bucket=bucket, Key="big.bin", UploadId=upload_id, PartNumber=1, Body=part)["ETag"]
    with pytest.raises(ClientError) as exc:
        boto3_client.complete_multipart_upload(
            Bucket=bucket,
            Key="big.bin",
            UploadId=upload_id,
            MultipartUpload={"Parts": [{"ETag": etag, "PartNumber": 1}]},
            IfNoneMatch="*",
        )

    assert _status(exc.value) == 412
    assert boto3_client.get_object(Bucket=bucket, Key="big.bin")["Body"].read() == b"existing"
    boto3_client.abort_multipart_upload(Bucket=bucket, Key="big.bin", UploadId=upload_id)


def test_create_only_complete_multipart_upload_creates_a_new_key(
    docker_services: Any,
    boto3_client: Any,
    unique_bucket_name: Callable[[str], str],
    cleanup_buckets: Callable[[str], None],
) -> None:
    bucket = unique_bucket_name("inm-mpu-new")
    cleanup_buckets(bucket)
    boto3_client.create_bucket(Bucket=bucket)

    upload_id = boto3_client.create_multipart_upload(Bucket=bucket, Key="big.bin")["UploadId"]
    part = b"q" * (5 * 1024 * 1024)
    etag = boto3_client.upload_part(Bucket=bucket, Key="big.bin", UploadId=upload_id, PartNumber=1, Body=part)["ETag"]
    boto3_client.complete_multipart_upload(
        Bucket=bucket,
        Key="big.bin",
        UploadId=upload_id,
        MultipartUpload={"Parts": [{"ETag": etag, "PartNumber": 1}]},
        IfNoneMatch="*",
    )
    assert boto3_client.get_object(Bucket=bucket, Key="big.bin")["Body"].read() == part


def test_etag_valued_if_none_match_on_put_is_not_implemented(
    docker_services: Any,
    boto3_client: Any,
    unique_bucket_name: Callable[[str], str],
    cleanup_buckets: Callable[[str], None],
) -> None:
    """S3 supports only "*" on writes. Anything else must be refused, not silently ignored (which is
    how a write-once client overwrites) — and it proves the header reaches the API through the
    gateway untouched rather than being consumed by the GET-side conditional/caching middleware."""
    bucket = unique_bucket_name("inm-etag")
    cleanup_buckets(bucket)
    boto3_client.create_bucket(Bucket=bucket)
    boto3_client.put_object(Bucket=bucket, Key="k", Body=b"existing")

    with pytest.raises(ClientError) as exc:
        boto3_client.put_object(Bucket=bucket, Key="k", Body=b"new", IfNoneMatch='"5d41402abc4b2a76b9719d911017c592"')

    assert _status(exc.value) == 501
    assert boto3_client.get_object(Bucket=bucket, Key="k")["Body"].read() == b"existing"
