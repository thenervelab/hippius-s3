"""X-Hippius-Body-Blake3 on the wire, over a real stack.

The unit tests cover the helper and the integration tests cover the SQL projection, but between
them sat a hole: both endpoints' header blocks could be deleted and every other test stayed green.
These assert the header actually reaches a client, and — the part that matters — that a multipart
object is not silently handed a prefix digest labelled as a whole-body one.
"""

import hashlib
from typing import Any
from typing import Callable

import blake3
import pytest


CHUNK = 4 * 1024 * 1024  # HIPPIUS_CHUNK_SIZE_BYTES default
DIGEST_HDR = "x-hippius-body-blake3"
SCOPE_HDR = "x-hippius-body-blake3-scope"
ARION_HDR = "x-hippius-arion-file-hash"

pytestmark = [pytest.mark.local, pytest.mark.hippius_headers]


def _headers(resp: dict) -> dict:
    return resp["ResponseMetadata"]["HTTPHeaders"]


def _b3(data: bytes) -> str:
    return blake3.blake3(data, max_threads=1).hexdigest()


def test_simple_put_reports_the_whole_body_digest(
    docker_services: Any,
    boto3_client: Any,
    unique_bucket_name: Callable[[str], str],
    cleanup_buckets: Callable[[str], None],
) -> None:
    bucket = unique_bucket_name("blake3-hdr")
    cleanup_buckets(bucket)
    boto3_client.create_bucket(Bucket=bucket)
    body = b"blake3 header e2e " * 512
    key = "simple.bin"
    boto3_client.put_object(Bucket=bucket, Key=key, Body=body)

    head = _headers(boto3_client.head_object(Bucket=bucket, Key=key))
    get = _headers(boto3_client.get_object(Bucket=bucket, Key=key))

    assert head[DIGEST_HDR] == _b3(body), "simple PUT must digest the whole body"
    assert head[SCOPE_HDR] == "full"
    assert get[DIGEST_HDR] == head[DIGEST_HDR], "HEAD and GET must not disagree"
    assert get[SCOPE_HDR] == head[SCOPE_HDR]
    # The sibling header is a different identifier entirely — never let them collapse.
    assert head[DIGEST_HDR] != head.get(ARION_HDR)
    # And the ETag is still the md5 it always was.
    assert get["etag"].strip('"') == hashlib.md5(body).hexdigest()


def test_range_get_still_carries_the_headers(
    docker_services: Any,
    boto3_client: Any,
    unique_bucket_name: Callable[[str], str],
    cleanup_buckets: Callable[[str], None],
) -> None:
    bucket = unique_bucket_name("blake3-hdr")
    cleanup_buckets(bucket)
    boto3_client.create_bucket(Bucket=bucket)
    body = b"C" * 4096
    key = "range.bin"
    boto3_client.put_object(Bucket=bucket, Key=key, Body=body)

    partial = _headers(boto3_client.get_object(Bucket=bucket, Key=key, Range="bytes=0-15"))
    assert partial[DIGEST_HDR] == _b3(body), "a 206 describes the whole object, like ETag does"
    assert partial[SCOPE_HDR] == "full"


def test_multipart_is_labelled_first_chunk_not_whole_body(
    docker_services: Any,
    boto3_client: Any,
    unique_bucket_name: Callable[[str], str],
    cleanup_buckets: Callable[[str], None],
) -> None:
    """The regression this file exists for.

    MPU hashes only chunk 0 of part 1. Unlabelled, a client verifying the download would get a
    guaranteed mismatch with no way to know why.
    """
    bucket = unique_bucket_name("blake3-hdr")
    cleanup_buckets(bucket)
    boto3_client.create_bucket(Bucket=bucket)
    part1 = b"A" * (CHUNK + 1024)
    part2 = b"B" * (5 * 1024 * 1024)
    key = "mpu.bin"

    upload_id = boto3_client.create_multipart_upload(Bucket=bucket, Key=key)["UploadId"]
    parts = []
    for number, chunk in ((1, part1), (2, part2)):
        etag = boto3_client.upload_part(
            Bucket=bucket, Key=key, UploadId=upload_id, PartNumber=number, Body=chunk
        )["ETag"]
        parts.append({"ETag": etag, "PartNumber": number})
    boto3_client.complete_multipart_upload(
        Bucket=bucket, Key=key, UploadId=upload_id, MultipartUpload={"Parts": parts}
    )

    head = _headers(boto3_client.head_object(Bucket=bucket, Key=key))
    assert head[SCOPE_HDR] == "first-chunk", "an MPU digest must never claim whole-body coverage"
    assert head[DIGEST_HDR] == _b3(part1[:CHUNK])
    assert head[DIGEST_HDR] != _b3(part1 + part2)


def test_zero_byte_object_reports_the_empty_digest(
    docker_services: Any,
    boto3_client: Any,
    unique_bucket_name: Callable[[str], str],
    cleanup_buckets: Callable[[str], None],
) -> None:
    bucket = unique_bucket_name("blake3-hdr")
    cleanup_buckets(bucket)
    boto3_client.create_bucket(Bucket=bucket)
    key = "empty.bin"
    boto3_client.put_object(Bucket=bucket, Key=key, Body=b"")

    head = _headers(boto3_client.head_object(Bucket=bucket, Key=key))
    assert head[DIGEST_HDR] == _b3(b""), "an empty object has a real digest, not an absent one"
    assert head[SCOPE_HDR] == "full"
