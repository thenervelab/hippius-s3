"""E2E: HEAD header contract after the light by-path query (HD-4/HD-5).

HD-4 repoints the common HEAD path at a lighter query (no download_chunks JSON_AGG / mpu subquery),
projecting append_version (so the per-request fallback JOIN never fires) and the Arion first-chunk
hash via LATERAL (HD-5). HEAD headers must stay byte-identical, so this pins the contract: size,
ETag, content-type, x-amz-meta-*, x-amz-version-id, the append-version header, and that the Arion
hash resolves to a real identifier (not just "pending") once the backend has the chunk.
"""

import hashlib
import os
import time
from typing import Any
from typing import Callable

import pytest

from .support.cache import wait_for_all_backends_ready


_E2E_DSN = os.environ.get("HIPPIUS_E2E_DB_DSN", "postgresql://postgres:postgres@localhost:5432/hippius")


@pytest.mark.local
def test_head_simple_object_contract(
    docker_services: Any,
    boto3_client: Any,
    unique_bucket_name: Callable[[str], str],
    cleanup_buckets: Callable[[str], None],
) -> None:
    bucket = unique_bucket_name("head-simple")
    cleanup_buckets(bucket)
    boto3_client.create_bucket(Bucket=bucket)
    key = "meta.bin"
    body = b"\x5a" * 8192

    boto3_client.put_object(
        Bucket=bucket,
        Key=key,
        Body=body,
        ContentType="application/octet-stream",
        Metadata={"foo": "bar", "team": "hippius"},
    )
    assert wait_for_all_backends_ready(bucket, key, min_count=1, timeout_seconds=60.0, dsn=_E2E_DSN)

    head = boto3_client.head_object(Bucket=bucket, Key=key)
    hdrs = head["ResponseMetadata"]["HTTPHeaders"]

    assert head["ContentLength"] == len(body)
    assert head["ETag"].strip('"') == hashlib.md5(body).hexdigest()
    assert hdrs["content-type"] == "application/octet-stream"
    assert hdrs["accept-ranges"] == "bytes"
    assert hdrs.get("x-amz-meta-foo") == "bar"
    assert hdrs.get("x-amz-meta-team") == "hippius"
    assert hdrs.get("x-amz-version-id") == "1"
    # HD-5: the Arion hash comes from the LATERAL join and must resolve to a real identifier.
    arion_hash = hdrs.get("x-hippius-arion-file-hash")
    assert arion_hash and arion_hash != "pending", "LATERAL Arion hash must resolve once the chunk is on the backend"


@pytest.mark.s4
def test_head_appended_object_shows_append_version(
    docker_services: Any,
    boto3_client: Any,
    unique_bucket_name: Callable[[str], str],
    cleanup_buckets: Callable[[str], None],
) -> None:
    bucket = unique_bucket_name("head-append")
    cleanup_buckets(bucket)
    boto3_client.create_bucket(Bucket=bucket)
    key = "log.txt"

    boto3_client.put_object(Bucket=bucket, Key=key, Body=b"a\n", ContentType="text/plain")
    head0 = boto3_client.head_object(Bucket=bucket, Key=key)
    v0 = head0["ResponseMetadata"]["HTTPHeaders"].get("x-amz-meta-append-version", "0")
    assert v0 == "0", "a fresh simple object reports append-version 0 (projected by the light query)"

    boto3_client.put_object(
        Bucket=bucket,
        Key=key,
        Body=b"b\n",
        ContentType="text/plain",
        Metadata={"append": "true", "append-if-version": v0, "append-id": "head-contract-test"},
    )
    time.sleep(1)

    head1 = boto3_client.head_object(Bucket=bucket, Key=key)
    hdrs = head1["ResponseMetadata"]["HTTPHeaders"]
    assert int(hdrs["x-amz-meta-append-version"]) == int(v0) + 1
    assert head1["ContentLength"] == len(b"a\nb\n")
