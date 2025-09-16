"""E2E tests for GetObject with Range requests."""

from typing import Any
from typing import Callable

import pytest
from botocore.exceptions import ClientError


def test_get_object_range_valid(
    docker_services: Any,
    boto3_client: Any,
    unique_bucket_name: Callable[[str], str],
    cleanup_buckets: Callable[[str], None],
    signed_http_get: Any,
) -> None:
    bucket = unique_bucket_name("range-valid")
    cleanup_buckets(bucket)

    boto3_client.create_bucket(Bucket=bucket)
    key = "range.txt"
    data = b"abcdefghijklmnopqrstuvwxyz"  # 26 bytes
    boto3_client.put_object(Bucket=bucket, Key=key, Body=data, ContentType="text/plain")

    # bytes=0-4
    resp = signed_http_get(bucket, key, {"Range": "bytes=0-4"})
    assert resp.status_code == 206
    assert resp.headers.get("x-hippius-source") == "cache"
    assert resp.headers.get("Content-Range").startswith("bytes 0-4/")
    assert resp.content == data[0:5]

    # bytes=5-
    resp2 = signed_http_get(bucket, key, {"Range": "bytes=5-"})
    assert resp2.status_code == 206
    assert resp2.headers.get("x-hippius-source") == "cache"
    assert resp2.headers.get("Content-Range").startswith("bytes 5-25/")
    assert resp2.content == data[5:]

    # bytes=0- (open-ended) should return full object with 206
    resp_open = signed_http_get(bucket, key, {"Range": "bytes=0-"})
    assert resp_open.status_code == 206
    assert resp_open.headers.get("x-hippius-source") == "cache"
    assert resp_open.headers.get("Content-Range") == f"bytes 0-{len(data) - 1}/{len(data)}"
    assert resp_open.content == data

    # bytes=-5
    resp3 = signed_http_get(bucket, key, {"Range": "bytes=-5"})
    assert resp3.status_code == 206
    assert resp3.headers.get("x-hippius-source") == "cache"
    assert resp3.content == data[-5:]


def test_get_object_range_invalid(
    docker_services: Any,
    boto3_client: Any,
    unique_bucket_name: Callable[[str], str],
    cleanup_buckets: Callable[[str], None],
) -> None:
    bucket = unique_bucket_name("range-invalid")
    cleanup_buckets(bucket)

    boto3_client.create_bucket(Bucket=bucket)
    key = "range.txt"
    data = b"abc"
    boto3_client.put_object(Bucket=bucket, Key=key, Body=data, ContentType="text/plain")

    # Deterministic: request a range starting beyond EOF to force 416 on AWS and local
    size = len(data)
    invalid_range = f"bytes={size + 10}-{size + 20}"
    with pytest.raises(ClientError) as excinfo:
        boto3_client.get_object(Bucket=bucket, Key=key, Range=invalid_range)
    status = excinfo.value.response.get("ResponseMetadata", {}).get("HTTPStatusCode")
    assert status == 416

    # bytes=-N with N > size should clamp to full content (common S3 behavior)
    resp_clamp = boto3_client.get_object(Bucket=bucket, Key=key, Range=f"bytes=-{len(data) + 100}")
    assert resp_clamp["ResponseMetadata"]["HTTPStatusCode"] == 206
    assert resp_clamp["ContentRange"] == f"bytes 0-{len(data) - 1}/{len(data)}"
    assert resp_clamp["Body"].read() == data
