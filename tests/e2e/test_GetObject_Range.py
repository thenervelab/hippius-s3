"""E2E tests for GetObject with Range requests."""

import time
from typing import Any
from typing import Callable

import pytest
from botocore.exceptions import ClientError


def _wait_get(boto3_client: Any, bucket: str, key: str, timeout: float = 20.0) -> Any:
    deadline = time.time() + timeout
    last_exc: Exception | None = None
    while time.time() < deadline:
        try:
            return boto3_client.get_object(Bucket=bucket, Key=key)
        except Exception as e:  # noqa: PERF203
            last_exc = e
            time.sleep(0.5)
    raise last_exc if last_exc else RuntimeError("GET not available in time")


def test_get_object_range_valid(
    docker_services: Any,
    boto3_client: Any,
    unique_bucket_name: Callable[[str], str],
    cleanup_buckets: Callable[[str], None],
) -> None:
    bucket = unique_bucket_name("range-valid")
    cleanup_buckets(bucket)

    boto3_client.create_bucket(Bucket=bucket)
    key = "range.txt"
    data = b"abcdefghijklmnopqrstuvwxyz"  # 26 bytes
    boto3_client.put_object(Bucket=bucket, Key=key, Body=data, ContentType="text/plain")

    _wait_get(boto3_client, bucket, key)

    # bytes=0-4
    resp = boto3_client.get_object(Bucket=bucket, Key=key, Range="bytes=0-4")
    assert resp["ResponseMetadata"]["HTTPStatusCode"] == 206
    assert resp["ContentRange"].startswith("bytes 0-4/")
    assert resp["Body"].read() == data[0:5]

    # bytes=5-
    resp2 = boto3_client.get_object(Bucket=bucket, Key=key, Range="bytes=5-")
    assert resp2["ResponseMetadata"]["HTTPStatusCode"] == 206
    assert resp2["ContentRange"].startswith("bytes 5-25/")
    assert resp2["Body"].read() == data[5:]

    # bytes=0- (open-ended) should return full object with 206
    resp_open = boto3_client.get_object(Bucket=bucket, Key=key, Range="bytes=0-")
    assert resp_open["ResponseMetadata"]["HTTPStatusCode"] == 206
    assert resp_open["ContentRange"] == f"bytes 0-{len(data) - 1}/{len(data)}"
    assert resp_open["Body"].read() == data

    # bytes=-5
    resp3 = boto3_client.get_object(Bucket=bucket, Key=key, Range="bytes=-5")
    assert resp3["ResponseMetadata"]["HTTPStatusCode"] == 206
    assert resp3["Body"].read() == data[-5:]


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

    _wait_get(boto3_client, bucket, key)

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
