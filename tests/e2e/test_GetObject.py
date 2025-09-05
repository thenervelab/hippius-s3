"""E2E test for GetObject (GET /{bucket}/{key})."""

import time
from typing import Any
from typing import Callable

import pytest


def test_get_object_downloads_and_matches_headers(
    docker_services: Any,
    boto3_client: Any,
    unique_bucket_name: Callable[[str], str],
    cleanup_buckets: Callable[[str], None],
) -> None:
    bucket_name = unique_bucket_name("get-object")
    cleanup_buckets(bucket_name)

    boto3_client.create_bucket(Bucket=bucket_name)

    key = "file.txt"
    content = b"hello get object"
    content_type = "text/plain"

    boto3_client.put_object(
        Bucket=bucket_name,
        Key=key,
        Body=content,
        ContentType=content_type,
        Metadata={"test-meta": "test-value"},
    )

    # Poll until available
    deadline = time.time() + 20
    last_exc: Exception | None = None
    while time.time() < deadline:
        try:
            resp = boto3_client.get_object(Bucket=bucket_name, Key=key)
            break
        except Exception as e:  # noqa: PERF203
            last_exc = e
            time.sleep(0.5)
    else:
        raise last_exc if last_exc else RuntimeError("GET object not available in time")

    assert resp["Body"].read() == content
    assert resp["ContentType"] == content_type
    assert resp["Metadata"]["test-meta"] == "test-value"
    assert "ETag" in resp
    assert "LastModified" in resp

