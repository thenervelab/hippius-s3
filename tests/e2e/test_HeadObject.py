"""E2E test for HeadObject (HEAD /{bucket}/{key})."""

import time
from typing import Any
from typing import Callable

import pytest


def test_head_object_returns_metadata(
    docker_services: Any,
    boto3_client: Any,
    unique_bucket_name: Callable[[str], str],
    cleanup_buckets: Callable[[str], None],
) -> None:
    bucket_name = unique_bucket_name("head-object")
    cleanup_buckets(bucket_name)

    boto3_client.create_bucket(Bucket=bucket_name)

    key = "file.txt"
    content = b"hello head object"
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
            resp = boto3_client.head_object(Bucket=bucket_name, Key=key)
            break
        except Exception as e:  # noqa: PERF203
            last_exc = e
            time.sleep(0.5)
    else:
        raise last_exc if last_exc else RuntimeError("HEAD object not available in time")

    assert resp["ContentType"] == content_type
    assert resp["ContentLength"] == len(content)
    assert resp["Metadata"]["test-meta"] == "test-value"

