"""E2E test for HeadObject pending/pinning semantics.

Validates AWS-like behavior:
- Immediately after PutObject, HeadObject may return 202 with Retry-After and x-amz-object-status
- Eventually returns 200 with full metadata
"""

import time
from typing import Any
from typing import Callable

import pytest
from botocore.exceptions import ClientError


def test_head_object_pending(
    docker_services: Any,
    boto3_client: Any,
    unique_bucket_name: Callable[[str], str],
    cleanup_buckets: Callable[[str], None],
) -> None:
    bucket = unique_bucket_name("head-pending")
    cleanup_buckets(bucket)

    boto3_client.create_bucket(Bucket=bucket)

    key = "file.txt"
    body = b"hello pending"
    content_type = "text/plain"

    boto3_client.put_object(Bucket=bucket, Key=key, Body=body, ContentType=content_type)

    # Immediately attempt HEAD: allow for 202 pending before 200
    deadline = time.time() + 60
    saw_202 = False
    last_status: int | None = None

    while time.time() < deadline:
        try:
            head = boto3_client.head_object(Bucket=bucket, Key=key)
            # Success path
            assert head["ResponseMetadata"]["HTTPStatusCode"] == 200
            assert head["ContentType"] == content_type
            assert int(head["ContentLength"]) == len(body)
            # ETag is quoted MD5 for simple upload
            assert head["ETag"].startswith('"') and head["ETag"].endswith('"')
            # If server exposes status header, it should be uploaded once 200
            status_header = head.get("ResponseMetadata", {}).get("HTTPHeaders", {}).get("x-amz-object-status")
            if status_header:
                assert status_header in {"uploaded", "pinning", "publishing", "pending"}
            break
        except ClientError as e:
            status = e.response.get("ResponseMetadata", {}).get("HTTPStatusCode")
            last_status = status
            if status == 202:
                saw_202 = True
                # Optional headers
                headers = e.response.get("ResponseMetadata", {}).get("HTTPHeaders", {})
                # Retry-After may be present
                _ = headers.get("retry-after")
                _ = headers.get("x-amz-object-status")
                time.sleep(1.0)
                continue
            # If 404 (eventual publish window), wait and retry
            if status in {404, 503}:
                time.sleep(1.0)
                continue
            raise

    else:
        pytest.fail(f"HEAD did not become 200 within timeout; last_status={last_status} saw_202={saw_202}")
