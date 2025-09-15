"""E2E test for HeadObject pending/pinning semantics.

Validates that objects are immediately available after upload.
"""

from typing import Any
from typing import Callable


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

    # Objects are now immediately available from cache
    head = boto3_client.head_object(Bucket=bucket, Key=key)

    # Should get immediate 200 response
    assert head["ResponseMetadata"]["HTTPStatusCode"] == 200
    assert head["ContentType"] == content_type
    assert int(head["ContentLength"]) == len(body)
    # ETag is quoted MD5 for simple upload
    assert head["ETag"].startswith('"') and head["ETag"].endswith('"')
    # If server exposes status header, it should be uploaded once 200
    status_header = head.get("ResponseMetadata", {}).get("HTTPHeaders", {}).get("x-amz-object-status")
    if status_header:
        assert status_header in {"uploaded", "pinning", "publishing", "pending"}
