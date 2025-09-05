"""E2E test for DeleteObject (DELETE /{bucket}/{key})."""

from typing import Any
from typing import Callable

import pytest
from botocore.exceptions import ClientError


def test_delete_object_idempotent(
    docker_services: Any,
    boto3_client: Any,
    unique_bucket_name: Callable[[str], str],
    cleanup_buckets: Callable[[str], None],
) -> None:
    bucket_name = unique_bucket_name("delete-object")
    cleanup_buckets(bucket_name)

    boto3_client.create_bucket(Bucket=bucket_name)

    key = "to-delete.txt"
    body = b"delete me"
    boto3_client.put_object(Bucket=bucket_name, Key=key, Body=body, ContentType="text/plain")

    # Ensure it exists
    head = boto3_client.head_object(Bucket=bucket_name, Key=key)
    assert head["ContentLength"] == len(body)

    # Delete the object
    resp = boto3_client.delete_object(Bucket=bucket_name, Key=key)
    assert resp["ResponseMetadata"]["HTTPStatusCode"] in (200, 204)

    # Idempotent: delete again should still succeed (object already gone)
    resp2 = boto3_client.delete_object(Bucket=bucket_name, Key=key)
    assert resp2["ResponseMetadata"]["HTTPStatusCode"] in (200, 204)

    # And head should now fail
    with pytest.raises(ClientError) as excinfo:
        boto3_client.head_object(Bucket=bucket_name, Key=key)
    # Expect a 404-style error
    status = excinfo.value.response.get("ResponseMetadata", {}).get("HTTPStatusCode")
    assert status in (404,)
