"""E2E tests for GetObject/HeadObject error surfaces."""

from typing import Any
from typing import Callable

import pytest
from botocore.exceptions import ClientError


def test_get_object_no_such_bucket(
    docker_services: Any,
    boto3_client: Any,
    unique_bucket_name: Callable[[str], str],
) -> None:
    bucket = unique_bucket_name("missing-bucket")
    key = "nope.txt"
    with pytest.raises(ClientError) as excinfo:
        boto3_client.get_object(Bucket=bucket, Key=key)
    status = excinfo.value.response.get("ResponseMetadata", {}).get("HTTPStatusCode")
    assert status == 404


def test_get_object_no_such_key(
    docker_services: Any,
    boto3_client: Any,
    unique_bucket_name: Callable[[str], str],
    cleanup_buckets: Callable[[str], None],
) -> None:
    bucket = unique_bucket_name("missing-key")
    cleanup_buckets(bucket)
    boto3_client.create_bucket(Bucket=bucket)
    with pytest.raises(ClientError) as excinfo:
        boto3_client.get_object(Bucket=bucket, Key="nope.txt")
    status = excinfo.value.response.get("ResponseMetadata", {}).get("HTTPStatusCode")
    assert status == 404


def test_head_object_no_such_key(
    docker_services: Any,
    boto3_client: Any,
    unique_bucket_name: Callable[[str], str],
    cleanup_buckets: Callable[[str], None],
) -> None:
    bucket = unique_bucket_name("missing-head")
    cleanup_buckets(bucket)
    boto3_client.create_bucket(Bucket=bucket)
    with pytest.raises(ClientError) as excinfo:
        boto3_client.head_object(Bucket=bucket, Key="nope.txt")
    status = excinfo.value.response.get("ResponseMetadata", {}).get("HTTPStatusCode")
    assert status == 404
