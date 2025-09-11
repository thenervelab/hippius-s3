from typing import Any
from typing import Callable

import pytest
from botocore.exceptions import ClientError


def test_delete_bucket_empty_returns_204(
    docker_services: Any,
    boto3_client: Any,
    unique_bucket_name: Callable[[str], str],
    cleanup_buckets: Callable[[str], None],
) -> None:
    bucket = unique_bucket_name("delete-empty")
    cleanup_buckets(bucket)

    boto3_client.create_bucket(Bucket=bucket)

    # Deleting an empty bucket should succeed with 204
    resp = boto3_client.delete_bucket(Bucket=bucket)
    assert resp["ResponseMetadata"]["HTTPStatusCode"] == 204


def test_delete_bucket_nonempty_returns_409(
    docker_services: Any,
    boto3_client: Any,
    unique_bucket_name: Callable[[str], str],
    cleanup_buckets: Callable[[str], None],
) -> None:
    bucket = unique_bucket_name("delete-nonempty")
    cleanup_buckets(bucket)

    boto3_client.create_bucket(Bucket=bucket)

    key = "file.txt"
    boto3_client.put_object(Bucket=bucket, Key=key, Body=b"hello", ContentType="text/plain")

    with pytest.raises(ClientError) as ei:
        boto3_client.delete_bucket(Bucket=bucket)
    err = ei.value.response["Error"]
    assert err["Code"] == "BucketNotEmpty"
    assert ei.value.response["ResponseMetadata"]["HTTPStatusCode"] == 409

    # Clean up: delete object then bucket
    boto3_client.delete_object(Bucket=bucket, Key=key)
    boto3_client.delete_bucket(Bucket=bucket)
