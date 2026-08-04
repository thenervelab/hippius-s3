"""E2E tests for CreateBucket input validation.

Covers:
- Bucket name with uppercase characters (AWS S3 requires lowercase)
- Bucket name format validation
"""

from typing import Any
from typing import Callable

import pytest
from botocore.exceptions import ClientError


@pytest.mark.parametrize(
    "bucket_suffix",
    [
        "MyBucket",
        "UPPERCASE",
        "mixedCase123",
        "Bucket-With-Caps",
        "testBUCKET",
    ],
)
def test_create_bucket_rejects_uppercase(
    docker_services: Any,
    boto3_client: Any,
    bucket_suffix: str,
) -> None:
    """Bucket names with uppercase characters must be rejected per AWS S3 spec."""
    with pytest.raises(ClientError) as excinfo:
        boto3_client.create_bucket(Bucket=bucket_suffix)

    error = excinfo.value.response.get("Error", {})
    assert (
        error.get("Code") == "InvalidBucketName" or excinfo.value.response["ResponseMetadata"]["HTTPStatusCode"] == 400
    )


@pytest.mark.parametrize(
    "bucket_suffix",
    [
        "ab",
        "x",
    ],
)
def test_create_bucket_rejects_too_short(
    docker_services: Any,
    boto3_client: Any,
    bucket_suffix: str,
) -> None:
    """Bucket names shorter than 3 characters must be rejected."""
    with pytest.raises(ClientError) as excinfo:
        boto3_client.create_bucket(Bucket=bucket_suffix)

    assert excinfo.value.response["ResponseMetadata"]["HTTPStatusCode"] == 400


@pytest.mark.local
@pytest.mark.parametrize("bucket_name", ["docs", "health", "metrics", "user", "redoc"])
def test_create_bucket_rejects_reserved_gateway_names(
    docker_services: Any,
    boto3_client: Any,
    bucket_name: str,
) -> None:
    """Names colliding with a reserved gateway route must be rejected outright —
    previously they were silently created with an empty/anonymous owner."""
    with pytest.raises(ClientError) as excinfo:
        boto3_client.create_bucket(Bucket=bucket_name)

    error = excinfo.value.response.get("Error", {})
    assert error.get("Code") == "InvalidBucketName"
    assert excinfo.value.response["ResponseMetadata"]["HTTPStatusCode"] == 400


@pytest.mark.local
@pytest.mark.parametrize("bucket_name", ["docs2", "metrics-test", "user-uploads"])
def test_create_bucket_accepts_reserved_prefixed_names(
    docker_services: Any,
    boto3_client: Any,
    bucket_name: str,
) -> None:
    """A name that merely starts with a reserved segment is an ordinary bucket. It
    authenticates normally now that auth_router matches the exact first segment, so it
    gets a real owner — the end-to-end proof that the prefix ban was unnecessary."""
    boto3_client.create_bucket(Bucket=bucket_name)
    try:
        assert bucket_name in {b["Name"] for b in boto3_client.list_buckets()["Buckets"]}
    finally:
        boto3_client.delete_bucket(Bucket=bucket_name)


def test_create_bucket_accepts_valid_lowercase(
    docker_services: Any,
    boto3_client: Any,
    unique_bucket_name: Callable[[str], str],
    cleanup_buckets: Callable[[str], None],
) -> None:
    """Valid lowercase bucket names should be accepted."""
    bucket_name = unique_bucket_name("valid-lowercase")
    cleanup_buckets(bucket_name)

    boto3_client.create_bucket(Bucket=bucket_name)
    boto3_client.head_bucket(Bucket=bucket_name)
