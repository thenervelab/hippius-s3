"""E2E test for Bucket Tagging (GET/PUT/DELETE /{bucket}?tagging)."""

from typing import Any
from typing import Callable

import pytest
from botocore.exceptions import ClientError


def test_bucket_tagging_roundtrip(
    docker_services: Any,
    boto3_client: Any,
    unique_bucket_name: Callable[[str], str],
    cleanup_buckets: Callable[[str], None],
) -> None:
    """Test setting, getting, and deleting bucket tags."""
    bucket_name = unique_bucket_name("bucket-tagging")
    cleanup_buckets(bucket_name)

    # Create bucket
    boto3_client.create_bucket(Bucket=bucket_name)

    # Set bucket tags
    tags = [
        {"Key": "Environment", "Value": "test"},
        {"Key": "Project", "Value": "hippius-s3"},
    ]
    boto3_client.put_bucket_tagging(Bucket=bucket_name, Tagging={"TagSet": tags})

    # Get bucket tags and verify
    response = boto3_client.get_bucket_tagging(Bucket=bucket_name)
    assert "TagSet" in response
    assert len(response["TagSet"]) == 2

    # Convert to dict for easier comparison
    returned_tags = {tag["Key"]: tag["Value"] for tag in response["TagSet"]}
    expected_tags = {tag["Key"]: tag["Value"] for tag in tags}
    assert returned_tags == expected_tags

    # Delete bucket tags
    boto3_client.delete_bucket_tagging(Bucket=bucket_name)

    # Verify tags are gone - get_bucket_tagging should raise NoSuchTagSet
    with pytest.raises(ClientError) as excinfo:
        boto3_client.get_bucket_tagging(Bucket=bucket_name)
    # boto3 may place code in nested Error or top-level Code
    error_code = excinfo.value.response.get("Error", {}).get("Code") or excinfo.value.response.get("Code")
    assert error_code == "NoSuchTagSet"


def test_bucket_tagging_on_missing_bucket(
    docker_services: Any,
    boto3_client: Any,
    unique_bucket_name: Callable[[str], str],
    cleanup_buckets: Callable[[str], None],
) -> None:
    """Test bucket tagging operations on non-existent bucket."""
    bucket_name = unique_bucket_name("missing-bucket-tagging")

    # All operations should fail with NoSuchBucket
    with pytest.raises(ClientError) as excinfo:
        boto3_client.put_bucket_tagging(Bucket=bucket_name, Tagging={"TagSet": [{"Key": "test", "Value": "value"}]})
    # boto3 may put the error code in different locations depending on parsing
    error_code = excinfo.value.response.get("Error", {}).get("Code") or excinfo.value.response.get("Code")
    assert error_code == "NoSuchBucket"

    with pytest.raises(ClientError) as excinfo:
        boto3_client.get_bucket_tagging(Bucket=bucket_name)
    error_code = excinfo.value.response.get("Error", {}).get("Code") or excinfo.value.response.get("Code")
    assert error_code == "NoSuchBucket"

    with pytest.raises(ClientError) as excinfo:
        boto3_client.delete_bucket_tagging(Bucket=bucket_name)
    error_code = excinfo.value.response.get("Error", {}).get("Code") or excinfo.value.response.get("Code")
    assert error_code == "NoSuchBucket"
