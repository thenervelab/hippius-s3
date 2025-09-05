"""E2E test for Object Tagging (GET/PUT/DELETE /{bucket}/{key}?tagging)."""

from typing import Any
from typing import Callable

import pytest
from botocore.exceptions import ClientError


def test_object_tagging_roundtrip(
    docker_services: Any,
    boto3_client: Any,
    unique_bucket_name: Callable[[str], str],
    cleanup_buckets: Callable[[str], None],
) -> None:
    """Test setting, getting, and deleting object tags."""
    bucket_name = unique_bucket_name("object-tagging")
    cleanup_buckets(bucket_name)

    # Create bucket and object
    boto3_client.create_bucket(Bucket=bucket_name)
    object_key = "test-file.txt"
    content = b"test content for object tagging"
    boto3_client.put_object(Bucket=bucket_name, Key=object_key, Body=content, ContentType="text/plain")

    boto3_client.put_object_tagging(
        Bucket=bucket_name,
        Key=object_key,
        Tagging={
            "TagSet": [
                {"Key": "Environment", "Value": "test"},
                {"Key": "Project", "Value": "hippius-s3"},
                {"Key": "Version", "Value": "1.0"},
            ]
        },
    )

    # Get object tags and verify
    response = boto3_client.get_object_tagging(Bucket=bucket_name, Key=object_key)
    assert "TagSet" in response
    assert len(response["TagSet"]) == 3

    # Convert to dict for easier comparison
    returned_tags = {tag["Key"]: tag["Value"] for tag in response["TagSet"]}
    expected_tags = {"Environment": "test", "Project": "hippius-s3", "Version": "1.0"}
    assert returned_tags == expected_tags

    # Delete object tags
    boto3_client.delete_object_tagging(Bucket=bucket_name, Key=object_key)

    # Verify tags are gone - get_object_tagging should return empty TagSet
    response = boto3_client.get_object_tagging(Bucket=bucket_name, Key=object_key)
    assert response["TagSet"] == []


def test_object_tagging_no_such_key(
    docker_services: Any,
    boto3_client: Any,
    unique_bucket_name: Callable[[str], str],
    cleanup_buckets: Callable[[str], None],
) -> None:
    """Test object tagging operations on non-existent key."""
    bucket_name = unique_bucket_name("object-tagging-missing")
    cleanup_buckets(bucket_name)

    # Create bucket
    boto3_client.create_bucket(Bucket=bucket_name)
    object_key = "non-existent-file.txt"

    # All operations should fail with NoSuchKey
    with pytest.raises(ClientError) as excinfo:
        boto3_client.put_object_tagging(
            Bucket=bucket_name, Key=object_key, Tagging={"TagSet": [{"Key": "test", "Value": "value"}]}
        )
    # boto3 may put the error code in different locations depending on parsing
    error_code = excinfo.value.response.get("Error", {}).get("Code") or excinfo.value.response.get("Code")
    assert error_code == "NoSuchKey"

    with pytest.raises(ClientError) as excinfo:
        boto3_client.get_object_tagging(Bucket=bucket_name, Key=object_key)
    error_code = excinfo.value.response.get("Error", {}).get("Code") or excinfo.value.response.get("Code")
    assert error_code == "NoSuchKey"

    with pytest.raises(ClientError) as excinfo:
        boto3_client.delete_object_tagging(Bucket=bucket_name, Key=object_key)
    error_code = excinfo.value.response.get("Error", {}).get("Code") or excinfo.value.response.get("Code")
    assert error_code == "NoSuchKey"


def test_object_tagging_no_such_bucket(
    docker_services: Any,
    boto3_client: Any,
    unique_bucket_name: Callable[[str], str],
    cleanup_buckets: Callable[[str], None],
) -> None:
    """Test object tagging operations on non-existent bucket."""
    bucket_name = unique_bucket_name("missing-bucket-tagging")
    object_key = "test-file.txt"

    # All operations should fail with NoSuchBucket
    with pytest.raises(ClientError) as excinfo:
        boto3_client.put_object_tagging(
            Bucket=bucket_name, Key=object_key, Tagging={"TagSet": [{"Key": "test", "Value": "value"}]}
        )
    error_code = excinfo.value.response.get("Error", {}).get("Code") or excinfo.value.response.get("Code")
    assert error_code == "NoSuchBucket"

    with pytest.raises(ClientError) as excinfo:
        boto3_client.get_object_tagging(Bucket=bucket_name, Key=object_key)
    error_code = excinfo.value.response.get("Error", {}).get("Code") or excinfo.value.response.get("Code")
    assert error_code == "NoSuchBucket"

    with pytest.raises(ClientError) as excinfo:
        boto3_client.delete_object_tagging(Bucket=bucket_name, Key=object_key)
    error_code = excinfo.value.response.get("Error", {}).get("Code") or excinfo.value.response.get("Code")
    assert error_code == "NoSuchBucket"
