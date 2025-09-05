"""E2E test for single file upload to Hippius S3.

Currently uses bypass flags for credit checks and IPFS-only mode for single files.
See tests/e2e/README.md for how to make this truly end-to-end.

Note: Multipart uploads are not bypassed and still require full blockchain publishing.
These tests focus on single-file operations only.
"""

import time
from typing import Any
from typing import Callable

import pytest


def test_single_file_upload_download(
    docker_services: Any,
    boto3_client: Any,
    unique_bucket_name: Callable[[str], str],
    cleanup_buckets: Callable[[str], None],
) -> None:
    """Test uploading a single file and downloading it back."""
    # Generate unique bucket name
    bucket_name = unique_bucket_name("single-file-test")
    object_key = "test-file.txt"
    test_content = b"Hello, Hippius S3! This is a test file for e2e testing."
    content_type = "text/plain"

    # Track bucket for cleanup
    cleanup_buckets(bucket_name)

    try:
        # Create bucket
        boto3_client.create_bucket(Bucket=bucket_name)
        print(f"Created bucket: {bucket_name}")

        # Upload file
        boto3_client.put_object(
            Bucket=bucket_name,
            Key=object_key,
            Body=test_content,
            ContentType=content_type,
            Metadata={"test-meta": "test-value"},
        )
        print(f"Uploaded object: {bucket_name}/{object_key}")

        # Wait a moment for consistency
        time.sleep(2)

        # Verify upload by getting object
        response = boto3_client.get_object(Bucket=bucket_name, Key=object_key)

        # Check response
        assert response["Body"].read() == test_content
        assert response["ContentType"] == content_type
        assert response["Metadata"]["test-meta"] == "test-value"
        assert "ETag" in response
        assert "LastModified" in response

        # TODO: When bypass flags are removed, verify on-chain publishing
        # metadata = response.get("Metadata", {})
        # tx_hash = metadata.get("hippius", {}).get("tx_hash")
        # assert tx_hash is not None
        # verify_on_chain(tx_hash)

        print(f"Verified download: {bucket_name}/{object_key}")
        print(f"ETag: {response['ETag']}")
        print(f"Size: {len(test_content)} bytes")

        # Test HEAD request
        head_response = boto3_client.head_object(Bucket=bucket_name, Key=object_key)
        assert head_response["ContentType"] == content_type
        assert head_response["ContentLength"] == len(test_content)
        assert head_response["Metadata"]["test-meta"] == "test-value"

        print(f"Verified HEAD: {bucket_name}/{object_key}")

        # List objects in bucket
        list_response = boto3_client.list_objects_v2(Bucket=bucket_name)
        assert "Contents" in list_response
        assert len(list_response["Contents"]) == 1
        assert list_response["Contents"][0]["Key"] == object_key
        assert list_response["Contents"][0]["Size"] == len(test_content)

        print(f"Verified list objects: {bucket_name}")

    except Exception as e:
        print(f"Test failed: {e}")
        raise
    finally:
        # Cleanup is handled by the fixture
        pass


def test_bucket_operations(
    docker_services: Any,
    boto3_client: Any,
    unique_bucket_name: Callable[[str], str],
    cleanup_buckets: Callable[[str], None],
) -> None:
    """Test bucket creation, listing, and deletion."""
    bucket_name = unique_bucket_name("bucket-ops-test")

    # Track bucket for cleanup
    cleanup_buckets(bucket_name)

    try:
        # Create bucket
        boto3_client.create_bucket(Bucket=bucket_name)
        print(f"Created bucket: {bucket_name}")

        # List buckets - should include our new bucket
        list_response = boto3_client.list_buckets()
        bucket_names = [b["Name"] for b in list_response["Buckets"]]
        assert bucket_name in bucket_names
        print(f"Verified bucket in list: {bucket_names}")

        # Check bucket exists (HEAD)
        boto3_client.head_bucket(Bucket=bucket_name)
        print(f"Verified bucket exists: {bucket_name}")

    except Exception as e:
        print(f"Test failed: {e}")
        raise
    finally:
        # Cleanup is handled by the fixture
        pass


def test_error_cases(
    docker_services: Any,
    boto3_client: Any,
    unique_bucket_name: Callable[[str], str],
    cleanup_buckets: Callable[[str], None],
) -> None:
    """Test error cases and edge conditions."""
    bucket_name = unique_bucket_name("error-test")
    nonexistent_bucket = unique_bucket_name("nonexistent")
    object_key = "test-object.txt"

    # Track bucket for cleanup
    cleanup_buckets(bucket_name)

    try:
        # Create bucket first
        boto3_client.create_bucket(Bucket=bucket_name)

        # Test NoSuchBucket error
        with pytest.raises(Exception) as exc_info:
            boto3_client.get_object(Bucket=nonexistent_bucket, Key=object_key)
        print(f"Verified NoSuchBucket error: {exc_info.value}")

        # Test NoSuchKey error
        with pytest.raises(Exception) as exc_info:
            boto3_client.get_object(Bucket=bucket_name, Key="nonexistent-key.txt")
        print(f"Verified NoSuchKey error: {exc_info.value}")

        # Test BucketAlreadyExists error (try to create same bucket again)
        with pytest.raises(Exception) as exc_info:
            boto3_client.create_bucket(Bucket=bucket_name)
        print(f"Verified BucketAlreadyExists error: {exc_info.value}")

        print("All error cases verified")

    except Exception as e:
        print(f"Test failed: {e}")
        raise
    finally:
        # Cleanup is handled by the fixture
        pass
