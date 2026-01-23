from typing import Any
from typing import Callable

import pytest

from .support.cache import wait_for_parts_cids
from .support.db import get_object_versioning_info


@pytest.mark.local
def test_simple_object_overwrite_creates_new_versions(
    docker_services: Any,
    boto3_client: Any,
    unique_bucket_name: Callable[[str], str],
    cleanup_buckets: Callable[[str], None],
) -> None:
    bucket_name = unique_bucket_name("versioning-simple")
    cleanup_buckets(bucket_name)

    boto3_client.create_bucket(Bucket=bucket_name)
    key = "test-versioning.txt"

    resp1 = boto3_client.put_object(
        Bucket=bucket_name,
        Key=key,
        Body=b"version 1 content",
        ContentType="text/plain",
    )
    assert resp1["ResponseMetadata"]["HTTPStatusCode"] in (200, 204)
    etag1 = resp1["ETag"].strip('"')

    resp2 = boto3_client.put_object(
        Bucket=bucket_name,
        Key=key,
        Body=b"version 2 content different",
        ContentType="text/plain",
    )
    assert resp2["ResponseMetadata"]["HTTPStatusCode"] in (200, 204)
    etag2 = resp2["ETag"].strip('"')

    assert etag1 != etag2

    info = get_object_versioning_info(bucket_name, key)
    assert info["current_object_version"] == 2, f"Expected current_object_version=2, got {info['current_object_version']}"

    versions = info["versions"]
    assert len(versions) == 2, f"Expected 2 versions, got {len(versions)}"

    assert versions[0][0] == 1
    assert versions[0][1] == len(b"version 1 content")

    assert versions[1][0] == 2
    assert versions[1][1] == len(b"version 2 content different")

    part_counts = info["part_counts"]
    assert len(part_counts) == 2, "Should have parts for both versions"
    assert part_counts[0][0] == 1
    assert part_counts[1][0] == 2

    resp_get = boto3_client.get_object(Bucket=bucket_name, Key=key)
    body = resp_get["Body"].read()
    assert body == b"version 2 content different"


@pytest.mark.local
def test_multipart_upload_overwrite_creates_new_versions(
    docker_services: Any,
    boto3_client: Any,
    unique_bucket_name: Callable[[str], str],
    cleanup_buckets: Callable[[str], None],
) -> None:
    bucket_name = unique_bucket_name("versioning-mpu")
    cleanup_buckets(bucket_name)

    boto3_client.create_bucket(Bucket=bucket_name)
    key = "large-versioned.bin"

    create1 = boto3_client.create_multipart_upload(Bucket=bucket_name, Key=key)
    upload_id1 = create1["UploadId"]

    part_size = 5 * 1024 * 1024
    etag1_p1 = boto3_client.upload_part(
        Bucket=bucket_name, Key=key, UploadId=upload_id1, PartNumber=1, Body=b"a" * part_size
    )["ETag"]

    completed1 = boto3_client.complete_multipart_upload(
        Bucket=bucket_name,
        Key=key,
        UploadId=upload_id1,
        MultipartUpload={"Parts": [{"ETag": etag1_p1, "PartNumber": 1}]}
    )
    etag1 = completed1["ETag"].strip('"')

    create2 = boto3_client.create_multipart_upload(Bucket=bucket_name, Key=key)
    upload_id2 = create2["UploadId"]

    etag2_p1 = boto3_client.upload_part(
        Bucket=bucket_name, Key=key, UploadId=upload_id2, PartNumber=1, Body=b"b" * part_size
    )["ETag"]
    etag2_p2 = boto3_client.upload_part(
        Bucket=bucket_name, Key=key, UploadId=upload_id2, PartNumber=2, Body=b"c" * part_size
    )["ETag"]

    completed2 = boto3_client.complete_multipart_upload(
        Bucket=bucket_name,
        Key=key,
        UploadId=upload_id2,
        MultipartUpload={"Parts": [
            {"ETag": etag2_p1, "PartNumber": 1},
            {"ETag": etag2_p2, "PartNumber": 2},
        ]}
    )
    etag2 = completed2["ETag"].strip('"')

    assert etag1 != etag2

    info = get_object_versioning_info(bucket_name, key)
    assert info["current_object_version"] == 2, f"Expected current_object_version=2, got {info['current_object_version']}"

    versions = info["versions"]
    assert len(versions) == 2
    assert versions[0][0] == 1
    assert versions[1][0] == 2

    part_counts = info["part_counts"]
    assert len(part_counts) == 2, "Should have parts for both versions"
    assert part_counts[0][1] == 1, f"Version 1 should have 1 part, got {part_counts[0][1]}"
    assert part_counts[1][1] == 2, f"Version 2 should have 2 parts, got {part_counts[1][1]}"


@pytest.mark.local
def test_three_overwrites_create_three_versions(
    docker_services: Any,
    boto3_client: Any,
    unique_bucket_name: Callable[[str], str],
    cleanup_buckets: Callable[[str], None],
) -> None:
    bucket_name = unique_bucket_name("versioning-triple")
    cleanup_buckets(bucket_name)

    boto3_client.create_bucket(Bucket=bucket_name)
    key = "triple-version.txt"

    for i in range(1, 4):
        boto3_client.put_object(
            Bucket=bucket_name,
            Key=key,
            Body=f"version {i}".encode(),
        )

    info = get_object_versioning_info(bucket_name, key)
    assert info["current_object_version"] == 3, f"Expected current_object_version=3, got {info['current_object_version']}"

    version_numbers = [v[0] for v in info["versions"]]
    assert version_numbers == [1, 2, 3], f"Expected versions [1, 2, 3], got {version_numbers}"

    resp = boto3_client.get_object(Bucket=bucket_name, Key=key)
    assert resp["Body"].read() == b"version 3"


@pytest.mark.local
def test_copy_over_existing_key_creates_new_version(
    docker_services: Any,
    boto3_client: Any,
    unique_bucket_name: Callable[[str], str],
    cleanup_buckets: Callable[[str], None],
) -> None:
    """Test that copying over an existing destination key creates a new version."""
    bucket_name = unique_bucket_name("versioning-copy")
    cleanup_buckets(bucket_name)

    boto3_client.create_bucket(Bucket=bucket_name)

    # Create source object
    source_key = "source.txt"
    boto3_client.put_object(
        Bucket=bucket_name,
        Key=source_key,
        Body=b"source content",
    )

    # Wait for source to be published before copying
    assert wait_for_parts_cids(bucket_name, source_key, min_count=1), "Source object not ready for copy"

    # Create destination object (v1)
    dest_key = "destination.txt"
    boto3_client.put_object(
        Bucket=bucket_name,
        Key=dest_key,
        Body=b"original destination content",
    )

    # Copy source to destination (should create destination v2)
    boto3_client.copy_object(
        Bucket=bucket_name,
        Key=dest_key,
        CopySource={"Bucket": bucket_name, "Key": source_key},
    )

    # Verify destination now has 2 versions
    info = get_object_versioning_info(bucket_name, dest_key)
    assert info["current_object_version"] == 2, f"Expected destination to be v2 after copy, got {info['current_object_version']}"

    versions = info["versions"]
    assert len(versions) == 2, f"Expected 2 versions for destination, got {len(versions)}"
    assert versions[0][0] == 1  # v1 (original content)
    assert versions[1][0] == 2  # v2 (copied content)

    # Verify v1 has original content size
    assert versions[0][1] == len(b"original destination content")

    # Verify v2 has source content size
    assert versions[1][1] == len(b"source content")

    # Verify download returns copied content
    resp = boto3_client.get_object(Bucket=bucket_name, Key=dest_key)
    assert resp["Body"].read() == b"source content"
