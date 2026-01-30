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
    assert info["current_object_version"] == 2, (
        f"Expected current_object_version=2, got {info['current_object_version']}"
    )

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
        MultipartUpload={"Parts": [{"ETag": etag1_p1, "PartNumber": 1}]},
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
        MultipartUpload={
            "Parts": [
                {"ETag": etag2_p1, "PartNumber": 1},
                {"ETag": etag2_p2, "PartNumber": 2},
            ]
        },
    )
    etag2 = completed2["ETag"].strip('"')

    assert etag1 != etag2

    info = get_object_versioning_info(bucket_name, key)
    assert info["current_object_version"] == 2, (
        f"Expected current_object_version=2, got {info['current_object_version']}"
    )

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
    assert info["current_object_version"] == 3, (
        f"Expected current_object_version=3, got {info['current_object_version']}"
    )

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
    assert info["current_object_version"] == 2, (
        f"Expected destination to be v2 after copy, got {info['current_object_version']}"
    )

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


@pytest.mark.local
def test_get_object_by_version_id(
    docker_services: Any,
    boto3_client: Any,
    unique_bucket_name: Callable[[str], str],
    cleanup_buckets: Callable[[str], None],
) -> None:
    """Test downloading specific object versions using versionId parameter."""
    bucket_name = unique_bucket_name("versioning-get-by-id")
    cleanup_buckets(bucket_name)

    boto3_client.create_bucket(Bucket=bucket_name)
    key = "versioned-file.txt"

    v1_content = b"version 1 content"
    v2_content = b"version 2 content different"
    v3_content = b"version 3 even more different"

    boto3_client.put_object(Bucket=bucket_name, Key=key, Body=v1_content)
    boto3_client.put_object(Bucket=bucket_name, Key=key, Body=v2_content)
    boto3_client.put_object(Bucket=bucket_name, Key=key, Body=v3_content)

    info = get_object_versioning_info(bucket_name, key)
    assert info["current_object_version"] == 3

    resp_no_version = boto3_client.get_object(Bucket=bucket_name, Key=key)
    assert resp_no_version["Body"].read() == v3_content
    assert "x-amz-version-id" in resp_no_version["ResponseMetadata"]["HTTPHeaders"]
    assert resp_no_version["ResponseMetadata"]["HTTPHeaders"]["x-amz-version-id"] == "3"

    resp_v1 = boto3_client.get_object(Bucket=bucket_name, Key=key, VersionId="1")
    assert resp_v1["Body"].read() == v1_content
    assert resp_v1["ResponseMetadata"]["HTTPHeaders"]["x-amz-version-id"] == "1"
    assert resp_v1["ContentLength"] == len(v1_content)

    resp_v2 = boto3_client.get_object(Bucket=bucket_name, Key=key, VersionId="2")
    assert resp_v2["Body"].read() == v2_content
    assert resp_v2["ResponseMetadata"]["HTTPHeaders"]["x-amz-version-id"] == "2"
    assert resp_v2["ContentLength"] == len(v2_content)

    resp_v3 = boto3_client.get_object(Bucket=bucket_name, Key=key, VersionId="3")
    assert resp_v3["Body"].read() == v3_content
    assert resp_v3["ResponseMetadata"]["HTTPHeaders"]["x-amz-version-id"] == "3"
    assert resp_v3["ContentLength"] == len(v3_content)


@pytest.mark.local
def test_head_object_by_version_id(
    docker_services: Any,
    boto3_client: Any,
    unique_bucket_name: Callable[[str], str],
    cleanup_buckets: Callable[[str], None],
) -> None:
    """Test HEAD requests with versionId parameter."""
    bucket_name = unique_bucket_name("versioning-head-by-id")
    cleanup_buckets(bucket_name)

    boto3_client.create_bucket(Bucket=bucket_name)
    key = "versioned-sizes.txt"

    v1_content = b"small"
    v2_content = b"much larger content here"

    boto3_client.put_object(Bucket=bucket_name, Key=key, Body=v1_content, ContentType="text/plain")
    boto3_client.put_object(Bucket=bucket_name, Key=key, Body=v2_content, ContentType="text/html")

    info = get_object_versioning_info(bucket_name, key)
    assert info["current_object_version"] == 2

    head_no_version = boto3_client.head_object(Bucket=bucket_name, Key=key)
    assert head_no_version["ContentLength"] == len(v2_content)
    assert head_no_version["ContentType"] == "text/html"
    assert head_no_version["ResponseMetadata"]["HTTPHeaders"]["x-amz-version-id"] == "2"

    head_v1 = boto3_client.head_object(Bucket=bucket_name, Key=key, VersionId="1")
    assert head_v1["ContentLength"] == len(v1_content)
    assert head_v1["ContentType"] == "text/plain"
    assert head_v1["ResponseMetadata"]["HTTPHeaders"]["x-amz-version-id"] == "1"

    head_v2 = boto3_client.head_object(Bucket=bucket_name, Key=key, VersionId="2")
    assert head_v2["ContentLength"] == len(v2_content)
    assert head_v2["ContentType"] == "text/html"
    assert head_v2["ResponseMetadata"]["HTTPHeaders"]["x-amz-version-id"] == "2"


@pytest.mark.local
def test_get_nonexistent_version_returns_404(
    docker_services: Any,
    boto3_client: Any,
    unique_bucket_name: Callable[[str], str],
    cleanup_buckets: Callable[[str], None],
) -> None:
    """Test that requesting a non-existent version returns 404 with NoSuchVersion."""
    bucket_name = unique_bucket_name("versioning-404")
    cleanup_buckets(bucket_name)

    boto3_client.create_bucket(Bucket=bucket_name)
    key = "single-version.txt"

    boto3_client.put_object(Bucket=bucket_name, Key=key, Body=b"only one version")

    info = get_object_versioning_info(bucket_name, key)
    assert info["current_object_version"] == 1

    from botocore.exceptions import ClientError

    with pytest.raises(ClientError) as exc_info:
        boto3_client.get_object(Bucket=bucket_name, Key=key, VersionId="999")

    error = exc_info.value.response["Error"]
    assert error["Code"] == "NoSuchVersion"
    assert "999" in error["Message"]

    with pytest.raises(ClientError) as exc_info:
        boto3_client.head_object(Bucket=bucket_name, Key=key, VersionId="999")

    status = exc_info.value.response.get("ResponseMetadata", {}).get("HTTPStatusCode")
    assert status == 404
    headers = exc_info.value.response.get("ResponseMetadata", {}).get("HTTPHeaders", {})
    assert headers.get("x-amz-error-code") == "NoSuchVersion"
    assert headers.get("x-amz-error-message") and "999" in headers.get("x-amz-error-message")


@pytest.mark.local
def test_invalid_version_id_format_returns_400(
    docker_services: Any,
    boto3_client: Any,
    unique_bucket_name: Callable[[str], str],
    cleanup_buckets: Callable[[str], None],
) -> None:
    """Test that invalid versionId formats return 400 InvalidArgument."""
    bucket_name = unique_bucket_name("versioning-invalid")
    cleanup_buckets(bucket_name)

    boto3_client.create_bucket(Bucket=bucket_name)
    key = "test.txt"

    boto3_client.put_object(Bucket=bucket_name, Key=key, Body=b"content")

    from botocore.exceptions import ClientError

    with pytest.raises(ClientError) as exc_info:
        boto3_client.get_object(Bucket=bucket_name, Key=key, VersionId="not-a-number")

    error = exc_info.value.response["Error"]
    assert error["Code"] == "InvalidArgument"

    with pytest.raises(ClientError) as exc_info:
        boto3_client.get_object(Bucket=bucket_name, Key=key, VersionId="0")

    error = exc_info.value.response["Error"]
    assert error["Code"] == "InvalidArgument"

    with pytest.raises(ClientError) as exc_info:
        boto3_client.get_object(Bucket=bucket_name, Key=key, VersionId="-1")

    error = exc_info.value.response["Error"]
    assert error["Code"] == "InvalidArgument"


@pytest.mark.local
def test_get_version_with_range_request(
    docker_services: Any,
    boto3_client: Any,
    unique_bucket_name: Callable[[str], str],
    cleanup_buckets: Callable[[str], None],
) -> None:
    """Test downloading a specific version with Range header."""
    bucket_name = unique_bucket_name("versioning-range")
    cleanup_buckets(bucket_name)

    boto3_client.create_bucket(Bucket=bucket_name)
    key = "range-versioned.txt"

    v1_content = b"0123456789abcdefghij"
    v2_content = b"ZYXWVUTSRQPONMLKJIHG"

    boto3_client.put_object(Bucket=bucket_name, Key=key, Body=v1_content)
    boto3_client.put_object(Bucket=bucket_name, Key=key, Body=v2_content)

    info = get_object_versioning_info(bucket_name, key)
    assert info["current_object_version"] == 2

    resp_range = boto3_client.get_object(Bucket=bucket_name, Key=key, VersionId="1", Range="bytes=5-9")
    assert resp_range["Body"].read() == b"56789"
    assert resp_range["ResponseMetadata"]["HTTPStatusCode"] == 206
    assert resp_range["ResponseMetadata"]["HTTPHeaders"]["x-amz-version-id"] == "1"
    assert resp_range["ContentLength"] == 5

    resp_range_v2 = boto3_client.get_object(Bucket=bucket_name, Key=key, VersionId="2", Range="bytes=0-4")
    assert resp_range_v2["Body"].read() == b"ZYXWV"
    assert resp_range_v2["ResponseMetadata"]["HTTPStatusCode"] == 206
    assert resp_range_v2["ResponseMetadata"]["HTTPHeaders"]["x-amz-version-id"] == "2"


@pytest.mark.local
def test_get_multipart_version_by_id(
    docker_services: Any,
    boto3_client: Any,
    unique_bucket_name: Callable[[str], str],
    cleanup_buckets: Callable[[str], None],
) -> None:
    """Test downloading specific versions of multipart objects."""
    bucket_name = unique_bucket_name("versioning-mpu-get")
    cleanup_buckets(bucket_name)

    boto3_client.create_bucket(Bucket=bucket_name)
    key = "multipart-versioned.bin"

    part_size = 5 * 1024 * 1024
    v1_data = b"A" * part_size
    v2_data_p1 = b"B" * part_size
    v2_data_p2 = b"C" * part_size

    create1 = boto3_client.create_multipart_upload(Bucket=bucket_name, Key=key)
    upload_id1 = create1["UploadId"]
    etag1_p1 = boto3_client.upload_part(Bucket=bucket_name, Key=key, UploadId=upload_id1, PartNumber=1, Body=v1_data)[
        "ETag"
    ]
    boto3_client.complete_multipart_upload(
        Bucket=bucket_name,
        Key=key,
        UploadId=upload_id1,
        MultipartUpload={"Parts": [{"ETag": etag1_p1, "PartNumber": 1}]},
    )

    create2 = boto3_client.create_multipart_upload(Bucket=bucket_name, Key=key)
    upload_id2 = create2["UploadId"]
    etag2_p1 = boto3_client.upload_part(
        Bucket=bucket_name, Key=key, UploadId=upload_id2, PartNumber=1, Body=v2_data_p1
    )["ETag"]
    etag2_p2 = boto3_client.upload_part(
        Bucket=bucket_name, Key=key, UploadId=upload_id2, PartNumber=2, Body=v2_data_p2
    )["ETag"]
    boto3_client.complete_multipart_upload(
        Bucket=bucket_name,
        Key=key,
        UploadId=upload_id2,
        MultipartUpload={
            "Parts": [
                {"ETag": etag2_p1, "PartNumber": 1},
                {"ETag": etag2_p2, "PartNumber": 2},
            ]
        },
    )

    info = get_object_versioning_info(bucket_name, key)
    assert info["current_object_version"] == 2

    resp_v1 = boto3_client.get_object(Bucket=bucket_name, Key=key, VersionId="1")
    v1_downloaded = resp_v1["Body"].read()
    assert v1_downloaded == v1_data
    assert resp_v1["ContentLength"] == part_size
    assert resp_v1["ResponseMetadata"]["HTTPHeaders"]["x-amz-version-id"] == "1"

    resp_v2 = boto3_client.get_object(Bucket=bucket_name, Key=key, VersionId="2")
    v2_downloaded = resp_v2["Body"].read()
    assert v2_downloaded == v2_data_p1 + v2_data_p2
    assert resp_v2["ContentLength"] == 2 * part_size
    assert resp_v2["ResponseMetadata"]["HTTPHeaders"]["x-amz-version-id"] == "2"
