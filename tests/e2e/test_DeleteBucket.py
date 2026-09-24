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


def _delete_every_version(client: Any, bucket: str) -> None:
    listing = client.list_object_versions(Bucket=bucket)
    for entry in listing.get("Versions", []) + listing.get("DeleteMarkers", []):
        client.delete_object(Bucket=bucket, Key=entry["Key"], VersionId=entry["VersionId"])


def test_delete_bucket_refuses_versions_hidden_behind_delete_markers(
    docker_services: Any,
    boto3_client: Any,
    unique_bucket_name: Callable[[str], str],
    cleanup_buckets: Callable[[str], None],
) -> None:
    """ListObjects shows nothing once every key is behind a delete marker, but the data versions
    are still there. DeleteBucket used to trust ListObjects and orphan them."""
    bucket = unique_bucket_name("delete-versioned")
    cleanup_buckets(bucket)
    boto3_client.create_bucket(Bucket=bucket)
    boto3_client.put_bucket_versioning(Bucket=bucket, VersioningConfiguration={"Status": "Enabled"})

    boto3_client.put_object(Bucket=bucket, Key="k", Body=b"v1")
    boto3_client.put_object(Bucket=bucket, Key="k", Body=b"v2")
    boto3_client.delete_object(Bucket=bucket, Key="k")
    assert "Contents" not in boto3_client.list_objects_v2(Bucket=bucket)

    with pytest.raises(ClientError) as ei:
        boto3_client.delete_bucket(Bucket=bucket)
    assert ei.value.response["Error"]["Code"] == "BucketNotEmpty"
    assert ei.value.response["ResponseMetadata"]["HTTPStatusCode"] == 409

    _delete_every_version(boto3_client, bucket)
    resp = boto3_client.delete_bucket(Bucket=bucket)
    assert resp["ResponseMetadata"]["HTTPStatusCode"] == 204


def test_delete_bucket_refuses_a_lone_delete_marker(
    docker_services: Any,
    boto3_client: Any,
    unique_bucket_name: Callable[[str], str],
    cleanup_buckets: Callable[[str], None],
) -> None:
    """AWS: delete markers must go too before a bucket is empty."""
    bucket = unique_bucket_name("delete-marker")
    cleanup_buckets(bucket)
    boto3_client.create_bucket(Bucket=bucket)
    boto3_client.put_bucket_versioning(Bucket=bucket, VersioningConfiguration={"Status": "Enabled"})

    version_id = boto3_client.put_object(Bucket=bucket, Key="k", Body=b"v1")["VersionId"]
    boto3_client.delete_object(Bucket=bucket, Key="k")
    boto3_client.delete_object(Bucket=bucket, Key="k", VersionId=version_id)

    with pytest.raises(ClientError) as ei:
        boto3_client.delete_bucket(Bucket=bucket)
    assert ei.value.response["Error"]["Code"] == "BucketNotEmpty"

    _delete_every_version(boto3_client, bucket)
    boto3_client.delete_bucket(Bucket=bucket)
