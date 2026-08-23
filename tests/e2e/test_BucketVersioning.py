from typing import Any
from typing import Callable

import pytest
from botocore.exceptions import ClientError

from .support.cache import wait_for_all_backends_ready


def _enable(client: Any, bucket: str) -> None:
    client.put_bucket_versioning(Bucket=bucket, VersioningConfiguration={"Status": "Enabled"})


def _keys(client: Any, bucket: str) -> list[str]:
    resp = client.list_objects_v2(Bucket=bucket)
    return [c["Key"] for c in resp.get("Contents", [])]


@pytest.mark.local
def test_put_get_bucket_versioning_roundtrip(
    docker_services: Any,
    boto3_client: Any,
    unique_bucket_name: Callable[[str], str],
    cleanup_buckets: Callable[[str], None],
) -> None:
    bucket = unique_bucket_name("versioning-toggle")
    cleanup_buckets(bucket)
    boto3_client.create_bucket(Bucket=bucket)

    # A never-versioned bucket reports no Status at all.
    assert "Status" not in boto3_client.get_bucket_versioning(Bucket=bucket)

    _enable(boto3_client, bucket)
    assert boto3_client.get_bucket_versioning(Bucket=bucket)["Status"] == "Enabled"

    # Idempotent.
    _enable(boto3_client, bucket)
    assert boto3_client.get_bucket_versioning(Bucket=bucket)["Status"] == "Enabled"


@pytest.mark.local
def test_suspended_is_not_implemented(
    docker_services: Any,
    boto3_client: Any,
    unique_bucket_name: Callable[[str], str],
    cleanup_buckets: Callable[[str], None],
) -> None:
    bucket = unique_bucket_name("versioning-suspend")
    cleanup_buckets(bucket)
    boto3_client.create_bucket(Bucket=bucket)
    _enable(boto3_client, bucket)

    with pytest.raises(ClientError) as exc:
        boto3_client.put_bucket_versioning(Bucket=bucket, VersioningConfiguration={"Status": "Suspended"})
    assert exc.value.response["Error"]["Code"] == "NotImplemented"
    # The bucket is left Enabled, not half-changed.
    assert boto3_client.get_bucket_versioning(Bucket=bucket)["Status"] == "Enabled"


@pytest.mark.local
def test_put_returns_version_id_and_list_versions_shows_history(
    docker_services: Any,
    boto3_client: Any,
    unique_bucket_name: Callable[[str], str],
    cleanup_buckets: Callable[[str], None],
) -> None:
    bucket = unique_bucket_name("versioning-list")
    cleanup_buckets(bucket)
    boto3_client.create_bucket(Bucket=bucket)
    _enable(boto3_client, bucket)

    key = "doc.txt"
    put_ids = [
        boto3_client.put_object(Bucket=bucket, Key=key, Body=f"body {i}".encode())["VersionId"] for i in range(1, 4)
    ]
    assert put_ids == ["1", "2", "3"], f"PutObject must return x-amz-version-id, got {put_ids}"

    resp = boto3_client.list_object_versions(Bucket=bucket)
    versions = resp["Versions"]
    assert [v["VersionId"] for v in versions] == ["3", "2", "1"]
    assert [v["IsLatest"] for v in versions] == [True, False, False]
    assert all(v["Key"] == key for v in versions)
    assert versions[0]["Size"] == len(b"body 3")

    # Every historical version is still readable by id.
    for i, vid in enumerate(["1", "2", "3"], start=1):
        got = boto3_client.get_object(Bucket=bucket, Key=key, VersionId=vid)["Body"].read()
        assert got == f"body {i}".encode()


@pytest.mark.local
def test_simple_delete_creates_marker_and_hides_key(
    docker_services: Any,
    boto3_client: Any,
    unique_bucket_name: Callable[[str], str],
    cleanup_buckets: Callable[[str], None],
) -> None:
    """A delete marker must hide the key from ListObjectsV2 without destroying anything.

    The subtle failure this guards: the repo-wide "serveable version" predicate
    (size>0 OR md5!='') skips a zero-size delete marker, which would make listing and GET
    silently fall back to the PREVIOUS version and serve deleted content.
    """
    bucket = unique_bucket_name("versioning-marker")
    cleanup_buckets(bucket)
    boto3_client.create_bucket(Bucket=bucket)
    _enable(boto3_client, bucket)

    key = "doc.txt"
    boto3_client.put_object(Bucket=bucket, Key=key, Body=b"original")
    boto3_client.put_object(Bucket=bucket, Key=key, Body=b"updated")
    assert _keys(boto3_client, bucket) == [key]

    deleted = boto3_client.delete_object(Bucket=bucket, Key=key)
    assert deleted.get("DeleteMarker") is True
    marker_id = deleted["VersionId"]

    # Hidden from listing, and NOT falling back to the older version.
    assert _keys(boto3_client, bucket) == []

    with pytest.raises(ClientError) as exc:
        boto3_client.get_object(Bucket=bucket, Key=key)
    assert exc.value.response["ResponseMetadata"]["HTTPStatusCode"] == 404
    assert exc.value.response["ResponseMetadata"]["HTTPHeaders"].get("x-amz-delete-marker") == "true"

    # The data itself survives, reachable by version id.
    assert boto3_client.get_object(Bucket=bucket, Key=key, VersionId="2")["Body"].read() == b"updated"

    # The marker shows up in the version listing.
    listing = boto3_client.list_object_versions(Bucket=bucket)
    assert [m["VersionId"] for m in listing["DeleteMarkers"]] == [marker_id]

    # HEAD must agree with GET on the status matrix — clients branch on exactly this.
    with pytest.raises(ClientError) as exc:
        boto3_client.head_object(Bucket=bucket, Key=key)
    head_meta = exc.value.response["ResponseMetadata"]
    assert head_meta["HTTPStatusCode"] == 404
    assert head_meta["HTTPHeaders"].get("x-amz-delete-marker") == "true"

    # Addressing the marker directly is a 405, not a download.
    with pytest.raises(ClientError) as exc:
        boto3_client.get_object(Bucket=bucket, Key=key, VersionId=marker_id)
    get_meta = exc.value.response["ResponseMetadata"]
    assert get_meta["HTTPStatusCode"] == 405
    assert get_meta["HTTPHeaders"].get("x-amz-delete-marker") == "true"

    with pytest.raises(ClientError) as exc:
        boto3_client.head_object(Bucket=bucket, Key=key, VersionId=marker_id)
    head_marker = exc.value.response["ResponseMetadata"]
    assert head_marker["HTTPStatusCode"] == 405
    assert head_marker["HTTPHeaders"].get("x-amz-delete-marker") == "true"
    assert head_marker["HTTPHeaders"].get("last-modified")

    # Removing the marker undeletes the object.
    boto3_client.delete_object(Bucket=bucket, Key=key, VersionId=marker_id)
    assert boto3_client.get_object(Bucket=bucket, Key=key)["Body"].read() == b"updated"
    assert _keys(boto3_client, bucket) == [key]


@pytest.mark.local
def test_versioned_delete_removes_only_that_version(
    docker_services: Any,
    boto3_client: Any,
    unique_bucket_name: Callable[[str], str],
    cleanup_buckets: Callable[[str], None],
) -> None:
    """Regression: this used to destroy the whole object, every version, on prod."""
    bucket = unique_bucket_name("versioning-onedel")
    cleanup_buckets(bucket)
    boto3_client.create_bucket(Bucket=bucket)

    key = "doc.txt"
    for body in (b"one", b"two", b"three"):
        boto3_client.put_object(Bucket=bucket, Key=key, Body=body)

    boto3_client.delete_object(Bucket=bucket, Key=key, VersionId="1")

    # The object and its other versions are untouched.
    assert boto3_client.get_object(Bucket=bucket, Key=key)["Body"].read() == b"three"
    assert boto3_client.get_object(Bucket=bucket, Key=key, VersionId="2")["Body"].read() == b"two"
    assert _keys(boto3_client, bucket) == [key]

    with pytest.raises(ClientError) as exc:
        boto3_client.get_object(Bucket=bucket, Key=key, VersionId="1")
    assert exc.value.response["Error"]["Code"] == "NoSuchVersion"


@pytest.mark.local
def test_deleting_current_version_exposes_previous(
    docker_services: Any,
    boto3_client: Any,
    unique_bucket_name: Callable[[str], str],
    cleanup_buckets: Callable[[str], None],
) -> None:
    bucket = unique_bucket_name("versioning-rollback")
    cleanup_buckets(bucket)
    boto3_client.create_bucket(Bucket=bucket)

    key = "doc.txt"
    boto3_client.put_object(Bucket=bucket, Key=key, Body=b"one")
    boto3_client.put_object(Bucket=bucket, Key=key, Body=b"two")

    boto3_client.delete_object(Bucket=bucket, Key=key, VersionId="2")

    assert boto3_client.get_object(Bucket=bucket, Key=key)["Body"].read() == b"one"
    assert boto3_client.head_object(Bucket=bucket, Key=key)["VersionId"] == "1"


@pytest.mark.local
def test_copy_from_source_version_id_restores_old_version(
    docker_services: Any,
    boto3_client: Any,
    unique_bucket_name: Callable[[str], str],
    cleanup_buckets: Callable[[str], None],
) -> None:
    """CopyObject used to silently drop ?versionId and copy the current version."""
    bucket = unique_bucket_name("versioning-copy")
    cleanup_buckets(bucket)
    boto3_client.create_bucket(Bucket=bucket)

    key = "doc.txt"
    boto3_client.put_object(Bucket=bucket, Key=key, Body=b"ORIGINAL")
    boto3_client.put_object(Bucket=bucket, Key=key, Body=b"REPLACED")

    # handle_streaming_copy fails fast with 503 while the source is still draining, so wait for
    # the source to be backend-ready before copying (same guard as test_ObjectVersioning.py).
    assert wait_for_all_backends_ready(bucket, key, min_count=1), "Source object not ready for copy"

    copied = boto3_client.copy_object(
        Bucket=bucket,
        Key="restored.txt",
        CopySource={"Bucket": bucket, "Key": key, "VersionId": "1"},
    )

    assert boto3_client.get_object(Bucket=bucket, Key="restored.txt")["Body"].read() == b"ORIGINAL"
    # AWS reports which source version was read, so a restore flow can confirm what it got.
    assert copied["ResponseMetadata"]["HTTPHeaders"].get("x-amz-copy-source-version-id") == "1"
