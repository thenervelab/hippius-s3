"""An aborted multipart upload must not hand its version number back to the next upload.

Abort used to delete the aborted version's reserved row and repoint current_object_version down,
which made the allocator (GREATEST(current, MAX(object_version)) + 1) reissue the same number. The
aborted attempt's drain rows are marked terminal 'failed' and have no FK to object_versions, so
they survived and poisoned the reused version — nothing re-drives a 'failed' row, so the object
ended up with no pool copy and no backend upload.

`x-amz-version-id` is the version the allocator handed out, so this asserts the invariant through
the real API rather than against the schema.
"""

from typing import Any
from typing import Callable

import pytest
from botocore.exceptions import ClientError


def _version_of(client: Any, bucket: str, key: str) -> int:
    return int(client.head_object(Bucket=bucket, Key=key)["ResponseMetadata"]["HTTPHeaders"]["x-amz-version-id"])


def test_abort_does_not_reissue_the_version_number(
    docker_services: Any,
    boto3_client: Any,
    unique_bucket_name: Callable[[str], str],
    cleanup_buckets: Callable[[str], None],
) -> None:
    bucket = unique_bucket_name("mpu-version-reuse")
    cleanup_buckets(bucket)
    boto3_client.create_bucket(Bucket=bucket)
    key = "reused.bin"

    boto3_client.put_object(Bucket=bucket, Key=key, Body=b"first")
    first = _version_of(boto3_client, bucket, key)

    # The aborted attempt consumes exactly one version number, which must never come back.
    create = boto3_client.create_multipart_upload(Bucket=bucket, Key=key, ContentType="application/octet-stream")
    upload_id = create["UploadId"]
    boto3_client.upload_part(Bucket=bucket, Key=key, UploadId=upload_id, PartNumber=1, Body=b"a" * 1024)
    boto3_client.abort_multipart_upload(Bucket=bucket, Key=key, UploadId=upload_id)

    boto3_client.put_object(Bucket=bucket, Key=key, Body=b"second")
    final = _version_of(boto3_client, bucket, key)

    assert final == first + 2, (
        f"version {first + 1} was reissued after the abort (first={first}, final={final}); "
        "the aborted attempt's terminal drain rows now sit under a live version"
    )
    assert boto3_client.get_object(Bucket=bucket, Key=key)["Body"].read() == b"second"


def test_aborted_version_is_not_fetchable_by_version_id(
    docker_services: Any,
    boto3_client: Any,
    unique_bucket_name: Callable[[str], str],
    cleanup_buckets: Callable[[str], None],
) -> None:
    """The reserved row is retained now, so the by-version read path must still refuse it — a
    reserved version has no parts, so serving it would return a 0-byte body instead of an error."""
    bucket = unique_bucket_name("mpu-aborted-versionid")
    cleanup_buckets(bucket)
    boto3_client.create_bucket(Bucket=bucket)
    key = "reused.bin"

    boto3_client.put_object(Bucket=bucket, Key=key, Body=b"first")
    aborted_version = _version_of(boto3_client, bucket, key) + 1

    create = boto3_client.create_multipart_upload(Bucket=bucket, Key=key, ContentType="application/octet-stream")
    upload_id = create["UploadId"]
    boto3_client.upload_part(Bucket=bucket, Key=key, UploadId=upload_id, PartNumber=1, Body=b"a" * 1024)
    boto3_client.abort_multipart_upload(Bucket=bucket, Key=key, UploadId=upload_id)

    with pytest.raises(ClientError, match="NoSuchVersion|NoSuchKey"):
        boto3_client.get_object(Bucket=bucket, Key=key, VersionId=str(aborted_version))
