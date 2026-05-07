"""E2E tests for the bucket soft-delete contract.

Phase 1 acceptance: DeleteBucket returns 204 in O(1), the bucket disappears
from all S3 reads, and the same name is immediately reusable for a fresh
CreateBucket.
"""

from typing import Any
from typing import Callable

import pytest
from botocore.exceptions import ClientError


def test_delete_bucket_returns_204_and_hides_bucket(
    docker_services: Any,
    boto3_client: Any,
    unique_bucket_name: Callable[[str], str],
) -> None:
    """Soft-delete contract: DeleteBucket → 204; HeadBucket / ListBuckets /
    GetObject all return 404 immediately."""
    bucket_name = unique_bucket_name("soft-delete")

    boto3_client.create_bucket(Bucket=bucket_name)
    boto3_client.head_bucket(Bucket=bucket_name)

    boto3_client.delete_bucket(Bucket=bucket_name)

    with pytest.raises(ClientError) as exc:
        boto3_client.head_bucket(Bucket=bucket_name)
    assert exc.value.response["Error"]["Code"] in {"404", "NoSuchBucket"}

    listed = {b["Name"] for b in boto3_client.list_buckets()["Buckets"]}
    assert bucket_name not in listed

    with pytest.raises(ClientError) as exc:
        boto3_client.get_object(Bucket=bucket_name, Key="anything")
    assert exc.value.response["Error"]["Code"] == "NoSuchBucket"


def test_delete_bucket_idempotent_returns_404(
    docker_services: Any,
    boto3_client: Any,
    unique_bucket_name: Callable[[str], str],
) -> None:
    """Second DeleteBucket against an already-deleted bucket must 404, not
    403 AccessDenied. Regression guard for the legacy endpoint's behavior."""
    bucket_name = unique_bucket_name("soft-delete-idem")

    boto3_client.create_bucket(Bucket=bucket_name)
    boto3_client.delete_bucket(Bucket=bucket_name)

    with pytest.raises(ClientError) as exc:
        boto3_client.delete_bucket(Bucket=bucket_name)
    code = exc.value.response["Error"]["Code"]
    assert code == "NoSuchBucket", f"expected NoSuchBucket, got {code}"


def test_bucket_name_reusable_after_delete(
    docker_services: Any,
    boto3_client: Any,
    unique_bucket_name: Callable[[str], str],
    cleanup_buckets: Callable[[str], None],
) -> None:
    """S3 spec: bucket name is immediately reusable after DeleteBucket. The
    partial unique index buckets_bucket_name_active_key WHERE deleted_at IS
    NULL is what makes this work."""
    bucket_name = unique_bucket_name("soft-delete-reuse")
    cleanup_buckets(bucket_name)

    boto3_client.create_bucket(Bucket=bucket_name)
    boto3_client.delete_bucket(Bucket=bucket_name)

    boto3_client.create_bucket(Bucket=bucket_name)
    boto3_client.head_bucket(Bucket=bucket_name)


def test_delete_bucket_with_objects_rejects(
    docker_services: Any,
    boto3_client: Any,
    unique_bucket_name: Callable[[str], str],
    cleanup_buckets: Callable[[str], None],
) -> None:
    """S3 spec: BucketNotEmpty when live objects exist. Soft-delete must not
    weaken this guard."""
    bucket_name = unique_bucket_name("soft-delete-notempty")
    cleanup_buckets(bucket_name)

    boto3_client.create_bucket(Bucket=bucket_name)
    boto3_client.put_object(Bucket=bucket_name, Key="obj", Body=b"hello")

    with pytest.raises(ClientError) as exc:
        boto3_client.delete_bucket(Bucket=bucket_name)
    assert exc.value.response["Error"]["Code"] == "BucketNotEmpty"
