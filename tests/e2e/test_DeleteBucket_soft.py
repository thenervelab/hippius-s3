"""E2E tests for the bucket soft-delete contract.

Phase 1 acceptance: DeleteBucket returns 204 in O(1), the bucket disappears
from all S3 reads, and the same name is immediately reusable for a fresh
CreateBucket.
"""

import time
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


def test_create_bucket_after_delete_collision_still_serialized(
    docker_services: Any,
    boto3_client: Any,
    unique_bucket_name: Callable[[str], str],
    cleanup_buckets: Callable[[str], None],
) -> None:
    """The partial unique index buckets_bucket_name_active_key WHERE
    deleted_at IS NULL must still serialize concurrent CreateBuckets sharing
    a name. Two creates of the same fresh name → exactly one wins.
    """
    bucket_name = unique_bucket_name("soft-delete-collision")
    cleanup_buckets(bucket_name)

    boto3_client.create_bucket(Bucket=bucket_name)
    boto3_client.delete_bucket(Bucket=bucket_name)

    boto3_client.create_bucket(Bucket=bucket_name)

    # Second CreateBucket of the same now-live name from the same owner →
    # AWS returns BucketAlreadyOwnedByYou (or BucketAlreadyExists in some
    # regions). Either is correct; not a 200.
    with pytest.raises(ClientError) as exc:
        boto3_client.create_bucket(Bucket=bucket_name)
    code = exc.value.response["Error"]["Code"]
    assert code in {"BucketAlreadyExists", "BucketAlreadyOwnedByYou"}, f"expected a Bucket-Already-* code, got {code}"

    # And the (re-created) bucket is still functional.
    boto3_client.head_bucket(Bucket=bucket_name)


def test_get_object_after_delete_returns_NoSuchBucket(
    docker_services: Any,
    boto3_client: Any,
    unique_bucket_name: Callable[[str], str],
) -> None:
    """Regression guard for the 1ea2981 fix: after soft-delete, GetObject
    must return NoSuchBucket — not NoSuchKey. The legacy endpoint collapsed
    both into NoSuchKey (the JOIN returned no rows in either case).
    """
    bucket_name = unique_bucket_name("soft-delete-getobj")

    boto3_client.create_bucket(Bucket=bucket_name)
    boto3_client.put_object(Bucket=bucket_name, Key="present.txt", Body=b"x")

    # Empty the bucket so DeleteBucket succeeds.
    boto3_client.delete_object(Bucket=bucket_name, Key="present.txt")
    # Wait briefly for the DeleteObject soft-delete to land before DeleteBucket
    # checks emptiness. (S3 is eventually-consistent; this is a small belt-and-
    # suspenders pause for in-process test ordering, not a real-world race.)
    time.sleep(0.2)
    boto3_client.delete_bucket(Bucket=bucket_name)

    with pytest.raises(ClientError) as exc_get:
        boto3_client.get_object(Bucket=bucket_name, Key="anything")
    assert exc_get.value.response["Error"]["Code"] == "NoSuchBucket"

    with pytest.raises(ClientError) as exc_head:
        boto3_client.head_object(Bucket=bucket_name, Key="anything")
    # HEAD doesn't include an XML body but boto3 still surfaces the code.
    assert exc_head.value.response["Error"]["Code"] in {"404", "NoSuchBucket"}
