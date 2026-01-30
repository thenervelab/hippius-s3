from typing import Any
from typing import Callable

from .conftest import is_real_aws
from .support.cache import wait_for_all_backends_ready


def test_put_object_overwrite_twice(
    docker_services: Any,
    boto3_client: Any,
    unique_bucket_name: Callable[[str], str],
    cleanup_buckets: Callable[[str], None],
) -> None:
    bucket_name = unique_bucket_name("put-overwrite")
    cleanup_buckets(bucket_name)

    boto3_client.create_bucket(Bucket=bucket_name)

    key = "overwrite.txt"

    resp1 = boto3_client.put_object(
        Bucket=bucket_name,
        Key=key,
        Body=b"first",
        ContentType="text/plain",
    )
    assert resp1["ResponseMetadata"]["HTTPStatusCode"] in (200, 204)

    # Ensure background uploader had a chance to run (local stack only)
    if not is_real_aws():
        assert wait_for_all_backends_ready(bucket_name, key, min_count=1, timeout_seconds=20.0)

    # Second PUT to same key should succeed (idempotent upload row)
    resp2 = boto3_client.put_object(
        Bucket=bucket_name,
        Key=key,
        Body=b"second",
        ContentType="text/plain",
    )
    assert resp2["ResponseMetadata"]["HTTPStatusCode"] in (200, 204)


def test_put_object_overwrite_manifest_pruned(
    docker_services: Any,
    boto3_client: Any,
    unique_bucket_name: Callable[[str], str],
    cleanup_buckets: Callable[[str], None],
) -> None:
    bucket_name = unique_bucket_name("put-overwrite-manifest")
    cleanup_buckets(bucket_name)

    boto3_client.create_bucket(Bucket=bucket_name)

    key = ".hippius_manifest_v1/pruned.json"

    resp1 = boto3_client.put_object(
        Bucket=bucket_name,
        Key=key,
        Body=b"{}",  # 2 bytes
        ContentType="application/json",
    )
    assert resp1["ResponseMetadata"]["HTTPStatusCode"] in (200, 204)

    if not is_real_aws():
        assert wait_for_all_backends_ready(bucket_name, key, min_count=1, timeout_seconds=20.0)

    resp2 = boto3_client.put_object(
        Bucket=bucket_name,
        Key=key,
        Body=b"{}",
        ContentType="application/json",
    )
    assert resp2["ResponseMetadata"]["HTTPStatusCode"] in (200, 204)
