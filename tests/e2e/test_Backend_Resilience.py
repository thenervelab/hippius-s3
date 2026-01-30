"""E2E tests for backend resilience: reads survive single backend failure."""

from contextlib import suppress
from typing import Any
from typing import Callable

import pytest

from .support.cache import clear_object_cache
from .support.cache import get_object_id
from .support.cache import wait_for_all_backends_ready
from .support.compose import disable_arion_proxy
from .support.compose import disable_ipfs_proxy
from .support.compose import enable_arion_proxy
from .support.compose import enable_ipfs_proxy


@pytest.mark.local
def test_read_succeeds_with_ipfs_down(
    docker_services: Any,
    boto3_client: Any,
    unique_bucket_name: Callable[[str], str],
    cleanup_buckets: Callable[[str], None],
) -> None:
    """Upload, wait for both backends, kill IPFS proxy, read still succeeds via Arion."""
    bucket = unique_bucket_name("resil-ipfs-down")
    cleanup_buckets(bucket)
    boto3_client.create_bucket(Bucket=bucket)

    key = "resilience-ipfs.bin"
    content = b"hello resilience ipfs test"
    boto3_client.put_object(Bucket=bucket, Key=key, Body=content)

    assert wait_for_all_backends_ready(bucket, key, min_count=1, timeout_seconds=30.0)

    object_id = get_object_id(bucket, key)
    clear_object_cache(object_id)

    disable_ipfs_proxy()
    try:
        resp = boto3_client.get_object(Bucket=bucket, Key=key)
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200
        assert resp["Body"].read() == content
    finally:
        with suppress(Exception):
            enable_ipfs_proxy()


@pytest.mark.local
def test_read_succeeds_with_arion_down(
    docker_services: Any,
    boto3_client: Any,
    unique_bucket_name: Callable[[str], str],
    cleanup_buckets: Callable[[str], None],
) -> None:
    """Upload, wait for both backends, kill Arion proxy, read still succeeds via IPFS."""
    bucket = unique_bucket_name("resil-arion-down")
    cleanup_buckets(bucket)
    boto3_client.create_bucket(Bucket=bucket)

    key = "resilience-arion.bin"
    content = b"hello resilience arion test"
    boto3_client.put_object(Bucket=bucket, Key=key, Body=content)

    assert wait_for_all_backends_ready(bucket, key, min_count=1, timeout_seconds=30.0)

    object_id = get_object_id(bucket, key)
    clear_object_cache(object_id)

    disable_arion_proxy()
    try:
        resp = boto3_client.get_object(Bucket=bucket, Key=key)
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200
        assert resp["Body"].read() == content
    finally:
        with suppress(Exception):
            enable_arion_proxy()
