"""E2E tests for backend resilience: reads survive backend failure and recovery."""

from contextlib import suppress
from typing import Any
from typing import Callable

import pytest

from .support.cache import clear_object_cache
from .support.cache import get_object_id
from .support.cache import wait_for_all_backends_ready
from .support.compose import compose_exec
from .support.compose import disable_arion_proxy
from .support.compose import enable_arion_proxy


@pytest.mark.local
def test_read_succeeds_with_arion_down_and_recovery(
    docker_services: Any,
    boto3_client: Any,
    unique_bucket_name: Callable[[str], str],
    cleanup_buckets: Callable[[str], None],
) -> None:
    """Upload, wait for Arion backend, kill Arion proxy, verify read fails, restore and verify recovery."""
    bucket = unique_bucket_name("resil-arion-down")
    cleanup_buckets(bucket)
    boto3_client.create_bucket(Bucket=bucket)

    key = "resilience-arion.bin"
    content = b"hello resilience arion test"
    boto3_client.put_object(Bucket=bucket, Key=key, Body=content)

    assert wait_for_all_backends_ready(bucket, key, min_count=1, timeout_seconds=30.0)

    object_id = get_object_id(bucket, key)
    clear_object_cache(object_id)
    # Also clear the FS cache inside the API container so the reader can't serve from disk
    compose_exec("api", ["rm", "-rf", f"/var/lib/hippius/object_cache/{object_id}"])

    disable_arion_proxy()
    try:
        # With Arion down and cache cleared, read should fail
        from botocore.exceptions import ClientError

        with pytest.raises(ClientError):
            boto3_client.get_object(Bucket=bucket, Key=key)
    finally:
        with suppress(Exception):
            enable_arion_proxy()

    # After restoring Arion, read should succeed
    resp = boto3_client.get_object(Bucket=bucket, Key=key)
    assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200
    assert resp["Body"].read() == content
