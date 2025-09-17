import uuid
from typing import Any

import pytest


try:
    import redis  # type: ignore[import-untyped]
except Exception:  # pragma: no cover - allow skipping if unavailable
    redis = None  # type: ignore[assignment]

from .support.cache import clear_object_cache
from .support.cache import get_object_id
from .support.cache import wait_for_parts_cids


def _put_object(boto3_client: Any, bucket: str, key: str, data: bytes, metadata: dict[str, str] | None = None) -> None:
    boto3_client.put_object(
        Bucket=bucket,
        Key=key,
        Body=data,
        ContentType="application/octet-stream",
        Metadata=metadata or {},
    )


@pytest.mark.s4
# Note: synchronous test body; no asyncio marker needed
def test_get_partial_cache_fallbacks(
    boto3_client: Any,
    unique_bucket_name: Any,
    cleanup_buckets: Any,
    signed_http_get: Any,
) -> None:
    if redis is None:
        pytest.skip("redis python client not available in test environment")

    bucket = unique_bucket_name("partial-cache")
    boto3_client.create_bucket(Bucket=bucket)
    cleanup_buckets(bucket)

    key = "logs/partial.txt"

    # Use unique payloads to locate cache keys reliably
    base_marker = str(uuid.uuid4()).encode()
    append_marker = str(uuid.uuid4()).encode()

    base = b"BASE-" + base_marker + b"\n"
    _put_object(boto3_client, bucket, key, base)

    # Capture append version for CAS
    ver = boto3_client.head_object(Bucket=bucket, Key=key)["ResponseMetadata"]["HTTPHeaders"].get(
        "x-amz-meta-append-version", "0"
    )

    delta = b"DELTA-" + append_marker + b"\n"
    _put_object(
        boto3_client,
        bucket,
        key,
        delta,
        metadata={"append": "true", "append-if-version": ver, "append-id": "partial-cache-test"},
    )

    # Wait until at least 2 parts have CIDs in DB
    assert wait_for_parts_cids(bucket, key, min_count=2), "parts not ready with CIDs"

    # Get object_id and clear appended part from obj: cache to simulate partial cache
    object_id = get_object_id(bucket, key)
    clear_object_cache(object_id, parts=[2])

    # GET auto: should succeed and return full content (allow brief retry for pipeline readiness)
    expected = base + delta
    resp_auto = signed_http_get(bucket, key)
    assert resp_auto.status_code == 200
    assert resp_auto.content == expected

    # Cross-boundary range: last 3 bytes of base and first 3 of delta
    start = max(0, len(base) - 3)
    end = len(base) + 2
    resp_range = signed_http_get(bucket, key, {"Range": f"bytes={start}-{end}"})
    assert resp_range.status_code == 206
    assert resp_range.content == expected[start : end + 1]
