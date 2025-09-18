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
    clear_object_cache(object_id, parts=[1])

    # Diagnostic: verify cache presence before first GET
    try:
        import redis as _redis  # type: ignore[import-untyped]

        r = _redis.Redis.from_url("redis://localhost:6379/0")
        has0 = bool(r.exists(f"obj:{object_id}:part:0"))
        has1 = bool(r.exists(f"obj:{object_id}:part:1"))
        print(f"DEBUG cache before GET: object_id={object_id} part0={has0} part1={has1}")
    except Exception as _e:  # pragma: no cover
        print(f"DEBUG cache probe failed: {_e}")

    # GET auto: should succeed and return full content
    expected = base + delta
    resp_auto = signed_http_get(bucket, key)
    assert resp_auto.status_code == 200
    assert resp_auto.content == expected
    # First request: since part 2 was cleared, this should use pipeline
    assert resp_auto.headers.get("x-hippius-source") == "pipeline"

    # Cross-boundary range: last 3 bytes of base and first 3 of delta
    # Second request should be served from cache (after first request populated it)
    start = max(0, len(base) - 3)
    end = len(base) + 2
    resp_range = signed_http_get(bucket, key, {"Range": f"bytes={start}-{end}"})
    assert resp_range.status_code == 206
    assert resp_range.content == expected[start : end + 1]
    # Range request should be served from cache (after first request populated it)
    assert resp_range.headers.get("x-hippius-source") == "cache"
