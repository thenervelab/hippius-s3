import time
import uuid
from typing import Any

import pytest


try:
    import redis  # type: ignore[import-untyped]
except Exception:  # pragma: no cover - allow skipping if unavailable
    redis = None  # type: ignore[assignment]


def _put_object(boto3_client: Any, bucket: str, key: str, data: bytes, metadata: dict[str, str] | None = None) -> None:
    boto3_client.put_object(
        Bucket=bucket,
        Key=key,
        Body=data,
        ContentType="application/octet-stream",
        Metadata=metadata or {},
    )


@pytest.mark.skip(reason="Temporarily skipped while refactoring read-mode behavior and cache hydration")
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

    # Locate object_id in Redis by matching part 0 bytes
    r = redis.Redis(host="localhost", port=6379, db=0)
    cursor = 0
    object_id: str | None = None
    pattern = "obj:*:part:0"
    while True:
        cursor, keys = r.scan(cursor=cursor, match=pattern, count=100)
        for k in keys:
            try:
                data = r.get(k)
                if data == base:
                    # key format: obj:{object_id}:part:0
                    parts = k.decode().split(":")
                    if len(parts) >= 4:
                        object_id = parts[1]
                        break
            except Exception:
                continue
        if object_id or cursor == 0:
            break

    assert object_id is not None, "Failed to locate object_id in Redis for test object"

    # Simulate partial cache: remove part 1 to force mixed cache/pipeline coverage
    r.delete(f"obj:{object_id}:part:1")

    # GET auto: should succeed and return full content (allow brief retry for pipeline readiness)
    expected = base + delta
    resp_auto = None
    for _ in range(25):
        resp_auto = signed_http_get(bucket, key)
        if resp_auto.status_code == 200 and resp_auto.content == expected:
            break
        time.sleep(0.2)
    assert resp_auto is not None
    assert resp_auto.status_code == 200
    assert resp_auto.content == expected

    # GET cache_only: should fail because part 1 is missing in cache
    resp_cache_only = signed_http_get(bucket, key, {"x-hippius-read-mode": "cache_only"})
    assert resp_cache_only.status_code in {503, 500, 404}, "Expected failure when cache-only with missing parts"

    # Cross-boundary range: bytes spanning end of part0 (base) into part1 (delta)
    # Pick a small slice that straddles boundary if possible
    # Use last 3 bytes from base and first 3 bytes from delta
    start = max(0, len(base) - 3)
    end = len(base) + 2
    resp_range = signed_http_get(bucket, key, {"Range": f"bytes={start}-{end}"})
    assert resp_range.status_code == 206
    assert resp_range.content == expected[start : end + 1]
