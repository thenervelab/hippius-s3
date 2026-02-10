import uuid
from typing import Any

import pytest

from .support.cache import clear_object_cache
from .support.cache import wait_for_all_backends_ready


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
    # Wait a bit for append to complete
    import time

    time.sleep(0.1)

    # Wait until at least 2 parts have CIDs in DB
    assert wait_for_all_backends_ready(bucket, key, min_count=2), "parts not ready with CIDs"

    # Get object_id and clear appended part from obj: cache to simulate partial cache
    from .support.cache import get_object_id_and_version

    object_id, ov = get_object_id_and_version(bucket, key)
    clear_object_cache(object_id, parts=[1])

    # Diagnostic: verify FS cache presence before first GET
    from pathlib import Path

    cache_dir = Path("/var/lib/hippius/object_cache")
    has1 = (cache_dir / object_id / f"v{ov}" / "part_1" / "meta.json").exists()
    has2 = (cache_dir / object_id / f"v{ov}" / "part_2" / "meta.json").exists()
    print(f"DEBUG cache before GET: object_id={object_id} part1meta={has1} part2meta={has2}")

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
    print(f"DEBUG range: base_len={len(base)} delta_len={len(delta)} start={start} end={end}")
    print(f"DEBUG expected content: {expected!r}")
    print(f"DEBUG expected range: {expected[start : end + 1]!r}")
    resp_range = signed_http_get(bucket, key, {"Range": f"bytes={start}-{end}"})
    assert resp_range.status_code == 206
    print(f"DEBUG actual range content: {resp_range.content}")
    assert resp_range.content == expected[start : end + 1]
    # Range request should be served from cache (after first request populated it)
    assert resp_range.headers.get("x-hippius-source") == "cache"
