"""E2E tests for GetObject with Range requests."""

import time
from typing import Any
from typing import Callable

import pytest
from botocore.exceptions import ClientError

from .support.cache import clear_object_cache
from .support.cache import wait_for_parts_cids


def test_get_object_range_valid(
    docker_services: Any,
    boto3_client: Any,
    unique_bucket_name: Callable[[str], str],
    cleanup_buckets: Callable[[str], None],
) -> None:
    bucket = unique_bucket_name("range-valid")
    cleanup_buckets(bucket)

    boto3_client.create_bucket(Bucket=bucket)
    key = "range.txt"
    data = b"abcdefghijklmnopqrstuvwxyz"  # 26 bytes
    boto3_client.put_object(Bucket=bucket, Key=key, Body=data, ContentType="text/plain")

    # bytes=0-4
    resp = boto3_client.get_object(Bucket=bucket, Key=key, Range="bytes=0-4")
    assert resp["ResponseMetadata"]["HTTPStatusCode"] == 206
    assert resp["ContentRange"].startswith("bytes 0-4/")
    assert resp["Body"].read() == data[0:5]

    # bytes=5-
    resp2 = boto3_client.get_object(Bucket=bucket, Key=key, Range="bytes=5-")
    assert resp2["ResponseMetadata"]["HTTPStatusCode"] == 206
    assert resp2["ContentRange"].startswith("bytes 5-25/")
    assert resp2["Body"].read() == data[5:]

    # bytes=0- (open-ended) should return full object with 206
    resp_open = boto3_client.get_object(Bucket=bucket, Key=key, Range="bytes=0-")
    assert resp_open["ResponseMetadata"]["HTTPStatusCode"] == 206
    assert resp_open["ContentRange"] == f"bytes 0-{len(data) - 1}/{len(data)}"
    assert resp_open["Body"].read() == data

    # bytes=-5
    resp3 = boto3_client.get_object(Bucket=bucket, Key=key, Range="bytes=-5")
    assert resp3["ResponseMetadata"]["HTTPStatusCode"] == 206
    assert resp3["Body"].read() == data[-5:]


@pytest.mark.local
def test_range_downloads_only_needed_chunks(
    docker_services: Any,
    boto3_client: Any,
    unique_bucket_name: Callable[[str], str],
    cleanup_buckets: Callable[[str], None],
    signed_http_get: Any,
) -> None:
    """After clearing cache, a small range should only materialize the in-range chunk(s)."""
    try:
        import redis  # type: ignore[import-untyped]
    except Exception:
        pytest.skip("redis client unavailable")

    bucket = unique_bucket_name("range-chunk-only")
    cleanup_buckets(bucket)
    boto3_client.create_bucket(Bucket=bucket)

    # Create a single large part (>= 2 chunks with default 4MiB chunk_size)
    part_size = 4 * 1024 * 1024
    key = "large/single-part.bin"
    body = b"A" * (part_size * 2 + 123)  # a bit over 2 chunks
    boto3_client.put_object(Bucket=bucket, Key=key, Body=body, ContentType="application/octet-stream")

    # Ensure server processed it
    assert wait_for_parts_cids(bucket, key, min_count=1, timeout_seconds=20.0)

    # Clear all cache for this object
    from .support.cache import get_object_id_and_version
    object_id, ov = get_object_id_and_version(bucket, key)
    clear_object_cache(object_id)

    # Request a small range within the first chunk
    r = signed_http_get(bucket, key, {"Range": "bytes=0-1048575"})
    assert r.status_code == 206
    assert r.headers.get("x-hippius-source") in {"pipeline", "cache"}

    # Inspect Redis for which chunk keys were created (versioned namespace)
    rcli = redis.Redis.from_url("redis://localhost:6379/0")
    # version already fetched above
    keys = sorted(
        [
            k.decode()
            for k in rcli.scan_iter(match=f"obj:{object_id}:v:{ov}:part:1:chunk:*", count=1000)  # 1-based part number
        ]
    )
    # Expect at minimum chunk:0 exists for the first-range request
    # (downloader may prefetch additional chunks, but the in-range chunk must be present)
    expected_chunk = f"obj:{object_id}:v:{ov}:part:1:chunk:0"
    assert expected_chunk in keys, f"expected chunk {expected_chunk} not found in {keys}"
    # Verify minimal hydration: should not have fetched chunks far outside the range
    # (allow adjacent chunks due to prefetch, but not all chunks)
    assert len(keys) <= 3, f"too many chunks hydrated: {keys}"

    # Clear cache again and request middle of the second chunk
    clear_object_cache(object_id)
    start = part_size + 256 * 1024
    end = start + 128 * 1024 - 1
    r2 = signed_http_get(bucket, key, {"Range": f"bytes={start}-{end}"})
    assert r2.status_code == 206
    keys2 = sorted([k.decode() for k in rcli.scan_iter(match=f"obj:{object_id}:v:{ov}:part:1:chunk:*", count=1000)])
    expected_chunk2 = f"obj:{object_id}:v:{ov}:part:1:chunk:1"
    assert expected_chunk2 in keys2, f"expected chunk {expected_chunk2} not found in {keys2}"
    # Verify minimal hydration: should not have fetched all chunks
    assert len(keys2) <= 3, f"too many chunks hydrated: {keys2}"


def test_get_object_range_invalid(
    docker_services: Any,
    boto3_client: Any,
    unique_bucket_name: Callable[[str], str],
    cleanup_buckets: Callable[[str], None],
) -> None:
    bucket = unique_bucket_name("range-invalid")
    cleanup_buckets(bucket)

    boto3_client.create_bucket(Bucket=bucket)
    key = "range.txt"
    data = b"abc"
    boto3_client.put_object(Bucket=bucket, Key=key, Body=data, ContentType="text/plain")

    # Deterministic: request a range starting beyond EOF to force 416 on AWS and local
    size = len(data)
    invalid_range = f"bytes={size + 10}-{size + 20}"
    with pytest.raises(ClientError) as excinfo:
        boto3_client.get_object(Bucket=bucket, Key=key, Range=invalid_range)
    status = excinfo.value.response.get("ResponseMetadata", {}).get("HTTPStatusCode")
    assert status == 416

    # bytes=-N with N > size should clamp to full content (common S3 behavior)
    resp_clamp = boto3_client.get_object(Bucket=bucket, Key=key, Range=f"bytes=-{len(data) + 100}")
    assert resp_clamp["ResponseMetadata"]["HTTPStatusCode"] == 206
    assert resp_clamp["ContentRange"] == f"bytes 0-{len(data) - 1}/{len(data)}"
    assert resp_clamp["Body"].read() == data


def test_get_object_range_single_byte_first_and_last(
    docker_services: Any,
    boto3_client: Any,
    unique_bucket_name: Callable[[str], str],
    cleanup_buckets: Callable[[str], None],
    signed_http_get: Any,
) -> None:
    bucket = unique_bucket_name("range-single-byte")
    cleanup_buckets(bucket)
    boto3_client.create_bucket(Bucket=bucket)

    key = "one/byte.txt"
    data = b"hello-world"  # len=11
    boto3_client.put_object(Bucket=bucket, Key=key, Body=data, ContentType="text/plain")

    # bytes=0-0 -> first byte
    r1 = signed_http_get(bucket, key, {"Range": "bytes=0-0"})
    assert r1.status_code == 206
    assert r1.content == data[0:1]

    # bytes=10-10 -> last byte
    r2 = signed_http_get(bucket, key, {"Range": f"bytes={len(data) - 1}-{len(data) - 1}"})
    assert r2.status_code == 206
    assert r2.content == data[-1:]


def test_get_object_range_on_part_boundaries(
    docker_services: Any,
    boto3_client: Any,
    unique_bucket_name: Callable[[str], str],
    cleanup_buckets: Callable[[str], None],
) -> None:
    """Create a multipart object and request ranges on and across part boundaries (AWS-compatible)."""
    bucket = unique_bucket_name("range-boundary-mpu")
    cleanup_buckets(bucket)
    boto3_client.create_bucket(Bucket=bucket)

    key = "log/boundary-mpu.txt"
    # S3 MPU requires each part except the last to be at least 5 MiB
    five_mib = 5 * 1024 * 1024
    # Make part1 end with 'abc' so we can assert across the boundary
    part1 = b"X" * (five_mib - 3) + b"abc"
    # Make part2 start with 'def' so first byte is 'd'
    part2 = b"defghi"

    # Multipart upload with two parts
    mpu = boto3_client.create_multipart_upload(Bucket=bucket, Key=key, ContentType="text/plain")
    upload_id = mpu["UploadId"]

    up1 = boto3_client.upload_part(Bucket=bucket, Key=key, PartNumber=1, UploadId=upload_id, Body=part1)
    up2 = boto3_client.upload_part(Bucket=bucket, Key=key, PartNumber=2, UploadId=upload_id, Body=part2)

    boto3_client.complete_multipart_upload(
        Bucket=bucket,
        Key=key,
        UploadId=upload_id,
        MultipartUpload={
            "Parts": [
                {"PartNumber": 1, "ETag": up1["ETag"]},
                {"PartNumber": 2, "ETag": up2["ETag"]},
            ]
        },
    )

    # bytes=five_mib-fist index in part 2 is 'd'
    r1 = boto3_client.get_object(Bucket=bucket, Key=key, Range=f"bytes={five_mib}-{five_mib}")
    assert r1["ResponseMetadata"]["HTTPStatusCode"] == 206
    assert r1["Body"].read() == b"d"

    # bytes=five_mib-1..five_mib crosses boundary -> 'cd'
    r2 = boto3_client.get_object(Bucket=bucket, Key=key, Range=f"bytes={five_mib - 1}-{five_mib}")
    assert r2["ResponseMetadata"]["HTTPStatusCode"] == 206
    assert r2["Body"].read() == b"cd"


@pytest.mark.local
@pytest.mark.hippius_cache
@pytest.mark.hippius_headers
def test_range_cache_source_when_irrelevant_part_missing(
    docker_services: Any,
    boto3_client: Any,
    unique_bucket_name: Callable[[str], str],
    cleanup_buckets: Callable[[str], None],
    signed_http_get: Any,
) -> None:
    """Clearing a part that is NOT needed for a given range should still serve from cache."""
    try:
        # Optional: skip if redis helper not available in this environment
        from .support.cache import clear_object_cache  # type: ignore[import-not-found]
        from .support.cache import get_object_id  # type: ignore[import-not-found]
    except Exception:
        pytest.skip("redis test helpers unavailable")

    bucket = unique_bucket_name("range-cache-src")
    cleanup_buckets(bucket)
    boto3_client.create_bucket(Bucket=bucket)

    key = "doc/irrelevant.txt"
    data = b"abcdefghijklmnop"  # 16 bytes
    boto3_client.put_object(Bucket=bucket, Key=key, Body=data)

    # Ensure object is processed; then clear a non-needed part (e.g., part 2) for a range entirely in base part 1
    object_id = get_object_id(bucket, key)
    clear_object_cache(object_id, parts=[2])

    # bytes=0-4 doesn't require part 2; should still be served from cache
    r = signed_http_get(bucket, key, {"Range": "bytes=0-4"})
    assert r.status_code == 206
    assert r.content == data[0:5]
    assert r.headers.get("x-hippius-source") == "cache"


@pytest.mark.local
def test_get_object_range_large_spanning_many_parts(
    docker_services: Any,
    boto3_client: Any,
    unique_bucket_name: Callable[[str], str],
    cleanup_buckets: Callable[[str], None],
    signed_http_get: Any,
) -> None:
    """Test range requests spanning many small parts (stress test for part assembly)."""
    bucket = unique_bucket_name("range-many-parts")
    cleanup_buckets(bucket)
    boto3_client.create_bucket(Bucket=bucket)

    key = "log/many-parts.txt"
    base = b"START"
    boto3_client.put_object(Bucket=bucket, Key=key, Body=base)

    # Add many small parts
    ver = boto3_client.head_object(Bucket=bucket, Key=key)["ResponseMetadata"]["HTTPHeaders"].get(
        "x-amz-meta-append-version", "0"
    )
    parts = []
    for i in range(10):
        part_data = f"PART{i:02d}".encode()
        parts.append(part_data)
        boto3_client.put_object(
            Bucket=bucket, Key=key, Body=part_data, Metadata={"append": "true", "append-if-version": ver}
        )

        ver = boto3_client.head_object(Bucket=bucket, Key=key)["ResponseMetadata"]["HTTPHeaders"].get(
            "x-amz-meta-append-version", "0"
        )

    whole = base + b"".join(parts)
    # Range that spans from middle of base through several parts
    start_idx = 3  # 'R' in START
    end_idx = len(base) + sum(len(p) for p in parts[:5]) + 2  # through 'RT05' in PART05
    r = signed_http_get(bucket, key, {"Range": f"bytes={start_idx}-{end_idx}"})
    assert r.status_code == 206
    assert r.content == whole[start_idx : end_idx + 1]


@pytest.mark.local
def test_get_object_range_irregular_part_sizes(
    docker_services: Any,
    boto3_client: Any,
    unique_bucket_name: Callable[[str], str],
    cleanup_buckets: Callable[[str], None],
    signed_http_get: Any,
) -> None:
    """Test range requests on objects with parts of varying sizes."""
    bucket = unique_bucket_name("range-irregular")
    cleanup_buckets(bucket)
    boto3_client.create_bucket(Bucket=bucket)

    key = "data/irregular.txt"
    base = b"A" * 100  # Large base
    boto3_client.put_object(Bucket=bucket, Key=key, Body=base)

    ver = boto3_client.head_object(Bucket=bucket, Key=key)["ResponseMetadata"]["HTTPHeaders"].get(
        "x-amz-meta-append-version", "0"
    )

    # Add parts of decreasing sizes
    parts = [b"B" * 50, b"C" * 25, b"D" * 10, b"E" * 5]
    for part_data in parts:
        boto3_client.put_object(
            Bucket=bucket, Key=key, Body=part_data, Metadata={"append": "true", "append-if-version": ver}
        )
        ver = boto3_client.head_object(Bucket=bucket, Key=key)["ResponseMetadata"]["HTTPHeaders"].get(
            "x-amz-meta-append-version", "0"
        )

    whole = base + b"".join(parts)

    # Range that spans different sized parts
    r = signed_http_get(bucket, key, {"Range": "bytes=80-135"})  # Spans base, first, and second parts
    assert r.status_code == 206
    assert r.content == whole[80:136]


@pytest.mark.local
def test_get_object_range_exact_part_boundaries(
    docker_services: Any,
    boto3_client: Any,
    unique_bucket_name: Callable[[str], str],
    cleanup_buckets: Callable[[str], None],
    signed_http_get: Any,
) -> None:
    """Test ranges that start/end exactly at part boundaries."""
    bucket = unique_bucket_name("range-exact-boundary")
    cleanup_buckets(bucket)
    boto3_client.create_bucket(Bucket=bucket)

    key = "exact/boundary.txt"
    base = b"BASE"  # 4 bytes
    boto3_client.put_object(Bucket=bucket, Key=key, Body=base)

    ver = boto3_client.head_object(Bucket=bucket, Key=key)["ResponseMetadata"]["HTTPHeaders"].get(
        "x-amz-meta-append-version", "0"
    )

    delta1 = b"DELTA"  # 5 bytes, total so far: 9
    boto3_client.put_object(Bucket=bucket, Key=key, Body=delta1, Metadata={"append": "true", "append-if-version": ver})

    ver = boto3_client.head_object(Bucket=bucket, Key=key)["ResponseMetadata"]["HTTPHeaders"].get(
        "x-amz-meta-append-version", "0"
    )

    delta2 = b"GAMMA"  # 5 bytes, total: 14
    boto3_client.put_object(Bucket=bucket, Key=key, Body=delta2, Metadata={"append": "true", "append-if-version": ver})

    whole = base + delta1 + delta2

    # Range exactly at part boundaries: bytes=4-8 (DELTA)
    r1 = signed_http_get(bucket, key, {"Range": "bytes=4-8"})
    assert r1.status_code == 206
    assert r1.content == whole[4:9]

    # Range exactly at next boundary: bytes=9-13 (GAMMA)
    r2 = signed_http_get(bucket, key, {"Range": "bytes=9-13"})
    assert r2.status_code == 206
    assert r2.content == whole[9:14]

    # Range spanning exactly from one boundary to another: bytes=4-13 (DELTA + GAMMA)
    r3 = signed_http_get(bucket, key, {"Range": "bytes=4-13"})
    assert r3.status_code == 206
    assert r3.content == whole[4:14]


@pytest.mark.local
def test_get_object_range_subset_of_part_chunks(
    docker_services: Any,
    boto3_client: Any,
    unique_bucket_name: Callable[[str], str],
    cleanup_buckets: Callable[[str], None],
    signed_http_get: Any,
) -> None:
    """Test ranges that require only a subset of chunks from within a part."""
    bucket = unique_bucket_name("range-subset-chunks")
    cleanup_buckets(bucket)
    boto3_client.create_bucket(Bucket=bucket)

    key = "chunk/subset.txt"
    # Create a part that's large enough to be chunked (if chunk size is reasonable)
    large_part = b"X" * 10000  # 10KB part
    boto3_client.put_object(Bucket=bucket, Key=key, Body=large_part)

    # Range that takes middle portion of the single part
    r = signed_http_get(bucket, key, {"Range": "bytes=3000-6999"})
    assert r.status_code == 206
    assert r.content == large_part[3000:7000]  # Note: end is inclusive


@pytest.mark.local
def test_get_object_range_with_skipped_parts(
    docker_services: Any,
    boto3_client: Any,
    unique_bucket_name: Callable[[str], str],
    cleanup_buckets: Callable[[str], None],
    signed_http_get: Any,
) -> None:
    """Test range requests that skip entire parts in the middle."""
    bucket = unique_bucket_name("range-skipped-parts")
    cleanup_buckets(bucket)
    boto3_client.create_bucket(Bucket=bucket)

    key = "skip/parts.txt"
    base = b"START"  # 5 bytes
    boto3_client.put_object(Bucket=bucket, Key=key, Body=base)

    ver = boto3_client.head_object(Bucket=bucket, Key=key)["ResponseMetadata"]["HTTPHeaders"].get(
        "x-amz-meta-append-version", "0"
    )

    # Add multiple parts
    parts = [b"MIDDLE1", b"MIDDLE2", b"MIDDLE3", b"END"]
    for part_data in parts:
        boto3_client.put_object(
            Bucket=bucket, Key=key, Body=part_data, Metadata={"append": "true", "append-if-version": ver}
        )
        ver = boto3_client.head_object(Bucket=bucket, Key=key)["ResponseMetadata"]["HTTPHeaders"].get(
            "x-amz-meta-append-version", "0"
        )

    whole = base + b"".join(parts)

    # Range that skips MIDDLE1 and MIDDLE2 entirely: from START to MIDDLE3
    start_pos = len(base) + len(parts[0]) + len(parts[1])  # After START + MIDDLE1 + MIDDLE2
    end_pos = start_pos + len(parts[2]) - 1  # Through MIDDLE3
    r = signed_http_get(bucket, key, {"Range": f"bytes={start_pos}-{end_pos}"})
    assert r.status_code == 206
    assert r.content == whole[start_pos : end_pos + 1]


def test_get_object_range_zero_length_ranges(
    docker_services: Any,
    boto3_client: Any,
    unique_bucket_name: Callable[[str], str],
    cleanup_buckets: Callable[[str], None],
) -> None:
    """Test edge cases with zero-length ranges and boundary conditions."""
    bucket = unique_bucket_name("range-zero-length")
    cleanup_buckets(bucket)
    boto3_client.create_bucket(Bucket=bucket)

    key = "zero/length.txt"
    data = b"HELLO"  # 5 bytes
    boto3_client.put_object(Bucket=bucket, Key=key, Body=data, ContentType="text/plain")

    # bytes=2-1 (start > end): AWS treats as no range (200, full object)
    r_invalid = boto3_client.get_object(Bucket=bucket, Key=key, Range="bytes=2-1")
    assert r_invalid["ResponseMetadata"]["HTTPStatusCode"] == 200
    assert r_invalid["Body"].read() == data
    assert "ContentRange" not in r_invalid  # No Content-Range for non-range requests

    # bytes=5-5 (exactly at EOF): observed AWS behavior is 416
    with pytest.raises(ClientError) as exc_eof:
        boto3_client.get_object(Bucket=bucket, Key=key, Range="bytes=5-5")
    status_eof = exc_eof.value.response.get("ResponseMetadata", {}).get("HTTPStatusCode")
    assert status_eof == 416

    # bytes=10-15 (way beyond EOF) should be invalid
    with pytest.raises(ClientError) as excinfo2:
        boto3_client.get_object(Bucket=bucket, Key=key, Range="bytes=10-15")
    status2 = excinfo2.value.response.get("ResponseMetadata", {}).get("HTTPStatusCode")
    assert status2 == 416


@pytest.mark.local
def test_get_object_range_very_large_object(
    docker_services: Any,
    boto3_client: Any,
    unique_bucket_name: Callable[[str], str],
    cleanup_buckets: Callable[[str], None],
    signed_http_get: Any,
) -> None:
    """Test range requests on very large multipart objects."""
    bucket = unique_bucket_name("range-large-obj")
    cleanup_buckets(bucket)
    boto3_client.create_bucket(Bucket=bucket)

    key = "large/object.bin"

    # Create a large object with multiple parts
    base_size = 50000  # 50KB
    base = b"A" * base_size
    boto3_client.put_object(Bucket=bucket, Key=key, Body=base)

    ver = boto3_client.head_object(Bucket=bucket, Key=key)["ResponseMetadata"]["HTTPHeaders"].get(
        "x-amz-meta-append-version", "0"
    )

    # Add several large parts
    parts = []
    part_sizes = [30000, 20000, 40000, 10000]  # Different sizes
    for size in part_sizes:
        part_data = b"B" * size
        parts.append(part_data)
        boto3_client.put_object(
            Bucket=bucket, Key=key, Body=part_data, Metadata={"append": "true", "append-if-version": ver}
        )
        ver = boto3_client.head_object(Bucket=bucket, Key=key)["ResponseMetadata"]["HTTPHeaders"].get(
            "x-amz-meta-append-version", "0"
        )

    whole = base + b"".join(parts)

    # Range that takes a slice from the middle of different parts
    start_pos = base_size + part_sizes[0] + 5000  # Middle of second part
    end_pos = start_pos + 25000  # Spans second and third parts
    r = signed_http_get(bucket, key, {"Range": f"bytes={start_pos}-{end_pos}"})
    assert r.status_code == 206
    assert r.content == whole[start_pos : end_pos + 1]
    assert len(r.content) == 25001  # Range length


@pytest.mark.local
def test_get_object_range_concurrent_appends(
    docker_services: Any,
    boto3_client: Any,
    unique_bucket_name: Callable[[str], str],
    cleanup_buckets: Callable[[str], None],
    signed_http_get: Any,
) -> None:
    """Test range requests when object is being appended to concurrently."""
    import threading

    bucket = unique_bucket_name("range-concurrent")
    cleanup_buckets(bucket)
    boto3_client.create_bucket(Bucket=bucket)

    key = "concurrent/test.log"
    base = b"INITIAL\n"
    boto3_client.put_object(Bucket=bucket, Key=key, Body=base)

    # Function to append data in background
    def append_worker() -> None:
        ver = boto3_client.head_object(Bucket=bucket, Key=key)["ResponseMetadata"]["HTTPHeaders"].get(
            "x-amz-meta-append-version", "0"
        )
        for i in range(3):
            time.sleep(0.1)  # Small delay
            part = f"APPEND{i}\n".encode()
            boto3_client.put_object(
                Bucket=bucket, Key=key, Body=part, Metadata={"append": "true", "append-if-version": ver}
            )
            ver = boto3_client.head_object(Bucket=bucket, Key=key)["ResponseMetadata"]["HTTPHeaders"].get(
                "x-amz-meta-append-version", "0"
            )

    # Start append worker
    thread = threading.Thread(target=append_worker, daemon=True)
    thread.start()

    # Wait a bit for some appends to happen
    time.sleep(0.2)

    # Request a range that covers the base content
    # This should work even if appends are happening
    r = signed_http_get(bucket, key, {"Range": "bytes=0-7"})  # "INITIAL\n"
    assert r.status_code == 206
    assert r.content == base

    # Wait for appends to complete
    thread.join(timeout=1.0)

    # Now request a range that covers the appended content
    whole_content = base + b"APPEND0\nAPPEND1\nAPPEND2\n"
    r2 = signed_http_get(bucket, key, {"Range": f"bytes=0-{len(whole_content) - 1}"})
    assert r2.status_code == 206
    assert r2.content == whole_content


@pytest.mark.local
@pytest.mark.hippius_cache
@pytest.mark.hippius_headers
def test_get_object_range_cache_invalidation_during_request(
    docker_services: Any,
    boto3_client: Any,
    unique_bucket_name: Callable[[str], str],
    cleanup_buckets: Callable[[str], None],
    signed_http_get: Any,
) -> None:
    """Test behavior when cache is invalidated mid-range request."""
    try:
        from .support.cache import clear_object_cache  # type: ignore[import-not-found]
        from .support.cache import get_object_id  # type: ignore[import-not-found]
    except Exception:
        pytest.skip("redis test helpers unavailable")

    bucket = unique_bucket_name("range-cache-inval")
    cleanup_buckets(bucket)
    boto3_client.create_bucket(Bucket=bucket)

    key = "cache/inval.txt"
    data = b"LONG_CONTENT_FOR_TESTING_CACHE_INVALIDATION_SCENARIOS"
    boto3_client.put_object(Bucket=bucket, Key=key, Body=data)

    # Ensure it's cached
    r1 = signed_http_get(bucket, key, {"Range": "bytes=0-10"})
    assert r1.status_code == 206
    assert r1.headers.get("x-hippius-source") == "cache"

    # Wait for object to be processed and cached
    assert wait_for_parts_cids(bucket, key, min_count=1, timeout_seconds=20.0)

    # Clear cache for the part that contains our range (base is part 1 under 1-based indexing)
    object_id = get_object_id(bucket, key)
    clear_object_cache(object_id, parts=[1])

    # Subsequent request should be from pipeline
    r2 = signed_http_get(bucket, key, {"Range": "bytes=5-15"})
    assert r2.status_code == 206
    assert r2.content == data[5:16]
    assert r2.headers.get("x-hippius-source") == "pipeline"
