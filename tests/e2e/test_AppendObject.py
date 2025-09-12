import base64
import time
from concurrent.futures import ThreadPoolExecutor
from concurrent.futures import as_completed
from typing import Any

import pytest
from botocore.exceptions import ClientError


def _put_object(boto3_client: Any, bucket: str, key: str, data: bytes, metadata: dict[str, str] | None = None) -> None:
    boto3_client.put_object(
        Bucket=bucket,
        Key=key,
        Body=data,
        ContentType="application/octet-stream",
        Metadata=metadata or {},
    )


@pytest.mark.s4
def test_append_single_writer(
    boto3_client: Any, unique_bucket_name: Any, cleanup_buckets: Any, wait_until_readable: Any
) -> None:
    bucket = unique_bucket_name("append-single")
    boto3_client.create_bucket(Bucket=bucket)
    cleanup_buckets(bucket)

    key = "log/append.txt"

    # Seed object
    initial = b"hello\n"
    _put_object(boto3_client, bucket, key, initial)

    # Wait until initial object is readable, then read append version for CAS
    wait_until_readable(bucket, key, 60)
    head = boto3_client.head_object(Bucket=bucket, Key=key)
    version = head["ResponseMetadata"]["HTTPHeaders"].get("x-amz-meta-append-version", "0")

    # Append delta
    delta = b"world\n"
    _put_object(
        boto3_client,
        bucket,
        key,
        delta,
        metadata={
            "append": "true",
            "append-if-version": version,
            "append-id": "single-writer-test",
        },
    )

    # Verify full content
    obj = boto3_client.get_object(Bucket=bucket, Key=key)
    data = obj["Body"].read()
    assert data == initial + delta


@pytest.mark.s4
def test_append_multi_writer_concurrent(
    boto3_client: Any, unique_bucket_name: Any, cleanup_buckets: Any, wait_until_readable: Any
) -> None:
    bucket = unique_bucket_name("append-concurrent")
    boto3_client.create_bucket(Bucket=bucket)
    cleanup_buckets(bucket)

    key = "log/append.txt"

    # Seed object
    initial = b"A\n"
    _put_object(boto3_client, bucket, key, initial)

    # Ensure seed object is readable before concurrent appends
    wait_until_readable(bucket, key, 60)
    # Prepare concurrent appends
    deltas = [f"line-{i}\n".encode() for i in range(20)]

    # Simple helper to attempt CAS append with retry on 412
    def append_with_retry(delta: bytes) -> bool:
        for attempt in range(100):
            head = boto3_client.head_object(Bucket=bucket, Key=key)
            version = head["ResponseMetadata"]["HTTPHeaders"].get("x-amz-meta-append-version", "0")
            try:
                _put_object(
                    boto3_client,
                    bucket,
                    key,
                    delta,
                    metadata={
                        "append": "true",
                        "append-if-version": version,
                        "append-id": base64.b64encode(delta).decode(),
                    },
                )
                return True
            except ClientError as e:  # noqa: PERF203
                # Expect a 412 on CAS failure (PreconditionFailed). Retry.
                status = e.response.get("ResponseMetadata", {}).get("HTTPStatusCode")
                code = e.response.get("Error", {}).get("Code")
                if status == 412 or code in {"PreconditionFailed", "412"}:
                    time.sleep(min(0.05 * (attempt + 1), 2.0))
                    continue
                # If server does not support append, skip test gracefully
                if code in {"InvalidRequest", "NotImplemented"}:
                    pytest.skip("Append extension not supported by server")
                raise
            except Exception as e:  # noqa: PERF203
                msg = str(e)
                if "412" in msg or "PreconditionFailed" in msg:
                    time.sleep(min(0.05 * (attempt + 1), 2.0))
                    continue
                if "InvalidRequest" in msg or "NotImplemented" in msg:
                    pytest.skip("Append extension not supported by server")
                raise
        return False

    # Run appends concurrently
    with ThreadPoolExecutor(max_workers=8) as ex:
        futs = [ex.submit(append_with_retry, d) for d in deltas]
        results = [f.result() for f in as_completed(futs)]
        assert all(results), "Some appends failed after retries"

    # Verify content contains all lines in some order following initial prefix.
    # Exact ordering depends on commit order under contention; we accept any order.
    obj = boto3_client.get_object(Bucket=bucket, Key=key)
    data = obj["Body"].read()
    text = data.decode()
    assert text.startswith(initial.decode())
    for d in deltas:
        assert d.decode() in text


@pytest.mark.s4
def test_append_stale_version_412(
    boto3_client: Any, unique_bucket_name: Any, cleanup_buckets: Any, wait_until_readable: Any
) -> None:
    bucket = unique_bucket_name("append-412")
    boto3_client.create_bucket(Bucket=bucket)
    cleanup_buckets(bucket)

    key = "log/append.txt"
    _put_object(boto3_client, bucket, key, b"X\n")

    # Ensure object is readable; then take an initial append-version snapshot
    wait_until_readable(bucket, key, 60)
    ver0 = boto3_client.head_object(Bucket=bucket, Key=key)["ResponseMetadata"]["HTTPHeaders"].get(
        "x-amz-meta-append-version", "0"
    )

    # Advance the object (valid append)
    _put_object(
        boto3_client,
        bucket,
        key,
        b"Y\n",
        metadata={"append": "true", "append-if-version": ver0},
    )

    # Try to append again using the stale version -> expect 412 and no change
    with pytest.raises(ClientError) as exc:
        _put_object(
            boto3_client,
            bucket,
            key,
            b"Z\n",
            metadata={"append": "true", "append-if-version": ver0},
        )
    status = exc.value.response.get("ResponseMetadata", {}).get("HTTPStatusCode")
    code = exc.value.response.get("Error", {}).get("Code")
    assert status == 412 or code in {"PreconditionFailed", "412"}

    body = boto3_client.get_object(Bucket=bucket, Key=key)["Body"].read()
    assert body == b"X\nY\n"


@pytest.mark.s4
def test_append_preserves_user_metadata(
    boto3_client: Any, unique_bucket_name: Any, cleanup_buckets: Any, wait_until_readable: Any
) -> None:
    bucket = unique_bucket_name("append-meta")
    boto3_client.create_bucket(Bucket=bucket)
    cleanup_buckets(bucket)

    key = "log/append.txt"
    _put_object(
        boto3_client,
        bucket,
        key,
        b"A",
        metadata={"foo": "bar", "append": "false"},
    )

    wait_until_readable(bucket, key, 60)
    head = boto3_client.head_object(Bucket=bucket, Key=key)
    version = head["ResponseMetadata"]["HTTPHeaders"].get("x-amz-meta-append-version", "0")

    # Append with control metadata; control keys should not persist as user metadata
    _put_object(
        boto3_client,
        bucket,
        key,
        b"B",
        metadata={"append": "true", "append-if-version": version, "append-id": "t1"},
    )

    head = boto3_client.head_object(Bucket=bucket, Key=key)
    # Boto surfaces user metadata under 'Metadata'
    md = {k.lower(): v for k, v in (head.get("Metadata") or {}).items()}
    assert md.get("foo") == "bar"
    assert "append" not in md and "append-if-etag" not in md and "append-id" not in md


@pytest.mark.s4
def test_append_missing_key_404(boto3_client: Any, unique_bucket_name: Any, cleanup_buckets: Any) -> None:
    bucket = unique_bucket_name("append-404")
    boto3_client.create_bucket(Bucket=bucket)
    cleanup_buckets(bucket)

    key = "log/missing.txt"
    # Append request on missing key should return NoSuchKey (404)
    with pytest.raises(ClientError) as exc:
        _put_object(
            boto3_client,
            bucket,
            key,
            b"X",
            metadata={"append": "true", "append-if-version": "0"},
        )
    status = exc.value.response.get("ResponseMetadata", {}).get("HTTPStatusCode")
    code = exc.value.response.get("Error", {}).get("Code")
    assert status == 404 or code == "NoSuchKey"


@pytest.mark.s4
def test_range_get_across_append_boundary(boto3_client: Any, unique_bucket_name: Any, cleanup_buckets: Any) -> None:
    bucket = unique_bucket_name("append-range")
    boto3_client.create_bucket(Bucket=bucket)
    cleanup_buckets(bucket)

    key = "log/range.txt"
    _put_object(boto3_client, bucket, key, b"abc")
    ver = boto3_client.head_object(Bucket=bucket, Key=key)["ResponseMetadata"]["HTTPHeaders"].get(
        "x-amz-meta-append-version", "0"
    )
    _put_object(boto3_client, bucket, key, b"defghi", metadata={"append": "true", "append-if-version": ver})

    # Request a range that spans both original and appended bytes: bytes=2-5 -> "cdef"
    obj = boto3_client.get_object(Bucket=bucket, Key=key, Range="bytes=2-5")
    data = obj["Body"].read()
    assert data == b"cdef"


@pytest.mark.s4
def test_append_idempotency_append_id(
    boto3_client: Any, unique_bucket_name: Any, cleanup_buckets: Any, wait_until_readable: Any
) -> None:
    bucket = unique_bucket_name("append-idemp")
    boto3_client.create_bucket(Bucket=bucket)
    cleanup_buckets(bucket)

    key = "log/idem.txt"
    _put_object(boto3_client, bucket, key, b"base")

    # Ensure seed object is readable before taking version snapshot
    wait_until_readable(bucket, key, 60)
    ver = boto3_client.head_object(Bucket=bucket, Key=key)["ResponseMetadata"]["HTTPHeaders"].get(
        "x-amz-meta-append-version", "0"
    )

    # Use a fixed append-id and send the same append twice
    append_id = "fixed-id-123"
    delta = b"-delta"
    for _ in range(2):
        _put_object(
            boto3_client,
            bucket,
            key,
            delta,
            metadata={
                "append": "true",
                "append-if-version": ver,
                "append-id": append_id,
            },
        )
        # Do not refresh etag to simulate exact retry of the same request

    # Validate only one append was applied
    body = boto3_client.get_object(Bucket=bucket, Key=key)["Body"].read()
    assert body == b"base" + delta
