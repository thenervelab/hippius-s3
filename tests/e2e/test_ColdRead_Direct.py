"""E2E: cold reads are served straight from the backend, into memory, with nothing written back.

The read path's lowest tier is the backend itself: a chunk on no local tier is fetched into
memory, decrypted and streamed, and no cache tier is warmed on the way. Two things worth pinning
at the stack level, because neither has a unit-level equivalent:

* concurrent cold readers of one object — there is no download coalescing any more, so N readers
  are N × chunks backend fetches under one per-pod budget, and every one of them must still be
  byte-exact and finish inside the first-chunk bound;
* the byte-exact body arrives with both cache tiers still empty afterwards.
"""

import os
from concurrent.futures import ThreadPoolExecutor
from typing import Any
from typing import Callable

import pytest

from .conftest import assert_hippius_source
from .support.cache import clear_object_cache
from .support.cache import get_object_id_and_version
from .support.cache import wait_for_all_backends_ready
from .support.compose import compose_exec


_E2E_DSN = os.environ.get("HIPPIUS_E2E_DB_DSN", "postgresql://postgres:postgres@localhost:5432/hippius")
_TIERS = ("/var/lib/hippius/local_object_cache", "/var/lib/hippius/object_cache")


def _evict_everywhere(object_id: str) -> None:
    clear_object_cache(object_id, dsn=_E2E_DSN)
    for tier in _TIERS:
        compose_exec("api", ["rm", "-rf", f"{tier}/{object_id}"])


def _tiers_holding(object_id: str) -> list[str]:
    held = []
    for tier in _TIERS:
        rc, _, _ = compose_exec("api", ["test", "-e", f"{tier}/{object_id}"])
        if rc == 0:
            held.append(tier)
    return held


@pytest.mark.local
def test_concurrent_cold_reads_are_byte_exact_without_coalescing(
    docker_services: Any,
    boto3_client: Any,
    unique_bucket_name: Callable[[str], str],
    cleanup_buckets: Callable[[str], None],
) -> None:
    bucket = unique_bucket_name("coldread-concurrent")
    cleanup_buckets(bucket)
    boto3_client.create_bucket(Bucket=bucket)

    key = "concurrent.bin"
    # 12 MiB => 3 chunks at the 4 MiB default; four readers => twelve backend fetches at once.
    content = bytes((i * 17 + 3) & 0xFF for i in range(12 * 1024 * 1024))
    boto3_client.put_object(Bucket=bucket, Key=key, Body=content)
    assert wait_for_all_backends_ready(bucket, key, min_count=1, timeout_seconds=60.0, dsn=_E2E_DSN)
    object_id, _ = get_object_id_and_version(bucket, key, dsn=_E2E_DSN)
    _evict_everywhere(object_id)

    def _read() -> tuple[bytes, dict[str, str]]:
        resp = boto3_client.get_object(Bucket=bucket, Key=key)
        return resp["Body"].read(), resp["ResponseMetadata"]["HTTPHeaders"]

    with ThreadPoolExecutor(max_workers=4) as pool:
        results = [f.result() for f in [pool.submit(_read) for _ in range(4)]]

    for body, headers in results:
        assert body == content, "every concurrent cold reader must get byte-exact content"
        assert_hippius_source(headers, allowed={"pipeline"})


@pytest.mark.local
def test_a_cold_read_writes_nothing_to_any_cache_tier(
    docker_services: Any,
    boto3_client: Any,
    unique_bucket_name: Callable[[str], str],
    cleanup_buckets: Callable[[str], None],
) -> None:
    bucket = unique_bucket_name("coldread-nowrite")
    cleanup_buckets(bucket)
    boto3_client.create_bucket(Bucket=bucket)

    key = "nowrite.bin"
    content = bytes((i * 31 + 7) & 0xFF for i in range(9 * 1024 * 1024))
    boto3_client.put_object(Bucket=bucket, Key=key, Body=content)
    assert wait_for_all_backends_ready(bucket, key, min_count=1, timeout_seconds=60.0, dsn=_E2E_DSN)
    object_id, _ = get_object_id_and_version(bucket, key, dsn=_E2E_DSN)
    _evict_everywhere(object_id)
    assert _tiers_holding(object_id) == []

    full = boto3_client.get_object(Bucket=bucket, Key=key)
    assert full["Body"].read() == content
    assert_hippius_source(full["ResponseMetadata"]["HTTPHeaders"], allowed={"pipeline"})
    assert _tiers_holding(object_id) == [], "a backend-served read warms no cache tier"

    # A Range inside the second chunk: only that chunk is fetched, and still nothing lands.
    start = 4 * 1024 * 1024 + 256 * 1024
    end = start + 128 * 1024 - 1
    ranged = boto3_client.get_object(Bucket=bucket, Key=key, Range=f"bytes={start}-{end}")
    assert ranged["ResponseMetadata"]["HTTPStatusCode"] == 206
    assert ranged["Body"].read() == content[start : end + 1]
    assert _tiers_holding(object_id) == []
