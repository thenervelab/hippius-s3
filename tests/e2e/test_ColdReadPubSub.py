"""E2E: multi-chunk cold reads round-trip byte-exact through the live pub/sub pipeline.

Gate for RQ-1/RQ-2 (rework the per-chunk pub/sub subscription). Forces a cache miss on a multi-chunk
object so the streamer waits on redis-queues notifications for each chunk as the downloader fills
them; also exercises concurrent cold reads of the same object (the coalescing path). The existing
resilience test only covers a single-chunk object.
"""

import os
from concurrent.futures import ThreadPoolExecutor
from typing import Any
from typing import Callable

import pytest

from .support.cache import clear_object_cache
from .support.cache import get_object_id
from .support.cache import wait_for_all_backends_ready
from .support.compose import compose_exec


# In CI the e2e Postgres is at localhost:5432 (the helpers' default). Locally it may be remapped off a
# host Postgres — set HIPPIUS_E2E_DB_DSN to point the db-querying helpers at the e2e db.
_E2E_DSN = os.environ.get("HIPPIUS_E2E_DB_DSN", "postgresql://postgres:postgres@localhost:5432/hippius")


def _evict(object_id: str) -> None:
    clear_object_cache(object_id, dsn=_E2E_DSN)
    compose_exec("api", ["rm", "-rf", f"/var/lib/hippius/object_cache/{object_id}"])


@pytest.mark.local
def test_multichunk_cold_read_is_byte_exact(
    docker_services: Any,
    boto3_client: Any,
    unique_bucket_name: Callable[[str], str],
    cleanup_buckets: Callable[[str], None],
) -> None:
    bucket = unique_bucket_name("coldread-multichunk")
    cleanup_buckets(bucket)
    boto3_client.create_bucket(Bucket=bucket)

    key = "multichunk.bin"
    # 12 MiB => 3 chunks at the 4 MiB default, so the cold read notifies per chunk (not just one).
    content = bytes((i * 31 + 7) & 0xFF for i in range(12 * 1024 * 1024))
    boto3_client.put_object(Bucket=bucket, Key=key, Body=content)
    assert wait_for_all_backends_ready(bucket, key, min_count=1, timeout_seconds=60.0, dsn=_E2E_DSN)

    _evict(get_object_id(bucket, key, dsn=_E2E_DSN))

    got = boto3_client.get_object(Bucket=bucket, Key=key)["Body"].read()
    assert got == content, "cold multi-chunk read must reassemble byte-exact via the pub/sub pipeline"


@pytest.mark.local
def test_concurrent_cold_reads_coalesce_and_match(
    docker_services: Any,
    boto3_client: Any,
    unique_bucket_name: Callable[[str], str],
    cleanup_buckets: Callable[[str], None],
) -> None:
    bucket = unique_bucket_name("coldread-coalesce")
    cleanup_buckets(bucket)
    boto3_client.create_bucket(Bucket=bucket)

    key = "coalesce.bin"
    content = bytes((i * 17 + 3) & 0xFF for i in range(12 * 1024 * 1024))
    boto3_client.put_object(Bucket=bucket, Key=key, Body=content)
    assert wait_for_all_backends_ready(bucket, key, min_count=1, timeout_seconds=60.0, dsn=_E2E_DSN)

    _evict(get_object_id(bucket, key, dsn=_E2E_DSN))

    # Several readers hit the cold object at once: exactly one enqueues the download (coalesce lock),
    # the rest wait on pub/sub. All must return identical, byte-exact content.
    def _read() -> bytes:
        return boto3_client.get_object(Bucket=bucket, Key=key)["Body"].read()

    with ThreadPoolExecutor(max_workers=4) as pool:
        results = [f.result() for f in [pool.submit(_read) for _ in range(4)]]

    for r in results:
        assert r == content, "every concurrent cold reader must get byte-exact content"
