"""E2E (F1): a cache-source read survives its data being evicted MID-STREAM.

`build_stream_context` decides `source="cache"` from a single existence check taken before the body
streams. If the janitor evicts a still-to-be-streamed chunk after that decision, the streamer used to
wait on a `notify:` no producer would ever publish and time out INSIDE the response body — silent
truncation after the 200 was already committed. The fix re-enqueues a download on the mid-stream miss
so a producer fills + notifies the chunk and the stream completes byte-exact.

Timing note: this drives a large object so the streamer's in-memory prefetch window cannot cover the
whole object; reading a small prefix then evicting the FS cache forces a genuine miss on the
not-yet-streamed tail. With the fix the read always completes byte-exact regardless of exactly where
the eviction lands (a late eviction is simply a no-op); without it a mid-stream eviction truncates.
"""

import os
import time
from typing import Any
from typing import Callable

import pytest

from .support.cache import clear_object_cache
from .support.cache import get_object_id
from .support.cache import wait_for_all_backends_ready
from .support.compose import compose_exec


_E2E_DSN = os.environ.get("HIPPIUS_E2E_DB_DSN", "postgresql://postgres:postgres@localhost:5432/hippius")


def _evict(object_id: str) -> None:
    clear_object_cache(object_id, dsn=_E2E_DSN)
    compose_exec("api", ["rm", "-rf", f"/var/lib/hippius/object_cache/{object_id}"])


@pytest.mark.local
def test_read_survives_mid_stream_eviction(
    docker_services: Any,
    boto3_client: Any,
    unique_bucket_name: Callable[[str], str],
    cleanup_buckets: Callable[[str], None],
) -> None:
    bucket = unique_bucket_name("midstream-evict")
    cleanup_buckets(bucket)
    boto3_client.create_bucket(Bucket=bucket)

    key = "midstream.bin"
    # 96 MiB => 24 chunks at the 4 MiB default, larger than the streamer's prefetch window (16), so
    # the tail is not yet read from FS when we evict after a small prefix.
    content = os.urandom(96 * 1024 * 1024)
    boto3_client.put_object(Bucket=bucket, Key=key, Body=content)
    assert wait_for_all_backends_ready(bucket, key, min_count=1, timeout_seconds=120.0, dsn=_E2E_DSN)

    object_id = get_object_id(bucket, key, dsn=_E2E_DSN)

    # Warm read decision: this GET's context sees a fully-cached object (source="cache").
    body = boto3_client.get_object(Bucket=bucket, Key=key)["Body"]

    # Drain a small prefix so the stream is genuinely open and committed, then evict the object's FS
    # cache out from under the still-streaming tail.
    prefix = body.read(1 * 1024 * 1024)
    _evict(object_id)
    time.sleep(0.2)

    rest = body.read()
    got = prefix + rest
    assert got == content, "mid-stream eviction must re-enqueue the tail and reassemble byte-exact"
