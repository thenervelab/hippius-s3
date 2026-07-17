import os
from typing import Any
from typing import Callable

import httpx
import pytest

from .support.cache import clear_object_cache
from .support.cache import get_object_id
from .support.cache import wait_for_all_backends_ready


MOCK_ARION_URL = os.environ.get("MOCK_ARION_URL", "http://localhost:8002")


def _reset_download_stats() -> None:
    httpx.post(f"{MOCK_ARION_URL}/debug/reset_download_stats", timeout=5.0).raise_for_status()


def _download_stats() -> tuple[int, int]:
    resp = httpx.get(f"{MOCK_ARION_URL}/debug/download_stats", timeout=5.0)
    resp.raise_for_status()
    body = resp.json()
    return int(body["connections"]), int(body["requests"])


@pytest.mark.local
@pytest.mark.hippius_cache
def test_downloader_reuses_one_connection_across_cold_reads(
    docker_services: Any,
    boto3_client: Any,
    unique_bucket_name: Callable[[str], str],
    cleanup_buckets: Callable[[str], None],
) -> None:
    """The downloader must serve sequential cold reads from one pooled connection.

    This is the assertion the rest of the suite cannot make: every other cold-read test
    passes whether the worker builds one ArionClient per pod or one per chunk, because both
    return the same bytes. Only the connection count distinguishes them.

    Sequential reads are the discriminator on purpose. Within a single request the chunks of
    one part are fetched concurrently (bounded by DOWNLOADER_SEMAPHORE), and HTTP/1.1 needs a
    separate connection per in-flight request — so a multi-chunk object would show N
    connections either way. Reuse is only observable across requests.
    """
    bucket_name = unique_bucket_name("conn-reuse")
    cleanup_buckets(bucket_name)
    boto3_client.create_bucket(Bucket=bucket_name)

    # Small single-chunk objects: one Arion fetch each, so requests == number of GETs and the
    # per-request connection count isn't muddied by intra-part concurrency.
    num_objects = 5
    objects: list[tuple[str, bytes, str]] = []
    for i in range(num_objects):
        key = f"reuse-{i}.bin"
        content = os.urandom(1024)
        boto3_client.put_object(Bucket=bucket_name, Key=key, Body=content)
        assert wait_for_all_backends_ready(bucket_name, key, min_count=1, timeout_seconds=30.0), (
            f"{key} never got chunk_backend rows; the uploader did not finish"
        )
        objects.append((key, content, get_object_id(bucket_name, key)))

    # Evict everything BEFORE measuring: clear_object_cache shells into the api container, and
    # those seconds between GETs would otherwise outlast httpx's 5s default keepalive_expiry
    # and retire the very connection under test.
    for _key, _content, object_id in objects:
        clear_object_cache(object_id)

    _reset_download_stats()

    for key, content, _object_id in objects:
        resp = boto3_client.get_object(Bucket=bucket_name, Key=key)
        assert resp["Body"].read() == content, f"cold read of {key} returned wrong bytes"

    connections, requests = _download_stats()

    assert requests >= num_objects, (
        f"expected at least {num_objects} Arion downloads (one per cold object), saw {requests}; "
        "the objects were probably still cached, so this test proved nothing"
    )
    # A client per fetch makes connections == requests exactly, so `<` fails closed on the old
    # behaviour. Any unrelated traffic raises both counters equally and cannot mask a
    # regression.
    assert connections < requests, (
        f"downloader opened {connections} connections for {requests} downloads — no reuse. "
        "Each fetch is paying a fresh TCP+TLS handshake, i.e. ArionClient is being "
        "constructed per fetch instead of once per pod."
    )
    assert connections <= 2, (
        f"expected ~1 pooled connection for {requests} sequential downloads, saw {connections}. "
        "Reuse is happening but the pool is churning more than expected — check whether "
        "keepalive_expiry is shorter than the gap between reads."
    )
