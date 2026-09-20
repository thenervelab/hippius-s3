"""The fetch client's bounds are derived from config as a pure function so they can be asserted
without reaching into httpx internals, and so a misconfiguration fails at build time with a
message instead of at 03:00 as a silent hang.
"""

from __future__ import annotations

from types import SimpleNamespace

import httpx
import pytest

from hippius_s3.reader.backend_fetch import fetch_client_settings


def _cfg(**overrides: object) -> SimpleNamespace:
    base: dict[str, object] = {
        "read_backend_fetch_concurrency": 32,
        "read_backend_fetch_queue_timeout_seconds": 10.0,
        "read_backend_fetch_connect_timeout_seconds": 4.0,
        "read_backend_fetch_read_timeout_seconds": 10.0,
        "read_backend_fetch_pool_timeout_seconds": 4.0,
        "stream_first_chunk_timeout_seconds": 25,
    }
    base.update(overrides)
    return SimpleNamespace(**base)


def test_pool_is_sized_to_the_fetch_semaphore() -> None:
    timeout, limits = fetch_client_settings(_cfg())
    # A pool exactly as wide as the semaphore never queues in a healthy process, so a PoolTimeout
    # can only mean connections are held by something that is not a live fetch.
    assert limits == httpx.Limits(max_connections=32, max_keepalive_connections=32)
    assert timeout == httpx.Timeout(connect=4.0, read=10.0, write=10.0, pool=4.0)


def test_bounds_that_could_outlive_the_first_chunk_budget_are_rejected() -> None:
    # connect + read must finish inside the reader's first-chunk bound, otherwise the reader's
    # cancel fires first and we are back to the silent path this exists to remove.
    with pytest.raises(ValueError, match="HIPPIUS_READ_BACKEND_FETCH_READ_TIMEOUT_SECONDS"):
        fetch_client_settings(_cfg(read_backend_fetch_read_timeout_seconds=30.0))


def test_the_queue_wait_counts_against_the_first_chunk_budget() -> None:
    # The slot wait happens before the httpx bounds start, so 15 + 4 + 10 = 29 s could outlive
    # the 25 s first-chunk bound even though each httpx bound is fine on its own.
    with pytest.raises(ValueError, match="HIPPIUS_READ_BACKEND_FETCH_QUEUE_TIMEOUT_SECONDS"):
        fetch_client_settings(_cfg(read_backend_fetch_queue_timeout_seconds=15.0))


def test_the_boundary_is_strict() -> None:
    # 10 + 4 + 11 == 25 is rejected; 10 + 4 + 10 == 24 is accepted.
    with pytest.raises(ValueError):
        fetch_client_settings(_cfg(read_backend_fetch_read_timeout_seconds=11.0))
    fetch_client_settings(_cfg(read_backend_fetch_read_timeout_seconds=10.0))


def test_pool_wait_longer_than_first_chunk_budget_is_rejected() -> None:
    with pytest.raises(ValueError, match="HIPPIUS_READ_BACKEND_FETCH_POOL_TIMEOUT_SECONDS"):
        fetch_client_settings(_cfg(read_backend_fetch_pool_timeout_seconds=40.0))


def test_zero_concurrency_still_yields_a_usable_pool() -> None:
    # Mirrors the semaphore's own max(1, ...) in BackendChunkFetcher.__init__.
    _, limits = fetch_client_settings(_cfg(read_backend_fetch_concurrency=0))
    assert limits == httpx.Limits(max_connections=1, max_keepalive_connections=1)
