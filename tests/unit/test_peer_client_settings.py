from __future__ import annotations

from types import SimpleNamespace

import httpx
import pytest
from hypothesis import given
from hypothesis import strategies as st

from hippius_s3.cache.peers import _PEER_POOL_PEER_COUNT
from hippius_s3.cache.peers import peer_client_settings


def _cfg(**overrides: object) -> SimpleNamespace:
    base: dict[str, object] = {
        "peer_fetch_timeout_seconds": 0.5,
        "peer_fetch_pool_timeout_seconds": 0.4,
        "peer_fetch_deadline_seconds": 2.0,
    }
    base.update(overrides)
    return SimpleNamespace(**base)


def test_pool_is_sized_to_inflight_times_peer_count() -> None:
    """The semaphore is per-peer; the pool is process-wide. Prod has 4 other ingest nodes."""
    timeout, limits = peer_client_settings(_cfg(), max_inflight=16)
    assert limits == httpx.Limits(
        max_connections=16 * _PEER_POOL_PEER_COUNT,
        max_keepalive_connections=16 * _PEER_POOL_PEER_COUNT,
    )
    assert timeout == httpx.Timeout(connect=0.5, read=0.5, write=0.5, pool=0.4)


def test_pool_plus_connect_must_fit_inside_the_wait_for_deadline() -> None:
    # Otherwise wait_for cancels the coroutine while httpx still holds the socket (CLOSE_WAIT leak).
    with pytest.raises(ValueError, match="HIPPIUS_PEER_FETCH_POOL_TIMEOUT_SECONDS"):
        peer_client_settings(_cfg(peer_fetch_pool_timeout_seconds=1.6), max_inflight=16)


def test_the_boundary_is_strict() -> None:
    # 0.4 + 0.5 = 0.9 < 2.0 is accepted; 1.5 + 0.5 = 2.0 is rejected.
    peer_client_settings(_cfg(), max_inflight=16)
    with pytest.raises(ValueError):
        peer_client_settings(_cfg(peer_fetch_pool_timeout_seconds=1.5), max_inflight=16)


def test_zero_inflight_still_yields_a_usable_pool() -> None:
    _, limits = peer_client_settings(_cfg(), max_inflight=0)
    assert limits.max_connections == 1 * _PEER_POOL_PEER_COUNT


@given(
    pool=st.floats(0.05, 5, allow_nan=False, allow_infinity=False),
    connect=st.floats(0.05, 5, allow_nan=False, allow_infinity=False),
    deadline=st.floats(0.1, 10, allow_nan=False, allow_infinity=False),
)
def test_accepted_settings_always_fail_inside_the_deadline(pool: float, connect: float, deadline: float) -> None:
    cfg = _cfg(
        peer_fetch_pool_timeout_seconds=pool,
        peer_fetch_timeout_seconds=connect,
        peer_fetch_deadline_seconds=deadline,
    )
    if pool + connect >= deadline:
        with pytest.raises(ValueError):
            peer_client_settings(cfg, max_inflight=16)
    else:
        timeout, _limits = peer_client_settings(cfg, max_inflight=16)
        assert timeout.pool + timeout.connect < deadline
