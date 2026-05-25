import asyncio

import asyncpg
import pytest

from hippius_s3.api.s3.errors import pool_saturation_response
from hippius_s3.db_pool import PoolAcquireTimeout


def test_pool_acquire_timeout_maps_to_503_slowdown() -> None:
    resp = pool_saturation_response(PoolAcquireTimeout())
    assert resp is not None
    assert resp.status_code == 503
    assert resp.headers.get("Retry-After") == "3"
    assert resp.headers.get("x-amz-error-code") == "SlowDown"
    assert b"SlowDown" in bytes(resp.body)


def test_too_many_connections_maps_to_503() -> None:
    resp = pool_saturation_response(asyncpg.TooManyConnectionsError("too many"))
    assert resp is not None
    assert resp.status_code == 503
    assert resp.headers.get("Retry-After") == "3"


def test_bare_timeout_error_is_not_mapped() -> None:
    """Regression guard: asyncpg raises bare asyncio.TimeoutError for per-statement
    command_timeouts (a slow/lock-blocked query), NOT only for pool-acquire. It must NOT be
    relabeled as pool saturation — only the dedicated PoolAcquireTimeout does that."""
    assert pool_saturation_response(asyncio.TimeoutError()) is None
    # asyncio.TimeoutError IS builtin TimeoutError on 3.11+, so cover the builtin too.
    assert pool_saturation_response(TimeoutError()) is None


@pytest.mark.parametrize("exc", [ValueError("x"), RuntimeError("y"), KeyError("z")])
def test_unrelated_exceptions_pass_through(exc: BaseException) -> None:
    assert pool_saturation_response(exc) is None
