import asyncio

import asyncpg
import pytest

from hippius_s3.api.s3.errors import pool_saturation_response


def test_timeout_maps_to_503_slowdown() -> None:
    resp = pool_saturation_response(asyncio.TimeoutError())
    assert resp is not None
    assert resp.status_code == 503
    assert resp.headers.get("Retry-After") == "1"
    assert resp.headers.get("x-amz-error-code") == "SlowDown"
    assert b"SlowDown" in bytes(resp.body)


def test_too_many_connections_maps_to_503() -> None:
    resp = pool_saturation_response(asyncpg.TooManyConnectionsError("too many"))
    assert resp is not None
    assert resp.status_code == 503
    assert resp.headers.get("Retry-After") == "1"


@pytest.mark.parametrize("exc", [ValueError("x"), RuntimeError("y"), KeyError("z")])
def test_unrelated_exceptions_pass_through(exc: BaseException) -> None:
    assert pool_saturation_response(exc) is None
