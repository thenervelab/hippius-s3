"""A 499 must not land on the API's error-rate panels.

Returning CLIENT_CLOSED_REQUEST instead of raising stops a client abort being counted as
`internal_error`, but the metrics middleware counts every `status_code >= 400`, so without an
explicit carve-out the same event simply reappears as `http_499` at the same volume. The
gateway's metrics middleware excludes 499 for exactly this reason; both hops must agree.
"""

from __future__ import annotations

from typing import Any
from unittest.mock import MagicMock
from unittest.mock import patch

import pytest
from fastapi import Response

from hippius_s3.api.middlewares import metrics as api_metrics
from hippius_s3.api.s3.errors import CLIENT_CLOSED_REQUEST


def _request() -> Any:
    request = MagicMock()
    request.method = "PUT"
    request.url.path = "/bucket/key"
    request.headers = {}
    request.state = MagicMock()
    # A MagicMock invents attributes on access; TTFB arithmetic needs a real absent-or-float here.
    request.state.gateway_start_time = None
    return request


async def _run(status_code: int) -> list[Any]:
    request = _request()

    async def call_next(_: Any) -> Response:
        return Response(status_code=status_code)

    collector = MagicMock()
    with patch.object(api_metrics, "get_metrics_collector", return_value=collector):
        await api_metrics.metrics_middleware(request, call_next)
    return collector.record_error.call_args_list


@pytest.mark.asyncio
async def test_client_closed_request_is_not_counted_as_an_error() -> None:
    assert await _run(CLIENT_CLOSED_REQUEST) == []


@pytest.mark.asyncio
@pytest.mark.parametrize("status_code", [500, 503, 404, 403])
async def test_real_errors_are_still_counted(status_code: int) -> None:
    """The carve-out must be exactly 499 — genuine failures still have to reach the counter."""
    calls = await _run(status_code)

    assert len(calls) == 1
    assert calls[0].kwargs["error_type"] == f"http_{status_code}"
