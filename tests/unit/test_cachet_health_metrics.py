"""The cachet worker records a health-check status + whether the status-page update stuck."""

from __future__ import annotations

from unittest.mock import AsyncMock
from unittest.mock import MagicMock
from unittest.mock import patch

import httpx
import pytest

from workers import cachet_health_check as ch


def _client(*, get_status: int = 200, put_status: int = 200, get_raises: bool = False) -> MagicMock:
    client = MagicMock()
    if get_raises:
        client.get = AsyncMock(side_effect=httpx.ConnectError("gateway down"))
    else:
        get_resp = MagicMock()
        get_resp.status_code = get_status
        client.get = AsyncMock(return_value=get_resp)
    put_resp = MagicMock()
    put_resp.status_code = put_status
    put_resp.text = ""
    client.put = AsyncMock(return_value=put_resp)
    return client


async def _run(client: MagicMock, collector: MagicMock) -> None:
    with patch.object(ch, "get_metrics_collector", return_value=collector):
        await ch._cachet_once(client, "http://gateway/health", "http://cachet/comp", {})


@pytest.mark.asyncio
async def test_healthy_records_operational_and_update_success() -> None:
    collector = MagicMock()
    await _run(_client(get_status=200, put_status=200), collector)
    collector.record_cachet_check.assert_called_once_with("operational", True)


@pytest.mark.asyncio
async def test_gateway_non_200_records_degraded() -> None:
    collector = MagicMock()
    await _run(_client(get_status=503), collector)
    collector.record_cachet_check.assert_called_once_with("degraded", True)


@pytest.mark.asyncio
async def test_gateway_unreachable_records_outage() -> None:
    collector = MagicMock()
    await _run(_client(get_raises=True), collector)
    collector.record_cachet_check.assert_called_once_with("outage", True)


@pytest.mark.asyncio
async def test_failed_cachet_update_records_update_false() -> None:
    collector = MagicMock()
    await _run(_client(get_status=200, put_status=500), collector)
    collector.record_cachet_check.assert_called_once_with("operational", False)
