"""The account-cacher cycle records the accounts-cached count + a success flag."""

from __future__ import annotations

from unittest.mock import AsyncMock
from unittest.mock import MagicMock
from unittest.mock import patch

import pytest

from workers import run_account_cacher_in_loop as ac


@pytest.mark.asyncio
async def test_cycle_records_accounts_cached() -> None:
    collector = MagicMock()
    with (
        patch.object(ac, "run_cacher_once", AsyncMock(return_value=42)),
        patch.object(ac, "get_metrics_collector", return_value=collector),
    ):
        ok = await ac.run_account_cacher_cycle()
    assert ok is True
    _, kwargs = collector.record_account_cacher_cycle.call_args
    assert kwargs["success"] is True and kwargs["accounts_cached"] == 42


@pytest.mark.asyncio
async def test_cycle_records_failure_without_raising() -> None:
    collector = MagicMock()
    with (
        patch.object(ac, "run_cacher_once", AsyncMock(side_effect=RuntimeError("substrate down"))),
        patch.object(ac, "get_metrics_collector", return_value=collector),
    ):
        ok = await ac.run_account_cacher_cycle()
    assert ok is False
    _, kwargs = collector.record_account_cacher_cycle.call_args
    assert kwargs["success"] is False and kwargs["accounts_cached"] == 0


@pytest.mark.asyncio
async def test_run_cacher_once_returns_the_cached_count_and_closes_redis() -> None:
    # The count plumbed to the metric comes from run_cache_update; the redis client is
    # always closed in the finally.
    cacher = MagicMock()
    cacher.connect = AsyncMock()
    cacher.run_cache_update = AsyncMock(return_value=7)
    cacher.redis_client = MagicMock()
    cacher.redis_client.aclose = AsyncMock()
    with patch.object(ac, "SubstrateCacher", return_value=cacher):
        n = await ac.run_cacher_once()
    assert n == 7
    cacher.redis_client.aclose.assert_awaited_once()
