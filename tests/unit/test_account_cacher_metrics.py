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
    args, _ = collector.record_account_cacher_cycle.call_args
    assert args[0] is True and args[1] == 42


@pytest.mark.asyncio
async def test_cycle_records_failure_without_raising() -> None:
    collector = MagicMock()
    with (
        patch.object(ac, "run_cacher_once", AsyncMock(side_effect=RuntimeError("substrate down"))),
        patch.object(ac, "get_metrics_collector", return_value=collector),
    ):
        ok = await ac.run_account_cacher_cycle()
    assert ok is False
    args, _ = collector.record_account_cacher_cycle.call_args
    assert args[0] is False and args[1] == 0
