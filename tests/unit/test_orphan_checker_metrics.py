"""The orphan-checker cycle records its scan/orphan counts + a success flag."""

from __future__ import annotations

from unittest.mock import AsyncMock
from unittest.mock import MagicMock
from unittest.mock import patch

import pytest

from workers import run_orphan_checker_in_loop as oc


@pytest.mark.asyncio
async def test_cycle_records_scan_and_orphan_counts() -> None:
    collector = MagicMock()
    with (
        patch.object(oc, "check_for_orphans", AsyncMock(return_value=(100, 2))),
        patch.object(oc, "get_metrics_collector", return_value=collector),
    ):
        ok = await oc.run_orphan_check_cycle(MagicMock())
    assert ok is True
    args, _ = collector.record_orphan_checker_cycle.call_args
    assert args[0] is True and args[1] == 100 and args[2] == 2


@pytest.mark.asyncio
async def test_cycle_records_failure_without_raising() -> None:
    collector = MagicMock()
    with (
        patch.object(oc, "check_for_orphans", AsyncMock(side_effect=RuntimeError("boom"))),
        patch.object(oc, "get_metrics_collector", return_value=collector),
    ):
        ok = await oc.run_orphan_check_cycle(MagicMock())
    assert ok is False
    args, _ = collector.record_orphan_checker_cycle.call_args
    assert args[0] is False and args[1] == 0 and args[2] == 0
