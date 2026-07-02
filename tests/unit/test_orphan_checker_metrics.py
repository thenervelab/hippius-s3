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
    _, kwargs = collector.record_orphan_checker_cycle.call_args
    assert kwargs["success"] is True and kwargs["files_scanned"] == 100 and kwargs["orphans_found"] == 2


@pytest.mark.asyncio
async def test_cycle_records_failure_without_raising() -> None:
    collector = MagicMock()
    with (
        patch.object(oc, "check_for_orphans", AsyncMock(side_effect=RuntimeError("boom"))),
        patch.object(oc, "get_metrics_collector", return_value=collector),
    ):
        ok = await oc.run_orphan_check_cycle(MagicMock())
    assert ok is False
    _, kwargs = collector.record_orphan_checker_cycle.call_args
    assert kwargs["success"] is False and kwargs["files_scanned"] == 0 and kwargs["orphans_found"] == 0


@pytest.mark.asyncio
async def test_check_for_orphans_counts_scanned_and_orphans() -> None:
    # Every listed file counts as scanned; only an s3- file whose CID is absent from the
    # DB is an orphan (and gets an unpin enqueued). A non-s3 file is scanned but skipped.
    s3_orphan = MagicMock(original_name="s3-abc", cid="cidA", file_id="f1", size_bytes=10)
    non_s3 = MagicMock(original_name="notes.txt", cid="cidB", file_id="f2", size_bytes=5)
    page = MagicMock(results=[s3_orphan, non_s3], next=None)

    api_client = MagicMock()
    api_client.list_files = AsyncMock(return_value=page)
    api_ctx = MagicMock()
    api_ctx.__aenter__ = AsyncMock(return_value=api_client)
    api_ctx.__aexit__ = AsyncMock(return_value=False)

    db = MagicMock()
    db.fetch = AsyncMock(return_value=[{"main_account_id": "acc1"}])
    db.fetchrow = AsyncMock(return_value={"exists": False})  # the s3 CID is not in the DB → orphan

    with (
        patch.object(oc, "HippiusApiClient", return_value=api_ctx),
        patch.object(oc, "enqueue_unpin_request", AsyncMock()) as enq,
    ):
        scanned, orphans = await oc.check_for_orphans(db)

    assert scanned == 2, "both files counted as scanned"
    assert orphans == 1, "only the s3- file with a missing CID is an orphan"
    enq.assert_awaited_once()
