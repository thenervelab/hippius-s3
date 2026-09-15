"""usage_service reads a maintained counter, and its guards are the whole reason it is not a one-liner.

The number this returns is what a plan customer's quota is enforced against, so the two ways it can
be wrong -- not yet seeded, or drifted negative -- have to be handled explicitly rather than served.
The rollup itself is checked against the canonical query in
tests/integration/test_storage_usage_rollup.py; these cases are about what the caller is told.
"""

from typing import Any

import pytest

from hippius_s3.services import usage_service


class FakeDb:
    def __init__(self, row: dict[str, Any]) -> None:
        self.row = row
        self.calls: list[tuple[tuple[Any, ...], dict[str, Any]]] = []

    async def fetchrow(self, _query: str, *args: Any, **kwargs: Any) -> dict[str, Any]:
        self.calls.append((args, kwargs))
        return self.row


def _row(**overrides: Any) -> dict[str, Any]:
    row = {"bytes_used": 0, "negative_buckets": 0, "missing_buckets": 0, "ready": True}
    row.update(overrides)
    return row


@pytest.mark.asyncio
async def test_returns_the_rollup_total() -> None:
    db = FakeDb(_row(bytes_used=4096))

    assert await usage_service.get_account_storage_bytes(db, "5Acct", timeout=30.0) == 4096


@pytest.mark.asyncio
async def test_timeout_is_passed_to_asyncpg_not_wrapped() -> None:
    """asyncpg's own timeout cancels server-side; an asyncio one would leave the query running."""
    db = FakeDb(_row())

    await usage_service.get_account_storage_bytes(db, "5Acct", timeout=7.5)

    args, kwargs = db.calls[0]
    assert args == ("5Acct",)
    assert kwargs == {"timeout": 7.5}


@pytest.mark.asyncio
async def test_raises_until_the_backfill_has_run() -> None:
    """Before the backfill the rollup holds deltas, not totals -- serving them would under-bill.

    Raising fails the plans-cacher cycle, which leaves the previous roll serving. That is the same
    degradation as any other failed cycle, and strictly better than publishing a small number as a
    customer's usage.
    """
    db = FakeDb(_row(bytes_used=123, ready=False))

    with pytest.raises(usage_service.StorageRollupNotBackfilled):
        await usage_service.get_account_storage_bytes(db, "5Acct", timeout=30.0)


@pytest.mark.asyncio
async def test_a_negative_counter_is_reported_but_still_served(caplog: pytest.LogCaptureFixture) -> None:
    """A negative counter is a defect, and the clamp lives on the read path rather than in the table.

    Served rather than raised on purpose: the error direction is under-counting, which is the same
    fail-open direction as every other degraded path in the quota gate, and the reconciler repairs
    the value within a pass. What must not happen is it going unremarked.
    """
    db = FakeDb(_row(bytes_used=0, negative_buckets=2))

    with caplog.at_level("ERROR"):
        assert await usage_service.get_account_storage_bytes(db, "5Acct", timeout=30.0) == 0

    assert "STORAGE_ROLLUP_NEGATIVE" in caplog.text
    assert "5Acct" in caplog.text
