"""The DLQ layer emits push/requeue metrics through the shared collector.

Uses a minimal concrete `BaseDLQManager` subclass + a fake Redis so the metric wiring
is exercised without a real queue. Patches `get_metrics_collector` (the house pattern).
"""

from __future__ import annotations

import json
from unittest.mock import AsyncMock
from unittest.mock import MagicMock
from unittest.mock import patch

import pytest
from pydantic import BaseModel

from hippius_s3.dlq.base import BaseDLQManager


KEY = "arion_upload_requests:dlq"


class _FakeReq(BaseModel):
    id: str
    attempts: int = 0


class _StubDLQ(BaseDLQManager[_FakeReq]):
    """Minimal subclass: the entry to 'find' is injected; enqueue is a no-op mock."""

    entry: dict | None = None

    def _get_identifier(self, payload: _FakeReq) -> str:
        return payload.id

    async def _find_and_remove_entry(self, identifier: str) -> dict | None:
        return self.entry

    async def _bulk_enqueue(self, payloads: list[_FakeReq]) -> None:
        return None


def _fake_redis() -> MagicMock:
    r = MagicMock()
    r.lpush = AsyncMock()
    r.set = AsyncMock(return_value="tok")  # lock acquired
    r.eval = AsyncMock(return_value=1)  # lock released
    return r


def _dlq(enqueue: AsyncMock | None = None) -> _StubDLQ:
    return _StubDLQ(_fake_redis(), KEY, enqueue or AsyncMock(), _FakeReq)


@pytest.mark.asyncio
async def test_push_records_the_queue_and_error_type() -> None:
    dlq = _dlq()
    collector = MagicMock()
    with patch("hippius_s3.dlq.base.get_metrics_collector", return_value=collector):
        await dlq.push(_FakeReq(id="a"), last_error="boom", error_type="transient")
    collector.record_dlq_push.assert_called_once_with(KEY, "transient")


@pytest.mark.asyncio
async def test_push_maps_bool_error_type_to_permanent() -> None:
    dlq = _dlq()
    collector = MagicMock()
    with patch("hippius_s3.dlq.base.get_metrics_collector", return_value=collector):
        await dlq.push(_FakeReq(id="a"), last_error="boom", error_type=False)  # type: ignore[arg-type]
    collector.record_dlq_push.assert_called_once_with(KEY, "permanent")


@pytest.mark.asyncio
async def test_requeue_success_records_one() -> None:
    dlq = _dlq()
    dlq.entry = {"payload": {"id": "a", "attempts": 3}, "error_type": "transient"}
    collector = MagicMock()
    with patch("hippius_s3.dlq.base.get_metrics_collector", return_value=collector):
        ok = await dlq.requeue("a")
    assert ok is True
    collector.record_dlq_requeue.assert_called_once_with(KEY, 1)


@pytest.mark.asyncio
async def test_requeue_permanent_refusal_records_nothing() -> None:
    # A permanent error (without --force) is pushed back, not requeued → no requeue count.
    dlq = _dlq()
    dlq.entry = {"payload": {"id": "a"}, "error_type": "permanent"}
    collector = MagicMock()
    with patch("hippius_s3.dlq.base.get_metrics_collector", return_value=collector):
        ok = await dlq.requeue("a")
    assert ok is False
    collector.record_dlq_requeue.assert_not_called()


def _pipeline_returning(entries: list[str]) -> MagicMock:
    """A fake redis pipeline whose execute() drains `entries` (rpop is a queued no-op)."""
    pipe = MagicMock()
    pipe.rpop = MagicMock()
    pipe.execute = AsyncMock(return_value=entries)
    return pipe


@pytest.mark.asyncio
async def test_requeue_all_records_the_batch_count() -> None:
    dlq = _dlq()
    entry = json.dumps({"payload": {"id": "x", "attempts": 1}, "error_type": "transient"})
    dlq.redis_client.pipeline = MagicMock(return_value=_pipeline_returning([entry, entry, entry]))
    collector = MagicMock()
    with patch("hippius_s3.dlq.base.get_metrics_collector", return_value=collector):
        total = await dlq.requeue_all()
    assert total == 3
    collector.record_dlq_requeue.assert_called_once_with(KEY, 3)


@pytest.mark.asyncio
async def test_requeue_all_permanent_only_records_nothing() -> None:
    # A batch of only permanent errors is pushed back, not requeued → no metric.
    dlq = _dlq()
    entry = json.dumps({"payload": {"id": "x"}, "error_type": "permanent"})
    dlq.redis_client.pipeline = MagicMock(return_value=_pipeline_returning([entry, entry]))
    collector = MagicMock()
    with patch("hippius_s3.dlq.base.get_metrics_collector", return_value=collector):
        total = await dlq.requeue_all()
    assert total == 0
    collector.record_dlq_requeue.assert_not_called()
