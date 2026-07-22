"""Unit test for DLQ requeue under the drain-direct per-part enqueue format.

The drain enqueues one UploadChainRequest per part, so a multi-part object lands one
DLQ entry per failed part. requeue(object_id) must re-enqueue every matching entry, not
just the first — otherwise some parts are never reprocessed (the e2e DLQ requeue test
saw exactly one of two parts come back).
"""

from __future__ import annotations

from unittest.mock import AsyncMock
from unittest.mock import MagicMock
from unittest.mock import patch

import pytest

from hippius_s3.scripts import dlq_requeue


OID = "466916c0-d61b-4518-b81b-9576b574270a"


def _entry(part: int) -> dict:
    return {
        "payload": {
            "address": "addr",
            "bucket_name": "b",
            "object_key": "k",
            "object_id": OID,
            "object_version": 1,
            "chunks": [{"id": part}],
        }
    }


@pytest.mark.asyncio
async def test_requeue_reenqueues_every_per_part_entry() -> None:
    manager = dlq_requeue.DLQManager(MagicMock())
    manager._acquire_lock = AsyncMock(return_value="tok")
    manager._release_lock = AsyncMock()
    # Two per-part DLQ entries for the object, then the queue is exhausted.
    manager._find_and_remove_entry = AsyncMock(side_effect=[_entry(1), _entry(2), None])

    with patch.object(dlq_requeue, "enqueue_upload_request", new=AsyncMock()) as enqueue:
        ok = await manager.requeue(OID)

    assert ok is True
    assert enqueue.await_count == 2, "both per-part DLQ entries are requeued, not just the first"
    requeued_parts = sorted(call.args[0].chunks[0].id for call in enqueue.await_args_list)
    assert requeued_parts == [1, 2]


@pytest.mark.asyncio
async def test_requeue_returns_false_when_no_entry() -> None:
    manager = dlq_requeue.DLQManager(MagicMock())
    manager._acquire_lock = AsyncMock(return_value="tok")
    manager._release_lock = AsyncMock()
    manager._find_and_remove_entry = AsyncMock(return_value=None)

    with patch.object(dlq_requeue, "enqueue_upload_request", new=AsyncMock()) as enqueue:
        ok = await manager.requeue(OID)

    assert ok is False
    assert enqueue.await_count == 0
