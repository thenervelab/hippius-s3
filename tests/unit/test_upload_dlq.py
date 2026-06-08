"""Upload DLQ routing for permanent errors.

Permanent errors must be dead-lettered with their error_type preserved and
must never be auto-requeued without an explicit force. (Billing-specific
routing lives in test_uploader_billing.py.)
"""

from typing import Any

import pytest
from fakeredis.aioredis import FakeRedis

from hippius_s3.dlq.upload_dlq import UploadDLQManager
from hippius_s3.queue import Chunk
from hippius_s3.queue import UploadChainRequest


def _req(object_id: str = "obj-err", upload_id: str = "up-err") -> UploadChainRequest:
    return UploadChainRequest(
        address="5Fake",
        bucket_name="bkt",
        object_key="k/o.bin",
        object_id=object_id,
        object_version=1,
        chunks=[Chunk(id=0)],
        upload_id=upload_id,
    )


@pytest.mark.asyncio
async def test_permanent_error_dead_lettered_with_type() -> None:
    mgr = UploadDLQManager(FakeRedis(), backend_name="arion")
    await mgr.push(_req(object_id="perm-1"), last_error="507 insufficient storage", error_type="permanent")

    entries = await mgr.peek()
    assert len(entries) == 1
    entry = entries[0]
    assert entry["error_type"] == "permanent"
    assert entry["object_id"] == "perm-1"
    assert entry["upload_id"] == "up-err"
    assert entry["last_error"] == "507 insufficient storage"
    assert (await mgr.stats())["error_types"]["permanent"] == 1


@pytest.mark.asyncio
async def test_requeue_refuses_permanent_without_force() -> None:
    mgr = UploadDLQManager(FakeRedis(), backend_name="arion")
    calls: list[Any] = []
    mgr.enqueue_func = lambda p: calls.append(p)  # must not be invoked  # type: ignore[assignment]
    await mgr.push(_req(object_id="perm-2"), last_error="malformed body", error_type="permanent")

    assert await mgr.requeue("perm-2") is False
    assert calls == [], "permanent errors must not be auto-requeued"
    assert (await mgr.stats())["total_entries"] == 1, "entry must be put back on refusal"


@pytest.mark.asyncio
async def test_requeue_permanent_with_force_reenqueues() -> None:
    mgr = UploadDLQManager(FakeRedis(), backend_name="arion")
    captured: list[UploadChainRequest] = []

    async def _capture(payload: UploadChainRequest) -> None:
        captured.append(payload)

    mgr.enqueue_func = _capture  # type: ignore[assignment]
    await mgr.push(_req(object_id="perm-3"), last_error="malformed body", error_type="permanent")

    assert await mgr.requeue("perm-3", force=True) is True
    assert [p.object_id for p in captured] == ["perm-3"]
    assert (await mgr.stats())["total_entries"] == 0
