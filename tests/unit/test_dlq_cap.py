"""A10: the DLQ LPUSH is capped so a permanent-error storm can't fill the 2GB noeviction
redis-queues instance and fail ALL writes pipeline-wide. At the cap push() is a no-op
(drop-newest) — Postgres is the durable source of truth, so a lost failure record is
re-derivable (scripts/resubmit_failed_pins.py). The alert fires long before the cap.
"""

import sys
from pathlib import Path
from unittest.mock import MagicMock
from unittest.mock import patch

import pytest
from fakeredis.aioredis import FakeRedis

from hippius_s3.dlq.upload_dlq import UploadDLQManager
from hippius_s3.metrics_collector_task import BackgroundMetricsCollector
from hippius_s3.queue import Chunk
from hippius_s3.queue import UploadChainRequest


sys.path.insert(0, str(Path(__file__).parent.parent.parent / "workers"))
from run_janitor_in_loop import get_all_dlq_object_ids  # noqa: E402


def _req(object_id: str) -> UploadChainRequest:
    return UploadChainRequest(
        address="5Fake",
        bucket_name="bkt",
        object_key=f"k/{object_id}.bin",
        object_id=object_id,
        object_version=1,
        chunks=[Chunk(id=0)],
        upload_id=f"up-{object_id}",
    )


@pytest.mark.asyncio
async def test_push_is_noop_at_cap_and_leaves_existing_entries_untouched() -> None:
    mgr = UploadDLQManager(FakeRedis(), backend_name="arion")
    mgr.max_entries = 3

    for i in range(3):
        await mgr.push(_req(f"obj-{i}"), last_error="boom", error_type="transient")
    assert await mgr.redis_client.llen(mgr.dlq_key) == 3

    # at the cap: the push is dropped, depth stays at 3, and the newest entry is absent
    await mgr.push(_req("obj-dropped"), last_error="boom", error_type="transient")

    assert await mgr.redis_client.llen(mgr.dlq_key) == 3
    ids = {e["object_id"] for e in await mgr.peek(limit=10)}
    assert ids == {"obj-0", "obj-1", "obj-2"}
    assert "obj-dropped" not in ids


@pytest.mark.asyncio
async def test_records_dropped_metric_at_cap_and_push_metric_below_cap() -> None:
    mgr = UploadDLQManager(FakeRedis(), backend_name="arion")
    mgr.max_entries = 1

    metrics = MagicMock()
    with patch("hippius_s3.dlq.base.get_metrics_collector", return_value=metrics):
        await mgr.push(_req("obj-ok"), last_error="boom", error_type="transient")
        await mgr.push(_req("obj-dropped"), last_error="boom", error_type="permanent")

    metrics.record_dlq_push.assert_called_once_with(mgr.dlq_key, "transient")
    metrics.record_dlq_dropped.assert_called_once_with(mgr.dlq_key, "permanent")


@pytest.mark.asyncio
async def test_cap_zero_disables_the_cap() -> None:
    mgr = UploadDLQManager(FakeRedis(), backend_name="arion")
    mgr.max_entries = 0

    for i in range(5):
        await mgr.push(_req(f"obj-{i}"), last_error="boom", error_type="transient")

    assert await mgr.redis_client.llen(mgr.dlq_key) == 5


@pytest.mark.asyncio
async def test_requeue_all_drains_a_capped_dlq() -> None:
    mgr = UploadDLQManager(FakeRedis(), backend_name="arion")
    mgr.max_entries = 3

    captured: list[str] = []

    async def _bulk_capture(payloads: list[UploadChainRequest]) -> None:
        captured.extend(p.object_id for p in payloads)

    mgr._bulk_enqueue = _bulk_capture  # type: ignore[assignment]

    for i in range(3):
        await mgr.push(_req(f"obj-{i}"), last_error="boom", error_type="transient")
    await mgr.push(_req("obj-dropped"), last_error="boom", error_type="transient")  # dropped at cap

    moved = await mgr.requeue_all()

    assert moved == 3
    assert sorted(captured) == ["obj-0", "obj-1", "obj-2"]
    assert await mgr.redis_client.llen(mgr.dlq_key) == 0


@pytest.mark.asyncio
async def test_janitor_still_protects_retained_entries_of_a_capped_dlq() -> None:
    redis = FakeRedis()
    mgr = UploadDLQManager(redis, backend_name="arion")
    mgr.max_entries = 2

    await mgr.push(_req("obj-a"), last_error="boom", error_type="transient")
    await mgr.push(_req("obj-b"), last_error="boom", error_type="transient")
    await mgr.push(_req("obj-dropped"), last_error="boom", error_type="transient")  # dropped

    config = MagicMock()
    config.upload_backends = ["arion"]
    with patch("run_janitor_in_loop.config", config):
        protected = await get_all_dlq_object_ids(redis)

    assert protected == {"obj-a", "obj-b"}


def test_metrics_collector_gauges_all_upload_and_unpin_dlqs() -> None:
    config = MagicMock()
    config.upload_backends = ["arion", "ovh"]
    with patch("hippius_s3.metrics_collector_task.get_config", return_value=config):
        dlqs = BackgroundMetricsCollector._dlq_queues()

    assert dlqs == ["arion_upload_requests:dlq", "ovh_upload_requests:dlq", "unpin_requests:dlq"]
