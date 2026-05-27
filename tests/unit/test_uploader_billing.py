from types import SimpleNamespace
from unittest.mock import AsyncMock
from unittest.mock import MagicMock

import pytest
from fakeredis.aioredis import FakeRedis

from hippius_s3.dlq.upload_dlq import UploadDLQManager
from hippius_s3.queue import Chunk
from hippius_s3.queue import UploadChainRequest
from hippius_s3.workers.uploader import ChunkUploadResult
from hippius_s3.workers.uploader import Uploader
from hippius_s3.workers.uploader import classify_error
from hippius_s3.workers.uploader import is_billing_error


def _billing_req(object_id: str = "bill-1", upload_id: str = "up-1") -> UploadChainRequest:
    return UploadChainRequest(
        address="5Fake",
        bucket_name="bkt",
        object_key="k/o.bin",
        object_id=object_id,
        object_version=1,
        chunks=[Chunk(id=0)],
        upload_id=upload_id,
    )


class FakeHTTP402(Exception):
    def __init__(self) -> None:
        super().__init__("Client error '402 Payment Required' for url 'https://arion.hippius.com/upload'")
        self.response = SimpleNamespace(status_code=402)


@pytest.fixture
def mock_config():
    config = MagicMock()
    config.uploader_multipart_max_concurrency = 5
    config.cache_ttl_seconds = 1800
    config.object_cache_dir = "/tmp/test_cache"
    return config


def test_is_billing_error_detects_402():
    assert is_billing_error(FakeHTTP402()) is True
    assert is_billing_error(Exception("payment required")) is True
    assert is_billing_error(Exception("connection timeout")) is False


def test_classify_error_402_is_billing():
    assert classify_error(FakeHTTP402()) == "billing"
    # billing is distinct from transient/permanent so the loop dead-letters it without retrying
    assert classify_error(Exception("502 bad gateway")) == "transient"


@pytest.mark.asyncio
async def test_upload_chunks_aborts_fanout_on_first_402(mock_config):
    """A 402 partway through must stop firing the remaining chunks at Arion."""
    redis = FakeRedis()
    redis_queues = FakeRedis()
    uploader = Uploader(MagicMock(), redis, redis_queues, mock_config, backend_name="arion", backend_client=MagicMock())

    async def fake_single(**kwargs):
        chunk = kwargs["chunk"]
        if chunk.id == 0:
            return ChunkUploadResult(cids=["cid0"], part_number=0)
        raise FakeHTTP402()

    single = AsyncMock(side_effect=fake_single)
    uploader._upload_single_chunk = single  # type: ignore[method-assign]

    chunks = [Chunk(id=i) for i in range(20)]

    with pytest.raises(FakeHTTP402):
        await uploader._upload_chunks(
            object_id="obj-1",
            object_key="k",
            chunks=chunks,
            upload_id="up-1",
            object_version=1,
            account_ss58="5Fake",
        )

    # chunk 0 succeeds, then the first batch (<= concurrency) 402s and sets the abort gate; the
    # remaining ~14 chunks short-circuit without ever touching the backend.
    assert single.call_count <= mock_config.uploader_multipart_max_concurrency + 1
    assert single.call_count < len(chunks)


@pytest.mark.asyncio
async def test_billing_error_dead_lettered_with_billing_type():
    """A 402 is dead-lettered as 'billing' (not transient), so the loop never retries it."""
    mgr = UploadDLQManager(FakeRedis(), backend_name="arion")
    await mgr.push(_billing_req(object_id="bill-2"), last_error="402 Payment Required", error_type="billing")

    entries = await mgr.peek()
    assert len(entries) == 1
    assert entries[0]["error_type"] == "billing"
    assert entries[0]["object_id"] == "bill-2"
    assert (await mgr.stats())["error_types"]["billing"] == 1


@pytest.mark.asyncio
async def test_requeue_billing_sets_bypass_and_reenqueues():
    """Recovery path: a billing-dead-lettered upload can be requeued with billing bypass (billing
    is not 'permanent', so it is not refused) and the bypass flag is set on the re-enqueued payload."""
    mgr = UploadDLQManager(FakeRedis(), backend_name="arion")
    captured: list[UploadChainRequest] = []

    async def _capture(payload: UploadChainRequest) -> None:
        captured.append(payload)

    mgr.enqueue_func = _capture  # type: ignore[assignment]
    await mgr.push(_billing_req(object_id="bill-3"), last_error="402 Payment Required", error_type="billing")

    assert await mgr.requeue("bill-3", bypass_billing=True) is True
    assert len(captured) == 1
    assert captured[0].object_id == "bill-3"
    assert captured[0].bypass_billing is True
    assert (await mgr.stats())["total_entries"] == 0


@pytest.mark.asyncio
async def test_uploader_push_to_dlq_records_billing_entry(mock_config):
    """Uploader._push_to_dlq routes a billing failure into the DLQ with the right type."""
    uploader = Uploader(
        MagicMock(), FakeRedis(), FakeRedis(), mock_config, backend_name="arion", backend_client=MagicMock()
    )
    await uploader._push_to_dlq(_billing_req(object_id="bill-4"), "402 Payment Required", "billing")

    entries = await uploader.dlq_manager.peek()
    assert len(entries) == 1
    assert entries[0]["error_type"] == "billing"
    assert entries[0]["object_id"] == "bill-4"
