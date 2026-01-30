"""Unit tests for backend routing: compute_effective_backends and resolve_object_backends."""

from __future__ import annotations

import json
import logging
from unittest.mock import AsyncMock, patch

import pytest
from fakeredis.aioredis import FakeRedis

from hippius_s3.backend_routing import compute_effective_backends, resolve_object_backends
from hippius_s3.queue import (
    Chunk,
    DownloadChainRequest,
    UnpinChainRequest,
    UploadChainRequest,
    enqueue_download_request,
    enqueue_unpin_request,
    enqueue_upload_to_backends,
    initialize_queue_client,
)


# ---------------------------------------------------------------------------
# compute_effective_backends
# ---------------------------------------------------------------------------


class TestComputeEffectiveBackends:
    def test_none_requested_returns_none(self) -> None:
        result = compute_effective_backends(None, ["ipfs", "arion"])
        assert result is None

    def test_full_overlap_returns_intersection(self) -> None:
        result = compute_effective_backends(["ipfs", "arion"], ["ipfs", "arion"])
        assert result == ["ipfs", "arion"]

    def test_partial_overlap_drops_extras(self, caplog: pytest.LogCaptureFixture) -> None:
        with caplog.at_level(logging.WARNING):
            result = compute_effective_backends(
                ["ipfs", "arion", "s3"],
                ["ipfs", "arion"],
                context={"test": True},
            )
        assert result == ["ipfs", "arion"]
        assert "Backends dropped" in caplog.text
        assert "s3" in caplog.text

    def test_empty_intersection_raise_on_empty_true(self) -> None:
        with pytest.raises(ValueError, match="No backends remain"):
            compute_effective_backends(
                ["s3"],
                ["ipfs", "arion"],
                raise_on_empty=True,
            )

    def test_empty_intersection_raise_on_empty_false(self) -> None:
        result = compute_effective_backends(
            ["s3"],
            ["ipfs", "arion"],
            raise_on_empty=False,
        )
        assert result is None

    def test_preserves_requested_order(self) -> None:
        result = compute_effective_backends(["arion", "ipfs"], ["ipfs", "arion"])
        assert result == ["arion", "ipfs"]

    def test_empty_requested_raise_on_empty_true(self) -> None:
        with pytest.raises(ValueError, match="No backends remain"):
            compute_effective_backends([], ["ipfs", "arion"], raise_on_empty=True)

    def test_empty_requested_raise_on_empty_false(self) -> None:
        result = compute_effective_backends([], ["ipfs", "arion"], raise_on_empty=False)
        assert result is None


# ---------------------------------------------------------------------------
# resolve_object_backends
# ---------------------------------------------------------------------------


class TestResolveObjectBackends:
    @pytest.mark.asyncio
    async def test_returns_backend_names(self) -> None:
        db = AsyncMock()
        db.fetch.return_value = [{"backend": "ipfs"}, {"backend": "arion"}]
        result = await resolve_object_backends(db, "obj-1", 1)
        assert result == ["ipfs", "arion"]
        db.fetch.assert_called_once()

    @pytest.mark.asyncio
    async def test_returns_empty_on_no_rows(self) -> None:
        db = AsyncMock()
        db.fetch.return_value = []
        result = await resolve_object_backends(db, "obj-1", 1)
        assert result == []

    @pytest.mark.asyncio
    async def test_returns_empty_on_none(self) -> None:
        db = AsyncMock()
        db.fetch.return_value = None
        result = await resolve_object_backends(db, "obj-1", 1)
        assert result == []

    @pytest.mark.asyncio
    async def test_passes_none_version(self) -> None:
        db = AsyncMock()
        db.fetch.return_value = [{"backend": "ipfs"}]
        await resolve_object_backends(db, "obj-1", None)
        call_args = db.fetch.call_args
        assert call_args[0][1] == "obj-1"
        assert call_args[0][2] is None


# ---------------------------------------------------------------------------
# Integration tests (FakeRedis + mock config)
# ---------------------------------------------------------------------------

def _mock_config(upload=None, download=None, delete=None):
    """Return a mock config with configurable backend lists."""
    cfg = AsyncMock()
    cfg.upload_backends = upload or ["ipfs", "arion"]
    cfg.download_backends = download or ["ipfs", "arion"]
    cfg.delete_backends = delete or ["ipfs", "arion"]
    return cfg


class TestUploadIntegration:
    @pytest.mark.asyncio
    async def test_intersection_narrows_backends(self) -> None:
        redis = FakeRedis()
        initialize_queue_client(redis)
        payload = UploadChainRequest(
            address="user1",
            bucket_name="b",
            object_key="k",
            object_id="obj-1",
            object_version=1,
            chunks=[Chunk(id=1)],
            upload_backends=["ipfs"],
        )
        with patch("hippius_s3.queue.get_config", return_value=_mock_config()):
            await enqueue_upload_to_backends(payload)
        # Should only enqueue to ipfs, not arion
        ipfs_len = await redis.llen("ipfs_upload_requests")
        arion_len = await redis.llen("arion_upload_requests")
        assert ipfs_len == 1
        assert arion_len == 0

    @pytest.mark.asyncio
    async def test_empty_intersection_raises(self) -> None:
        redis = FakeRedis()
        initialize_queue_client(redis)
        payload = UploadChainRequest(
            address="user1",
            bucket_name="b",
            object_key="k",
            object_id="obj-1",
            object_version=1,
            chunks=[Chunk(id=1)],
            upload_backends=["s3"],
        )
        with patch("hippius_s3.queue.get_config", return_value=_mock_config()):
            with pytest.raises(ValueError, match="No backends remain"):
                await enqueue_upload_to_backends(payload)

    @pytest.mark.asyncio
    async def test_no_preference_uses_config(self) -> None:
        redis = FakeRedis()
        initialize_queue_client(redis)
        payload = UploadChainRequest(
            address="user1",
            bucket_name="b",
            object_key="k",
            object_id="obj-1",
            object_version=1,
            chunks=[Chunk(id=1)],
            upload_backends=None,
        )
        with patch("hippius_s3.queue.get_config", return_value=_mock_config()):
            await enqueue_upload_to_backends(payload)
        ipfs_len = await redis.llen("ipfs_upload_requests")
        arion_len = await redis.llen("arion_upload_requests")
        assert ipfs_len == 1
        assert arion_len == 1


class TestDownloadIntegration:
    @pytest.mark.asyncio
    async def test_db_resolved_backends_used(self) -> None:
        redis = FakeRedis()
        initialize_queue_client(redis)
        payload = DownloadChainRequest(
            object_id="obj-1",
            object_version=1,
            object_key="k",
            bucket_name="b",
            address="user1",
            subaccount="user1",
            subaccount_seed_phrase="",
            substrate_url="http://test",
            size=100,
            multipart=False,
            chunks=[],
            download_backends=["arion"],
        )
        with patch("hippius_s3.queue.get_config", return_value=_mock_config()):
            await enqueue_download_request(payload)
        arion_len = await redis.llen("arion_download_requests")
        ipfs_len = await redis.llen("ipfs_download_requests")
        assert arion_len == 1
        assert ipfs_len == 0

    @pytest.mark.asyncio
    async def test_no_backends_falls_to_config(self) -> None:
        redis = FakeRedis()
        initialize_queue_client(redis)
        payload = DownloadChainRequest(
            object_id="obj-1",
            object_version=1,
            object_key="k",
            bucket_name="b",
            address="user1",
            subaccount="user1",
            subaccount_seed_phrase="",
            substrate_url="http://test",
            size=100,
            multipart=False,
            chunks=[],
            download_backends=None,
        )
        with patch("hippius_s3.queue.get_config", return_value=_mock_config()):
            await enqueue_download_request(payload)
        ipfs_len = await redis.llen("ipfs_download_requests")
        arion_len = await redis.llen("arion_download_requests")
        assert ipfs_len == 1
        assert arion_len == 1


    @pytest.mark.asyncio
    async def test_misconfig_does_not_enqueue(self, caplog: pytest.LogCaptureFixture) -> None:
        redis = FakeRedis()
        initialize_queue_client(redis)
        payload = DownloadChainRequest(
            object_id="obj-1",
            object_version=1,
            object_key="k",
            bucket_name="b",
            address="user1",
            subaccount="user1",
            subaccount_seed_phrase="",
            substrate_url="http://test",
            size=100,
            multipart=False,
            chunks=[],
            download_backends=["s3"],
        )
        with patch("hippius_s3.queue.get_config", return_value=_mock_config()):
            with caplog.at_level(logging.ERROR):
                await enqueue_download_request(payload)
        # Nothing enqueued
        ipfs_len = await redis.llen("ipfs_download_requests")
        arion_len = await redis.llen("arion_download_requests")
        s3_len = await redis.llen("s3_download_requests")
        assert ipfs_len == 0
        assert arion_len == 0
        assert s3_len == 0
        assert "All requested download backends disallowed" in caplog.text


class TestUnpinIntegration:
    @pytest.mark.asyncio
    async def test_db_resolved_backends_used(self) -> None:
        redis = FakeRedis()
        initialize_queue_client(redis)
        payload = UnpinChainRequest(
            address="user1",
            object_id="obj-1",
            object_version=1,
            delete_backends=["arion"],
        )
        with patch("hippius_s3.queue.get_config", return_value=_mock_config()):
            await enqueue_unpin_request(payload)
        arion_len = await redis.llen("arion_unpin_requests")
        ipfs_len = await redis.llen("ipfs_unpin_requests")
        assert arion_len == 1
        assert ipfs_len == 0

    @pytest.mark.asyncio
    async def test_no_backends_fans_to_config(self) -> None:
        redis = FakeRedis()
        initialize_queue_client(redis)
        payload = UnpinChainRequest(
            address="user1",
            object_id="obj-1",
            object_version=1,
            delete_backends=None,
        )
        with patch("hippius_s3.queue.get_config", return_value=_mock_config()):
            await enqueue_unpin_request(payload)
        ipfs_len = await redis.llen("ipfs_unpin_requests")
        arion_len = await redis.llen("arion_unpin_requests")
        assert ipfs_len == 1
        assert arion_len == 1

    @pytest.mark.asyncio
    async def test_misconfig_does_not_enqueue(self, caplog: pytest.LogCaptureFixture) -> None:
        redis = FakeRedis()
        initialize_queue_client(redis)
        payload = UnpinChainRequest(
            address="user1",
            object_id="obj-1",
            object_version=1,
            delete_backends=["s3"],
        )
        with patch("hippius_s3.queue.get_config", return_value=_mock_config()):
            with caplog.at_level(logging.ERROR):
                await enqueue_unpin_request(payload)
        # Nothing enqueued
        ipfs_len = await redis.llen("ipfs_unpin_requests")
        arion_len = await redis.llen("arion_unpin_requests")
        s3_len = await redis.llen("s3_unpin_requests")
        assert ipfs_len == 0
        assert arion_len == 0
        assert s3_len == 0
        assert "All requested delete backends disallowed" in caplog.text

    @pytest.mark.asyncio
    async def test_queue_name_overrides_delete_backends(self) -> None:
        redis = FakeRedis()
        initialize_queue_client(redis)
        payload = UnpinChainRequest(
            address="user1",
            object_id="obj-1",
            object_version=1,
            delete_backends=["arion"],
        )
        with patch("hippius_s3.queue.get_config", return_value=_mock_config()):
            await enqueue_unpin_request(payload, queue_name="ipfs_unpin_requests")
        # queue_name takes precedence — only ipfs queue
        ipfs_len = await redis.llen("ipfs_unpin_requests")
        arion_len = await redis.llen("arion_unpin_requests")
        assert ipfs_len == 1
        assert arion_len == 0
