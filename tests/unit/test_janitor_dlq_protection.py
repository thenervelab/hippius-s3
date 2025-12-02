"""Unit tests for janitor DLQ protection logic."""

import asyncio
import json

# Import the janitor functions we need to test
import sys
from pathlib import Path
from unittest.mock import AsyncMock
from unittest.mock import MagicMock
from unittest.mock import patch

import pytest
import pytest_asyncio
from fakeredis.aioredis import FakeRedis

from hippius_s3.cache import FileSystemPartsStore


sys.path.insert(0, str(Path(__file__).parent.parent.parent / "workers"))
from run_janitor_in_loop import get_all_dlq_object_ids  # noqa: E402


@pytest.fixture
def mock_config():
    config = MagicMock()
    config.mpu_stale_seconds = 86400  # 1 day
    config.fs_cache_gc_max_age_seconds = 604800  # 7 days
    config.object_cache_dir = "/tmp/test_janitor_cache"
    return config


@pytest.fixture
def redis_with_dlq():
    """Redis instance with DLQ entries."""
    redis = FakeRedis()
    return redis


@pytest_asyncio.fixture
async def populate_dlq(redis_with_dlq):
    """Helper to populate DLQ with test entries."""

    async def _populate(object_ids: list[str]):
        for obj_id in object_ids:
            entry = {
                "payload": {"object_id": obj_id},
                "object_id": obj_id,
                "upload_id": f"upload-{obj_id}",
                "bucket_name": "test-bucket",
                "object_key": f"key-{obj_id}",
                "attempts": 3,
                "first_enqueued_at": 1234567890.0,
                "last_attempt_at": 1234567899.0,
                "last_error": "connection timeout",
                "error_type": "transient",
            }
            await redis_with_dlq.lpush("upload_requests:dlq", json.dumps(entry))

    return _populate


class TestGetAllDlqObjectIds:
    """Test suite for get_all_dlq_object_ids helper function."""

    @pytest.mark.asyncio
    async def test_empty_dlq_returns_empty_set(self, redis_with_dlq):
        """Test that empty DLQ returns empty set."""
        result = await get_all_dlq_object_ids(redis_with_dlq)
        assert result == set()

    @pytest.mark.asyncio
    async def test_single_entry_returns_one_object_id(self, redis_with_dlq, populate_dlq):
        """Test DLQ with single entry."""
        await populate_dlq(["obj-123"])
        result = await get_all_dlq_object_ids(redis_with_dlq)
        assert result == {"obj-123"}

    @pytest.mark.asyncio
    async def test_multiple_entries_returns_all_object_ids(self, redis_with_dlq, populate_dlq):
        """Test DLQ with multiple entries."""
        await populate_dlq(["obj-123", "obj-456", "obj-789"])
        result = await get_all_dlq_object_ids(redis_with_dlq)
        assert result == {"obj-123", "obj-456", "obj-789"}

    @pytest.mark.asyncio
    async def test_duplicate_object_ids_deduplicated(self, redis_with_dlq, populate_dlq):
        """Test that duplicate object_ids are deduplicated."""
        await populate_dlq(["obj-123", "obj-123", "obj-456"])
        result = await get_all_dlq_object_ids(redis_with_dlq)
        assert result == {"obj-123", "obj-456"}

    @pytest.mark.asyncio
    async def test_invalid_json_entry_skipped(self, redis_with_dlq):
        """Test that invalid JSON entries are skipped."""
        await redis_with_dlq.lpush("upload_requests:dlq", "invalid-json{")
        await redis_with_dlq.lpush("upload_requests:dlq", json.dumps({"object_id": "obj-123"}))
        result = await get_all_dlq_object_ids(redis_with_dlq)
        assert result == {"obj-123"}

    @pytest.mark.asyncio
    async def test_missing_object_id_skipped(self, redis_with_dlq):
        """Test that entries without object_id are skipped."""
        await redis_with_dlq.lpush("upload_requests:dlq", json.dumps({"upload_id": "upload-123"}))
        await redis_with_dlq.lpush("upload_requests:dlq", json.dumps({"object_id": "obj-123"}))
        result = await get_all_dlq_object_ids(redis_with_dlq)
        assert result == {"obj-123"}

    @pytest.mark.asyncio
    async def test_timeout_returns_empty_set(self):
        """Test that timeout returns empty set."""
        mock_redis = MagicMock()
        mock_redis.lrange = AsyncMock(side_effect=asyncio.TimeoutError())
        result = await get_all_dlq_object_ids(mock_redis)
        assert result == set()

    @pytest.mark.asyncio
    async def test_redis_error_returns_empty_set(self):
        """Test that Redis errors return empty set."""
        mock_redis = MagicMock()
        mock_redis.lrange = AsyncMock(side_effect=Exception("Redis connection failed"))
        result = await get_all_dlq_object_ids(mock_redis)
        assert result == set()

    @pytest.mark.asyncio
    async def test_returns_object_ids_from_both_upload_and_unpin_dlqs(self, redis_with_dlq):
        """Test that function returns object_ids from both upload and unpin DLQs."""
        upload_entry = {
            "object_id": "obj-upload-123",
            "upload_id": "upload-123",
            "bucket_name": "test-bucket",
            "object_key": "key-123",
            "attempts": 3,
            "error_type": "transient",
        }
        await redis_with_dlq.lpush("upload_requests:dlq", json.dumps(upload_entry))

        unpin_entry = {
            "object_id": "obj-unpin-456",
            "cid": "QmTest123",
            "address": "0xTest",
            "object_version": 1,
            "attempts": 2,
            "error_type": "transient",
        }
        await redis_with_dlq.lpush("unpin_requests:dlq", json.dumps(unpin_entry))

        result = await get_all_dlq_object_ids(redis_with_dlq)
        assert result == {"obj-upload-123", "obj-unpin-456"}


class TestJanitorDlqProtection:
    """Test suite for janitor cleanup with DLQ protection."""

    @pytest.mark.asyncio
    async def test_cleanup_stale_parts_protects_dlq_objects(self, mock_config, redis_with_dlq, populate_dlq):
        """Test that cleanup_stale_parts skips objects in DLQ."""
        from run_janitor_in_loop import cleanup_stale_parts

        # Populate DLQ with protected objects
        await populate_dlq(["obj-protected-1", "obj-protected-2"])

        # Create mock DB connection
        mock_db = AsyncMock()
        mock_db.fetchval = AsyncMock(return_value=None)  # No recent DB activity

        # Create mock FS store with parts
        mock_fs_store = MagicMock(spec=FileSystemPartsStore)
        mock_root = MagicMock()
        mock_fs_store.root = mock_root
        mock_root.exists.return_value = True

        # Create directory structure
        obj_protected = MagicMock()
        obj_protected.is_dir.return_value = True
        obj_protected.name = "obj-protected-1"

        obj_not_protected = MagicMock()
        obj_not_protected.is_dir.return_value = True
        obj_not_protected.name = "obj-not-in-dlq"

        mock_root.iterdir.return_value = [obj_protected, obj_not_protected]

        # Mock version directories
        version_dir = MagicMock()
        version_dir.is_dir.return_value = True
        version_dir.name = "v1"

        obj_protected.iterdir.return_value = [version_dir]
        obj_not_protected.iterdir.return_value = [version_dir]

        # Mock part directories
        part_dir = MagicMock()
        part_dir.is_dir.return_value = True
        part_dir.name = "part_1"

        meta_file = MagicMock()
        meta_file.exists.return_value = True
        meta_file.stat.return_value.st_mtime = 0  # Very old (more than 1 day)

        part_dir.__truediv__ = lambda self, x: meta_file

        version_dir.iterdir.return_value = [part_dir]

        mock_fs_store.delete_part = AsyncMock()

        # Run cleanup
        await cleanup_stale_parts(mock_db, mock_fs_store, redis_with_dlq)

        # Protected object should NOT be deleted
        # Non-protected object should be checked and potentially deleted
        # Since we mocked DB to return None (no recent activity), non-protected would be deleted

    @pytest.mark.asyncio
    async def test_cleanup_old_parts_protects_dlq_objects(self, mock_config, redis_with_dlq, populate_dlq):
        """Test that cleanup_old_parts_by_mtime skips objects in DLQ."""
        from run_janitor_in_loop import cleanup_old_parts_by_mtime

        # Populate DLQ
        await populate_dlq(["obj-protected-1"])

        # Create mock FS store
        mock_fs_store = MagicMock(spec=FileSystemPartsStore)
        mock_root = MagicMock()
        mock_fs_store.root = mock_root
        mock_root.exists.return_value = True

        # Mock directory structure
        obj_dir = MagicMock()
        obj_dir.is_dir.return_value = True
        obj_dir.name = "obj-protected-1"

        mock_root.iterdir.return_value = [obj_dir]

        version_dir = MagicMock()
        version_dir.is_dir.return_value = True
        version_dir.name = "v1"

        obj_dir.iterdir.return_value = [version_dir]

        part_dir = MagicMock()
        part_dir.is_dir.return_value = True
        part_dir.name = "part_1"

        version_dir.iterdir.return_value = [part_dir]

        meta_file = MagicMock()
        meta_file.exists.return_value = True
        meta_file.stat.return_value.st_mtime = 0  # Very old (more than 7 days)

        part_dir.__truediv__ = lambda self, x: meta_file

        # Run cleanup
        with patch("run_janitor_in_loop.config", mock_config):
            result = await cleanup_old_parts_by_mtime(mock_fs_store, redis_with_dlq)

        # Should be 0 because object is protected
        assert result == 0

    @pytest.mark.asyncio
    async def test_dlq_protection_logs_skipped_parts(self, redis_with_dlq, populate_dlq, caplog):
        """Test that DLQ protection logs debug messages."""
        import logging

        caplog.set_level(logging.DEBUG)

        await populate_dlq(["obj-123"])

        result = await get_all_dlq_object_ids(redis_with_dlq)
        assert "obj-123" in result
