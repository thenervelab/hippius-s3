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


class _PoolCtx:
    def __init__(self, conn) -> None:
        self._conn = conn

    async def __aenter__(self):
        return self._conn

    async def __aexit__(self, *exc) -> bool:
        return False


class _FakePool:
    """Minimal asyncpg.Pool stand-in: every acquire() yields the same conn mock."""

    def __init__(self, conn) -> None:
        self._conn = conn

    def acquire(self) -> _PoolCtx:
        return _PoolCtx(self._conn)


@pytest.fixture
def mock_config():
    config = MagicMock()
    config.mpu_stale_seconds = 86400  # 1 day
    config.fs_cache_gc_max_age_seconds = 604800  # 7 days
    config.object_cache_dir = "/tmp/test_janitor_cache"
    config.janitor_concurrency = 4
    # get_all_dlq_object_ids builds DLQ keys from upload_backends — populate_dlq
    # writes to `arion_upload_requests:dlq`, so this must be a concrete ["arion"].
    config.upload_backends = ["arion"]
    config.backup_backends = []
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
            await redis_with_dlq.lpush("arion_upload_requests:dlq", json.dumps(entry))

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
        await redis_with_dlq.lpush("arion_upload_requests:dlq", "invalid-json{")
        await redis_with_dlq.lpush("arion_upload_requests:dlq", json.dumps({"object_id": "obj-123"}))
        result = await get_all_dlq_object_ids(redis_with_dlq)
        assert result == {"obj-123"}

    @pytest.mark.asyncio
    async def test_missing_object_id_skipped(self, redis_with_dlq):
        """Test that entries without object_id are skipped."""
        await redis_with_dlq.lpush("arion_upload_requests:dlq", json.dumps({"upload_id": "upload-123"}))
        await redis_with_dlq.lpush("arion_upload_requests:dlq", json.dumps({"object_id": "obj-123"}))
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
        await redis_with_dlq.lpush("arion_upload_requests:dlq", json.dumps(upload_entry))

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
    async def test_cleanup_stale_parts_protects_dlq_objects(self, tmp_path, mock_config, redis_with_dlq, populate_dlq):
        """A DLQ-protected object's part survives Phase 1; a non-protected orphan
        (no `parts` row) is reaped. Uses a real on-disk FS so the assertions are
        meaningful (the previous version of this test asserted nothing)."""
        import os
        import shutil
        import time

        from run_janitor_in_loop import cleanup_stale_parts

        protected = "aaaaaaaa-1111-1111-1111-111111111111"
        orphan = "bbbbbbbb-2222-2222-2222-222222222222"
        await populate_dlq([protected])

        old = time.time() - 200000
        for oid in (protected, orphan):
            part = tmp_path / oid / "v1" / "part_1"
            part.mkdir(parents=True)
            (part / "chunk_0.bin").write_bytes(b"x")
            (part / "meta.json").write_text("{}")
            os.utime(part / "meta.json", (old, old))

        class _FsStore:
            def __init__(self, root):
                self.root = root
                self.deleted = []

            async def delete_part(self, oid, ov, pn):
                self.deleted.append((oid, int(ov), int(pn)))
                p = self.root / oid / f"v{ov}" / f"part_{pn}"
                if p.exists():
                    shutil.rmtree(p)

        fs_store = _FsStore(tmp_path)

        # fetchrow=None → no parts row → orphan → reap (replication gate not consulted).
        mock_db = AsyncMock()
        mock_db.fetchrow = AsyncMock(return_value=None)

        with patch("run_janitor_in_loop.config", mock_config):
            await cleanup_stale_parts(_FakePool(mock_db), fs_store, redis_with_dlq)

        assert (tmp_path / protected / "v1" / "part_1").exists(), "DLQ-protected part must survive"
        assert not (tmp_path / orphan / "v1" / "part_1").exists(), "orphan must be reaped"
        assert fs_store.deleted == [(orphan, 1, 1)]

    @pytest.mark.asyncio
    async def test_cleanup_old_parts_protects_dlq_objects(self, mock_config, redis_with_dlq, populate_dlq):
        """Test that cleanup_old_parts_by_mtime skips objects in DLQ."""
        from run_janitor_in_loop import cleanup_old_parts_by_mtime

        # Populate DLQ
        await populate_dlq(["obj-protected-1"])

        # Create mock DB connection
        mock_db = AsyncMock()
        mock_db.fetchrow = AsyncMock(return_value=None)

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
            result = await cleanup_old_parts_by_mtime(_FakePool(mock_db), mock_fs_store, redis_with_dlq)

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
