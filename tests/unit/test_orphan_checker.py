import json
from unittest.mock import AsyncMock
from unittest.mock import patch

import pytest
from fakeredis.aioredis import FakeRedis

from hippius_s3.services.hippius_api_service import FileItem
from hippius_s3.services.hippius_api_service import ListFilesResponse


@pytest.fixture
def mock_db():
    """Mock asyncpg connection"""
    conn = AsyncMock()
    conn.fetchrow = AsyncMock()
    conn.fetch = AsyncMock()
    return conn


@pytest.fixture
def mock_redis_queues():
    """FakeRedis for queue testing"""
    redis_queues = FakeRedis()
    from hippius_s3.queue import initialize_queue_client

    initialize_queue_client(redis_queues)
    return redis_queues


@pytest.mark.asyncio
async def test_orphan_file_queued_for_unpinning(mock_db, mock_redis_queues):
    """Test that orphan files (on chain but not in DB) are queued for unpinning"""
    mock_db.fetch.return_value = [{"main_account_id": "5TestAccount"}]
    mock_db.fetchrow.return_value = {"exists": False}

    with patch("workers.run_orphan_checker_in_loop.HippiusApiClient") as mock_api:
        mock_api_instance = AsyncMock()
        mock_api_instance.list_files = AsyncMock(
            return_value=ListFilesResponse(
                count=1,
                next=None,
                previous=None,
                results=[
                    FileItem(
                        file_id="file123",
                        cid="QmOrphanFile123",
                        original_name="s3-orphan-test",
                        size_bytes=1024,
                        status="active",
                        pinned_node_ids=["node1"],
                        active_replica_count=1,
                        miners="miner1",
                        updated_at="2025-01-01T00:00:00Z",
                        created_at="2025-01-01T00:00:00Z",
                    )
                ],
            )
        )
        mock_api_instance.__aenter__ = AsyncMock(return_value=mock_api_instance)
        mock_api_instance.__aexit__ = AsyncMock()
        mock_api.return_value = mock_api_instance

        from workers.run_orphan_checker_in_loop import check_for_orphans

        await check_for_orphans(mock_db)

        # Fan-out enqueues to all configured unpin queues (ipfs + arion)
        ipfs_items = await mock_redis_queues.lrange("ipfs_unpin_requests", 0, -1)
        arion_items = await mock_redis_queues.lrange("arion_unpin_requests", 0, -1)
        assert len(ipfs_items) == 1
        assert len(arion_items) == 1

        request = json.loads(ipfs_items[0])
        assert request["cid"] == "QmOrphanFile123"
        assert request["address"] == "5TestAccount"
        assert request["object_id"] == "00000000-0000-0000-0000-000000000000"
        assert request["object_version"] == 0


@pytest.mark.asyncio
async def test_non_orphan_file_not_queued(mock_db, mock_redis_queues):
    """Test that non-orphan files (exist in DB) are not queued for unpinning"""
    mock_db.fetch.return_value = [{"main_account_id": "5TestAccount"}]
    mock_db.fetchrow.return_value = {"exists": True}

    with patch("workers.run_orphan_checker_in_loop.HippiusApiClient") as mock_api:
        mock_api_instance = AsyncMock()
        mock_api_instance.list_files = AsyncMock(
            return_value=ListFilesResponse(
                count=1,
                next=None,
                previous=None,
                results=[
                    FileItem(
                        file_id="file456",
                        cid="QmValidFile456",
                        original_name="s3-valid-file",
                        size_bytes=2048,
                        status="active",
                        pinned_node_ids=["node1"],
                        active_replica_count=1,
                        miners="miner1",
                        updated_at="2025-01-01T00:00:00Z",
                        created_at="2025-01-01T00:00:00Z",
                    )
                ],
            )
        )
        mock_api_instance.__aenter__ = AsyncMock(return_value=mock_api_instance)
        mock_api_instance.__aexit__ = AsyncMock()
        mock_api.return_value = mock_api_instance

        from workers.run_orphan_checker_in_loop import check_for_orphans

        await check_for_orphans(mock_db)

        ipfs_items = await mock_redis_queues.lrange("ipfs_unpin_requests", 0, -1)
        arion_items = await mock_redis_queues.lrange("arion_unpin_requests", 0, -1)
        assert len(ipfs_items) == 0
        assert len(arion_items) == 0


@pytest.mark.asyncio
async def test_pagination_processes_all_pages(mock_db, mock_redis_queues):
    """Test that pagination is handled correctly and all pages are processed"""
    mock_db.fetch.return_value = [{"main_account_id": "5TestAccount"}]
    mock_db.fetchrow.return_value = {"exists": False}

    with patch("workers.run_orphan_checker_in_loop.HippiusApiClient") as mock_api:
        mock_api_instance = AsyncMock()

        page1_response = ListFilesResponse(
            count=2,
            next="http://api.hippius.com/storage-control/files/?page=2",
            previous=None,
            results=[
                FileItem(
                    file_id="file1",
                    cid="QmPage1File1",
                    original_name="s3-page1-file1",
                    size_bytes=1024,
                    status="active",
                    pinned_node_ids=["node1"],
                    active_replica_count=1,
                    miners="miner1",
                    updated_at="2025-01-01T00:00:00Z",
                    created_at="2025-01-01T00:00:00Z",
                )
            ],
        )

        page2_response = ListFilesResponse(
            count=2,
            next=None,
            previous="http://api.hippius.com/storage-control/files/?page=1",
            results=[
                FileItem(
                    file_id="file2",
                    cid="QmPage2File1",
                    original_name="s3-page2-file1",
                    size_bytes=2048,
                    status="active",
                    pinned_node_ids=["node1"],
                    active_replica_count=1,
                    miners="miner1",
                    updated_at="2025-01-01T00:00:00Z",
                    created_at="2025-01-01T00:00:00Z",
                )
            ],
        )

        mock_api_instance.list_files = AsyncMock(side_effect=[page1_response, page2_response])
        mock_api_instance.__aenter__ = AsyncMock(return_value=mock_api_instance)
        mock_api_instance.__aexit__ = AsyncMock()
        mock_api.return_value = mock_api_instance

        from workers.run_orphan_checker_in_loop import check_for_orphans

        await check_for_orphans(mock_db)

        # Fan-out enqueues to all configured unpin queues
        ipfs_items = await mock_redis_queues.lrange("ipfs_unpin_requests", 0, -1)
        assert len(ipfs_items) == 2

        cids = [json.loads(item)["cid"] for item in ipfs_items]
        assert "QmPage1File1" in cids
        assert "QmPage2File1" in cids


@pytest.mark.asyncio
async def test_chunk_cid_in_part_chunks_not_orphaned(mock_db, mock_redis_queues):
    """Test that chunk CIDs stored in part_chunks are not marked as orphans"""
    mock_db.fetch.return_value = [{"main_account_id": "5TestAccount"}]
    mock_db.fetchrow.return_value = {"exists": True}

    with patch("workers.run_orphan_checker_in_loop.HippiusApiClient") as mock_api:
        mock_api_instance = AsyncMock()
        mock_api_instance.list_files = AsyncMock(
            return_value=ListFilesResponse(
                count=1,
                next=None,
                previous=None,
                results=[
                    FileItem(
                        file_id="chunk789",
                        cid="QmChunkCID789",
                        original_name="s3-chunk-test",
                        size_bytes=512,
                        status="active",
                        pinned_node_ids=["node1"],
                        active_replica_count=1,
                        miners="miner1",
                        updated_at="2025-01-01T00:00:00Z",
                        created_at="2025-01-01T00:00:00Z",
                    )
                ],
            )
        )
        mock_api_instance.__aenter__ = AsyncMock(return_value=mock_api_instance)
        mock_api_instance.__aexit__ = AsyncMock()
        mock_api.return_value = mock_api_instance

        from workers.run_orphan_checker_in_loop import check_for_orphans

        await check_for_orphans(mock_db)

        ipfs_items = await mock_redis_queues.lrange("ipfs_unpin_requests", 0, -1)
        arion_items = await mock_redis_queues.lrange("arion_unpin_requests", 0, -1)
        assert len(ipfs_items) == 0
        assert len(arion_items) == 0


@pytest.mark.asyncio
async def test_orphan_chunk_cid_queued_for_unpinning(mock_db, mock_redis_queues):
    """Test that orphaned chunk CIDs (not in cids or part_chunks) are queued"""
    mock_db.fetch.return_value = [{"main_account_id": "5TestAccount"}]
    mock_db.fetchrow.return_value = {"exists": False}

    with patch("workers.run_orphan_checker_in_loop.HippiusApiClient") as mock_api:
        mock_api_instance = AsyncMock()
        mock_api_instance.list_files = AsyncMock(
            return_value=ListFilesResponse(
                count=1,
                next=None,
                previous=None,
                results=[
                    FileItem(
                        file_id="orphan_chunk",
                        cid="QmOrphanChunk999",
                        original_name="s3-orphan-chunk",
                        size_bytes=256,
                        status="active",
                        pinned_node_ids=["node1"],
                        active_replica_count=1,
                        miners="miner1",
                        updated_at="2025-01-01T00:00:00Z",
                        created_at="2025-01-01T00:00:00Z",
                    )
                ],
            )
        )
        mock_api_instance.__aenter__ = AsyncMock(return_value=mock_api_instance)
        mock_api_instance.__aexit__ = AsyncMock()
        mock_api.return_value = mock_api_instance

        from workers.run_orphan_checker_in_loop import check_for_orphans

        await check_for_orphans(mock_db)

        ipfs_items = await mock_redis_queues.lrange("ipfs_unpin_requests", 0, -1)
        assert len(ipfs_items) == 1

        request = json.loads(ipfs_items[0])
        assert request["cid"] == "QmOrphanChunk999"
        assert request["address"] == "5TestAccount"
