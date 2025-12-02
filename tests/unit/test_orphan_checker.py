import json
import pytest
from unittest.mock import AsyncMock, patch
from fakeredis.aioredis import FakeRedis


@pytest.fixture
def mock_db():
    """Mock asyncpg connection"""
    conn = AsyncMock()
    conn.fetchrow = AsyncMock()
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
    mock_db.fetchrow.return_value = {"exists": False}

    with patch("workers.run_orphan_checker_in_loop.HippiusApiClient") as mock_api:
        mock_api_instance = AsyncMock()
        mock_api_instance.list_files = AsyncMock(
            return_value={
                "results": [
                    {
                        "cid": "QmOrphanFile123",
                        "original_name": "s3-orphan-test",
                        "account_ss58": "5TestAccount",
                        "size_bytes": 1024,
                    }
                ],
                "next": None,
            }
        )
        mock_api_instance.__aenter__ = AsyncMock(return_value=mock_api_instance)
        mock_api_instance.__aexit__ = AsyncMock()
        mock_api.return_value = mock_api_instance

        from workers.run_orphan_checker_in_loop import check_for_orphans

        await check_for_orphans(mock_db)

        queued_items = await mock_redis_queues.lrange("unpin_requests", 0, -1)
        assert len(queued_items) == 1

        request = json.loads(queued_items[0])
        assert request["cid"] == "QmOrphanFile123"
        assert request["address"] == "5TestAccount"
        assert request["object_id"] == "00000000-0000-0000-0000-000000000000"
        assert request["object_version"] == 0


@pytest.mark.asyncio
async def test_non_orphan_file_not_queued(mock_db, mock_redis_queues):
    """Test that non-orphan files (exist in DB) are not queued for unpinning"""
    mock_db.fetchrow.return_value = {"exists": True}

    with patch("workers.run_orphan_checker_in_loop.HippiusApiClient") as mock_api:
        mock_api_instance = AsyncMock()
        mock_api_instance.list_files = AsyncMock(
            return_value={
                "results": [
                    {
                        "cid": "QmValidFile456",
                        "original_name": "s3-valid-file",
                        "account_ss58": "5TestAccount",
                        "size_bytes": 2048,
                    }
                ],
                "next": None,
            }
        )
        mock_api_instance.__aenter__ = AsyncMock(return_value=mock_api_instance)
        mock_api_instance.__aexit__ = AsyncMock()
        mock_api.return_value = mock_api_instance

        from workers.run_orphan_checker_in_loop import check_for_orphans

        await check_for_orphans(mock_db)

        queued_items = await mock_redis_queues.lrange("unpin_requests", 0, -1)
        assert len(queued_items) == 0


@pytest.mark.asyncio
async def test_pagination_processes_all_pages(mock_db, mock_redis_queues):
    """Test that pagination is handled correctly and all pages are processed"""
    mock_db.fetchrow.return_value = {"exists": False}

    with patch("workers.run_orphan_checker_in_loop.HippiusApiClient") as mock_api:
        mock_api_instance = AsyncMock()

        page1_response = {
            "results": [
                {
                    "cid": "QmPage1File1",
                    "original_name": "s3-page1-file1",
                    "account_ss58": "5TestAccount",
                    "size_bytes": 1024,
                }
            ],
            "next": "http://api.hippius.com/storage-control/files/?page=2",
        }

        page2_response = {
            "results": [
                {
                    "cid": "QmPage2File1",
                    "original_name": "s3-page2-file1",
                    "account_ss58": "5TestAccount",
                    "size_bytes": 2048,
                }
            ],
            "next": None,
        }

        mock_api_instance.list_files = AsyncMock(side_effect=[page1_response, page2_response])
        mock_api_instance.__aenter__ = AsyncMock(return_value=mock_api_instance)
        mock_api_instance.__aexit__ = AsyncMock()
        mock_api.return_value = mock_api_instance

        from workers.run_orphan_checker_in_loop import check_for_orphans

        await check_for_orphans(mock_db)

        queued_items = await mock_redis_queues.lrange("unpin_requests", 0, -1)
        assert len(queued_items) == 2

        cids = [json.loads(item)["cid"] for item in queued_items]
        assert "QmPage1File1" in cids
        assert "QmPage2File1" in cids
