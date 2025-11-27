from unittest.mock import AsyncMock
from unittest.mock import MagicMock
from unittest.mock import patch

import pytest
from fakeredis.aioredis import FakeRedis

from hippius_s3.cache import FileSystemPartsStore
from hippius_s3.queue import Chunk
from hippius_s3.queue import UploadChainRequest
from hippius_s3.services.hippius_api_service import FileStatusResponse
from hippius_s3.services.hippius_api_service import UploadResponse
from hippius_s3.workers.uploader import Uploader


@pytest.fixture
def mock_config():
    config = MagicMock()
    config.uploader_multipart_max_concurrency = 5
    config.cache_ttl_seconds = 1800
    config.object_cache_dir = "/tmp/test_cache"
    config.publish_to_chain = False
    return config


@pytest.fixture
def mock_db_pool():
    pool = MagicMock()
    conn = AsyncMock()

    conn.fetchrow = AsyncMock()
    conn.fetch = AsyncMock()
    conn.execute = AsyncMock()

    async def mock_acquire():
        return conn

    pool.acquire = MagicMock(return_value=MagicMock(__aenter__=AsyncMock(return_value=conn)))

    return pool


@pytest.fixture
def mock_fs_store():
    store = MagicMock(spec=FileSystemPartsStore)
    store.get_chunk = AsyncMock(return_value=b"encrypted_chunk_data_123")
    store.get_meta = AsyncMock(return_value={"num_chunks": 1, "chunk_size": 1024})
    return store


@pytest.mark.asyncio
async def test_upload_single_chunk_calls_new_api(mock_config, mock_db_pool, mock_fs_store):
    redis = FakeRedis()
    ipfs_client = MagicMock()

    uploader = Uploader(mock_db_pool, ipfs_client, redis, mock_config)
    uploader.fs_store = mock_fs_store

    mock_upload_response = UploadResponse(
        id="file-uuid-123",
        original_name="test.part1.chunk0",
        content_type="application/octet-stream",
        size_bytes=1024,
        sha256_hex="abc123",
        cid="QmChunk123",
        status="completed",
        file_url="https://example.com/file",
        created_at="2025-11-27T12:00:00Z",
        updated_at="2025-11-27T12:00:00Z",
    )

    mock_status_response = FileStatusResponse(
        id="file-uuid-123",
        original_name="test.part1.chunk0",
        content_type="application/octet-stream",
        size_bytes=1024,
        sha256_hex="abc123",
        cid="QmChunk123",
        status="completed",
        file_url="https://example.com/file",
        created_at="2025-11-27T12:00:00Z",
        updated_at="2025-11-27T12:00:00Z",
    )

    mock_conn = AsyncMock()

    class MockRow(dict):
        def __getitem__(self, key):
            if isinstance(key, int):
                keys = list(self.keys())
                return self[keys[key]]
            return super().__getitem__(key)

    mock_conn.fetchrow = AsyncMock(
        side_effect=[
            MockRow({"part_id": "part-uuid", "chunk_size_bytes": 1024, "size_bytes": 1024}),
            MockRow({"id": "cid-uuid-123"}),
        ]
    )
    mock_conn.execute = AsyncMock()
    mock_conn.fetch = AsyncMock(return_value=[])

    mock_db_pool.acquire = MagicMock(return_value=MagicMock(__aenter__=AsyncMock(return_value=mock_conn)))

    with patch("hippius_s3.workers.uploader.HippiusApiClient") as mock_api_client_class:
        mock_api_instance = AsyncMock()
        mock_api_instance.upload_file_and_get_cid = AsyncMock(return_value=mock_upload_response)
        mock_api_instance.get_file_status = AsyncMock(return_value=mock_status_response)
        mock_api_instance.__aenter__ = AsyncMock(return_value=mock_api_instance)
        mock_api_instance.__aexit__ = AsyncMock()

        mock_api_client_class.return_value = mock_api_instance

        result = await uploader._upload_single_chunk(
            object_id="obj-123", object_key="test-key", chunk=Chunk(id=1), upload_id="upload-123", object_version=1
        )

        assert mock_api_instance.upload_file_and_get_cid.call_count == 1
        call_args = mock_api_instance.upload_file_and_get_cid.call_args
        assert call_args.kwargs["file_data"] == b"encrypted_chunk_data_123"
        assert call_args.kwargs["file_name"] == "test-key.part1.chunk0"
        assert call_args.kwargs["content_type"] == "application/octet-stream"

        assert mock_api_instance.get_file_status.call_count == 1
        status_call_args = mock_api_instance.get_file_status.call_args
        assert status_call_args.args[0] == "file-uuid-123"

        assert mock_conn.execute.call_count >= 1


@pytest.mark.asyncio
async def test_build_and_upload_manifest_uses_new_api(mock_config, mock_db_pool, mock_fs_store):
    redis = FakeRedis()
    ipfs_client = MagicMock()

    uploader = Uploader(mock_db_pool, ipfs_client, redis, mock_config)

    mock_upload_response = UploadResponse(
        id="manifest-uuid-456",
        original_name="test.manifest",
        content_type="application/json",
        size_bytes=512,
        sha256_hex="def456",
        cid="QmManifest456",
        status="completed",
        file_url="https://example.com/manifest",
        created_at="2025-11-27T12:00:00Z",
        updated_at="2025-11-27T12:00:00Z",
    )

    mock_status_response = FileStatusResponse(
        id="manifest-uuid-456",
        original_name="test.manifest",
        content_type="application/json",
        size_bytes=512,
        sha256_hex="def456",
        cid="QmManifest456",
        status="completed",
        file_url="https://example.com/manifest",
        created_at="2025-11-27T12:00:00Z",
        updated_at="2025-11-27T12:00:00Z",
    )

    mock_conn = AsyncMock()

    class MockRow(dict):
        def __getitem__(self, key):
            if isinstance(key, int):
                keys = list(self.keys())
                return self[keys[key]]
            return super().__getitem__(key)

    mock_conn.fetch = AsyncMock(
        return_value=[
            MockRow({"part_number": 1, "cid": "QmPart1", "size_bytes": 1024}),
            MockRow({"part_number": 2, "cid": "QmPart2", "size_bytes": 2048}),
        ]
    )
    mock_conn.fetchrow = AsyncMock(
        side_effect=[
            MockRow({"content_type": "application/octet-stream"}),
            MockRow({"id": "cid-uuid-123"}),
        ]
    )
    mock_conn.execute = AsyncMock()

    mock_db_pool.acquire = MagicMock(return_value=MagicMock(__aenter__=AsyncMock(return_value=mock_conn)))

    with patch("hippius_s3.workers.uploader.HippiusApiClient") as mock_api_client_class:
        mock_api_instance = AsyncMock()
        mock_api_instance.upload_file_and_get_cid = AsyncMock(return_value=mock_upload_response)
        mock_api_instance.get_file_status = AsyncMock(return_value=mock_status_response)
        mock_api_instance.__aenter__ = AsyncMock(return_value=mock_api_instance)
        mock_api_instance.__aexit__ = AsyncMock()

        mock_api_client_class.return_value = mock_api_instance

        result = await uploader._build_and_upload_manifest(
            object_id="obj-123", object_key="test-key", object_version=1
        )

        assert result["manifest_cid"] == "QmManifest456"
        assert result["manifest_size_bytes"] == 512

        assert mock_api_instance.upload_file_and_get_cid.call_count == 1
        call_args = mock_api_instance.upload_file_and_get_cid.call_args
        assert call_args.kwargs["file_name"] == "test-key.manifest"
        assert call_args.kwargs["content_type"] == "application/json"
        assert b"QmPart1" in call_args.kwargs["file_data"]
        assert b"QmPart2" in call_args.kwargs["file_data"]

        assert mock_api_instance.get_file_status.call_count == 1
        status_call_args = mock_api_instance.get_file_status.call_args
        assert status_call_args.args[0] == "manifest-uuid-456"

        execute_calls = [call.args for call in mock_conn.execute.call_args_list]
        update_call = [call for call in execute_calls if "UPDATE object_versions" in call[0]]
        assert len(update_call) == 1
        assert update_call[0][3] == "QmManifest456"
        assert update_call[0][5] == "manifest-uuid-456"


@pytest.mark.asyncio
async def test_upload_stores_api_file_id_in_database(mock_config, mock_db_pool, mock_fs_store):
    redis = FakeRedis()
    ipfs_client = MagicMock()

    uploader = Uploader(mock_db_pool, ipfs_client, redis, mock_config)
    uploader.fs_store = mock_fs_store

    mock_upload_response = UploadResponse(
        id="file-uuid-999",
        original_name="test.chunk",
        content_type="application/octet-stream",
        size_bytes=1024,
        sha256_hex="xyz789",
        cid="QmTestCID",
        status="completed",
        file_url="https://example.com/file",
        created_at="2025-11-27T12:00:00Z",
        updated_at="2025-11-27T12:00:00Z",
    )

    mock_status_response = FileStatusResponse(
        id="file-uuid-999",
        original_name="test.chunk",
        content_type="application/octet-stream",
        size_bytes=1024,
        sha256_hex="xyz789",
        cid="QmTestCID",
        status="completed",
        file_url="https://example.com/file",
        created_at="2025-11-27T12:00:00Z",
        updated_at="2025-11-27T12:00:00Z",
    )

    mock_conn = AsyncMock()

    class MockRow(dict):
        def __getitem__(self, key):
            if isinstance(key, int):
                keys = list(self.keys())
                return self[keys[key]]
            return super().__getitem__(key)

    mock_conn.fetchrow = AsyncMock(
        side_effect=[
            MockRow({"part_id": "part-uuid", "chunk_size_bytes": 1024, "size_bytes": 1024}),
            MockRow({"id": "cid-uuid-123"}),
        ]
    )
    mock_conn.execute = AsyncMock()

    mock_db_pool.acquire = MagicMock(return_value=MagicMock(__aenter__=AsyncMock(return_value=mock_conn)))

    with patch("hippius_s3.workers.uploader.HippiusApiClient") as mock_api_client_class:
        mock_api_instance = AsyncMock()
        mock_api_instance.upload_file_and_get_cid = AsyncMock(return_value=mock_upload_response)
        mock_api_instance.get_file_status = AsyncMock(return_value=mock_status_response)
        mock_api_instance.__aenter__ = AsyncMock(return_value=mock_api_instance)
        mock_api_instance.__aexit__ = AsyncMock()

        mock_api_client_class.return_value = mock_api_instance

        with patch("hippius_s3.workers.uploader.get_query") as mock_get_query:
            mock_get_query.return_value = "MOCKED_QUERY"

            await uploader._upload_single_chunk(
                object_id="obj-123",
                object_key="test-key",
                chunk=Chunk(id=1),
                upload_id="upload-123",
                object_version=1,
            )

            execute_calls = mock_conn.execute.call_args_list
            upsert_call = None
            for call in execute_calls:
                if call.args[0] == "MOCKED_QUERY":
                    upsert_call = call
                    break

            assert upsert_call is not None
            args = upsert_call.args
            assert len(args) == 8
            assert args[7] == "file-uuid-999"


@pytest.mark.asyncio
async def test_process_upload_no_longer_calls_pin_on_api(mock_config, mock_db_pool):
    redis = FakeRedis()
    ipfs_client = MagicMock()
    mock_config.publish_to_chain = True

    uploader = Uploader(mock_db_pool, ipfs_client, redis, mock_config)

    mock_conn = AsyncMock()
    mock_conn.execute = AsyncMock()
    mock_db_pool.acquire = MagicMock(return_value=MagicMock(__aenter__=AsyncMock(return_value=mock_conn)))

    payload = UploadChainRequest(
        substrate_url="http://test",
        ipfs_node="http://test",
        address="user1",
        subaccount="user1",
        subaccount_seed_phrase="test-seed",
        bucket_name="test-bucket",
        object_key="test-key",
        should_encrypt=False,
        object_id="obj-123",
        object_version=1,
        chunks=[Chunk(id=1)],
        upload_id="upload-123",
        attempts=1,
        first_enqueued_at=1234567890.0,
        request_id="req-456",
    )

    with (
        patch.object(uploader, "_upload_chunks", new_callable=AsyncMock) as mock_upload_chunks,
        patch.object(uploader, "_build_and_upload_manifest", new_callable=AsyncMock) as mock_build_manifest,
    ):
        mock_upload_chunks.return_value = ["QmCID1"]
        mock_build_manifest.return_value = {
            "parts": [{"cid": "QmCID1", "size_bytes": 1024}],
            "manifest_cid": "QmManifest",
            "manifest_size_bytes": 512,
        }

        result = await uploader.process_upload(payload)

        assert "QmCID1" in result
        assert "QmManifest" in result

        assert not hasattr(uploader, "pin_on_api")
