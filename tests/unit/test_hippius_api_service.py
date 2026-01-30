import io
from unittest.mock import AsyncMock
from unittest.mock import MagicMock
from unittest.mock import patch

import httpx
import pytest

from hippius_s3.services.hippius_api_service import FileStatusResponse
from hippius_s3.services.hippius_api_service import HippiusApiClient
from hippius_s3.services.hippius_api_service import UploadResponse


@pytest.mark.asyncio
async def test_upload_file_and_get_cid_success():
    client = HippiusApiClient()
    mock_response = MagicMock()
    mock_response.status_code = 200
    mock_response.json.return_value = {
        "id": "file-uuid-123",
        "original_name": "test.bin",
        "content_type": "application/octet-stream",
        "size_bytes": 1024,
        "sha256_hex": "abc123",
        "cid": "QmTest123",
        "status": "completed",
        "file_url": "https://example.com/file",
        "created_at": "2025-11-27T12:00:00Z",
        "updated_at": "2025-11-27T12:00:00Z",
    }

    with patch.object(client._client, "post", new_callable=AsyncMock) as mock_post:
        mock_post.return_value = mock_response

        result = await client.upload_file_and_get_cid(
            file_data=b"test data",
            file_name="test.bin",
            content_type="application/octet-stream",
            account_ss58="5FakeTestAccountAddress123456789012345678901234",
        )

        assert isinstance(result, UploadResponse)
        assert result.id == "file-uuid-123"
        assert result.cid == "QmTest123"
        assert result.size_bytes == 1024
        assert result.status == "completed"

        mock_post.assert_called_once()
        call_args = mock_post.call_args
        assert call_args.args[0] == "/storage-control/upload/"
        assert "file" in call_args.kwargs["files"]
        assert "Authorization" in call_args.kwargs["headers"]
        assert "ServiceToken" in call_args.kwargs["headers"]["Authorization"]


@pytest.mark.asyncio
async def test_upload_file_and_get_cid_multipart_format():
    client = HippiusApiClient()
    mock_response = MagicMock()
    mock_response.status_code = 200
    mock_response.json.return_value = {
        "id": "file-uuid-123",
        "original_name": "test.bin",
        "content_type": "application/octet-stream",
        "size_bytes": 1024,
        "sha256_hex": "abc123",
        "cid": "QmTest123",
        "status": "completed",
        "file_url": "https://example.com/file",
        "created_at": "2025-11-27T12:00:00Z",
        "updated_at": "2025-11-27T12:00:00Z",
    }

    with patch.object(client._client, "post", new_callable=AsyncMock) as mock_post:
        mock_post.return_value = mock_response

        file_data = b"test file contents"
        await client.upload_file_and_get_cid(
            file_data=file_data,
            file_name="test.bin",
            content_type="application/octet-stream",
            account_ss58="5FakeTestAccountAddress123456789012345678901234",
        )

        call_args = mock_post.call_args
        files_param = call_args.kwargs["files"]
        assert "file" in files_param

        file_tuple = files_param["file"]
        assert file_tuple[0] == "test.bin"
        assert isinstance(file_tuple[1], io.BytesIO)
        assert file_tuple[2] == "application/octet-stream"

        headers = call_args.kwargs["headers"]
        assert "Content-Type" not in headers


@pytest.mark.asyncio
async def test_upload_file_and_get_cid_retry_on_failure():
    client = HippiusApiClient()
    mock_response_fail = MagicMock()
    mock_response_fail.status_code = 500
    mock_response_fail.raise_for_status.side_effect = httpx.HTTPStatusError(
        "Server error", request=MagicMock(), response=mock_response_fail
    )

    mock_response_success = MagicMock()
    mock_response_success.status_code = 200
    mock_response_success.json.return_value = {
        "id": "file-uuid-123",
        "original_name": "test.bin",
        "content_type": "application/octet-stream",
        "size_bytes": 1024,
        "sha256_hex": "abc123",
        "cid": "QmTest123",
        "status": "completed",
        "file_url": "https://example.com/file",
        "created_at": "2025-11-27T12:00:00Z",
        "updated_at": "2025-11-27T12:00:00Z",
    }

    with patch.object(client._client, "post", new_callable=AsyncMock) as mock_post:
        mock_post.side_effect = [mock_response_fail, mock_response_success]

        result = await client.upload_file_and_get_cid(
            file_data=b"test data",
            file_name="test.bin",
            content_type="application/octet-stream",
            account_ss58="5FakeTestAccountAddress123456789012345678901234",
        )

        assert result.cid == "QmTest123"
        assert mock_post.call_count == 2


@pytest.mark.asyncio
async def test_get_file_status_success():
    client = HippiusApiClient()
    mock_response = MagicMock()
    mock_response.status_code = 200
    mock_response.json.return_value = {
        "id": "file-uuid-123",
        "original_name": "test.bin",
        "content_type": "application/octet-stream",
        "size_bytes": 1024,
        "sha256_hex": "abc123",
        "cid": "QmTest123",
        "status": "completed",
        "file_url": "https://example.com/file",
        "created_at": "2025-11-27T12:00:00Z",
        "updated_at": "2025-11-27T12:00:00Z",
    }

    with patch.object(client._client, "get", new_callable=AsyncMock) as mock_get:
        mock_get.return_value = mock_response

        result = await client.get_file_status("file-uuid-123")

        assert isinstance(result, FileStatusResponse)
        assert result.id == "file-uuid-123"
        assert result.cid == "QmTest123"
        assert result.status == "completed"
        assert result.size_bytes == 1024

        mock_get.assert_called_once_with("/storage-control/files/file-uuid-123/", headers=client._get_headers())


@pytest.mark.asyncio
async def test_get_file_status_not_found():
    client = HippiusApiClient()
    mock_response = MagicMock()
    mock_response.status_code = 404
    mock_response.raise_for_status.side_effect = httpx.HTTPStatusError(
        "Not found", request=MagicMock(), response=mock_response
    )

    with patch.object(client._client, "get", new_callable=AsyncMock) as mock_get:
        mock_get.return_value = mock_response

        with pytest.raises(httpx.HTTPStatusError):
            await client.get_file_status("nonexistent-file-id")


@pytest.mark.asyncio
async def test_upload_file_encrypted_data():
    client = HippiusApiClient()
    mock_response = MagicMock()
    mock_response.status_code = 200
    mock_response.json.return_value = {
        "id": "file-uuid-123",
        "original_name": "encrypted.bin",
        "content_type": "application/octet-stream",
        "size_bytes": 2048,
        "sha256_hex": "def456",
        "cid": "QmEncrypted123",
        "status": "completed",
        "file_url": "https://example.com/file",
        "created_at": "2025-11-27T12:00:00Z",
        "updated_at": "2025-11-27T12:00:00Z",
    }

    with patch.object(client._client, "post", new_callable=AsyncMock) as mock_post:
        mock_post.return_value = mock_response

        encrypted_data = b"\x00\x01\x02\x03\x04\x05" * 100
        result = await client.upload_file_and_get_cid(
            file_data=encrypted_data,
            file_name="encrypted.bin",
            content_type="application/octet-stream",
            account_ss58="5FakeTestAccountAddress123456789012345678901234",
        )

        assert result.cid == "QmEncrypted123"
        assert result.size_bytes == 2048


@pytest.mark.asyncio
async def test_upload_file_json_content():
    client = HippiusApiClient()
    mock_response = MagicMock()
    mock_response.status_code = 200
    mock_response.json.return_value = {
        "id": "manifest-uuid-456",
        "original_name": "manifest.json",
        "content_type": "application/json",
        "size_bytes": 512,
        "sha256_hex": "ghi789",
        "cid": "QmManifest456",
        "status": "completed",
        "file_url": "https://example.com/manifest",
        "created_at": "2025-11-27T12:00:00Z",
        "updated_at": "2025-11-27T12:00:00Z",
    }

    with patch.object(client._client, "post", new_callable=AsyncMock) as mock_post:
        mock_post.return_value = mock_response

        manifest_data = b'{"object_id":"123","parts":[]}'
        result = await client.upload_file_and_get_cid(
            file_data=manifest_data,
            file_name="test.manifest",
            content_type="application/json",
            account_ss58="5FakeTestAccountAddress123456789012345678901234",
        )

        assert result.cid == "QmManifest456"
        assert result.original_name == "manifest.json"
        assert result.content_type == "application/json"

        call_args = mock_post.call_args
        files_param = call_args.kwargs["files"]
        file_tuple = files_param["file"]
        assert file_tuple[2] == "application/json"
