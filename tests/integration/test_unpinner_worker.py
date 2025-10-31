import json
from typing import Any
from unittest.mock import AsyncMock
from unittest.mock import MagicMock
from unittest.mock import patch

import pytest

from hippius_s3.queue import UnpinChainRequest


@pytest.mark.asyncio
async def test_unpinner_worker_manifest_structure() -> None:
    """Test that unpinner creates correct manifest structure for IPFS."""
    fake_cids = ["bafytest111", "bafytest222", "bafytest333"]
    user_address = "5TestUserAddress789"

    unpin_requests = [
        UnpinChainRequest(
            address=user_address,
            object_id="test-obj-789",
            object_version=1,
            cid=cid,
        )
        for cid in fake_cids
    ]

    captured_manifest: list[dict[str, str]] | None = None
    captured_cancel_cid: str | None = None

    async def mock_httpx_post(url: str, files: dict[str, Any] | None = None, **_: Any) -> AsyncMock:
        nonlocal captured_manifest

        mock_response = AsyncMock()
        mock_response.status_code = 200
        mock_response.url = url

        if "/api/v0/add" in url and files:
            file_tuple = files["file"]
            file_data = file_tuple[1]
            manifest_json = file_data.decode("utf-8") if isinstance(file_data, bytes) else file_data
            captured_manifest = json.loads(manifest_json)
            mock_response.text = '{"Hash":"bafymanifest123456789"}\n'
        elif "/api/v0/pin/add" in url:
            mock_response.text = '{"Pins":["bafymanifest123456789"]}'
        elif "/api/v0/pin/rm" in url:
            mock_response.text = '{"Pins":["removed_cid"]}'
        else:
            mock_response.text = "{}"

        mock_response.raise_for_status = lambda: None
        return mock_response

    async def mock_cancel_storage_request(cid: str, seed_phrase: str) -> str:
        nonlocal captured_cancel_cid
        captured_cancel_cid = cid
        return "0xtest_transaction_hash"

    mock_substrate_client = MagicMock()
    mock_substrate_client.cancel_storage_request = mock_cancel_storage_request

    with patch("httpx.AsyncClient") as mock_client_class:
        mock_client_instance = AsyncMock()
        mock_client_instance.post = mock_httpx_post
        mock_client_instance.__aenter__ = AsyncMock(return_value=mock_client_instance)
        mock_client_instance.__aexit__ = AsyncMock(return_value=None)
        mock_client_class.return_value = mock_client_instance

        with patch("workers.run_unpinner_in_loop.SubstrateClient", return_value=mock_substrate_client):
            from workers.run_unpinner_in_loop import process_unpin_request

            success = await process_unpin_request(unpin_requests)
            assert success, "process_unpin_request should return True"

    assert captured_manifest is not None, "Manifest was not captured"
    assert len(captured_manifest) == len(fake_cids), f"Manifest should have {len(fake_cids)} entries"

    for i, entry in enumerate(captured_manifest):
        assert "cid" in entry, "Manifest entry must have 'cid' field"
        assert "filename" in entry, "Manifest entry must have 'filename' field"

        cid = entry["cid"]
        filename = entry["filename"]

        assert cid == fake_cids[i], f"CID should match: expected {fake_cids[i]}, got {cid}"
        assert filename == f"s3-{cid}", f"Filename should be 's3-{{cid}}', got {filename}"

    assert captured_cancel_cid == "bafymanifest123456789", "cancel_storage_request should be called with manifest CID"


@pytest.mark.asyncio
async def test_unpinner_worker_handles_empty_cids() -> None:
    """Test that unpinner handles requests with empty CIDs gracefully."""
    unpin_requests = [
        UnpinChainRequest(
            address="5TestUser",
            object_id="test-obj-empty",
            object_version=1,
            cid="",
        )
    ]

    with patch("workers.run_unpinner_in_loop.SubstrateClient"):
        from workers.run_unpinner_in_loop import process_unpin_request

        success = await process_unpin_request(unpin_requests)
        assert success, "Should handle empty CIDs gracefully and return True"
