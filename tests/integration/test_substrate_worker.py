from typing import Any
from unittest.mock import AsyncMock
from unittest.mock import MagicMock
from unittest.mock import patch

import pytest

from hippius_s3.queue import SubstratePinningRequest
from hippius_s3.workers.substrate import SubstrateWorker


class MockConfig:
    substrate_url = "ws://localhost:9944"
    resubmission_seed_phrase = "test seed phrase here"
    ipfs_store_url = "http://ipfs:5001"
    substrate_retry_base_ms = 500
    substrate_retry_max_ms = 5000
    substrate_max_retries = 3
    substrate_call_timeout_seconds = 20.0


@pytest.mark.asyncio
async def test_substrate_worker_file_list_structure() -> None:
    """Test that SubstrateWorker creates correct file list structure."""
    fake_cids = ["bafytest123", "bafytest456", "bafytest789"]

    request = SubstratePinningRequest(
        cids=fake_cids,
        address="5TestUserAddress123",
        object_id="test-obj-123",
        object_version=1,
    )

    captured_file_lists: list[list[dict[str, str]]] = []

    async def mock_upload_file_list(file_list: list[dict[str, str]]) -> str:
        captured_file_lists.append(file_list)
        return "bafymanifest123456789"

    config = MockConfig()
    mock_db = AsyncMock()
    mock_db.execute = AsyncMock()

    worker = SubstrateWorker(mock_db, config)

    def mock_connect() -> None:
        mock_substrate = MagicMock()
        mock_substrate.compose_call = MagicMock(return_value=MagicMock())
        mock_substrate.create_signed_extrinsic = MagicMock(return_value=MagicMock())
        worker.substrate = mock_substrate
        worker.keypair = MagicMock()

    async def mock_submit_extrinsic(
        extrinsic: Any,
        max_retries: int = 3,
        timeout_seconds: float = 20.0,
    ) -> Any:
        mock_receipt = MagicMock()
        mock_receipt.is_success = True
        mock_receipt.extrinsic_hash = "0xtest123"
        return mock_receipt

    with patch.object(worker, "_upload_file_list_to_ipfs", side_effect=mock_upload_file_list):  # noqa: SIM117
        with patch.object(worker, "_submit_extrinsic_with_retry", side_effect=mock_submit_extrinsic):
            with patch.object(worker, "connect", side_effect=mock_connect):
                await worker.process_batch([request])

    assert len(captured_file_lists) == 1, "Should have captured one file list"
    file_list = captured_file_lists[0]

    assert len(file_list) == len(fake_cids), f"File list should have {len(fake_cids)} entries"

    for i, entry in enumerate(file_list):
        assert "cid" in entry, "File list entry must have 'cid' field"
        assert "filename" in entry, "File list entry must have 'filename' field"

        cid = entry["cid"]
        filename = entry["filename"]

        assert cid == fake_cids[i], f"CID should match: expected {fake_cids[i]}, got {cid}"
        assert filename == f"s3-{cid}", f"Filename should be 's3-{{cid}}', got {filename}"


@pytest.mark.asyncio
async def test_substrate_worker_batch_call_params() -> None:
    """Test that SubstrateWorker creates correct batch call parameters."""
    fake_cids = ["bafytest123", "bafytest456"]
    user_address = "5TestUserAddress456"

    request = SubstratePinningRequest(
        cids=fake_cids,
        address=user_address,
        object_id="test-obj-456",
        object_version=1,
    )

    captured_compose_calls: list[dict[str, Any]] = []

    async def mock_upload_file_list(
        file_list: list[dict[str, str]],
    ) -> str:
        return "bafymanifest999"

    def capturing_compose_call(call_module: str, call_function: str, call_params: dict[str, Any]) -> Any:
        captured_compose_calls.append(
            {
                "module": call_module,
                "function": call_function,
                "params": call_params,
            }
        )
        return MagicMock()

    config = MockConfig()
    mock_db = AsyncMock()
    mock_db.execute = AsyncMock()

    worker = SubstrateWorker(mock_db, config)

    def mock_connect() -> None:
        mock_substrate = MagicMock()
        mock_substrate.compose_call = capturing_compose_call
        mock_substrate.create_signed_extrinsic = MagicMock(return_value=MagicMock())
        worker.substrate = mock_substrate
        worker.keypair = MagicMock()

    async def mock_submit_extrinsic(
        extrinsic: Any,
        max_retries: int = 3,
        timeout_seconds: float = 20.0,
    ) -> Any:
        mock_receipt = MagicMock()
        mock_receipt.is_success = True
        mock_receipt.extrinsic_hash = "0xtest789"
        return mock_receipt

    with patch.object(worker, "_upload_file_list_to_ipfs", side_effect=mock_upload_file_list):  # noqa: SIM117
        with patch.object(worker, "_submit_extrinsic_with_retry", side_effect=mock_submit_extrinsic):
            with patch.object(worker, "connect", side_effect=mock_connect):
                await worker.process_batch([request])

    storage_request_calls = [c for c in captured_compose_calls if c["function"] == "submit_storage_request_for_user"]
    batch_calls = [c for c in captured_compose_calls if c["function"] == "batch"]

    assert len(storage_request_calls) >= 1, "Should have at least one storage request call"
    assert len(batch_calls) == 1, "Should have exactly one batch call"

    storage_call = storage_request_calls[0]
    assert storage_call["module"] == "IpfsPallet"

    params = storage_call["params"]
    assert "owner" in params, "Storage request must have 'owner' param"
    assert params["owner"] == user_address, f"Owner should be {user_address}"

    assert "file_inputs" in params, "Storage request must have 'file_inputs' param"
    assert isinstance(params["file_inputs"], list), "file_inputs must be a list"
    assert len(params["file_inputs"]) > 0, "file_inputs must not be empty"

    file_input = params["file_inputs"][0]
    assert "file_hash" in file_input, "file_input must have 'file_hash'"
    assert "file_name" in file_input, "file_input must have 'file_name'"
    assert file_input["file_hash"] == "bafymanifest999", "file_hash should be the manifest CID"

    assert "miner_ids" in params, "Storage request must have 'miner_ids' param"
    assert isinstance(params["miner_ids"], list), "miner_ids must be a list"

    batch_call = batch_calls[0]
    assert batch_call["module"] == "Utility"
    assert "calls" in batch_call["params"], "Batch call must have 'calls' param"
