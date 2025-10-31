from typing import Any
from unittest.mock import MagicMock
from unittest.mock import patch

import pytest

from hippius_s3.queue import UnpinChainRequest
from hippius_s3.workers.unpinner import UnpinnerWorker


class MockConfig:
    substrate_url = "ws://localhost:9944"
    resubmission_seed_phrase = "test seed phrase here"
    ipfs_store_url = "http://ipfs:5001"
    substrate_retry_base_ms = 500
    substrate_retry_max_ms = 5000
    substrate_max_retries = 3
    substrate_call_timeout_seconds = 20.0


@pytest.mark.asyncio
async def test_unpinner_worker_batch_call_params() -> None:
    fake_cids = ["bafytest111", "bafytest222"]
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

    manifest_cid = "bafymanifest999"
    manifest_cids = {user_address: manifest_cid}

    captured_compose_calls: list[dict[str, Any]] = []

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

    worker = UnpinnerWorker(config)

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

    with patch.object(worker, "_submit_extrinsic_with_retry", side_effect=mock_submit_extrinsic):  # noqa: SIM117
        with patch.object(worker, "connect", side_effect=mock_connect):
            await worker.process_batch(unpin_requests, manifest_cids)

    unpin_calls = [c for c in captured_compose_calls if c["function"] == "storage_unpin_request"]
    batch_calls = [c for c in captured_compose_calls if c["function"] == "batch"]

    assert len(unpin_calls) >= 1, "Should have at least one unpin request call"
    assert len(batch_calls) == 1, "Should have exactly one batch call"

    unpin_call = unpin_calls[0]
    assert unpin_call["module"] == "Marketplace"

    params = unpin_call["params"]
    assert "file_hash" in params, "Unpin request must have 'file_hash' param"
    assert params["file_hash"] == manifest_cid, f"file_hash should be {manifest_cid}"

    batch_call = batch_calls[0]
    assert batch_call["module"] == "Utility"
    assert "calls" in batch_call["params"], "Batch call must have 'calls' param"


@pytest.mark.asyncio
async def test_unpinner_worker_multiple_users_batching() -> None:
    user1_address = "5TestUser1"
    user2_address = "5TestUser2"

    unpin_requests = [
        UnpinChainRequest(
            address=user1_address,
            object_id="obj1",
            object_version=1,
            cid="bafytest1",
        ),
        UnpinChainRequest(
            address=user2_address,
            object_id="obj2",
            object_version=1,
            cid="bafytest2",
        ),
    ]

    manifest_cids = {
        user1_address: "bafymanifest111",
        user2_address: "bafymanifest222",
    }

    captured_compose_calls: list[dict[str, Any]] = []

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
    worker = UnpinnerWorker(config)

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
        mock_receipt.extrinsic_hash = "0xtest_multi"
        return mock_receipt

    with patch.object(worker, "_submit_extrinsic_with_retry", side_effect=mock_submit_extrinsic):  # noqa: SIM117
        with patch.object(worker, "connect", side_effect=mock_connect):
            await worker.process_batch(unpin_requests, manifest_cids)

    unpin_calls = [c for c in captured_compose_calls if c["function"] == "storage_unpin_request"]
    batch_calls = [c for c in captured_compose_calls if c["function"] == "batch"]

    assert len(unpin_calls) == 2, "Should have two unpin calls (one per user)"
    assert len(batch_calls) == 1, "Should have exactly one batch call"

    manifest_hashes = [call["params"]["file_hash"] for call in unpin_calls]
    assert "bafymanifest111" in manifest_hashes, "Should include manifest for user1"
    assert "bafymanifest222" in manifest_hashes, "Should include manifest for user2"
