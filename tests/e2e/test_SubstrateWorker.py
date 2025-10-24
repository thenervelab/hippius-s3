import asyncio
import os
from typing import Any
from typing import Callable
from unittest.mock import MagicMock
from unittest.mock import patch

import asyncpg
import pytest

from hippius_s3.queue import SubstratePinningRequest
from hippius_s3.workers.substrate import SubstrateWorker
from tests.e2e.conftest import is_real_aws
from tests.e2e.support.cache import get_object_cids
from tests.e2e.support.cache import wait_for_parts_cids


class MockConfig:
    substrate_url = "ws://localhost:9944"
    resubmission_seed_phrase = "test seed phrase here"
    ipfs_store_url = "http://toxiproxy:15001"
    substrate_retry_base_ms = 500
    substrate_retry_max_ms = 5000
    substrate_max_retries = 3
    substrate_call_timeout_seconds = 20.0


@pytest.mark.skipif(
    is_real_aws(),
    reason="Substrate tests only run locally",
)
def test_substrate_worker_batch_call_structure(
    docker_services: Any,
    boto3_client: Any,
    unique_bucket_name: Callable[[str], str],
    cleanup_buckets: Callable[[str], None],
) -> None:
    bucket_name = unique_bucket_name("substrate-test")
    cleanup_buckets(bucket_name)

    boto3_client.create_bucket(Bucket=bucket_name)

    key = "test-file.bin"
    content = os.urandom(6 * 1024 * 1024)

    boto3_client.put_object(
        Bucket=bucket_name,
        Key=key,
        Body=content,
        ContentType="application/octet-stream",
    )

    assert wait_for_parts_cids(
        bucket_name,
        key,
        min_count=1,
        timeout_seconds=20.0,
    )

    captured_batch_call: Any = None
    captured_storage_requests: list[Any] | None = None
    captured_file_lists: list[list[dict[str, str]]] = []
    all_cids: list[str] = []

    async def mock_upload_file_list(
        file_list: list[dict[str, str]],
    ) -> str:
        captured_file_lists.append(file_list)
        return "bafytest123456789"

    async def mock_submit_extrinsic(
        extrinsic: Any,
        max_retries: int = 3,
        timeout_seconds: float = 20.0,
    ) -> Any:
        mock_receipt = MagicMock()
        mock_receipt.is_success = True
        mock_receipt.extrinsic_hash = "0xtest123"
        return mock_receipt

    def mock_compose_call(
        call_module: str,
        call_function: str,
        call_params: dict[str, Any],
    ) -> Any:
        call_obj = MagicMock()
        call_obj.call_module = call_module
        call_obj.call_function = call_function
        call_obj.call_args = call_params
        return call_obj

    def mock_create_signed_extrinsic(
        call: Any = None,
        keypair: Any = None,
    ) -> Any:
        return MagicMock()

    async def run_test() -> None:
        nonlocal captured_batch_call, captured_storage_requests, all_cids

        config = MockConfig()
        db = await asyncpg.connect("postgresql://postgres:postgres@localhost:5432/hippius")

        worker = SubstrateWorker(db, config)

        object_id, object_version, main_account, part_cids, manifest_cid = get_object_cids(bucket_name, key)

        all_cids = part_cids + ([manifest_cid] if manifest_cid else [])

        request = SubstratePinningRequest(
            cids=all_cids,
            address=main_account,
            object_id=object_id,
            object_version=object_version,
        )

        compose_calls = []

        def capturing_compose_call(
            call_module: str,
            call_function: str,
            call_params: dict[str, Any],
        ) -> Any:
            call_obj = mock_compose_call(call_module, call_function, call_params)
            compose_calls.append(call_obj)
            return call_obj

        def mock_connect() -> None:
            mock_substrate = MagicMock()
            mock_substrate.compose_call = capturing_compose_call
            mock_substrate.create_signed_extrinsic = mock_create_signed_extrinsic
            worker.substrate = mock_substrate
            worker.keypair = MagicMock()

        with patch.object(worker, "_upload_file_list_to_ipfs", side_effect=mock_upload_file_list):  # noqa: SIM117
            with patch.object(worker, "_submit_extrinsic_with_retry", side_effect=mock_submit_extrinsic):
                with patch.object(worker, "connect", side_effect=mock_connect):
                    await worker.process_batch([request])

        for call in compose_calls:
            if call.call_module == "Utility" and call.call_function == "batch":
                captured_batch_call = call
            elif call.call_module == "IpfsPallet" and call.call_function == "submit_storage_request_for_user":
                if captured_storage_requests is None:
                    captured_storage_requests = []
                captured_storage_requests.append(call)

        await db.close()

    asyncio.run(run_test())

    assert captured_batch_call is not None, "Batch call was not captured"
    assert captured_storage_requests is not None, "Storage requests were not captured"

    assert captured_batch_call.call_module == "Utility"
    assert captured_batch_call.call_function == "batch"

    assert len(captured_storage_requests) > 0

    for call in captured_storage_requests:
        assert call.call_module == "IpfsPallet"
        assert call.call_function == "submit_storage_request_for_user"

        call_params = call.call_args
        assert "owner" in call_params
        assert "file_inputs" in call_params
        assert len(call_params["file_inputs"]) > 0
        assert "file_hash" in call_params["file_inputs"][0]
        assert "file_name" in call_params["file_inputs"][0]
        assert "miner_ids" in call_params
        assert isinstance(call_params["miner_ids"], list)

    assert len(captured_file_lists) > 0, "No file lists were captured"

    for file_list in captured_file_lists:
        assert len(file_list) > 0, "File list should not be empty"

        for file_entry in file_list:
            assert "cid" in file_entry, "File entry missing 'cid' field"
            assert "filename" in file_entry, "File entry missing 'filename' field"

            cid = file_entry["cid"]
            filename = file_entry["filename"]

            assert isinstance(cid, str), f"CID should be string, got {type(cid)}"
            assert cid.startswith("baf") or cid.startswith("Qm"), f"CID should be valid IPFS CID, got {cid}"
            assert len(cid) > 10, f"CID should be at least 10 characters, got {len(cid)}"

            assert filename == f"s3-{cid}", f"Filename should be 's3-{{cid}}', got {filename}"

    all_file_list_cids = [entry["cid"] for file_list in captured_file_lists for entry in file_list]
    for expected_cid in all_cids:
        assert expected_cid in all_file_list_cids, f"Expected CID {expected_cid} not found in file lists"
