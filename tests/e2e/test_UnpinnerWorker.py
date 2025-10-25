import asyncio
import json
import os
from typing import Any
from typing import Callable
from unittest.mock import AsyncMock
from unittest.mock import MagicMock
from unittest.mock import patch

import pytest
import redis.asyncio as async_redis

from hippius_s3.queue import dequeue_unpin_request
from tests.e2e.conftest import is_real_aws
from tests.e2e.support.cache import get_object_cids
from tests.e2e.support.cache import wait_for_parts_cids


@pytest.mark.skipif(
    is_real_aws(),
    reason="Unpinner tests only run locally",
)
def test_unpinner_worker_manifest_structure(
    docker_services: Any,
    boto3_client: Any,
    unique_bucket_name: Callable[[str], str],
    cleanup_buckets: Callable[[str], None],
) -> None:
    import subprocess

    subprocess.run(
        ["docker", "compose", "-f", "docker-compose.yml", "-f", "docker-compose.e2e.yml", "stop", "unpinner"],
        env={**os.environ, "COMPOSE_PROJECT_NAME": "hippius-e2e"},
        check=True,
        capture_output=True,
    )

    bucket_name = unique_bucket_name("unpinner-test")
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
        min_count=2,
        timeout_seconds=60.0,
    )

    captured_manifest: list[dict[str, str]] | None = None
    captured_cancel_cid: str | None = None
    captured_seed_phrase: str | None = None

    async def run_test() -> None:
        nonlocal captured_manifest, captured_cancel_cid, captured_seed_phrase

        object_id, object_version, main_account, part_cids, manifest_cid = get_object_cids(bucket_name, key)
        all_cids = list(set(part_cids + ([manifest_cid] if manifest_cid else [])))
        assert len(all_cids) > 0, "No CIDs found for object"

        boto3_client.delete_object(Bucket=bucket_name, Key=key)

        await asyncio.sleep(5)

        redis_client = async_redis.from_url("redis://localhost:6379/0")

        unpin_requests = []
        max_attempts = len(all_cids) + 1
        for _ in range(max_attempts):
            unpin_request = await dequeue_unpin_request(redis_client)
            if unpin_request is None:
                break
            unpin_requests.append(unpin_request)

        assert len(unpin_requests) > 0, f"No unpin requests were enqueued (expected {len(all_cids)})"
        assert len(unpin_requests) == len(all_cids), f"Expected {len(all_cids)} requests, got {len(unpin_requests)}"

        dequeued_cids = [req.cid for req in unpin_requests]
        for expected_cid in all_cids:
            assert expected_cid in dequeued_cids, f"Expected CID {expected_cid} not found in dequeued requests"

        async def mock_httpx_post(
            url: str,
            files: dict[str, Any] | None = None,
            **_: Any,
        ) -> AsyncMock:
            nonlocal captured_manifest

            mock_response = AsyncMock()
            mock_response.status_code = 200
            mock_response.url = url

            if "/api/v0/add" in url and files:
                file_tuple = files["file"]
                file_data = file_tuple[1]
                manifest_json = file_data.decode("utf-8") if isinstance(file_data, bytes) else file_data
                captured_manifest = json.loads(manifest_json)

                mock_response.text = '{"Hash":"bafytestmanifest123456789"}\n'
            elif "/api/v0/pin/add" in url:
                mock_response.text = '{"Pins":["bafytestmanifest123456789"]}'
            else:
                mock_response.text = "{}"

            mock_response.raise_for_status = lambda: None
            return mock_response

        async def mock_cancel_storage_request(
            cid: str,
            seed_phrase: str,
        ) -> str:
            nonlocal captured_cancel_cid, captured_seed_phrase
            captured_cancel_cid = cid
            captured_seed_phrase = seed_phrase
            return "0xtest_transaction_hash_123"

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
                assert success, "process_unpin_request returned False"

        await redis_client.close()

        assert captured_manifest is not None, "Manifest was not captured"
        assert len(captured_manifest) > 0, "Manifest is empty"

        for entry in captured_manifest:
            assert "cid" in entry, "Manifest entry missing 'cid' field"
            assert "filename" in entry, "Manifest entry missing 'filename' field"

            cid = entry["cid"]
            filename = entry["filename"]

            assert isinstance(cid, str), f"CID should be string, got {type(cid)}"
            assert cid.startswith("baf") or cid.startswith("Qm"), f"CID should be valid IPFS CID, got {cid}"
            assert filename == f"s3-{cid}", f"Filename should be 's3-{{cid}}', got {filename}"

        manifest_cids = [entry["cid"] for entry in captured_manifest]
        for expected_cid in all_cids:
            assert expected_cid in manifest_cids, f"Expected CID {expected_cid} not found in manifest"

        assert captured_cancel_cid == "bafytestmanifest123456789", "cancel_storage_request not called with manifest CID"
        assert captured_seed_phrase is not None, "seed_phrase not passed to cancel_storage_request"

    asyncio.run(run_test())
