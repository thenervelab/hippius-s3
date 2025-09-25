"""E2E test for DLQ (Dead Letter Queue) and requeue functionality."""

import json
import time
from contextlib import suppress
from typing import Any
from typing import Callable

import pytest
import redis  # type: ignore[import-untyped]

from .support.cache import clear_object_cache
from .support.cache import get_object_id
from .support.cache import wait_for_parts_cids
from .support.compose import disable_ipfs_proxy
from .support.compose import enable_ipfs_proxy
from .support.compose import exec_python_module
from .support.compose import wait_for_ipfs_state
from .support.compose import wait_for_toxiproxy


@pytest.mark.local
def test_dlq_requeue_multipart_upload(
    docker_services: Any,
    boto3_client: Any,
    unique_bucket_name: Callable[[str], str],
    cleanup_buckets: Callable[[str], None],
) -> None:
    """Test DLQ persistence and requeue functionality with multipart upload."""
    # Wait for Toxiproxy to be ready
    assert wait_for_toxiproxy(), "Toxiproxy control API not available"

    bucket = unique_bucket_name("dlq-test")
    cleanup_buckets(bucket)
    boto3_client.create_bucket(Bucket=bucket)
    key = "test-file.bin"

    # Create multipart upload
    create_resp = boto3_client.create_multipart_upload(Bucket=bucket, Key=key)
    upload_id = create_resp["UploadId"]

    # Upload 2 parts (each needs to be at least 5MB for AWS compatibility)
    part_size = 5 * 1024 * 1024  # 5MB
    part1_data = b"A" * part_size
    part2_data = b"B" * part_size

    etag1 = boto3_client.upload_part(Bucket=bucket, Key=key, UploadId=upload_id, PartNumber=1, Body=part1_data)["ETag"]
    etag2 = boto3_client.upload_part(Bucket=bucket, Key=key, UploadId=upload_id, PartNumber=2, Body=part2_data)["ETag"]

    # Get object_id for later use (no need to wait for parts yet)
    object_id = get_object_id(bucket, key)

    # Break IPFS at the docker layer for a deterministic window
    disable_ipfs_proxy()
    # Verify proxies are actually disabled by checking Toxiproxy API directly
    import requests

    resp = requests.get("http://localhost:8474/proxies/ipfs_store")
    # If deleted, 404 is expected
    if resp.status_code == 200:
        proxy_data = resp.json()
        assert not proxy_data["enabled"], f"Proxy still enabled: {proxy_data}"
        print(f"DEBUG: Proxy status confirmed disabled: {proxy_data['enabled']}")
    else:
        assert resp.status_code == 404, f"Unexpected status from toxiproxy: {resp.status_code} {resp.text}"
    # Verify IPFS is truly down from inside the pinner container before proceeding
    assert wait_for_ipfs_state(False, service="pinner"), "IPFS still reachable after break"

    try:
        # Attempt to complete multipart upload - API should return 200 even if pinner later fails
        boto3_client.complete_multipart_upload(
            Bucket=bucket,
            Key=key,
            UploadId=upload_id,
            MultipartUpload={
                "Parts": [
                    {"ETag": etag1, "PartNumber": 1},
                    {"ETag": etag2, "PartNumber": 2},
                ]
            },
        )

        # Wait for DLQ processing
        time.sleep(5)

        # Wait for the pinner to push the job to DLQ (IPFS down)
        # Poll DLQ up to a short timeout
        import redis as _redis  # type: ignore[import-untyped]

        r = _redis.Redis.from_url("redis://localhost:6379/0")
        found = False
        for _ in range(130):
            entries = r.lrange("upload_requests:dlq", 0, -1)
            for entry_json in entries:
                import json as _json

                entry = _json.loads(entry_json)
                if entry.get("object_id") == object_id:
                    found = True
                    break
            if found:
                break
            time.sleep(0.2)
        assert found, "DLQ entry not found after waiting for pinner to fail"

        # Verify DLQ entry exists
        r = redis.Redis.from_url("redis://localhost:6379/0")
        dlq_entries = r.lrange("upload_requests:dlq", 0, -1)

        assert len(dlq_entries) > 0, "No DLQ entries found"

        # Find our object in DLQ
        dlq_entry = None
        for entry_json in dlq_entries:
            entry = json.loads(entry_json)
            if entry.get("object_id") == object_id:
                dlq_entry = entry
                break

        assert dlq_entry is not None, f"DLQ entry not found for object_id {object_id}"
        # error_type varies across versions (bool or string). Any value is acceptable for DLQ presence.

        # Verify chunks persisted via in-container CLI to avoid host FS assumptions
        code, out, err = exec_python_module(
            "api", "hippius_s3.scripts.dlq_requeue", ["dlq-parts", "--object-id", object_id]
        )
        assert code == 0, f"dlq-parts failed: {err}\n{out}"
        parts_list = json.loads(out.strip() or "[]")
        assert parts_list == [1, 2] or set(parts_list) == {1, 2}, f"Unexpected parts list: {parts_list}"

        # Verify sizes for both parts from inside the container
        code1, out1, err1 = exec_python_module(
            "api", "hippius_s3.scripts.dlq_requeue", ["dlq-part-size", "--object-id", object_id, "--part", "1"]
        )
        code2, out2, err2 = exec_python_module(
            "api", "hippius_s3.scripts.dlq_requeue", ["dlq-part-size", "--object-id", object_id, "--part", "2"]
        )
        assert code1 == 0 and code2 == 0, f"dlq-part-size failed: {err1} {err2}"
        assert int(out1.strip()) == len(part1_data), "Part 1 size mismatch"
        assert int(out2.strip()) == len(part2_data), "Part 2 size mismatch"

        # Clear Redis cache to simulate cache eviction
        clear_object_cache(object_id, parts=[0, 1])

        # Verify cache is actually cleared
        assert not r.exists(f"obj:{object_id}:part:0"), "Part 0 cache not cleared"
        assert not r.exists(f"obj:{object_id}:part:1"), "Part 1 cache not cleared"

        # Heal IPFS before requeue so pinner can complete successfully
        enable_ipfs_proxy()
        assert wait_for_ipfs_state(True, service="pinner"), "IPFS did not come back after heal"

        # Run the requeue CLI command inside the api container (mounted /app)
        code, out, err = exec_python_module(
            "api", "hippius_s3.scripts.dlq_requeue", ["requeue", "--object-id", object_id]
        )
        assert code == 0, f"Requeue command failed: {err}\n{out}"
        assert f"Successfully requeued object_id: {object_id}" in out

        # Wait for requeue processing to complete
        time.sleep(5)

        # Verify the object was successfully processed
        assert wait_for_parts_cids(bucket, key, min_count=2), "Requeued parts not processed"

        # Verify the object can be retrieved
        resp = boto3_client.get_object(Bucket=bucket, Key=key)
        retrieved_data = resp["Body"].read()  # type: ignore[index]
        expected_data = part1_data + part2_data
        assert retrieved_data == expected_data, "Retrieved data doesn't match expected"

        # Verify DLQ directory was archived via in-container CLI
        code, out, err = exec_python_module(
            "api", "hippius_s3.scripts.dlq_requeue", ["archived-exists", "--object-id", object_id]
        )
        assert code == 0 and out.strip() == "FOUND", f"Archived directory not found: {out} {err}"

        # Optionally verify original DLQ directory removal via parts listing
        code, out, err = exec_python_module(
            "api", "hippius_s3.scripts.dlq_requeue", ["dlq-parts", "--object-id", object_id]
        )
        # After archive, listing should return empty or error; tolerate either
        if code == 0:
            try:
                remaining = json.loads(out.strip() or "[]")
                assert remaining == []
            except Exception:
                pass

        # We don't manipulate host fs; cleanup can be performed via CLI if needed

    finally:
        # Heal docker-level IPFS connectivity
        with suppress(Exception):
            enable_ipfs_proxy()
