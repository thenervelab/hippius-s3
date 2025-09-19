import time
import uuid
from typing import Any

import pytest

from tests.e2e.support.manifest import fetch_ipfs_json
from tests.e2e.support.manifest import wait_for_manifest_cid


@pytest.mark.local
@pytest.mark.s4
def test_manifest_builder_publishes_after_stabilization(boto3_client: Any, unique_bucket_name: Any) -> None:
    bucket = unique_bucket_name("manifest")
    key = f"obj-{uuid.uuid4().hex[:8]}"

    boto3_client.create_bucket(Bucket=bucket)

    base = b"A" * (1024 * 1024)
    boto3_client.put_object(Bucket=bucket, Key=key, Body=base)

    head = boto3_client.head_object(Bucket=bucket, Key=key)
    version = head.get("Metadata", {}).get("append-version", "0")
    delta = b"B" * (1024 * 1024)
    boto3_client.put_object(
        Bucket=bucket,
        Key=key,
        Body=delta,
        Metadata={
            "append": "true",
            "append-if-version": str(version),
            "append-id": str(uuid.uuid4()),
        },
    )

    # Wait for short stabilization window (5s configured) and manifest builder cycle
    time.sleep(2)
    manifest_cid = wait_for_manifest_cid(bucket, key, timeout_sec=30.0, interval=1.0)
    assert manifest_cid, "manifest_cid was not published in time"

    manifest = fetch_ipfs_json(manifest_cid)
    assert manifest.get("append_version") is not None
    assert manifest.get("total_size") == len(base) + len(delta)
    parts = manifest.get("parts") or []
    assert len(parts) >= 2
