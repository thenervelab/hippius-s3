"""E2E test for AbortMultipartUpload (DELETE with uploadId)."""

import time
from typing import Any
from typing import Callable

import pytest

from .support.db import count_residency_rows
from .support.db import get_multipart_upload_version


# docker-compose.e2e.yml sets NODE_NAME (api) and CEPHOR_NODE_ID (drain-agent) to this.
E2E_NODE = "e2e-node"


def test_abort_multipart_upload_deletes_upload(
    docker_services: Any,
    boto3_client: Any,
    unique_bucket_name: Callable[[str], str],
    cleanup_buckets: Callable[[str], None],
) -> None:
    bucket = unique_bucket_name("mpu-abort")
    cleanup_buckets(bucket)
    boto3_client.create_bucket(Bucket=bucket)
    key = "large.bin"

    create = boto3_client.create_multipart_upload(Bucket=bucket, Key=key, ContentType="application/octet-stream")
    upload_id = create["UploadId"]

    # Upload a part to ensure existence
    boto3_client.upload_part(Bucket=bucket, Key=key, UploadId=upload_id, PartNumber=1, Body=b"a" * 1024)

    # Abort
    resp = boto3_client.abort_multipart_upload(Bucket=bucket, Key=key, UploadId=upload_id)
    assert resp["ResponseMetadata"]["HTTPStatusCode"] in (200, 204)


def _wait_for_residency_rows(object_id: str, object_version: int, expected: int, *, timeout_s: float = 30.0) -> None:
    """The drain-agent claims a landed part off the `cephor:landed:{node}` queue and records it
    in cephor_ssd_residency; that is asynchronous to UploadPart returning. Wait for EVERY part:
    the drain writes residency as its own statement just before it commits, so a part still
    mid-copy at abort time would land its row after the abort's delete and fail the assertion
    below for a reason that has nothing to do with the abort."""
    deadline = time.monotonic() + timeout_s
    while time.monotonic() < deadline:
        if count_residency_rows(E2E_NODE, object_id, object_version) >= expected:
            return
        time.sleep(0.25)
    raise AssertionError(
        f"drain never recorded {expected} residency rows for {object_id} v{object_version} on {E2E_NODE}"
    )


@pytest.mark.local
def test_abort_multipart_upload_drops_this_nodes_residency_rows(
    docker_services: Any,
    boto3_client: Any,
    unique_bucket_name: Callable[[str], str],
    cleanup_buckets: Callable[[str], None],
) -> None:
    """Abort rmtree's the version on the ingest node's SSD; the drain's per-node ledger rows for
    it must go with the directory, or node_cache_bytes counts bytes the disk no longer holds."""
    bucket = unique_bucket_name("mpu-abort-residency")
    cleanup_buckets(bucket)
    boto3_client.create_bucket(Bucket=bucket)
    key = "large.bin"

    create = boto3_client.create_multipart_upload(Bucket=bucket, Key=key, ContentType="application/octet-stream")
    upload_id = create["UploadId"]
    boto3_client.upload_part(Bucket=bucket, Key=key, UploadId=upload_id, PartNumber=1, Body=b"a" * 1024)
    boto3_client.upload_part(Bucket=bucket, Key=key, UploadId=upload_id, PartNumber=2, Body=b"b" * 1024)

    object_id, object_version = get_multipart_upload_version(upload_id)
    _wait_for_residency_rows(object_id, object_version, expected=2)

    resp = boto3_client.abort_multipart_upload(Bucket=bucket, Key=key, UploadId=upload_id)
    assert resp["ResponseMetadata"]["HTTPStatusCode"] in (200, 204)

    # The fresh-part hints are NOT asserted here: the e2e api runs without
    # HIPPIUS_PEER_FETCH_ENABLED / POD_IP, so no PeerRegistry is installed and UploadPart never
    # writes a hint to forget. Their removal is pinned by the unit tests in test_peer_fetch.py.
    assert count_residency_rows(E2E_NODE, object_id, object_version) == 0
