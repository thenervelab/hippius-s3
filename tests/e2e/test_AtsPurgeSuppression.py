"""E2E for the ATS purge fan-out: creations skip the PURGE, overwrites and deletes still fire it.

The e2e api points ATS_CACHE_ENDPOINT at mock-arion, which records every PURGE it receives
(mock_arion_api.py /debug/purges). Purge delivery is fire-and-forget from the gateway, so the
positive assertions poll with a deadline and the negative one waits out a grace window.
"""

import os
import time
from typing import Any
from typing import Callable

import httpx
import pytest


pytestmark = pytest.mark.local

MOCK_ARION_URL = os.environ.get("MOCK_ARION_URL", "http://localhost:8002")

# Fire-and-forget: the PURGE task is created before the response leaves the gateway, so on a
# healthy stack it lands in single-digit milliseconds. The windows are sized for CI jitter.
PURGE_WAIT_S = 5.0
NO_PURGE_GRACE_S = 1.5


def _purges_for(path: str) -> list[dict]:
    resp = httpx.get(f"{MOCK_ARION_URL}/debug/purges", timeout=5.0)
    resp.raise_for_status()
    return [p for p in resp.json() if p["path"] == path]


def _wait_for_purge_count(path: str, count: int) -> list[dict]:
    deadline = time.monotonic() + PURGE_WAIT_S
    purges: list[dict] = []
    while time.monotonic() < deadline:
        purges = _purges_for(path)
        if len(purges) >= count:
            return purges
        time.sleep(0.1)
    return purges


def test_create_skips_purge_overwrite_and_delete_fire_it(
    docker_services: Any,
    boto3_client: Any,
    unique_bucket_name: Callable[[str], str],
    cleanup_buckets: Callable[[str], None],
) -> None:
    bucket_name = unique_bucket_name("ats-purge")
    cleanup_buckets(bucket_name)
    boto3_client.create_bucket(Bucket=bucket_name)

    key = "purge-flow.txt"
    purge_path = f"/{bucket_name}/{key}"

    # 1. Creation: the key never existed, so there is nothing cached to purge. The middleware
    # must skip the fan-out (put_object_endpoint sets ats_object_created on version 1).
    put1 = boto3_client.put_object(Bucket=bucket_name, Key=key, Body=b"v1", ContentType="text/plain")
    assert put1.get("VersionId") == "1", "first PUT of a fresh key must allocate version 1"
    time.sleep(NO_PURGE_GRACE_S)
    assert _purges_for(purge_path) == [], "creating a brand-new object must not fire a PURGE"

    # 2. Overwrite: a stale cache entry may exist, purge must fire exactly as before.
    # >= rather than ==: botocore silently retries a write whose response was lost, and each
    # server-side success legitimately fires its own purge.
    put2 = boto3_client.put_object(Bucket=bucket_name, Key=key, Body=b"v2", ContentType="text/plain")
    assert int(put2["VersionId"]) >= 2, "overwrite must allocate a version > 1"
    overwrite_purges = len(_wait_for_purge_count(purge_path, 1))
    assert overwrite_purges >= 1, "overwrite must fire a PURGE"

    # 3. Delete: same contract.
    boto3_client.delete_object(Bucket=bucket_name, Key=key)
    purges = _wait_for_purge_count(purge_path, overwrite_purges + 1)
    assert len(purges) >= overwrite_purges + 1, f"delete must fire a PURGE, saw {purges}"


def test_recreate_after_delete_still_skips_purge(
    docker_services: Any,
    boto3_client: Any,
    unique_bucket_name: Callable[[str], str],
    cleanup_buckets: Callable[[str], None],
) -> None:
    """A re-PUT of a (soft-)deleted key revives the surviving objects row at version >= 2, so it
    does NOT count as a creation and the purge fires. Covers the ordinary delete-then-recreate
    lifecycle; the row-removed corners (bucket name reuse, janitor hard-delete) are handled by the
    warm-bucket exclusion in the middleware and bounded by the 5-min TTL otherwise."""
    bucket_name = unique_bucket_name("ats-repurge")
    cleanup_buckets(bucket_name)
    boto3_client.create_bucket(Bucket=bucket_name)

    key = "deleted-then-recreated.txt"
    purge_path = f"/{bucket_name}/{key}"

    boto3_client.put_object(Bucket=bucket_name, Key=key, Body=b"v1")
    boto3_client.delete_object(Bucket=bucket_name, Key=key)
    baseline = len(_wait_for_purge_count(purge_path, 1))
    assert baseline >= 1, "delete must fire a PURGE"

    put = boto3_client.put_object(Bucket=bucket_name, Key=key, Body=b"reborn")
    assert int(put["VersionId"]) >= 2, "re-PUT of a deleted key must NOT reuse version 1"
    purges = _wait_for_purge_count(purge_path, baseline + 1)
    assert len(purges) >= baseline + 1, "re-PUT of a previously deleted key must still purge"


def _mpu(boto3_client: Any, bucket: str, key: str, marker: bytes) -> str:
    """Smallest legal MPU: one 5 MiB part (only the LAST part may be under the minimum)."""
    upload_id = boto3_client.create_multipart_upload(Bucket=bucket, Key=key)["UploadId"]
    etag = boto3_client.upload_part(
        Bucket=bucket, Key=key, UploadId=upload_id, PartNumber=1, Body=marker * (5 * 1024 * 1024)
    )["ETag"]
    completed = boto3_client.complete_multipart_upload(
        Bucket=bucket,
        Key=key,
        UploadId=upload_id,
        MultipartUpload={"Parts": [{"ETag": etag, "PartNumber": 1}]},
    )
    return str(completed.get("VersionId", ""))


def test_mpu_create_skips_purge_mpu_overwrite_fires_it(
    docker_services: Any,
    boto3_client: Any,
    unique_bucket_name: Callable[[str], str],
    cleanup_buckets: Callable[[str], None],
) -> None:
    """CompleteMultipartUpload suppression, through the REAL version-resolution path.

    The middleware unit tests drive the created-flag from a test header, so they would keep
    passing if complete_multipart_upload resolved the wrong version — e.g. by reading
    objects.current_object_version (which a concurrent write advances) instead of THIS upload's
    own version from its parts. Only a real MPU exercises that resolution.
    """
    bucket_name = unique_bucket_name("ats-purge-mpu")
    cleanup_buckets(bucket_name)
    boto3_client.create_bucket(Bucket=bucket_name)

    key = "mpu-purge-flow.bin"
    purge_path = f"/{bucket_name}/{key}"

    # 1. MPU that CREATES the key -> version 1 -> no purge.
    version = _mpu(boto3_client, bucket_name, key, b"a")
    assert version == "1", f"first MPU on a fresh key must allocate version 1, got {version!r}"
    time.sleep(NO_PURGE_GRACE_S)
    assert _purges_for(purge_path) == [], "an MPU that created its key must not fire a PURGE"

    # 2. MPU that OVERWRITES it -> version >= 2 -> purge, exactly as before the change.
    version2 = _mpu(boto3_client, bucket_name, key, b"b")
    assert int(version2) >= 2, f"second MPU must allocate version >= 2, got {version2!r}"
    purges = _wait_for_purge_count(purge_path, 1)
    assert len(purges) >= 1, "an MPU that overwrote an existing key must fire a PURGE"
