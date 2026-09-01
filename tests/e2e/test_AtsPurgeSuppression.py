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
    put2 = boto3_client.put_object(Bucket=bucket_name, Key=key, Body=b"v2", ContentType="text/plain")
    assert int(put2["VersionId"]) >= 2, "overwrite must allocate a version > 1"
    purges = _wait_for_purge_count(purge_path, 1)
    assert len(purges) == 1, f"overwrite must fire exactly one PURGE, saw {purges}"

    # 3. Delete: same contract.
    boto3_client.delete_object(Bucket=bucket_name, Key=key)
    purges = _wait_for_purge_count(purge_path, 2)
    assert len(purges) == 2, f"delete must fire a PURGE, saw {purges}"


def test_recreate_after_delete_still_skips_purge(
    docker_services: Any,
    boto3_client: Any,
    unique_bucket_name: Callable[[str], str],
    cleanup_buckets: Callable[[str], None],
) -> None:
    """A re-PUT of a deleted key allocates version >= 2, so it does NOT count as a creation and
    the purge fires. This pins the safety property the suppression leans on: only a genuinely
    never-before-seen key (version 1) skips the fan-out."""
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
    assert len(purges) == baseline + 1, "re-PUT of a previously deleted key must still purge"
