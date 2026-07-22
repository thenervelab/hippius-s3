"""Postgres-level drain assertions (optional, gated on CEPHOR_DATABASE_URL).

Skipped unless a staging Postgres URL is reachable. Coarse by design — see
drain_state.py for why a per-object assertion waits on the api<->drain contract.
"""

import os

import pytest

from .drain_state import replication_status_counts


def _database_url() -> str:
    url = os.getenv("CEPHOR_DATABASE_URL") or os.getenv("DATABASE_URL")
    if not url:
        pytest.skip("set CEPHOR_DATABASE_URL (or DATABASE_URL) to run the Postgres-level drain checks")
    return url


def test_replication_table_exists_and_reports_statuses() -> None:
    """The cephor_replication_status table is present and queryable (migrations applied)."""
    counts = replication_status_counts(_database_url())
    assert set(counts) == {"pending", "draining", "replicated", "failed"}


def test_drain_commits_replicated_rows_after_uploads(s3, bucket: str) -> None:
    """After draining, replicated rows exist — the drain reached its terminal commit.

    Uploads a handful of objects, then asserts the replicated count rose. This is a
    fleet-wide signal (not per-object) until the object->chunk_key contract lands.
    """
    url = _database_url()
    before = replication_status_counts(url)["replicated"]
    for i in range(3):
        s3.put_object(Bucket=bucket, Key=f"state-{i}.bin", Body=bytes([i]) * (64 * 1024))

    import time

    deadline = time.monotonic() + 90
    while time.monotonic() < deadline:
        if replication_status_counts(url)["replicated"] > before:
            return
        time.sleep(3)
    pytest.fail("no new cephor_replication_status rows reached 'replicated' after uploads within 90s")
