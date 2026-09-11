"""Compactor and reconciler control flow, without a database.

The arithmetic lives in SQL and is checked against the canonical query in
tests/integration/test_storage_usage_rollup.py. What is worth pinning here is the loop behaviour
around it: that a compactor which cannot get the advisory lock does not touch the ledger, that
draining terminates, and that drift is reported rather than quietly repaired.
"""

import uuid
from typing import Any

import pytest

from hippius_s3.services import storage_rollup_service


class FakeTransaction:
    async def __aenter__(self) -> "FakeTransaction":
        return self

    async def __aexit__(self, *_exc: object) -> bool:
        return False


class FakeConn:
    """Answers the two calls compact_once makes, and records what it was asked."""

    def __init__(
        self,
        *,
        lock: bool = True,
        batches: list[dict[str, int]] | None = None,
        ready: bool = True,
    ) -> None:
        self.lock = lock
        self.ready = ready
        self.batches = batches or []
        self.queries: list[str] = []
        self.rows: list[Any] = []

    def transaction(self) -> FakeTransaction:
        return FakeTransaction()

    async def fetchval(self, query: str, *_args: Any, **_kwargs: Any) -> Any:
        self.queries.append(query)
        # Two different fetchval callers: the advisory-lock probe and the backfilled-yet? probe.
        if "backfilled_at" in query:
            return self.ready
        return self.lock

    async def fetchrow(self, query: str, *_args: Any, **_kwargs: Any) -> dict[str, int]:
        self.queries.append(query)
        if self.batches:
            return self.batches.pop(0)
        return {"rows_claimed": 0, "buckets_folded": 0, "buckets_applied": 0}

    async def fetch(self, query: str, *_args: Any, **_kwargs: Any) -> list[Any]:
        self.queries.append(query)
        return self.rows


@pytest.mark.asyncio
async def test_compaction_skipped_entirely_when_the_lock_is_held() -> None:
    """A recompute holds the lock, and folding underneath it can discard a delta permanently.

    The compactor must not merely skip applying -- it must not CLAIM either, or the rows are gone
    with nothing having been added.
    """
    conn = FakeConn(lock=False)

    result = await storage_rollup_service.compact_once(conn, batch_size=100)

    assert result == storage_rollup_service.CompactionResult(0, 0, 0)
    assert len(conn.queries) == 1
    assert "pg_try_advisory_xact_lock" in conn.queries[0]


@pytest.mark.asyncio
async def test_draining_stops_on_a_short_batch() -> None:
    conn = FakeConn(
        batches=[
            {"rows_claimed": 10, "buckets_folded": 2, "buckets_applied": 2},
            {"rows_claimed": 3, "buckets_folded": 1, "buckets_applied": 1},
        ]
    )

    result = await storage_rollup_service.compact_until_drained(conn, batch_size=10)

    assert result.rows_claimed == 13
    assert result.buckets_folded == 3


@pytest.mark.asyncio
async def test_draining_is_bounded_so_a_filling_ledger_cannot_starve_the_reconciler() -> None:
    conn = FakeConn(
        batches=[{"rows_claimed": 5, "buckets_folded": 1, "buckets_applied": 1}] * 10_000,
    )

    result = await storage_rollup_service.compact_until_drained(conn, batch_size=5)

    assert result.rows_claimed == 5 * storage_rollup_service.MAX_BATCHES_PER_CYCLE


@pytest.mark.asyncio
async def test_a_lock_lost_mid_drain_ends_the_pass() -> None:
    """Returning zeros is indistinguishable from an empty ledger, which is the intended behaviour."""
    conn = FakeConn(lock=False)

    assert (await storage_rollup_service.compact_until_drained(conn, batch_size=10)).rows_claimed == 0


def test_drift_is_after_minus_before() -> None:
    bucket_id = uuid.uuid4()

    assert storage_rollup_service.RecomputeResult(bucket_id, 1000, 1000).drift_bytes == 0
    assert storage_rollup_service.RecomputeResult(bucket_id, 1000, 1250).drift_bytes == 250
    assert storage_rollup_service.RecomputeResult(bucket_id, 1000, 400).drift_bytes == -600


@pytest.mark.asyncio
async def test_reconcile_logs_every_drifting_bucket(caplog: pytest.LogCaptureFixture) -> None:
    """Expected drift is ZERO, so a drifting bucket is a defect report and must name itself."""
    drifting = uuid.uuid4()
    clean = uuid.uuid4()

    conn = FakeConn()
    conn.rows = [{"bucket_id": drifting}, {"bucket_id": clean}]
    answers = {
        drifting: {"bytes_before": 500, "bytes_after": 900},
        clean: {"bytes_before": 100, "bytes_after": 100},
    }

    async def fetchrow(query: str, *args: Any, **_kwargs: Any) -> dict[str, int]:
        conn.queries.append(query)
        return answers[args[0]]

    conn.fetchrow = fetchrow  # type: ignore[method-assign]

    with caplog.at_level("ERROR"):
        results = await storage_rollup_service.reconcile_buckets(conn, limit=10, timeout=60.0)

    assert [r.drift_bytes for r in results] == [400, 0]
    assert caplog.text.count("STORAGE_ROLLUP_DRIFT") == 1
    assert str(drifting) in caplog.text
    assert str(clean) not in caplog.text


@pytest.mark.asyncio
async def test_seeding_before_the_backfill_is_not_reported_as_drift(caplog: pytest.LogCaptureFixture) -> None:
    """Before the backfill EVERY counter is 0 while truth is the bucket's whole contents, so every
    recompute moves the number. Calling that drift fires once per bucket across the estate, blames a
    write path that is behaving correctly, and trains whoever reads the log to ignore the one alert
    this design depends on.

    Observed on the staging rollout: 22 of the first 25 reconciled buckets logged as drift, every one
    of them correct behaviour.
    """
    bucket = uuid.uuid4()
    conn = FakeConn(ready=False)
    conn.rows = [{"bucket_id": bucket}]

    async def fetchrow(query: str, *_args: Any, **_kwargs: Any) -> dict[str, int]:
        conn.queries.append(query)
        return {"bytes_before": 0, "bytes_after": 24}

    conn.fetchrow = fetchrow  # type: ignore[method-assign]

    with caplog.at_level("INFO"):
        results = await storage_rollup_service.reconcile_buckets(conn, limit=10, timeout=60.0)

    assert [r.drift_bytes for r in results] == [24], "the delta is still measured and returned"
    assert "STORAGE_ROLLUP_DRIFT" not in caplog.text, "must not claim drift before the rollup is seeded"
    assert "STORAGE_ROLLUP_SEEDING" in caplog.text
    assert str(bucket) in caplog.text


@pytest.mark.asyncio
async def test_drift_is_still_loud_once_the_backfill_has_run(caplog: pytest.LogCaptureFixture) -> None:
    """The quietening is conditional, not a downgrade: after seeding, expected drift is ZERO and a
    drifting bucket is a defect that must still name itself at ERROR."""
    bucket = uuid.uuid4()
    conn = FakeConn(ready=True)
    conn.rows = [{"bucket_id": bucket}]

    async def fetchrow(query: str, *_args: Any, **_kwargs: Any) -> dict[str, int]:
        conn.queries.append(query)
        return {"bytes_before": 500, "bytes_after": 900}

    conn.fetchrow = fetchrow  # type: ignore[method-assign]

    with caplog.at_level("INFO"):
        await storage_rollup_service.reconcile_buckets(conn, limit=10, timeout=60.0)

    assert "STORAGE_ROLLUP_DRIFT" in caplog.text
    assert "STORAGE_ROLLUP_SEEDING" not in caplog.text
