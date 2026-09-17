"""Compactor and reconciler control flow, without a database.

The arithmetic lives in SQL and is checked against the canonical query in
tests/integration/test_storage_usage_rollup.py. What is worth pinning here is the loop behaviour
around it: that a compactor which cannot get the advisory lock does not touch the ledger, that
draining terminates, that drift is reported rather than quietly repaired, and -- added after the
prod incident of 2026-09-15 -- that a bucket which can never be aggregated cannot starve every
other bucket's verification.
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
    """Routes on SQL content, because get_query() returns the file body rather than its name.

    Matching on a distinctive fragment of each query keeps these tests honest: rename a query file
    and nothing breaks, but change what a query SAYS and the router stops recognising it, which is
    the failure you want to be loud.
    """

    def __init__(
        self,
        *,
        lock: bool = True,
        ready: bool = True,
        reconcile_queue: list[dict[str, Any]] | None = None,
        ledger_buckets: list[uuid.UUID] | None = None,
        batches: dict[uuid.UUID, list[dict[str, int]]] | None = None,
        recompute: Any = None,
        slices: dict[uuid.UUID, list[dict[str, Any]]] | None = None,
        verify_state: dict[uuid.UUID, dict[str, Any]] | None = None,
        failures_after_mark: int = 1,
    ) -> None:
        self.lock = lock
        self.ready = ready
        self.reconcile_queue = reconcile_queue or []
        self.ledger_buckets = list(ledger_buckets or [])
        self.batches = batches or {}
        self.recompute = recompute
        self.slices = slices or {}
        self.verify_state = verify_state or {}
        self.failures_after_mark = failures_after_mark
        self.queries: list[str] = []
        self.calls: list[tuple[str, tuple[Any, ...]]] = []
        self.lock_keys: list[tuple[Any, ...]] = []
        self.marked_failed: list[uuid.UUID] = []
        self.finished_sweeps: list[uuid.UUID] = []
        self.deleted_state: list[uuid.UUID] = []

    def transaction(self) -> FakeTransaction:
        return FakeTransaction()

    def _record(self, tag: str, query: str, args: tuple[Any, ...]) -> None:
        self.queries.append(query)
        self.calls.append((tag, args))

    async def fetchval(self, query: str, *args: Any, **_kwargs: Any) -> Any:
        if "pg_try_advisory_xact_lock" in query:
            self._record("lock", query, args)
            self.lock_keys.append(args)
            return self.lock
        if "backfilled_at" in query:
            self._record("ready", query, args)
            return self.ready
        if "recompute_failures = bsu.recompute_failures + 1" in query:
            self._record("mark_failed", query, args)
            self.marked_failed.append(args[0])
            return self.failures_after_mark
        if "INSERT INTO bucket_storage_verify_state" in query:
            self._record("start_sweep", query, args)
            self.verify_state.setdefault(
                args[0],
                {
                    "cursor_key": "",
                    "partial_bytes": 0,
                    "objects_scanned": 0,
                    "slices_done": 0,
                    "started_at": None,
                    "start_churn_bytes": 0,
                    "bytes_used": 0,
                    "churn_bytes": 0,
                },
            )
            return 0
        self._record("fetchval", query, args)
        return None

    async def fetchrow(self, query: str, *args: Any, **_kwargs: Any) -> Any:
        if "WITH claimed AS" in query:
            self._record("compact", query, args)
            queue = self.batches.get(args[0], [])
            return queue.pop(0) if queue else {"rows_claimed": 0, "buckets_applied": 0}
        if "recompute_bucket_storage_usage($1)" in query:
            self._record("recompute", query, args)
            if callable(self.recompute):
                return self.recompute(args[0])
            return self.recompute
        if "WITH page AS" in query:
            self._record("slice", query, args)
            pages = self.slices.get(args[0], [])
            page = pages.pop(0) if pages else {"bytes": 0, "objects_scanned": 0, "next_cursor": None}
            state = self.verify_state.get(args[0])
            if state is not None and page["objects_scanned"]:
                state["partial_bytes"] += int(page["bytes"])
                state["objects_scanned"] += int(page["objects_scanned"])
                state["slices_done"] += 1
                state["cursor_key"] = page["next_cursor"]
            return page
        if "FROM bucket_storage_verify_state vs" in query:
            self._record("get_state", query, args)
            return self.verify_state.get(args[0])
        if "UPDATE bucket_storage_verify_state" in query:
            self._record("advance", query, args)
            return self.verify_state.get(args[0])
        self._record("fetchrow", query, args)
        return None

    async def fetch(self, query: str, *args: Any, **_kwargs: Any) -> list[Any]:
        if "GROUP BY bucket_id" in query:
            self._record("ledger_buckets", query, args)
            return [{"bucket_id": b} for b in self.ledger_buckets]
        if "recompute_failures" in query and "ORDER BY" in query:
            self._record("reconcile_queue", query, args)
            return self.reconcile_queue
        self._record("fetch", query, args)
        return []

    async def execute(self, query: str, *args: Any, **_kwargs: Any) -> None:
        if "SET recomputed_at = now()" in query:
            self._record("finish_sweep", query, args)
            self.finished_sweeps.append(args[0])
            return
        if "DELETE FROM bucket_storage_verify_state" in query:
            self._record("delete_state", query, args)
            self.deleted_state.append(args[0])
            return
        self._record("execute", query, args)

    def tags(self) -> list[str]:
        return [tag for tag, _ in self.calls]


def _queue(*entries: tuple[uuid.UUID, int]) -> list[dict[str, Any]]:
    return [{"bucket_id": b, "recompute_failures": f} for b, f in entries]


# --------------------------------------------------------------------------------------------
# Compaction
# --------------------------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_compaction_skipped_entirely_when_that_buckets_lock_is_held() -> None:
    """A recompute holds the lock, and folding underneath it can discard a delta permanently.

    The compactor must not merely skip applying -- it must not CLAIM either, or the rows are gone
    with nothing having been added.
    """
    bucket = uuid.uuid4()
    conn = FakeConn(lock=False)

    result = await storage_rollup_service.compact_bucket_once(conn, bucket, batch_size=100)

    assert result == storage_rollup_service.CompactionResult(0, 0, 0)
    assert conn.tags() == ["lock"], "claimed rows despite losing the lock"


@pytest.mark.asyncio
async def test_the_compaction_lock_is_keyed_on_the_BUCKET_not_a_constant() -> None:
    """The whole point of the 2026-09-15 fix.

    With a hardcoded second argument every recompute and every fold contended on one estate-wide
    key, so a 300s recompute of one bucket stalled compaction for every unrelated bucket -- measured
    as the ledger's oldest row aging to 412s against a normal 2s. Two different buckets must produce
    two different lock keys, or that starvation is back.
    """
    a, b = uuid.uuid4(), uuid.uuid4()
    conn = FakeConn()

    await storage_rollup_service.compact_bucket_once(conn, a, batch_size=10)
    await storage_rollup_service.compact_bucket_once(conn, b, batch_size=10)

    assert conn.lock_keys == [(a,), (b,)], "the lock must be parameterised by bucket"
    assert "storage_usage_bucket_lock_key" in conn.queries[0], (
        "the lock's second argument is not the per-bucket key function"
    )


@pytest.mark.asyncio
async def test_draining_folds_every_bucket_with_pending_rows() -> None:
    a, b = uuid.uuid4(), uuid.uuid4()
    conn = FakeConn(
        ledger_buckets=[a, b],
        batches={
            a: [{"rows_claimed": 10, "buckets_applied": 1}],
            b: [{"rows_claimed": 3, "buckets_applied": 1}],
        },
    )

    result = await storage_rollup_service.compact_until_drained(conn, batch_size=10)

    assert result.rows_claimed == 13
    assert result.buckets_folded == 2


@pytest.mark.asyncio
async def test_draining_terminates_when_no_bucket_moves() -> None:
    """The termination condition had to change with the per-bucket lock.

    A zero now means "empty OR being recomputed", so the old "stop on a short batch" test would end
    the cycle at the first locked bucket while others still had work. Stopping only when NOTHING
    moved is what keeps both properties: it drains everything available and still terminates.
    """
    a, b = uuid.uuid4(), uuid.uuid4()
    conn = FakeConn(ledger_buckets=[a, b], batches={})

    result = await storage_rollup_service.compact_until_drained(conn, batch_size=10)

    assert result.rows_claimed == 0
    assert conn.tags().count("ledger_buckets") == 1, "re-listed candidates after nothing moved"


@pytest.mark.asyncio
async def test_draining_is_bounded_so_a_filling_ledger_cannot_starve_the_reconciler() -> None:
    bucket = uuid.uuid4()
    conn = FakeConn(
        ledger_buckets=[bucket],
        batches={bucket: [{"rows_claimed": 5, "buckets_applied": 1}] * 10_000},
    )

    result = await storage_rollup_service.compact_until_drained(conn, batch_size=5)

    assert result.rows_claimed == 5 * storage_rollup_service.MAX_BATCHES_PER_CYCLE


@pytest.mark.asyncio
async def test_an_empty_ledger_costs_one_query() -> None:
    """The steady state. The compactor runs every 5s, so an idle cycle must be nearly free."""
    conn = FakeConn(ledger_buckets=[])

    result = await storage_rollup_service.compact_until_drained(conn, batch_size=10)

    assert result.rows_claimed == 0
    assert conn.tags() == ["ledger_buckets"]


@pytest.mark.asyncio
async def test_one_locked_bucket_does_not_stop_the_others_being_folded() -> None:
    """The regression this release exists to prevent, at the loop level."""
    locked, free = uuid.uuid4(), uuid.uuid4()
    conn = FakeConn(
        ledger_buckets=[locked, free],
        batches={free: [{"rows_claimed": 7, "buckets_applied": 1}]},
    )
    # The locked bucket loses the lock; the free one gets it.
    real_fetchval = conn.fetchval

    async def fetchval(query: str, *args: Any, **kwargs: Any) -> Any:
        if "pg_try_advisory_xact_lock" in query:
            conn.lock = args[0] != locked
        return await real_fetchval(query, *args, **kwargs)

    conn.fetchval = fetchval  # type: ignore[method-assign]

    result = await storage_rollup_service.compact_until_drained(conn, batch_size=10)

    assert result.rows_claimed == 7, "a bucket under recompute blocked an unrelated bucket's fold"


# --------------------------------------------------------------------------------------------
# Drift arithmetic
# --------------------------------------------------------------------------------------------


def test_drift_is_after_minus_before() -> None:
    bucket_id = uuid.uuid4()

    assert storage_rollup_service.RecomputeResult(bucket_id, 1000, 1000).drift_bytes == 0
    assert storage_rollup_service.RecomputeResult(bucket_id, 1000, 1250).drift_bytes == 250
    assert storage_rollup_service.RecomputeResult(bucket_id, 1000, 400).drift_bytes == -600


def test_pre_backfill_seeding_is_not_reported_as_drift_to_the_metrics() -> None:
    """The seeding-vs-drift distinction has to hold for the METRIC, not just the log line.

    #512 fixed the log: before the backfill every recompute legitimately moves a counter off zero,
    so it is logged as STORAGE_ROLLUP_SEEDING at INFO rather than STORAGE_ROLLUP_DRIFT at ERROR.
    But `record_storage_rollup_recompute` increments `storage_rollup_drifted_buckets_total` on ANY
    non-zero drift_bytes, so the metric did not get the same treatment -- and the alert added in
    this release is `increase(storage_rollup_drifted_buckets_total[1h]) > 0`.

    On a production rollout that fires from the first reconcile pass and keeps firing for the whole
    pre-backfill window, which is precisely the cry-wolf failure the log fix existed to prevent.
    Whichever half is left ungated makes the other pointless.
    """
    seeding = storage_rollup_service.RecomputeResult(
        bucket_id=uuid.uuid4(), bytes_before=0, bytes_after=1_000_000, seeding=True
    )
    real = storage_rollup_service.RecomputeResult(
        bucket_id=uuid.uuid4(), bytes_before=1_000_000, bytes_after=1_000_500, seeding=False
    )

    assert seeding.drift_bytes == 1_000_000, "the raw delta is still reported, for the seeding log"
    assert not seeding.counts_as_drift, (
        "a pre-backfill seeding recompute was flagged as drift; the alert would fire for the whole rollout window"
    )
    assert real.counts_as_drift, "a post-backfill correction must still count as drift"


def test_a_zero_delta_is_never_drift_either_way() -> None:
    """The steady state: every bucket already correct, nothing reported, on both paths."""
    for seeding in (True, False):
        result = storage_rollup_service.RecomputeResult(
            bucket_id=uuid.uuid4(), bytes_before=4096, bytes_after=4096, seeding=seeding
        )
        assert not result.counts_as_drift


# --------------------------------------------------------------------------------------------
# Reconciler: routing and failure handling
# --------------------------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_reconcile_logs_every_drifting_bucket(caplog: pytest.LogCaptureFixture) -> None:
    """Expected drift is ZERO, so a drifting bucket is a defect report and must name itself."""
    drifting, clean = uuid.uuid4(), uuid.uuid4()
    answers = {
        drifting: {"bytes_before": 500, "bytes_after": 900},
        clean: {"bytes_before": 100, "bytes_after": 100},
    }
    conn = FakeConn(reconcile_queue=_queue((drifting, 0), (clean, 0)), recompute=lambda b: answers[b])

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
    conn = FakeConn(ready=False, reconcile_queue=_queue((bucket, 0)), recompute={"bytes_before": 0, "bytes_after": 24})

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
    conn = FakeConn(reconcile_queue=_queue((bucket, 0)), recompute={"bytes_before": 500, "bytes_after": 900})

    with caplog.at_level("INFO"):
        await storage_rollup_service.reconcile_buckets(conn, limit=10, timeout=60.0)

    assert "STORAGE_ROLLUP_DRIFT" in caplog.text
    assert "STORAGE_ROLLUP_SEEDING" not in caplog.text


@pytest.mark.asyncio
async def test_a_failed_recompute_records_the_attempt(caplog: pytest.LogCaptureFixture) -> None:
    """THE STARVATION FIX.

    recompute_bucket_storage_usage() stamps only on success, and the work queue orders on that
    stamp. So a bucket whose aggregate cannot complete used to keep a NULL timestamp, stay at the
    HEAD of the queue forever, and be retried every cycle while every bucket behind it went
    unverified. Prod 2026-09-15: 168 failures in 24h on one bucket, 47 of 50 slots used per pass.

    Recording the ATTEMPT -- in its own transaction, because the recompute's has already rolled
    back -- is what rotates it out.
    """
    boom, fine = uuid.uuid4(), uuid.uuid4()

    def recompute(bucket: uuid.UUID) -> dict[str, int]:
        if bucket == boom:
            raise TimeoutError
        return {"bytes_before": 7, "bytes_after": 7}

    conn = FakeConn(reconcile_queue=_queue((boom, 0), (fine, 0)), recompute=recompute)

    with caplog.at_level("ERROR"):
        results = await storage_rollup_service.reconcile_buckets(conn, limit=10, timeout=60.0)

    assert conn.marked_failed == [boom], "the failed attempt was not recorded, so the bucket stays queue head"
    assert [r.bucket_id for r in results] == [fine], "the rest of the pass must continue"
    assert "STORAGE_ROLLUP_RECOMPUTE_FAILED" in caplog.text


@pytest.mark.asyncio
async def test_a_bucket_over_the_failure_threshold_is_not_recomputed_again() -> None:
    """Once a bucket has proven it cannot be aggregated, retrying the aggregate is pure waste --
    and worse, it holds that bucket's lock for the whole timeout every cycle."""
    giant = uuid.uuid4()
    conn = FakeConn(
        reconcile_queue=_queue((giant, 2)),
        slices={giant: [{"bytes": 10, "objects_scanned": 1, "next_cursor": "k"}]},
    )

    results = await storage_rollup_service.reconcile_buckets(conn, limit=10, timeout=60.0, slice_after_failures=2)

    assert "recompute" not in conn.tags(), "an oversized bucket must not be re-aggregated"
    assert "slice" in conn.tags(), "it must be verified by slices instead"
    assert results == [], "a sweep produces no RecomputeResult -- it never moves the counter"


@pytest.mark.asyncio
async def test_one_transient_failure_does_not_divert_a_healthy_bucket() -> None:
    """The threshold is 2, not 1, so a lock wait or replica hiccup does not push an aggregatable
    bucket onto the slow path permanently."""
    bucket = uuid.uuid4()
    conn = FakeConn(reconcile_queue=_queue((bucket, 1)), recompute={"bytes_before": 1, "bytes_after": 1})

    await storage_rollup_service.reconcile_buckets(conn, limit=10, timeout=60.0, slice_after_failures=2)

    assert "recompute" in conn.tags(), "one failure should still get the fast path"
    assert "slice" not in conn.tags()


# --------------------------------------------------------------------------------------------
# Sliced verification
# --------------------------------------------------------------------------------------------


def _state(**over: Any) -> dict[str, Any]:
    base = {
        "cursor_key": "",
        "partial_bytes": 0,
        "objects_scanned": 0,
        "slices_done": 0,
        "started_at": None,
        "start_churn_bytes": 0,
        "bytes_used": 0,
        "churn_bytes": 0,
    }
    base.update(over)
    return base


@pytest.mark.asyncio
async def test_a_short_page_completes_the_sweep_and_compares() -> None:
    bucket = uuid.uuid4()
    conn = FakeConn(
        verify_state={bucket: _state(bytes_used=300)},
        slices={bucket: [{"bytes": 300, "objects_scanned": 3, "next_cursor": "c"}]},
    )

    sweep = await storage_rollup_service.verify_bucket_sliced(conn, bucket, page_objects=10, max_slices=4)

    assert sweep.complete
    assert sweep.swept_bytes == 300
    assert sweep.counter_bytes == 300
    assert sweep.gap_bytes == 0
    assert not sweep.exceeds_tolerance
    assert conn.finished_sweeps == [bucket], "a completed sweep must clear the failure counter"
    assert conn.deleted_state == [bucket], "sweep state must not be left behind"


@pytest.mark.asyncio
async def test_a_full_page_leaves_the_sweep_open_for_the_next_cycle() -> None:
    bucket = uuid.uuid4()
    conn = FakeConn(
        verify_state={bucket: _state()},
        slices={bucket: [{"bytes": 5, "objects_scanned": 2, "next_cursor": "b"}] * 2},
    )

    sweep = await storage_rollup_service.verify_bucket_sliced(conn, bucket, page_objects=2, max_slices=2)

    assert not sweep.complete
    assert sweep.slices_run == 2
    assert conn.finished_sweeps == [], "an incomplete sweep must not claim the bucket is verified"
    assert conn.deleted_state == []


@pytest.mark.asyncio
async def test_a_gap_within_concurrent_churn_is_not_drift() -> None:
    """The tolerance that makes this rigorous rather than a guess.

    A sweep reads across many cycles while writes land, so its total is a smear and the counter is a
    point reading. They can legitimately differ by up to the absolute byte movement the ledger
    carried during the sweep. Below that, claiming drift would fire on every busy bucket.
    """
    bucket = uuid.uuid4()
    conn = FakeConn(
        verify_state={bucket: _state(bytes_used=1_000, start_churn_bytes=0, churn_bytes=5_000)},
        slices={bucket: [{"bytes": 3_000, "objects_scanned": 1, "next_cursor": "z"}]},
    )

    sweep = await storage_rollup_service.verify_bucket_sliced(conn, bucket, page_objects=10, max_slices=1)

    assert sweep.complete
    assert sweep.gap_bytes == 2_000
    assert sweep.tolerance_bytes == 5_000
    assert not sweep.exceeds_tolerance, "a gap smaller than in-flight churn is not provable drift"


@pytest.mark.asyncio
async def test_a_gap_larger_than_churn_can_explain_IS_drift() -> None:
    bucket = uuid.uuid4()
    conn = FakeConn(
        verify_state={bucket: _state(bytes_used=1_000, start_churn_bytes=100, churn_bytes=200)},
        slices={bucket: [{"bytes": 101_000, "objects_scanned": 1, "next_cursor": "z"}]},
    )

    sweep = await storage_rollup_service.verify_bucket_sliced(conn, bucket, page_objects=10, max_slices=1)

    assert sweep.gap_bytes == 100_000
    assert sweep.tolerance_bytes == 100
    assert sweep.exceeds_tolerance, "100 GB-class drift must survive the tolerance"


@pytest.mark.asyncio
async def test_the_tolerance_uses_churn_measured_from_the_sweeps_own_start() -> None:
    """Not the lifetime churn. `churn_bytes` is monotonic and never resets, so comparing against its
    absolute value would hand a long-lived bucket an unbounded tolerance and the test would never
    fire again."""
    bucket = uuid.uuid4()
    conn = FakeConn(
        verify_state={bucket: _state(bytes_used=0, start_churn_bytes=1_000_000_000, churn_bytes=1_000_000_050)},
        slices={bucket: [{"bytes": 500, "objects_scanned": 1, "next_cursor": "z"}]},
    )

    sweep = await storage_rollup_service.verify_bucket_sliced(conn, bucket, page_objects=10, max_slices=1)

    assert sweep.tolerance_bytes == 50, "tolerance must be the DELTA in churn, not its lifetime total"
    assert sweep.exceeds_tolerance


@pytest.mark.asyncio
async def test_an_incomplete_sweep_never_reports_drift() -> None:
    """exceeds_tolerance is gated on completion: a partial total is meaningless against the counter."""
    partial = storage_rollup_service.SliceSweepResult(
        bucket_id=uuid.uuid4(),
        slices_run=1,
        objects_scanned=10,
        complete=False,
        swept_bytes=1,
        counter_bytes=10**12,
        tolerance_bytes=0,
    )

    assert not partial.exceeds_tolerance


@pytest.mark.asyncio
async def test_a_sweep_starts_from_scratch_when_there_is_no_state() -> None:
    bucket = uuid.uuid4()
    conn = FakeConn(slices={bucket: [{"bytes": 1, "objects_scanned": 1, "next_cursor": "a"}]})

    await storage_rollup_service.verify_bucket_sliced(conn, bucket, page_objects=10, max_slices=1)

    assert "start_sweep" in conn.tags()


@pytest.mark.asyncio
async def test_a_page_of_only_delete_markers_still_advances_the_cursor() -> None:
    """Objects whose current version is deleted or a delete marker contribute 0 bytes but MUST move
    the cursor, or a page made entirely of them stalls the sweep on the same range forever."""
    bucket = uuid.uuid4()
    conn = FakeConn(
        verify_state={bucket: _state()},
        slices={bucket: [{"bytes": 0, "objects_scanned": 2, "next_cursor": "moved"}]},
    )

    await storage_rollup_service.verify_bucket_sliced(conn, bucket, page_objects=2, max_slices=1)

    assert conn.verify_state[bucket]["cursor_key"] == "moved"
    assert "advance" in conn.tags(), "a zero-byte page must still persist progress"


@pytest.mark.asyncio
async def test_a_bucket_deleted_mid_sweep_does_not_raise() -> None:
    bucket = uuid.uuid4()
    conn = FakeConn()

    async def fetchval(query: str, *args: Any, **_kwargs: Any) -> Any:
        return 0 if "INSERT INTO bucket_storage_verify_state" in query else None

    conn.fetchval = fetchval  # type: ignore[method-assign]

    sweep = await storage_rollup_service.verify_bucket_sliced(conn, bucket, page_objects=10, max_slices=1)

    assert not sweep.complete
    assert sweep.slices_run == 0


@pytest.mark.asyncio
async def test_a_sweep_that_exceeds_tolerance_logs_drift_with_the_shared_prefix(
    caplog: pytest.LogCaptureFixture,
) -> None:
    """It must use STORAGE_ROLLUP_DRIFT, the same token the alert and the runbook already key on.

    A second spelling for the same condition means the alert misses it.
    """
    giant = uuid.uuid4()
    conn = FakeConn(
        reconcile_queue=_queue((giant, 3)),
        verify_state={giant: _state(bytes_used=0, churn_bytes=0)},
        slices={giant: [{"bytes": 999_999, "objects_scanned": 1, "next_cursor": "z"}]},
    )

    with caplog.at_level("INFO"):
        await storage_rollup_service.reconcile_buckets(conn, limit=10, timeout=60.0, slice_after_failures=2)

    assert "STORAGE_ROLLUP_DRIFT" in caplog.text
    assert str(giant) in caplog.text


@pytest.mark.asyncio
async def test_a_sliced_sweep_before_the_backfill_is_seeding_not_drift(caplog: pytest.LogCaptureFixture) -> None:
    """Same gating as the recompute path: pre-backfill the counter is a delta, not a total."""
    giant = uuid.uuid4()
    conn = FakeConn(
        ready=False,
        reconcile_queue=_queue((giant, 3)),
        verify_state={giant: _state(bytes_used=0, churn_bytes=0)},
        slices={giant: [{"bytes": 999_999, "objects_scanned": 1, "next_cursor": "z"}]},
    )

    with caplog.at_level("INFO"):
        await storage_rollup_service.reconcile_buckets(conn, limit=10, timeout=60.0, slice_after_failures=2)

    assert "STORAGE_ROLLUP_DRIFT" not in caplog.text
    assert "STORAGE_ROLLUP_SLICE_SEEDING" in caplog.text


@pytest.mark.asyncio
async def test_a_failing_sweep_is_loud_because_nothing_else_verifies_that_bucket(
    caplog: pytest.LogCaptureFixture,
) -> None:
    giant = uuid.uuid4()
    conn = FakeConn(reconcile_queue=_queue((giant, 5)))

    async def fetchrow(query: str, *args: Any, **_kwargs: Any) -> Any:
        if "WITH page AS" in query:
            raise TimeoutError
        return await FakeConn.fetchrow(conn, query, *args, **_kwargs)

    conn.fetchrow = fetchrow  # type: ignore[method-assign]

    with caplog.at_level("ERROR"):
        await storage_rollup_service.reconcile_buckets(conn, limit=10, timeout=60.0, slice_after_failures=2)

    assert "STORAGE_ROLLUP_SLICE_FAILED" in caplog.text
