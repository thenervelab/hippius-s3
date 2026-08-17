"""Unit tests for the account purge worker (issue #421)."""

import uuid
from types import SimpleNamespace
from typing import Any

import pytest

import hippius_s3.workers.purger as purger


ACCOUNT = "5FHneW46xGXgs5mUiveU4sbTyGBzmstUspZC92UhjJM694ty"
JOB_ID = uuid.uuid4()


def _config(**overrides: Any) -> SimpleNamespace:
    defaults = dict(
        delete_backends=["arion"],
        purger_batch_size=500,
        purger_unpin_queue_high_water=50000,
        purger_backpressure_sleep_seconds=0,
        purger_lease_seconds=600,
        purger_interval_seconds=0,
    )
    defaults.update(overrides)
    return SimpleNamespace(**defaults)


class FakePool:
    """Routes queries by SQL substring, records calls in order."""

    def __init__(self, batch_rows: list[list[dict[str, Any]]], claim_rows: list[Any]) -> None:
        self.batch_rows = list(batch_rows)
        self.claim_rows = list(claim_rows)
        self.calls: list[tuple[str, str, tuple]] = []

    def _record(self, method: str, query: str, args: tuple) -> None:
        self.calls.append((method, query, args))

    async def fetchrow(self, query: str, *args: Any) -> Any:
        self._record("fetchrow", query, args)
        if "SET state = 'running'" in query:
            return self.claim_rows.pop(0) if self.claim_rows else None
        if "UPDATE buckets" in query:
            return {"bucket_id": args[0], "bucket_name": "bucket-one"}
        return None

    async def fetch(self, query: str, *args: Any) -> Any:
        self._record("fetch", query, args)
        if "FROM buckets" in query:
            return [{"bucket_id": "b1", "bucket_name": "bucket-one", "created_at": None, "is_public": False}]
        if "WITH candidates" in query:
            return self.batch_rows.pop(0) if self.batch_rows else []
        if "SELECT access_key_id FROM sub_token_scopes" in query:
            return [{"access_key_id": "hip_scoped_key"}]
        return []

    async def execute(self, query: str, *args: Any) -> None:
        self._record("execute", query, args)


class FakeQueueRedis:
    def __init__(self, depths: list[int] | None = None) -> None:
        # llen answers pop from this list; empty -> 0 (headroom).
        self.depths = list(depths or [])
        self.llen_calls = 0

    async def llen(self, key: str) -> int:
        self.llen_calls += 1
        return self.depths.pop(0) if self.depths else 0


class FakeCacheRedis:
    def __init__(self) -> None:
        self.deleted: list[str] = []

    async def delete(self, key: str) -> None:
        self.deleted.append(key)


def _claim_row(deleted_objects: int = 0, deleted_bytes: int = 0) -> dict[str, Any]:
    return {
        "job_id": JOB_ID,
        "account_id": ACCOUNT,
        "deleted_objects": deleted_objects,
        "deleted_bytes": deleted_bytes,
    }


def _batch(n: int, bytes_each: int = 100) -> list[dict[str, Any]]:
    return [
        {"object_id": uuid.uuid4(), "total_bytes": bytes_each, "backends": ["arion"]}
        for _ in range(n)
    ]


@pytest.mark.asyncio
async def test_no_job_returns_false() -> None:
    pool = FakePool(batch_rows=[], claim_rows=[])
    worked = await purger.process_one_job(pool, FakeQueueRedis(), FakeCacheRedis(), _config())
    assert worked is False


@pytest.mark.asyncio
async def test_happy_path_purges_and_marks_done(monkeypatch: pytest.MonkeyPatch) -> None:
    enqueued: list[Any] = []

    async def fake_enqueue(payload: Any) -> None:
        enqueued.append(payload)

    monkeypatch.setattr(purger, "enqueue_unpin_request", fake_enqueue)

    pool = FakePool(batch_rows=[_batch(2)], claim_rows=[_claim_row()])
    cache = FakeCacheRedis()
    worked = await purger.process_one_job(pool, FakeQueueRedis(), cache, _config())

    assert worked is True

    # Real, resolvable unpin payloads: object_id + version=None (all versions) + backends.
    assert len(enqueued) == 2
    for payload in enqueued:
        assert payload.address == ACCOUNT
        assert payload.object_version is None
        assert payload.delete_backends == ["arion"]
        uuid.UUID(payload.object_id)

    queries = [q for _, q, _a in pool.calls]
    assert any("DELETE FROM multipart_uploads" in q for q in queries)
    assert any("UPDATE buckets" in q for q in queries)
    assert any("DELETE FROM sub_token_scopes" in q for q in queries)
    assert any("SET state = 'done'" in q for q in queries)
    assert not any("SET state = 'failed'" in q for q in queries)

    # Sub-token scope cache invalidated for the account's keys.
    assert cache.deleted == ["hippius_subscope:hip_scoped_key"]

    # Final progress update carries the totals: 2 objects, 200 logical bytes.
    progress_updates = [a for _, q, a in pool.calls if "SET deleted_objects" in q]
    assert progress_updates[-1][1] == 2
    assert progress_updates[-1][2] == 200


@pytest.mark.asyncio
async def test_counters_resume_from_claimed_row(monkeypatch: pytest.MonkeyPatch) -> None:
    async def fake_enqueue(payload: Any) -> None:
        return None

    monkeypatch.setattr(purger, "enqueue_unpin_request", fake_enqueue)

    pool = FakePool(batch_rows=[_batch(3)], claim_rows=[_claim_row(deleted_objects=10, deleted_bytes=1000)])
    await purger.process_one_job(pool, FakeQueueRedis(), FakeCacheRedis(), _config())

    progress_updates = [a for _, q, a in pool.calls if "SET deleted_objects" in q]
    assert progress_updates[-1][1] == 13
    assert progress_updates[-1][2] == 1300


@pytest.mark.asyncio
async def test_backpressure_waits_for_unpin_headroom(monkeypatch: pytest.MonkeyPatch) -> None:
    async def fake_enqueue(payload: Any) -> None:
        return None

    monkeypatch.setattr(purger, "enqueue_unpin_request", fake_enqueue)

    # First depth probe is over the high water; second is under. The purger must park
    # (heartbeating) and only then proceed.
    queue_redis = FakeQueueRedis(depths=[60000, 10000])
    pool = FakePool(batch_rows=[_batch(1)], claim_rows=[_claim_row()])
    await purger.process_one_job(pool, queue_redis, FakeCacheRedis(), _config())

    assert queue_redis.llen_calls >= 2
    heartbeats = [q for _, q, _a in pool.calls if "SET heartbeat_at = now() WHERE job_id" in q]
    assert len(heartbeats) >= 1


@pytest.mark.asyncio
async def test_failure_marks_job_failed(monkeypatch: pytest.MonkeyPatch) -> None:
    async def exploding_enqueue(payload: Any) -> None:
        raise RuntimeError("redis-queues unreachable")

    monkeypatch.setattr(purger, "enqueue_unpin_request", exploding_enqueue)

    pool = FakePool(batch_rows=[_batch(1)], claim_rows=[_claim_row()])
    worked = await purger.process_one_job(pool, FakeQueueRedis(), FakeCacheRedis(), _config())

    assert worked is True
    fail_calls = [(q, a) for _, q, a in pool.calls if "SET state = 'failed'" in q]
    assert len(fail_calls) == 1
    assert "RuntimeError" in fail_calls[0][1][1]
    assert not any("SET state = 'done'" in q for _, q, _a in pool.calls)
