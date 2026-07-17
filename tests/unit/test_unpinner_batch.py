"""Adversarial tests for the HCFS batch-delete unpinner path (POST /delete_files).

Two layers are exercised:
  1. process_unpin_batch(...) directly — fan-out per HCFS item status, invariant A9 (never soft-delete
     a chunk whose backend delete didn't succeed AND whose DB soft-delete didn't succeed), per-request
     routing to retry/DLQ, sub-batch splitting, and the 404 fallback to the per-file path.
  2. run_unpinner_loop(...) with batch mode enabled — request grouping by (ss58, folder_hash),
     empty-identifier retry-then-drop, feature-flag OFF, and graceful shutdown ordering.

Everything is faked — no live services.
"""

from __future__ import annotations

import asyncio
from unittest.mock import AsyncMock
from unittest.mock import MagicMock
from unittest.mock import patch

import pytest

from hippius_s3.queue import UnpinChainRequest
from hippius_s3.services.arion_service import BatchDeleteError
from hippius_s3.services.arion_service import BatchDeleteItem
from hippius_s3.services.arion_service import BatchDeleteResult
from hippius_s3.services.arion_service import BatchEndpointUnavailable
from hippius_s3.workers import unpinner as un


# --------------------------------------------------------------------------- #
# Helpers
# --------------------------------------------------------------------------- #


def _cfg() -> MagicMock:
    cfg = MagicMock()
    cfg.unpinner_max_attempts = 5
    cfg.unpinner_backoff_base_ms = 1
    cfg.unpinner_backoff_max_ms = 10
    return cfg


def _ureq(object_id: str, *, address: str = "5Addr", attempts: int = 0) -> UnpinChainRequest:
    return UnpinChainRequest(
        address=address,
        object_id=object_id,
        object_version=1,
        attempts=attempts,
        request_id=f"req-{object_id}",
    )


def _all_deleted(file_ids: list[str], *, status: str = "deleted") -> BatchDeleteResult:
    return BatchDeleteResult(
        deleted=[BatchDeleteItem(file_id=f, status=status) for f in file_ids],
        errors=[],
        files_deleted=len(file_ids),
    )


class _BatchClient:
    """Fake ArionClient for direct process_unpin_batch tests. `resp_fn` maps the sent file_ids to a
    BatchDeleteResult; `raise_exc` (if set) makes every call raise (whole-batch failure / 404)."""

    def __init__(self, resp_fn=None, raise_exc: BaseException | None = None) -> None:
        self.calls: list[tuple[list[str], str, str]] = []
        self.resp_fn = resp_fn or (lambda fids: _all_deleted(fids))
        self.raise_exc = raise_exc
        self.per_file: list[str] = []

    async def unpin_files_batch(self, file_ids, account_ss58, folder_hash=""):
        self.calls.append((list(file_ids), account_ss58, folder_hash))
        if self.raise_exc is not None:
            raise self.raise_exc
        return self.resp_fn(list(file_ids))

    async def unpin_file(self, file_id, **kw):
        self.per_file.append(file_id)
        return None


def _group(specs, *, folder_hash: str = "", address: str = "5Addr") -> un._BatchGroup:
    """specs: list of (request, [(file_id, chunk_id), ...])."""
    g = un._BatchGroup(address, folder_hash)
    for req, rows in specs:
        g.add(req, [{"backend_identifier": fid, "chunk_id": cid} for fid, cid in rows])
    return g


def _pool(rows_by_object=None, fail_chunk_ids=frozenset()):
    rows_by_object = rows_by_object or {}
    conn = AsyncMock()

    async def _fetch(query, backend, object_id, object_version):
        return rows_by_object.get(object_id, [])

    async def _fetchval(query, backend, chunk_id):
        if chunk_id in fail_chunk_ids:
            raise RuntimeError("db down")
        return chunk_id

    conn.fetch = AsyncMock(side_effect=_fetch)
    conn.fetchval = AsyncMock(side_effect=_fetchval)
    pool = MagicMock()
    # __aexit__ MUST return False, else a truthy AsyncMock return suppresses exceptions raised in the
    # `async with db_pool.acquire()` body (which is exactly what the A9/soft-delete-failure test needs).
    pool.acquire = MagicMock(
        return_value=MagicMock(__aenter__=AsyncMock(return_value=conn), __aexit__=AsyncMock(return_value=False))
    )
    pool._conn = conn
    return pool


def _softdeleted(pool) -> set[int]:
    return {c.args[2] for c in pool._conn.fetchval.await_args_list}


def _success_count(metrics) -> int:
    return sum(1 for c in metrics.record_unpinner_operation.call_args_list if c.kwargs.get("success") is True)


async def _run_batch(group, client, pool, *, max_files: int = 1000, dlq=None):
    dlq = dlq or MagicMock(push=AsyncMock())
    metrics = MagicMock()
    cfg = _cfg()
    with (
        patch.object(un, "get_config", return_value=cfg),
        patch.object(un, "get_metrics_collector", return_value=metrics),
        patch.object(un, "get_query", return_value="SQL"),
        patch.object(un, "enqueue_unpin_retry_request", new_callable=AsyncMock) as retry,
    ):
        await un.process_unpin_batch(
            group,
            backend_name="arion",
            client=client,
            worker_logger=MagicMock(),
            dlq_manager=dlq,
            db_pool=pool,
            sem=asyncio.Semaphore(5),
            config=cfg,
            max_files=max_files,
        )
    return retry, dlq, metrics


# --------------------------------------------------------------------------- #
# 1. Batch of N single-chunk requests -> one call, all soft-deleted, all acked
# --------------------------------------------------------------------------- #


@pytest.mark.asyncio
async def test_batch_of_single_chunk_requests_all_acked():
    specs = [(_ureq(f"o{i}"), [(f"f{i}", i)]) for i in range(4)]
    g = _group(specs)
    client = _BatchClient()
    pool = _pool()

    retry, dlq, metrics = await _run_batch(g, client, pool)

    assert len(client.calls) == 1
    sent_file_ids, ss58, folder_hash = client.calls[0]
    assert sorted(sent_file_ids) == ["f0", "f1", "f2", "f3"]
    assert ss58 == "5Addr"
    assert _softdeleted(pool) == {0, 1, 2, 3}
    assert _success_count(metrics) == 4
    retry.assert_not_called()
    dlq.push.assert_not_called()


# --------------------------------------------------------------------------- #
# 2. Multi-chunk request -> acked only after ALL its chunks soft-deleted
# --------------------------------------------------------------------------- #


@pytest.mark.asyncio
async def test_multi_chunk_request_acked_when_all_chunks_softdeleted():
    req = _ureq("obj")
    g = _group([(req, [("fa", 1), ("fb", 2), ("fc", 3)])])
    client = _BatchClient()
    pool = _pool()

    retry, dlq, metrics = await _run_batch(g, client, pool)

    assert len(client.calls) == 1
    assert sorted(client.calls[0][0]) == ["fa", "fb", "fc"]
    assert _softdeleted(pool) == {1, 2, 3}
    assert _success_count(metrics) == 1
    retry.assert_not_called()


# --------------------------------------------------------------------------- #
# 3. Request whose file_ids span TWO sub-batches -> acked only when all resolved
# --------------------------------------------------------------------------- #


@pytest.mark.asyncio
async def test_request_spanning_two_subbatches_acked_when_all_resolved():
    req = _ureq("obj")
    g = _group([(req, [("fa", 1), ("fb", 2), ("fc", 3)])])
    client = _BatchClient()
    pool = _pool()

    retry, dlq, metrics = await _run_batch(g, client, pool, max_files=2)

    assert len(client.calls) == 2, "3 file_ids with cap 2 must split into 2 calls"
    assert all(len(call[0]) <= 2 for call in client.calls)
    assert _softdeleted(pool) == {1, 2, 3}
    assert _success_count(metrics) == 1
    retry.assert_not_called()


# --------------------------------------------------------------------------- #
# 4. Partial failure -> owning request retried, its chunk NOT soft-deleted;
#    OTHER requests still acked + soft-deleted
# --------------------------------------------------------------------------- #


@pytest.mark.asyncio
async def test_partial_failure_isolates_to_owning_request():
    req_ok = _ureq("ok")
    req_bad = _ureq("bad")
    g = _group([(req_ok, [("fok", 1)]), (req_bad, [("fbad", 2)])])

    def _resp(file_ids):
        deleted = [BatchDeleteItem(file_id="fok", status="deleted")] if "fok" in file_ids else []
        errors = (
            [BatchDeleteError(file_id="fbad", code="throttled", message="rate limit hit")] if "fbad" in file_ids else []
        )
        return BatchDeleteResult(deleted=deleted, errors=errors, files_deleted=len(deleted))

    client = _BatchClient(resp_fn=_resp)
    pool = _pool()

    retry, dlq, metrics = await _run_batch(g, client, pool)

    assert _softdeleted(pool) == {1}, "only the successful chunk is soft-deleted (A9)"
    assert _success_count(metrics) == 1
    assert retry.await_count == 1, "the errored request is retried (throttled = transient)"
    assert retry.await_args.args[0] is req_bad
    dlq.push.assert_not_called()


# --------------------------------------------------------------------------- #
# 5. already_deleted items -> success (idempotent), soft-deleted
# --------------------------------------------------------------------------- #


@pytest.mark.asyncio
async def test_already_deleted_is_success():
    req = _ureq("obj")
    g = _group([(req, [("fa", 1)])])
    client = _BatchClient(resp_fn=lambda fids: _all_deleted(fids, status="already_deleted"))
    pool = _pool()

    retry, dlq, metrics = await _run_batch(g, client, pool)

    assert _softdeleted(pool) == {1}
    assert _success_count(metrics) == 1
    retry.assert_not_called()


# --------------------------------------------------------------------------- #
# 6. invalid_file_id per-item error -> owning request DLQ'd (permanent)
# --------------------------------------------------------------------------- #


@pytest.mark.asyncio
async def test_invalid_file_id_is_permanent_dlq():
    req = _ureq("obj")
    g = _group([(req, [("bad", 1)])])

    def _resp(file_ids):
        return BatchDeleteResult(
            deleted=[],
            errors=[BatchDeleteError(file_id="bad", code="invalid_file_id", message="not hex")],
            files_deleted=0,
        )

    client = _BatchClient(resp_fn=_resp)
    pool = _pool()

    retry, dlq, metrics = await _run_batch(g, client, pool)

    assert _softdeleted(pool) == set(), "permanent per-item error must NOT soft-delete"
    retry.assert_not_called()  # invalid_file_id is permanent — not retried
    assert dlq.push.await_count == 1
    assert dlq.push.await_args.args[0] is req
    assert dlq.push.await_args.args[2] == "permanent"
    assert _success_count(metrics) == 0


# --------------------------------------------------------------------------- #
# 7. Whole-batch failure (timeout/network/500) -> NO soft-deletes; ALL retried
# --------------------------------------------------------------------------- #


@pytest.mark.asyncio
async def test_whole_batch_failure_retries_all_no_softdelete():
    reqs = [_ureq(f"o{i}") for i in range(3)]
    g = _group([(r, [(f"f{i}", i)]) for i, r in enumerate(reqs)])
    client = _BatchClient(raise_exc=asyncio.TimeoutError("arion timed out"))
    pool = _pool()

    retry, dlq, metrics = await _run_batch(g, client, pool)

    assert _softdeleted(pool) == set(), "no soft-deletes when the batch call itself failed"
    assert retry.await_count == 3, "every request in the failed batch is retried (transient)"
    retried_ids = {c.args[0].object_id for c in retry.await_args_list}
    assert retried_ids == {r.object_id for r in reqs}
    dlq.push.assert_not_called()
    assert _success_count(metrics) == 0


# --------------------------------------------------------------------------- #
# 8. Batch endpoint 404 -> fall back to per-file for EVERY request in the group
# --------------------------------------------------------------------------- #


@pytest.mark.asyncio
async def test_404_falls_back_to_per_file():
    req_a = _ureq("oa")
    req_b = _ureq("ob")
    g = _group([(req_a, [("fa", 1)]), (req_b, [("fb", 2)])])
    client = _BatchClient(raise_exc=BatchEndpointUnavailable("no endpoint"))
    # process_unpin_request re-fetches rows per request during the fallback.
    pool = _pool(
        rows_by_object={
            "oa": [{"backend_identifier": "fa", "chunk_id": 1}],
            "ob": [{"backend_identifier": "fb", "chunk_id": 2}],
        }
    )

    retry, dlq, metrics = await _run_batch(g, client, pool)

    assert sorted(client.per_file) == ["fa", "fb"], "per-file unpin_file called for every chunk"
    assert _softdeleted(pool) == {1, 2}, "fallback soft-deletes happen"
    assert _success_count(metrics) == 2
    dlq.push.assert_not_called()


# --------------------------------------------------------------------------- #
# 9. Duplicate file_id (same backend_identifier on 2 chunks across 2 requests)
#    -> sent ONCE; on success BOTH chunk_ids soft-deleted and BOTH requests acked
# --------------------------------------------------------------------------- #


@pytest.mark.asyncio
async def test_duplicate_file_id_deduped_but_both_chunks_and_requests_resolved():
    req_a = _ureq("oa")
    req_b = _ureq("ob")
    # "dup" is the same backend_identifier for chunk 1 (req_a) and chunk 2 (req_b).
    g = _group([(req_a, [("dup", 1)]), (req_b, [("dup", 2)])])
    client = _BatchClient()
    pool = _pool()

    retry, dlq, metrics = await _run_batch(g, client, pool)

    assert len(client.calls) == 1
    assert client.calls[0][0] == ["dup"], "duplicate file_id sent exactly once"
    assert _softdeleted(pool) == {1, 2}, "both chunk_ids behind the deduped file_id are soft-deleted"
    assert _success_count(metrics) == 2, "both owning requests resolved"
    retry.assert_not_called()


# --------------------------------------------------------------------------- #
# 10. Response MISSING a sent file_id (neither deleted nor errors) -> failure;
#     owning request retried, chunk NOT soft-deleted
# --------------------------------------------------------------------------- #


@pytest.mark.asyncio
async def test_missing_file_id_in_response_is_failure():
    req_ok = _ureq("ok")
    req_missing = _ureq("missing")
    g = _group([(req_ok, [("fok", 1)]), (req_missing, [("fmiss", 2)])])
    # Response only mentions fok; fmiss is silently absent (contract violation).
    client = _BatchClient(resp_fn=lambda fids: _all_deleted([f for f in fids if f == "fok"]))
    pool = _pool()

    retry, dlq, metrics = await _run_batch(g, client, pool)

    assert _softdeleted(pool) == {1}, "missing file_id's chunk is never soft-deleted"
    assert _success_count(metrics) == 1
    assert retry.await_count == 1
    assert retry.await_args.args[0] is req_missing
    dlq.push.assert_not_called()


# --------------------------------------------------------------------------- #
# 11. Soft-delete DB failure for a succeeded item -> request retried (A9: never
#     ack a request whose row didn't get marked)
# --------------------------------------------------------------------------- #


@pytest.mark.asyncio
async def test_softdelete_db_failure_retries_request():
    req = _ureq("obj")
    g = _group([(req, [("fa", 1)])])
    client = _BatchClient()
    pool = _pool(fail_chunk_ids={1})  # backend delete succeeds, DB soft-delete blows up

    retry, dlq, metrics = await _run_batch(g, client, pool)

    assert _success_count(metrics) == 0, "request not acked when its soft-delete failed"
    assert retry.await_count == 1
    assert retry.await_args.args[0] is req
    dlq.push.assert_not_called()


# --------------------------------------------------------------------------- #
# 13. >1000 accumulated file_ids -> split into multiple <=1000 batches
# --------------------------------------------------------------------------- #


@pytest.mark.asyncio
async def test_over_1000_file_ids_split_into_1000_caps():
    req = _ureq("obj")
    rows = [(f"f{i}", i) for i in range(2100)]
    g = _group([(req, rows)])
    client = _BatchClient()
    pool = _pool()

    # max_files above the hard cap must be clamped to 1000 by process_unpin_batch.
    retry, dlq, metrics = await _run_batch(g, client, pool, max_files=5000)

    assert [len(c[0]) for c in client.calls] == [1000, 1000, 100]
    assert all(len(c[0]) <= 1000 for c in client.calls)
    assert len(_softdeleted(pool)) == 2100
    assert _success_count(metrics) == 1
    retry.assert_not_called()


# --------------------------------------------------------------------------- #
# 17 (direct). folder_hash default "" is sent in the payload
# --------------------------------------------------------------------------- #


@pytest.mark.asyncio
async def test_folder_hash_empty_default_sent_in_payload():
    req = _ureq("obj")
    g = _group([(req, [("fa", 1)])], folder_hash="")
    client = _BatchClient()
    pool = _pool()

    await _run_batch(g, client, pool)

    assert client.calls[0][2] == "", "empty (root) folder_hash is sent by default"


# =========================================================================== #
# Loop-level tests: grouping, empty identifiers, flag OFF, shutdown ordering.
# =========================================================================== #


def _loop_config(*, batch_enabled=True, batch_max_files=1000, folder_hash="") -> MagicMock:
    cfg = MagicMock()
    cfg.unpinner_max_inflight = 8
    cfg.unpinner_parallelism = 5
    cfg.unpinner_db_pool_max = 16
    cfg.unpinner_max_attempts = 5
    cfg.unpinner_backoff_base_ms = 1
    cfg.unpinner_backoff_max_ms = 10
    cfg.unpinner_batch_delete_enabled = batch_enabled
    cfg.unpinner_batch_max_files = batch_max_files
    cfg.unpinner_folder_hash = folder_hash
    cfg.redis_url = "redis://localhost:6379"
    cfg.redis_queues_url = "redis://localhost:6382"
    cfg.database_url = "postgresql://localhost/test"
    return cfg


class _LoopClient:
    def __init__(self) -> None:
        self.batch_calls: list[tuple[list[str], str, str]] = []
        self.per_file_calls: list[str] = []
        self.block: asyncio.Event | None = None
        self.events: list[str] | None = None

    async def __aenter__(self) -> "_LoopClient":
        return self

    async def __aexit__(self, *a) -> bool:
        if self.events is not None:
            self.events.append("client_closed")
        return False

    async def unpin_files_batch(self, file_ids, account_ss58, folder_hash=""):
        self.batch_calls.append((list(file_ids), account_ss58, folder_hash))
        if self.block is not None:
            try:
                await self.block.wait()
            except asyncio.CancelledError:
                if self.events is not None:
                    self.events.append("task_cancelled")
                raise
        return _all_deleted(list(file_ids))

    async def unpin_file(self, file_id, **kw):
        self.per_file_calls.append(file_id)
        return None


class _BatchHarness:
    def __init__(self, *, config, client=None, rows_by_object=None, fail_chunk_ids=frozenset()) -> None:
        self.config = config
        self.client = client or _LoopClient()
        self.rows_by_object = rows_by_object or {}
        self.fail_chunk_ids = fail_chunk_ids
        self.dequeue_sequence: list = []
        self.softdeleted: list[int] = []
        self.retry_calls: list = []

    async def _dequeue(self, queue_name, block_timeout=3):
        await asyncio.sleep(0)
        if self.dequeue_sequence:
            item = self.dequeue_sequence.pop(0)
            if isinstance(item, BaseException):
                raise item
            return item
        raise KeyboardInterrupt()

    async def _with_redis_retry(self, func, client, url, name, **kw):
        return await func(client), client

    async def run(self) -> None:
        conn = AsyncMock()

        async def _fetch(query, backend, object_id, object_version):
            return self.rows_by_object.get(object_id, [])

        async def _fetchval(query, backend, chunk_id):
            if chunk_id in self.fail_chunk_ids:
                raise RuntimeError("db down")
            self.softdeleted.append(chunk_id)
            return chunk_id

        conn.fetch = AsyncMock(side_effect=_fetch)
        conn.fetchval = AsyncMock(side_effect=_fetchval)
        pool = MagicMock()
        pool.acquire = MagicMock(return_value=MagicMock(__aenter__=AsyncMock(return_value=conn), __aexit__=AsyncMock()))
        pool.close = AsyncMock()

        async def _create_pool(*a, **k):
            return pool

        async def _retry(request, **kw):
            self.retry_calls.append((request, kw))

        with (
            patch.object(un, "get_config", return_value=self.config),
            patch.object(un, "asyncpg") as mock_asyncpg,
            patch.object(un, "create_redis_client", return_value=MagicMock()),
            patch.object(un, "async_redis") as mock_async_redis,
            patch.object(un, "with_redis_retry", side_effect=self._with_redis_retry),
            patch.object(un, "dequeue_unpin_request", side_effect=self._dequeue),
            patch.object(un, "move_due_unpin_retries", new=AsyncMock(return_value=0)),
            patch.object(un, "initialize_metrics_collector"),
            patch.object(un, "get_metrics_collector", return_value=MagicMock()),
            patch.object(un, "enqueue_unpin_retry_request", side_effect=_retry),
            patch.object(un, "UnpinDLQManager", return_value=MagicMock(push=AsyncMock())),
            patch.object(un, "get_logger_with_ray_id", return_value=MagicMock()),
            patch.object(un, "get_query", return_value="SQL"),
            patch("hippius_s3.queue.initialize_queue_client"),
            patch("hippius_s3.redis_cache.initialize_cache_client"),
        ):
            mock_asyncpg.create_pool = AsyncMock(side_effect=_create_pool)
            mock_async_redis.from_url = MagicMock(return_value=MagicMock())
            await un.run_unpinner_loop(
                backend_name="arion",
                backend_client_factory=lambda: self.client,
                queue_name="arion_unpin_requests",
            )


# --------------------------------------------------------------------------- #
# 12. Different ss58 -> separate batch calls (never mixed in one payload)
# --------------------------------------------------------------------------- #


@pytest.mark.asyncio
async def test_different_ss58_never_mixed_in_one_payload():
    req_x = _ureq("oa", address="5X")
    req_y = _ureq("ob", address="5Y")
    h = _BatchHarness(
        config=_loop_config(),
        rows_by_object={
            "oa": [{"backend_identifier": "fa", "chunk_id": 1}],
            "ob": [{"backend_identifier": "fb", "chunk_id": 2}],
        },
    )
    h.dequeue_sequence = [req_x, req_y, None, None, KeyboardInterrupt()]

    await asyncio.wait_for(h.run(), timeout=5.0)

    assert len(h.client.batch_calls) == 2, "one batch call per ss58 group"
    by_ss58 = {c[1]: c[0] for c in h.client.batch_calls}
    assert by_ss58 == {"5X": ["fa"], "5Y": ["fb"]}
    assert sorted(h.softdeleted) == [1, 2]


# --------------------------------------------------------------------------- #
# 14. Empty identifiers -> retry-then-drop preserved; NOT sent to a batch
# --------------------------------------------------------------------------- #


@pytest.mark.asyncio
async def test_empty_identifiers_retry_then_not_batched():
    req = _ureq("empty")  # object_id "empty" is absent from rows_by_object -> fetch returns []
    h = _BatchHarness(config=_loop_config(), rows_by_object={})
    h.dequeue_sequence = [req, None, KeyboardInterrupt()]

    await asyncio.wait_for(h.run(), timeout=5.0)

    assert h.client.batch_calls == [], "a request with no chunk_backend rows is never batched"
    assert len(h.retry_calls) == 1
    assert h.retry_calls[0][1]["last_error"] == "no_chunk_backend_rows"


# --------------------------------------------------------------------------- #
# 15. Feature flag OFF -> per-file path, no batch call at all
# --------------------------------------------------------------------------- #


@pytest.mark.asyncio
async def test_flag_off_uses_per_file_path():
    req = _ureq("oa")
    h = _BatchHarness(
        config=_loop_config(batch_enabled=False),
        rows_by_object={"oa": [{"backend_identifier": "fa", "chunk_id": 1}]},
    )
    h.dequeue_sequence = [req, None, KeyboardInterrupt()]

    await asyncio.wait_for(h.run(), timeout=5.0)

    assert h.client.batch_calls == [], "batch endpoint must never be called when flag is OFF"
    assert h.client.per_file_calls == ["fa"], "per-file path used instead"
    assert h.softdeleted == [1]


# --------------------------------------------------------------------------- #
# 16. Graceful shutdown mid-batch -> in-flight batch task drained BEFORE client
#     closed (same ordering guarantee as the per-file loop)
# --------------------------------------------------------------------------- #


@pytest.mark.asyncio
async def test_shutdown_drains_inflight_batch_before_client_close():
    events: list[str] = []
    client = _LoopClient()
    client.block = asyncio.Event()  # never set -> the batch call blocks
    client.events = events

    req = _ureq("oa")
    h = _BatchHarness(
        config=_loop_config(),
        client=client,
        rows_by_object={"oa": [{"backend_identifier": "fa", "chunk_id": 1}]},
    )
    # dispatch the batch task, let it reach the blocking call, then shut down.
    h.dequeue_sequence = [req, None, None, KeyboardInterrupt()]

    await asyncio.wait_for(h.run(), timeout=5.0)

    assert events == ["task_cancelled", "client_closed"], (
        f"in-flight batch task must be cancelled/drained before the shared client closes: {events}"
    )


# --------------------------------------------------------------------------- #
# 17 (loop). A non-empty configured folder_hash is threaded into the payload
# --------------------------------------------------------------------------- #


@pytest.mark.asyncio
async def test_configured_folder_hash_threaded_into_payload():
    req = _ureq("oa")
    h = _BatchHarness(
        config=_loop_config(folder_hash="abcdef0123456789"),
        rows_by_object={"oa": [{"backend_identifier": "fa", "chunk_id": 1}]},
    )
    h.dequeue_sequence = [req, None, None, KeyboardInterrupt()]

    await asyncio.wait_for(h.run(), timeout=5.0)

    assert len(h.client.batch_calls) == 1
    assert h.client.batch_calls[0][2] == "abcdef0123456789"
