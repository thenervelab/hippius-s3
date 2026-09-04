"""Serving a part locally has to tell the evictor, or eviction stays FIFO.

The drain-agent evicts on `COALESCE(last_read_at, resident_at)`. Nothing fills `last_read_at`
except this, so if it stops working the ordering silently degrades to arrival order — which for
a working set re-read every epoch tends to evict exactly the parts about to be read again, each
costing a peer-or-pool read plus a local write to restore. The failure is invisible: reads still
succeed, just slower and against the wrong tier.
"""

from __future__ import annotations

import asyncio

import asyncpg
import pytest

from hippius_s3.cache.dual_fs_store import DualFileSystemPartsStore
from hippius_s3.cache.read_recency import ReadRecencyRecorder
from hippius_s3.cache.read_recency import create_read_recency_recorder


OBJ = "466916c0-d61b-4518-b81b-9576b574270a"


class FakeConn:
    def __init__(self, sink: list[tuple], fail: bool) -> None:
        self._sink = sink
        self._fail = fail

    async def execute(self, sql: str, *args: object, timeout: float | None = None) -> None:
        if self._fail:
            raise asyncpg.PostgresError("db down")
        self._sink.append((sql, args))

    async def __aenter__(self) -> "FakeConn":
        return self

    async def __aexit__(self, *_a: object) -> None:
        return None


class FakePool:
    def __init__(self, fail: bool = False) -> None:
        self.executed: list[tuple] = []
        self.acquire_timeouts: list[float | None] = []
        self.fail = fail

    def acquire(self, *, timeout: float | None = None) -> FakeConn:  # noqa: ASYNC109 (mirrors asyncpg pool.acquire)
        self.acquire_timeouts.append(timeout)
        return FakeConn(self.executed, self.fail)


@pytest.mark.asyncio
async def test_a_local_read_stamps_this_nodes_row_only() -> None:
    """Recency is per (node, part).

    Stamping a peer's row would protect a copy on a disk this node cannot see while leaving its
    own unprotected — and the evictor reading the column is scoped to its own node_id, so the
    stamp would simply be invisible to the node that needed it.
    """
    pool = FakePool()
    await ReadRecencyRecorder(pool, "node-a")(OBJ, 3, 7)

    sql, args = pool.executed[0]
    assert "last_read_at = now()" in sql
    assert "node_id = $1" in sql, "the update must be node-scoped"
    assert args == ("node-a", OBJ, 3, 7)


@pytest.mark.asyncio
async def test_repeat_reads_inside_the_window_cost_no_writes() -> None:
    """THE sampling property. This sits on the read path.

    A DB write per chunk read would cost far more than the eviction it prevents, and a hot part
    is read constantly by definition — so the write rate has to be bounded by the sampling
    window, not by read throughput. The evictor orders at hour scale, so minutes of staleness
    cannot change which part it picks.
    """
    pool = FakePool()
    recorder = ReadRecencyRecorder(pool, "node-a")

    for _ in range(500):
        await recorder(OBJ, 1, 1)

    assert len(pool.executed) == 1, f"500 reads produced {len(pool.executed)} writes"


@pytest.mark.asyncio
async def test_different_parts_are_sampled_independently() -> None:
    """Sampling must not let one hot part suppress recency for every other part on the node."""
    pool = FakePool()
    recorder = ReadRecencyRecorder(pool, "node-a")

    await recorder(OBJ, 1, 1)
    await recorder(OBJ, 1, 2)
    await recorder(OBJ, 2, 1)

    assert len(pool.executed) == 3


@pytest.mark.asyncio
async def test_a_db_outage_never_reaches_the_read() -> None:
    """The chunk is already served when this runs. Losing the stamp means the part is evicted
    somewhat earlier than it deserves — exactly the FIFO behaviour this replaces — which is a
    far better outcome than failing a read that succeeded."""
    await ReadRecencyRecorder(FakePool(fail=True), "node-a")(OBJ, 1, 1)


@pytest.mark.asyncio
async def test_a_closing_pool_never_reaches_the_caller() -> None:
    """asyncpg.InterfaceError is neither PostgresError nor OSError, and a closing or
    uninitialised pool raises exactly it at acquire time. write_meta awaits this recorder
    bare, so an escape here fails the client PUT and skips the landed announcement — the
    regression this test pins closed."""

    class ClosingPool:
        def acquire(self) -> FakeConn:
            raise asyncpg.InterfaceError("pool is closing")

    await ReadRecencyRecorder(ClosingPool(), "node-a")(OBJ, 1, 1)


def test_no_node_identity_means_no_recorder() -> None:
    """Without a node there is no way to say whose copy was read, and the evictor is
    node-scoped — a stamp would either miss or land on another node's row."""
    assert create_read_recency_recorder(FakePool(), "") is None
    assert create_read_recency_recorder(None, "node-a") is None


@pytest.mark.asyncio
async def test_only_a_local_hit_records_recency(tmp_path) -> None:
    """Recency means "this node's copy was used".

    A peer- or pool-served read has no local copy to protect: promotion is what creates one, and
    it stamps residency separately. Recording recency for a part this node does not hold would
    order the evictor on reads of somebody else's disk.
    """
    primary = tmp_path / "local"
    fallback = tmp_path / "pool"
    primary.mkdir()
    fallback.mkdir()

    seen: list[tuple] = []

    async def on_local_read(*args: object) -> None:
        seen.append(args)

    payload = b"ciphertext"
    store = DualFileSystemPartsStore(str(primary), str(fallback), on_local_read=on_local_read)

    # Pool-only: served from the fallback, so no local copy exists to keep.
    await store.fallback.set_meta(OBJ, 1, 1, chunk_size=10, num_chunks=1, size_bytes=10)
    await store.fallback.set_chunk(OBJ, 1, 1, 0, payload)
    assert await store.get_chunk(OBJ, 1, 1, 0) == payload
    assert seen == [], "a pool read recorded local recency"

    # Now the same chunk on local flash.
    await store.set_meta(OBJ, 1, 1, chunk_size=10, num_chunks=1, size_bytes=10)
    await store.set_chunk(OBJ, 1, 1, 0, payload)
    assert await store.get_chunk(OBJ, 1, 1, 0) == payload
    assert seen == [(OBJ, 1, 1)], "a local hit must record recency"


@pytest.mark.asyncio
async def test_a_stamp_is_counted_so_the_write_rate_is_visible(monkeypatch) -> None:
    """The recency write is a DB UPDATE on the read path, and it is silent by design.

    It is sampled to at most one write per part per window, so the rate is bounded by DISTINCT
    parts read per window rather than by read throughput. That bound is weakest for exactly the
    workload this tier exists for: a full-shard scan touches far more distinct parts than the
    sampler's memo holds, so the memo stops absorbing and every part read becomes a write.

    `failed` is counted separately because the write is best-effort and swallowed — without it,
    a recency path erroring on every read looks identical to one the sampler is fully absorbing.
    """
    seen: list[str] = []
    monkeypatch.setattr("hippius_s3.cache.read_recency._record_write", seen.append)

    pool = FakePool()
    recorder = ReadRecencyRecorder(pool, "node-a")

    await recorder(OBJ, 1, 3)
    assert seen == ["written"], "a stamp that reaches the database is counted"

    # Same part inside the sampling window: absorbed by the memo, so no write and no count.
    await recorder(OBJ, 1, 3)
    assert seen == ["written"], "a sampled-out read costs neither a write nor a count"

    failing = ReadRecencyRecorder(FakePool(fail=True), "node-a")
    await failing(OBJ, 1, 9)
    assert seen == ["written", "failed"], "a swallowed failure is still counted, under its own outcome"


# ------------------------------------------------------------------------ touch_parts (bulk stamp)


@pytest.mark.asyncio
async def test_touch_parts_issues_one_node_scoped_update_for_all_parts() -> None:
    """A multi-part read stamps every part in ONE statement, not one round trip per part."""
    pool = FakePool()
    await ReadRecencyRecorder(pool, "node-a").touch_parts(OBJ, 3, [1, 2, 3])

    assert len(pool.executed) == 1
    sql, args = pool.executed[0]
    assert "last_read_at = now()" in sql
    assert "node_id = $1" in sql, "the update must be node-scoped"
    assert "part_number = ANY($4::bigint[])" in sql, "the column is BIGINT; no cross-type compare"
    assert args == ("node-a", OBJ, 3, [1, 2, 3])


@pytest.mark.asyncio
async def test_touch_parts_sends_only_the_parts_the_memo_has_not_seen() -> None:
    """The bulk stamp shares the per-chunk sampler's memo, so a part the store just stamped is
    left out of the array — and once every part is memoised nothing is written at all."""
    pool = FakePool()
    recorder = ReadRecencyRecorder(pool, "node-a")

    await recorder(OBJ, 1, 2)
    await recorder.touch_parts(OBJ, 1, [1, 2, 3])
    assert len(pool.executed) == 2
    assert pool.executed[1][1] == ("node-a", OBJ, 1, [1, 3])

    await recorder.touch_parts(OBJ, 1, [1, 2, 3])
    assert len(pool.executed) == 2, "every part inside the window costs no write"

    # And the bulk stamp memoises for the per-chunk path in turn.
    await recorder(OBJ, 1, 3)
    assert len(pool.executed) == 2


@pytest.mark.asyncio
async def test_touch_parts_db_outage_never_reaches_the_caller() -> None:
    await ReadRecencyRecorder(FakePool(fail=True), "node-a").touch_parts(OBJ, 1, [1, 2])


@pytest.mark.asyncio
async def test_touch_parts_re_stamps_once_the_window_has_expired(monkeypatch) -> None:
    """The memo is a sampler, not a "done" set: a part evicted and re-promoted inside a long-lived
    pod must be stampable again once its window lapses, or it would look cold forever."""
    clock = {"t": 1000.0}
    monkeypatch.setattr("hippius_s3.cache.part_memo.time.monotonic", lambda: clock["t"])
    pool = FakePool()
    recorder = ReadRecencyRecorder(pool, "node-a")

    await recorder.touch_parts(OBJ, 1, [1, 2])
    clock["t"] += 299.0
    await recorder.touch_parts(OBJ, 1, [1, 2])
    assert len(pool.executed) == 1, "inside the window the bulk stamp is fully absorbed"

    clock["t"] += 2.0
    await recorder.touch_parts(OBJ, 1, [1, 2])
    assert len(pool.executed) == 2
    assert pool.executed[1][1] == ("node-a", OBJ, 1, [1, 2])


@pytest.mark.asyncio
async def test_touch_parts_re_stamps_a_part_the_memo_cap_evicted(monkeypatch) -> None:
    """Over-evicting the memo costs one redundant UPDATE, never a lost stamp."""
    monkeypatch.setattr("hippius_s3.cache.read_recency._SAMPLE_ENTRIES", 2)
    pool = FakePool()
    recorder = ReadRecencyRecorder(pool, "node-a")

    await recorder.touch_parts(OBJ, 1, [1, 2, 3])
    await recorder.touch_parts(OBJ, 1, [1, 2, 3])
    # A memo smaller than the plan thrashes (oldest-first): every part is written again, which is
    # the redundant UPDATE the cap is allowed to cost — a lost stamp is what it may not cost.
    assert len(pool.executed) == 2
    assert pool.executed[1][1] == ("node-a", OBJ, 1, [1, 2, 3])


@pytest.mark.asyncio
async def test_touch_parts_dedupes_repeated_and_string_part_numbers() -> None:
    pool = FakePool()
    await ReadRecencyRecorder(pool, "node-a").touch_parts(OBJ, "2", [3, "3", 1])  # type: ignore[arg-type,list-item]
    assert pool.executed[0][1] == ("node-a", OBJ, 2, [3, 1])


@pytest.mark.asyncio
async def test_touch_parts_with_nothing_to_touch_costs_no_round_trip() -> None:
    pool = FakePool()
    await ReadRecencyRecorder(pool, "node-a").touch_parts(OBJ, 1, [])
    assert pool.executed == []
    assert pool.acquire_timeouts == [], "an empty touch must not even take a connection"


@pytest.mark.asyncio
async def test_touch_parts_failure_is_counted_like_a_single_stamp(monkeypatch) -> None:
    seen: list[str] = []
    monkeypatch.setattr("hippius_s3.cache.read_recency._record_write", seen.append)

    await ReadRecencyRecorder(FakePool(), "node-a").touch_parts(OBJ, 1, [1, 2])
    await ReadRecencyRecorder(FakePool(fail=True), "node-a").touch_parts(OBJ, 1, [1, 2])
    assert seen == ["written", "failed"], "one statement, one count — whatever the part count"


@pytest.mark.asyncio
async def test_every_stamp_bounds_its_wait_on_the_pool() -> None:
    """Both stamps sit on the read path — ahead of the first chunk in read_response, and inside
    the peer-serve in-flight slot — so a saturated pool has to cost a lost sample, not a stalled
    read. asyncpg honours `acquire(timeout=)`; this pins that the recorder asks for it."""
    from hippius_s3.cache.read_recency import _STAMP_TIMEOUT_SECONDS

    pool = FakePool()
    recorder = ReadRecencyRecorder(pool, "node-a")
    await recorder(OBJ, 1, 1)
    await recorder.touch_parts(OBJ, 1, [2, 3])
    assert pool.acquire_timeouts == [_STAMP_TIMEOUT_SECONDS, _STAMP_TIMEOUT_SECONDS]


@pytest.mark.asyncio
async def test_a_pool_that_times_out_on_acquire_never_reaches_the_caller() -> None:
    """What asyncpg raises when the bounded acquire lapses is asyncio.TimeoutError — neither a
    PostgresError nor an OSError, so the broad except is what keeps the read alive."""

    class SaturatedPool:
        def acquire(self, *, timeout: float | None = None) -> FakeConn:  # noqa: ASYNC109
            raise asyncio.TimeoutError()

    recorder = ReadRecencyRecorder(SaturatedPool(), "node-a")
    await recorder(OBJ, 1, 1)
    await recorder.touch_parts(OBJ, 1, [2, 3])
