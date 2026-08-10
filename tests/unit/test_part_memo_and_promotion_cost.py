"""Regressions for three per-chunk costs that should be per-part.

All three were found reviewing the SSD read-tier work: the read path calls into these once
per CHUNK, but each answer is a property of the PART. Left alone they turn a 64-chunk part
into 64 Postgres round-trips, 64 Redis GETs, or 64 fsync pairs — on a tier that exists to
save ~34 ms per chunk.

The residency one is not merely a cost: a permanent "already recorded" memo silently skips
re-recording a part that was promoted, evicted, and promoted again, leaving a copy on disk
that this node's evictor can never see.
"""

from __future__ import annotations

import asyncio
import shutil

import pytest

from hippius_s3.cache.dual_fs_store import DualFileSystemPartsStore
from hippius_s3.cache.fs_store import FileSystemPartsStore
from hippius_s3.cache.part_memo import PartMemo
from hippius_s3.cache.residency import ResidencyRecorder


OBJ = "466916c0-d61b-4518-b81b-9576b574270a"


class FakeConn:
    def __init__(self, log: list[tuple[object, ...]], sql_log: list[str]) -> None:
        self._log = log
        self._sql_log = sql_log

    async def execute(self, sql: str, *args: object) -> None:
        # The SQL is recorded, not discarded. It used to be (`_sql`), and that is exactly how a
        # commit which deleted the residency UPSERT's `DO UPDATE` clause passed this suite: a
        # mock accepts any string, so a statement Postgres would reject looked fine here.
        self._sql_log.append(sql)
        self._log.append(args)


class FakePool:
    def __init__(self) -> None:
        self.executed: list[tuple[object, ...]] = []
        self.statements: list[str] = []

    def acquire(self):  # noqa: ANN201 - async context manager double
        conn = FakeConn(self.executed, self.statements)

        class _Ctx:
            async def __aenter__(self):  # noqa: ANN204
                return conn

            async def __aexit__(self, *_: object) -> None:
                return None

        return _Ctx()


def test_the_memo_forgets_after_its_ttl() -> None:
    """Expiry is the whole point: anything the evictor can undo must be re-derivable."""
    memo: PartMemo[str, str] = PartMemo(ttl_seconds=60.0, max_entries=10)
    memo.put("part", "node-b", now=1_000.0)

    assert memo.get("part", now=1_030.0) == "node-b", "still fresh"
    assert memo.get("part", now=1_061.0) is None, "expired, so the caller re-derives it"


def test_the_memo_is_bounded() -> None:
    """A long-lived api pod serving a large working set must not grow this without limit."""
    memo: PartMemo[int, int] = PartMemo(ttl_seconds=60.0, max_entries=3)
    for i in range(10):
        memo.put(i, i, now=1_000.0)

    live = [i for i in range(10) if memo.get(i, now=1_000.0) is not None]
    assert len(live) <= 3, f"kept {live}"
    assert 9 in live, "the newest entry survives"


@pytest.mark.asyncio
async def test_promotion_after_eviction_restores_both_meta_and_residency(tmp_path) -> None:
    """The cross-process leak: the evictor is a DIFFERENT process from the promoter.

    api-local holds the promotion state; drain-agent's evictor unlinks the part dir (meta
    included) and deletes the residency row. It cannot invalidate an in-process memo, so a
    memo that says "already did this part" survives the eviction and the next promotion
    writes chunks with NO meta and NO residency row.

    That is doubly broken: meta is the readiness gate, so the copy is unreadable; and the
    evictor is scoped entirely to the residency table, so nothing can ever reclaim it. The
    reclaimer is not a backstop either — this branch made it skip `replicated` parts outright.
    """
    recorded: list[tuple[str, int, int, int]] = []

    async def _record(object_id: str, version: int, part_number: int, size_bytes: int) -> bool:
        recorded.append((object_id, version, part_number, size_bytes))
        return True

    dual = DualFileSystemPartsStore(str(tmp_path / "ssd"), str(tmp_path / "pool"), promote=True, on_promote=_record)
    await dual.fallback.set_chunk(OBJ, 1, 1, 0, b"payload")
    await dual.fallback.set_meta(OBJ, 1, 1, chunk_size=7, num_chunks=1, size_bytes=7)

    assert await dual.get_chunk(OBJ, 1, 1, 0) == b"payload"
    local = FileSystemPartsStore(str(tmp_path / "ssd"))
    assert await local.get_meta(OBJ, 1, 1) is not None
    assert len(recorded) == 1

    # The evictor, out of process: unlink the whole part dir and drop the residency row.
    shutil.rmtree(tmp_path / "ssd" / OBJ)
    recorded.clear()

    assert await dual.get_chunk(OBJ, 1, 1, 0) == b"payload"

    assert await local.get_meta(OBJ, 1, 1) is not None, "meta must be rewritten or the copy is unreadable"
    assert await local.get_chunk(OBJ, 1, 1, 0) == b"payload", "and the chunk must be readable again"
    assert recorded, "residency must be re-recorded or nothing can ever evict this copy"


@pytest.mark.asyncio
async def test_residency_accumulates_the_bytes_actually_promoted() -> None:
    """`bytes` must track what is ON the disk, not the part's declared total.

    Promotion fires per chunk, and a range GET touches only the chunks it needs — so a
    64-chunk part can be 1/64th resident. Recording the whole-part size on the first chunk
    inflates the number the evictor sums to decide it has freed enough, which makes a pass
    stop early while believing it succeeded.
    """
    pool = FakePool()
    recorder = ResidencyRecorder(pool, "node-a")

    for _ in range(3):
        await recorder(OBJ, 1, 3, 4096)

    assert len(pool.executed) == 3, "every promoted chunk is accounted, not just the first"
    assert all(args[4] == 4096 for args in pool.executed), "each records its own chunk's bytes"


@pytest.mark.asyncio
async def test_promoting_a_part_writes_its_meta_once_not_once_per_chunk(tmp_path, monkeypatch) -> None:
    """`set_meta` is a tmp-write + file fsync + rename + dir fsync.

    Paying that per chunk can cost more than the pool read promotion is meant to avoid,
    making the first read of a large part slower rather than faster.
    """
    dual = DualFileSystemPartsStore(str(tmp_path / "ssd"), str(tmp_path / "pool"), promote=True)
    chunks = [f"chunk-{i}".encode() for i in range(8)]
    for index, data in enumerate(chunks):
        await dual.fallback.set_chunk(OBJ, 1, 1, index, data)
    await dual.fallback.set_meta(OBJ, 1, 1, chunk_size=8, num_chunks=len(chunks), size_bytes=64)

    calls = {"n": 0}
    original = FileSystemPartsStore.set_meta

    async def _counting(self, *args, **kwargs):  # noqa: ANN001, ANN202
        calls["n"] += 1
        return await original(self, *args, **kwargs)

    monkeypatch.setattr(FileSystemPartsStore, "set_meta", _counting)

    for index in range(len(chunks)):
        assert await dual.get_chunk(OBJ, 1, 1, index) == chunks[index]

    assert calls["n"] == 1, f"8 promoted chunks wrote meta {calls['n']} times"


@pytest.mark.asyncio
async def test_concurrent_readers_promote_a_chunk_only_once(tmp_path) -> None:
    """Two readers missing the same cold chunk must not both promote it.

    Raised in review as merely wasteful — duplicate writes are safe, since `set_chunk` is
    tmp+rename. It stopped being merely wasteful once the residency upsert became
    accumulating: a duplicate promotion now DOUBLE-COUNTS the chunk's bytes, inflating the
    figure the evictor sums against its deficit, so a pass frees less than it believes while
    reporting success. Same failure class as recording the whole part's size per chunk.

    The guard skips rather than waits: the caller already holds the bytes, so waiting on
    another reader's write would add latency for no benefit to this request.
    """
    recorded: list[tuple[str, int, int, int]] = []
    gate = asyncio.Event()

    async def _record(object_id: str, version: int, part_number: int, size_bytes: int) -> bool:
        await gate.wait()
        recorded.append((object_id, version, part_number, size_bytes))
        return True

    dual = DualFileSystemPartsStore(str(tmp_path / "ssd"), str(tmp_path / "pool"), promote=True, on_promote=_record)
    await dual.fallback.set_chunk(OBJ, 1, 1, 0, b"payload")
    await dual.fallback.set_meta(OBJ, 1, 1, chunk_size=7, num_chunks=1, size_bytes=7)

    both = asyncio.gather(dual.get_chunk(OBJ, 1, 1, 0), dual.get_chunk(OBJ, 1, 1, 0))
    await asyncio.sleep(0)
    gate.set()
    assert await both == [b"payload", b"payload"], "both readers still get their bytes"

    assert len(recorded) == 1, f"the chunk was promoted {len(recorded)} times"


@pytest.mark.asyncio
async def test_the_residency_upsert_has_a_conflict_action() -> None:
    """`ON CONFLICT` without an action is a syntax error, and the failure is invisible.

    This is asserted on the SQL text rather than on behaviour because a mocked connection
    accepts any string — which is how a commit that replaced the `DO UPDATE SET` line with a
    comment explaining it passed the whole suite. Postgres would reject the statement, every
    claim would fall into the best-effort `except`, and promoted chunks would land on disk with
    no residency row: unowned by the node-scoped evictor, skipped by `ssd_reclaim` as read
    tier, and therefore a permanent SSD leak. Reads keep succeeding the entire time.

    Both halves are pinned: that a conflict action exists at all, and that it ACCUMULATES —
    promotion learns a part one chunk at a time, so overwriting would report only the last
    chunk's bytes to the evictor.
    """
    pool = FakePool()
    await ResidencyRecorder(pool, "node-a")(OBJ, 1, 1, 4096)

    sql = pool.statements[0]
    normalised = " ".join(sql.split())
    assert "ON CONFLICT" in normalised
    assert "DO UPDATE SET bytes = cephor_ssd_residency.bytes + EXCLUDED.bytes" in normalised, (
        f"the conflict action is missing or no longer accumulates: {normalised}"
    )


@pytest.mark.asyncio
async def test_the_release_statement_subtracts_the_claimed_bytes_with_a_zero_floor() -> None:
    """Pinned as text for the same reason as the UPSERT above: `release` swallows every DB
    failure by design, so a statement Postgres rejects passes a mocked suite while the phantom
    bytes it exists to remove keep accumulating. The zero floor matters too — the evictor SUMS
    this column against its deficit, and a negative row corrupts that sum, while an under-count
    only costs one extra eviction candidate.
    """
    pool = FakePool()
    await ResidencyRecorder(pool, "node-a").release(OBJ, 1, 1, 4096)

    normalised = " ".join(pool.statements[0].split())
    assert "GREATEST(cephor_ssd_residency.bytes - $5, 0)" in normalised, (
        f"the release no longer subtracts, or lost its floor: {normalised}"
    )
    assert pool.executed[0] == ("node-a", OBJ, 1, 1, 4096), "the release must target exactly the claimed key"
