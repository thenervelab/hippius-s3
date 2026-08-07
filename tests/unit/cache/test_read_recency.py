"""Serving a part locally has to tell the evictor, or eviction stays FIFO.

The drain-agent evicts on `COALESCE(last_read_at, resident_at)`. Nothing fills `last_read_at`
except this, so if it stops working the ordering silently degrades to arrival order — which for
a working set re-read every epoch tends to evict exactly the parts about to be read again, each
costing a peer-or-pool read plus a local write to restore. The failure is invisible: reads still
succeed, just slower and against the wrong tier.
"""

from __future__ import annotations

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

    async def execute(self, sql: str, *args: object) -> None:
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
        self.fail = fail

    def acquire(self) -> FakeConn:
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
