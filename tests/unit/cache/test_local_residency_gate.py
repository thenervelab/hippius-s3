"""The node-local tier is a CACHE, so it may only serve parts this node is recorded as holding.

`get_chunk` used to read the local file and return it with no ownership check at all. Two
concurrent `UploadPart`s for the same part, routed to different api pods, each publish their own
attempt to their own node's flash — correctly and atomically per node. Only one wins the `parts`
row and gets residency and replication rows; the loser's bytes stay on the other node with no
record anywhere, and that node serves them for as long as the file exists. Promotion never
corrects it, because promotion fills a MISSING local copy and never overwrites one already
present. Reproduced live on staging at 4 of 8 attempts.

Nothing downstream can catch it: the AAD binds (bucket, object, part, chunk) and NOT the attempt,
so the unowned bytes decrypt and authenticate perfectly in place, and `meta.json` is byte-identical
across the divergent copies.

The predicate is deliberately NOT "is there a residency row". A freshly ingested part has no row
yet — the drain writes one when it commits — and the pool does not have it either, because that is
the same event. Gating on residency alone would refuse the only copy that exists and break
read-after-write on every upload. Hence: refuse only when the part is REPLICATED and this node is
not recorded as holding it.
"""

from __future__ import annotations

from typing import Any
from typing import Optional

import asyncpg
import pytest

from hippius_s3.cache.local_residency import LocalResidencyGate
from hippius_s3.cache.local_residency import create_local_residency_gate


OBJ = "466916c0-d61b-4518-b81b-9576b574270a"
NODE = "k8s-v3-node2"


class _Pool:
    """Answers the ownership probe with a canned row, or raises a canned error."""

    def __init__(self, row: Optional[dict[str, Any]] = None, raises: Optional[BaseException] = None) -> None:
        self._row = row
        self._raises = raises
        self.calls = 0

    def acquire(self) -> Any:
        pool = self

        class _Ctx:
            async def __aenter__(self) -> Any:
                return _Conn(pool)

            async def __aexit__(self, *_: object) -> None:
                return None

        return _Ctx()


class _Conn:
    def __init__(self, pool: _Pool) -> None:
        self._pool = pool

    async def fetchrow(self, _sql: str, *_args: Any) -> Optional[dict[str, Any]]:
        self._pool.calls += 1
        if self._pool._raises is not None:
            raise self._pool._raises
        return self._pool._row


@pytest.mark.asyncio
async def test_an_unowned_copy_of_a_replicated_part_is_refused() -> None:
    """The bug. Replicated elsewhere, no residency row here — these bytes are the race's loser."""
    gate = LocalResidencyGate(_Pool({"resident": False, "status": "replicated"}), NODE)

    assert await gate.may_serve_local(OBJ, 1, 1) is False


@pytest.mark.asyncio
async def test_a_part_this_node_holds_is_served() -> None:
    """The ordinary case: ingested or promoted here, so there is a residency row."""
    gate = LocalResidencyGate(_Pool({"resident": True, "status": "replicated"}), NODE)

    assert await gate.may_serve_local(OBJ, 1, 1) is True


@pytest.mark.asyncio
@pytest.mark.parametrize("status", ["pending", "draining", "failed", "corrupt"])
async def test_a_not_yet_replicated_part_is_served_even_without_a_residency_row(status: str) -> None:
    """READ-AFTER-WRITE. This is the case that would make the fix worse than the bug.

    The drain writes the residency row when it COMMITS, so between the upload and that commit a
    part has no row — and the pool does not have it either, because they are the same event.
    Refusing here would fall through to a pool that cannot answer, turning every fresh upload
    into a failed read.
    """
    gate = LocalResidencyGate(_Pool({"resident": False, "status": status}), NODE)

    assert await gate.may_serve_local(OBJ, 1, 1) is True


@pytest.mark.asyncio
async def test_a_part_the_drain_has_never_seen_is_served() -> None:
    """No status row at all: it cannot be replicated, so this copy is the only one there is."""
    gate = LocalResidencyGate(_Pool({"resident": False, "status": None}), NODE)

    assert await gate.may_serve_local(OBJ, 1, 1) is True


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "err",
    [
        asyncpg.PostgresError("db down"),
        asyncpg.InterfaceError("pool exhausted"),
        OSError("connection refused"),
    ],
)
async def test_a_probe_failure_serves_locally_rather_than_stampeding_the_pool(err: BaseException) -> None:
    """Fails OPEN on an error, deliberately, and only on an error.

    Failing closed on a DB outage would take every local read on the fleet to the pool at once,
    converting a database blip into a latency and load incident on the tier the drain is also
    writing to. An unowned copy is rare and bounded; that is not.
    """
    gate = LocalResidencyGate(_Pool(raises=err), NODE)

    assert await gate.may_serve_local(OBJ, 1, 1) is True


@pytest.mark.asyncio
async def test_a_probe_failure_is_not_memoised() -> None:
    """Caching an error would extend one blip into a 30s window of decisions taken on nothing."""
    pool = _Pool(raises=asyncpg.PostgresError("blip"))
    gate = LocalResidencyGate(pool, NODE)

    await gate.may_serve_local(OBJ, 1, 1)
    await gate.may_serve_local(OBJ, 1, 1)

    assert pool.calls == 2, "an error must be retried, not cached"


@pytest.mark.asyncio
async def test_a_definitive_answer_is_memoised() -> None:
    """Ownership changes only on a drain commit or an eviction, so a short memo is safe — and it
    is what keeps this off the hot read path for all but the first read of a part per window."""
    pool = _Pool({"resident": False, "status": "replicated"})
    gate = LocalResidencyGate(pool, NODE)

    for _ in range(5):
        assert await gate.may_serve_local(OBJ, 1, 1) is False

    assert pool.calls == 1


@pytest.mark.asyncio
async def test_the_memo_is_per_part_not_per_object() -> None:
    """Two parts of one object can legitimately have different owners — resolution is per PART."""
    pool = _Pool({"resident": False, "status": "replicated"})
    gate = LocalResidencyGate(pool, NODE)

    await gate.may_serve_local(OBJ, 1, 1)
    await gate.may_serve_local(OBJ, 1, 2)

    assert pool.calls == 2


@pytest.mark.asyncio
async def test_a_pre_drain_deployment_disables_the_gate_after_one_probe() -> None:
    """Prod today has no cephor tables. One failed query, then never again."""
    pool = _Pool(raises=asyncpg.UndefinedTableError("relation does not exist"))
    gate = LocalResidencyGate(pool, NODE)

    for _ in range(4):
        assert await gate.may_serve_local(OBJ, 1, 1) is True

    assert pool.calls == 1, "the absence of the tables must be learned once, not per read"


def test_no_pool_or_no_node_identity_means_no_gate() -> None:
    """Workers, scripts and tests have neither. A gate that cannot identify itself would refuse
    every local read, so it must not exist at all rather than exist and deny."""
    assert create_local_residency_gate(None, NODE) is None
    assert create_local_residency_gate(_Pool(), "") is None
    assert create_local_residency_gate(_Pool(), NODE) is not None
