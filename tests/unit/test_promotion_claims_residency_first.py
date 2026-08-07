"""A promoted chunk must be claimed before it is written, or it is unreclaimable.

The drain-agent's evictor is scoped to `cephor_ssd_residency.node_id`, and `ssd_reclaim`
skips `replicated` parts outright as the read tier. So a promoted copy on disk with no
residency row has NO owner in either process: nothing frees it until some later read happens
to promote the same chunk again. It leaks on the very disk whose filling makes the api answer
503 to every PUT.

Writing first and claiming after therefore trades a bounded cost (a skipped cache warm) for
an unbounded one (one unreclaimable copy per promoted chunk, for as long as the residency DB
is unreachable). These tests pin the inverted ordering and the fail-closed behaviour that
follows from it, plus the one property that must never regress: the read still succeeds.
"""

from __future__ import annotations

import pytest

from hippius_s3.cache.dual_fs_store import DualFileSystemPartsStore
from hippius_s3.cache.fs_store import FileSystemPartsStore


OBJ = "466916c0-d61b-4518-b81b-9576b574270a"
PAYLOAD = b"ciphertext-chunk"


class RecordingCollector:
    """Stands in for the real metrics collector, capturing the `reason` label."""

    def __init__(self) -> None:
        self.skips: list[str] = []

    def record_promotion_skipped(self, reason: str) -> None:
        self.skips.append(reason)

    def record_chunk_read_tier(self, tier: str) -> None:
        return None


def _collect_metrics(monkeypatch: pytest.MonkeyPatch) -> RecordingCollector:
    collector = RecordingCollector()
    monkeypatch.setattr("hippius_s3.monitoring.get_metrics_collector", lambda: collector)
    return collector


async def _seed_pool(store: DualFileSystemPartsStore, *chunks: bytes) -> None:
    for index, data in enumerate(chunks):
        await store.fallback.set_chunk(OBJ, 1, 1, index, data)
    await store.fallback.set_meta(
        OBJ, 1, 1, chunk_size=len(chunks[0]), num_chunks=len(chunks), size_bytes=sum(len(c) for c in chunks)
    )


def _store(tmp_path, claim_succeeds: bool, claimed: list[tuple[str, int, int, int]]) -> DualFileSystemPartsStore:
    async def _claim(object_id: str, version: int, part_number: int, size_bytes: int) -> bool:
        claimed.append((object_id, version, part_number, size_bytes))
        return claim_succeeds

    return DualFileSystemPartsStore(str(tmp_path / "ssd"), str(tmp_path / "pool"), promote=True, on_promote=_claim)


@pytest.mark.asyncio
async def test_a_failed_claim_leaves_no_chunk_on_the_local_tier(tmp_path) -> None:
    """Fail closed. An unclaimed copy is a copy nothing in either process can ever free.

    Asserted through the parent's local-only read, because `DualFileSystemPartsStore.get_chunk`
    would happily answer from the pool and hide a local write that did happen.
    """
    claimed: list[tuple[str, int, int, int]] = []
    store = _store(tmp_path, claim_succeeds=False, claimed=claimed)
    await _seed_pool(store, PAYLOAD)

    await store.get_chunk(OBJ, 1, 1, 0)

    assert claimed, "the claim must be attempted, not skipped"
    assert await FileSystemPartsStore.get_chunk(store, OBJ, 1, 1, 0) is None, (
        "a chunk was written that no evictor can see"
    )


@pytest.mark.asyncio
async def test_a_failed_claim_counts_a_residency_failed_promotion_skip(tmp_path, monkeypatch) -> None:
    """The failure that leaks disk must be at least as visible as the one that costs a stamp.

    `read_recency_writes_total{outcome=failed}` already exists for the strictly less
    consequential `last_read_at` write; a dropped claim was a `logger.debug` and nothing else.
    """
    collector = _collect_metrics(monkeypatch)
    store = _store(tmp_path, claim_succeeds=False, claimed=[])
    await _seed_pool(store, PAYLOAD)

    await store.get_chunk(OBJ, 1, 1, 0)

    assert collector.skips == ["residency_failed"], f"skips recorded: {collector.skips}"


@pytest.mark.asyncio
async def test_a_failed_claim_still_serves_the_bytes(tmp_path) -> None:
    """Promotion is an optimisation. It must never turn a served read into a failed one.

    The caller already holds the pool's bytes by the time promotion is attempted, and the pool
    copy is authoritative — so a residency outage costs a cache warm and nothing else.
    """
    store = _store(tmp_path, claim_succeeds=False, claimed=[])
    await _seed_pool(store, PAYLOAD)

    assert await store.get_chunk(OBJ, 1, 1, 0) == PAYLOAD


@pytest.mark.asyncio
async def test_a_successful_claim_writes_the_chunk_and_claims_only_its_own_bytes(tmp_path) -> None:
    """The no-regression half: claim-first must not disable the tier it protects.

    Each chunk still reports the bytes IT wrote rather than the part's declared total — a range
    GET promotes only the chunks it touches, and over-stating the part inflates the figure the
    evictor sums against its deficit.
    """
    claimed: list[tuple[str, int, int, int]] = []
    store = _store(tmp_path, claim_succeeds=True, claimed=claimed)
    first, second = b"chunk-aaaa", b"chunk-bb"
    await _seed_pool(store, first, second)

    assert await store.get_chunk(OBJ, 1, 1, 0) == first
    assert await store.get_chunk(OBJ, 1, 1, 1) == second

    assert claimed == [(OBJ, 1, 1, len(first)), (OBJ, 1, 1, len(second))]
    assert await FileSystemPartsStore.get_chunk(store, OBJ, 1, 1, 0) == first
    assert await FileSystemPartsStore.get_chunk(store, OBJ, 1, 1, 1) == second
