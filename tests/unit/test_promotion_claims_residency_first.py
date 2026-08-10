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

Claim-first has a failure path of its own: the claim ACCUMULATES bytes on conflict, so a claim
whose disk write then fails must be subtracted back, or every retried promotion against a
persistently failing disk adds the same phantom bytes again — inflating node_cache_bytes and
the allocator's weights without bound. The release tests pin that compensation.
"""

from __future__ import annotations

import pytest

from hippius_s3.cache.dual_fs_store import DualFileSystemPartsStore
from hippius_s3.cache.fs_store import FileSystemPartsStore
from hippius_s3.cache.residency import ResidencyRecorder


OBJ = "466916c0-d61b-4518-b81b-9576b574270a"
PAYLOAD = b"ciphertext-chunk"


class RecordingCollector:
    """Stands in for the real metrics collector, capturing the `reason` label."""

    def __init__(self) -> None:
        self.skips: list[str] = []
        self.release_failures = 0

    def record_promotion_skipped(self, reason: str) -> None:
        self.skips.append(reason)

    def record_chunk_read_tier(self, tier: str) -> None:
        return None

    def record_residency_release_failure(self) -> None:
        self.release_failures += 1


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


class AccumulatingRecorder:
    """A ledger with the real recorder's conflict semantics: claims ADD, releases subtract.

    The unit seam is the accumulation itself — the property under test is that the dual store's
    claim/release pairing leaves the accumulated figure where it started, which a return-value
    stub cannot express. The SQL that implements these semantics is pinned separately
    (`test_part_memo_and_promotion_cost.py`, `tests/integration/test_residency_upsert_sql.py`).
    """

    def __init__(self) -> None:
        self.bytes = 0
        self.claims = 0
        self.releases = 0

    async def __call__(self, object_id: str, object_version: int, part_number: int, size_bytes: int) -> bool:
        self.claims += 1
        self.bytes += size_bytes
        return True

    async def release(self, object_id: str, object_version: int, part_number: int, size_bytes: int) -> None:
        self.releases += 1
        self.bytes = max(self.bytes - size_bytes, 0)


def _failing_disk_store(tmp_path, recorder: AccumulatingRecorder, monkeypatch) -> DualFileSystemPartsStore:
    store = DualFileSystemPartsStore(
        str(tmp_path / "ssd"),
        str(tmp_path / "pool"),
        promote=True,
        on_promote=recorder,
        on_promote_release=recorder.release,
    )

    async def _no_space(*_: object, **__: object) -> None:
        raise OSError(28, "No space left on device")

    monkeypatch.setattr(store, "set_chunk", _no_space)
    return store


@pytest.mark.asyncio
async def test_a_write_that_fails_after_a_successful_claim_releases_the_same_bytes(tmp_path, monkeypatch) -> None:
    """The claim's other half. A claim for bytes that never landed must be subtracted back,
    because nothing else ever will: the evictor trusts the row, and the disk holds nothing.
    The read itself must still be served — promotion stays an optimisation on its failure
    path too."""
    recorder = AccumulatingRecorder()
    store = _failing_disk_store(tmp_path, recorder, monkeypatch)
    await _seed_pool(store, PAYLOAD)

    assert await store.get_chunk(OBJ, 1, 1, 0) == PAYLOAD

    assert recorder.claims == 1
    assert recorder.releases == 1
    assert recorder.bytes == 0, "the claim outlived the write it paid for"


@pytest.mark.asyncio
async def test_retried_promotions_against_a_failing_disk_do_not_inflate_residency(tmp_path, monkeypatch) -> None:
    """The unbounded arm. The claim accumulates on conflict, so without the release every
    retry adds the chunk's bytes AGAIN — a persistently failing disk grows phantom bytes
    linearly in reads, and node_cache_bytes drives DiskPressure and the allocator weights."""
    recorder = AccumulatingRecorder()
    store = _failing_disk_store(tmp_path, recorder, monkeypatch)
    await _seed_pool(store, PAYLOAD)

    for _ in range(5):
        assert await store.get_chunk(OBJ, 1, 1, 0) == PAYLOAD

    assert recorder.claims == 5, "every retry re-claims — the in-flight guard drains between reads"
    assert recorder.releases == 5
    assert recorder.bytes == 0, f"phantom bytes accumulated across retries: {recorder.bytes}"


@pytest.mark.asyncio
async def test_a_write_that_lands_keeps_its_claim(tmp_path) -> None:
    """Compensation must fire only on failure — releasing a landed write would leave a real
    copy under-accounted, and the evictor would credit less than an eviction actually frees."""
    recorder = AccumulatingRecorder()
    store = DualFileSystemPartsStore(
        str(tmp_path / "ssd"),
        str(tmp_path / "pool"),
        promote=True,
        on_promote=recorder,
        on_promote_release=recorder.release,
    )
    await _seed_pool(store, PAYLOAD)

    assert await store.get_chunk(OBJ, 1, 1, 0) == PAYLOAD

    assert (recorder.claims, recorder.releases) == (1, 0)
    assert recorder.bytes == len(PAYLOAD)


@pytest.mark.asyncio
async def test_a_release_that_fails_is_logged_and_counted_but_never_raises(tmp_path, monkeypatch, caplog) -> None:
    """The release runs on promotion's failure path, where raising fails a read that already
    holds its bytes. Swallowing is bounded harm — reaching it takes the DB failing inside the
    promotion whose claim just succeeded on the same pool — but it must not be SILENT harm:
    each occurrence is phantom bytes the evictor will account against a disk not holding them,
    so a warning and a counter are the only trace left."""
    collector = _collect_metrics(monkeypatch)

    class _DownPool:
        def acquire(self) -> None:
            raise OSError("connection refused")

    with caplog.at_level("WARNING", logger="hippius_s3.cache.residency"):
        await ResidencyRecorder(_DownPool(), "node-a").release(OBJ, 1, 1, 4096)  # type: ignore[arg-type]

    assert collector.release_failures == 1
    assert any("releasing a residency claim failed" in record.message for record in caplog.records)
