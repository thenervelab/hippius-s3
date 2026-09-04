"""DualFileSystemPartsStore read-fallback coverage for the batch existence check.

Under the drain-direct model the drain copies a part to the shared pool (the fallback
tier) and unlinks the node-local SSD copy (the primary). The read path decides
cache-vs-pipeline via `chunks_exist_batch`; if that batch check ignores the fallback,
a part that is durably present on the pool is wrongly fetched through the download
pipeline (observed as x-hippius-source=pipeline in the append e2e tests).
"""

from __future__ import annotations

import pytest

from hippius_s3.cache.dual_fs_store import DualFileSystemPartsStore
from hippius_s3.cache.fs_store import FileSystemPartsStore


OBJ = "466916c0-d61b-4518-b81b-9576b574270a"


async def _write_part(store: FileSystemPartsStore, *, part_number: int, chunk: bytes) -> None:
    await store.set_chunk(OBJ, 1, part_number, 0, chunk)
    await store.set_meta(OBJ, 1, part_number, chunk_size=len(chunk), num_chunks=1, size_bytes=len(chunk))


@pytest.mark.asyncio
async def test_chunks_exist_batch_consults_the_fallback(tmp_path) -> None:
    dual = DualFileSystemPartsStore(str(tmp_path / "ssd"), str(tmp_path / "pool"))
    # The part lives ONLY on the fallback (pool) — the drain unlinked the primary SSD
    # copy after replicating. The batch check must still report it present.
    await _write_part(dual.fallback, part_number=1, chunk=b"pool-only")
    assert await dual.chunks_exist_batch(OBJ, 1, [(1, 0)]) == [True]


@pytest.mark.asyncio
async def test_chunks_exist_batch_primary_hit_and_genuine_miss(tmp_path) -> None:
    dual = DualFileSystemPartsStore(str(tmp_path / "ssd"), str(tmp_path / "pool"))
    # Written to the primary (dual IS the primary store); part 2 is in neither tier.
    await _write_part(dual, part_number=1, chunk=b"on-ssd")
    assert await dual.chunks_exist_batch(OBJ, 1, [(1, 0), (2, 0)]) == [True, False]


@pytest.mark.asyncio
async def test_chunks_exist_batch_mixed_primary_and_fallback(tmp_path) -> None:
    dual = DualFileSystemPartsStore(str(tmp_path / "ssd"), str(tmp_path / "pool"))
    await _write_part(dual, part_number=1, chunk=b"on-ssd")  # primary
    await _write_part(dual.fallback, part_number=2, chunk=b"on-pool")  # fallback
    # Order preserved; each found in whichever tier holds it; a third is a true miss.
    assert await dual.chunks_exist_batch(OBJ, 1, [(1, 0), (2, 0), (3, 0)]) == [True, True, False]


@pytest.mark.asyncio
async def test_a_fallback_read_promotes_the_part_onto_the_local_tier(tmp_path) -> None:
    """A pool read warms the node-local NVMe so the NEXT read is served from flash.

    Routing sends a GET to the node that ingested the part, which retains it — but a
    request that lands elsewhere (routing off, target unready, or a re-ingested object)
    reads from the pool at ~94 MB/s / ~40 ms per chunk instead of ~705 MB/s / ~6 ms.
    Promotion is what stops that being permanent for objects read repeatedly.
    """
    dual = DualFileSystemPartsStore(str(tmp_path / "ssd"), str(tmp_path / "pool"), promote=True)
    await _write_part(dual.fallback, part_number=1, chunk=b"pool-only")

    assert await dual.get_chunk(OBJ, 1, 1, 0) == b"pool-only"

    # The promotion is what the assertion is about: read the PRIMARY directly, so a
    # pass cannot come from the fallback path running again.
    primary = FileSystemPartsStore(str(tmp_path / "ssd"))
    assert await primary.get_chunk(OBJ, 1, 1, 0) == b"pool-only", "the chunk was promoted to local flash"


@pytest.mark.asyncio
async def test_promotion_is_off_by_default(tmp_path) -> None:
    """Promotion duplicates pool bytes onto a node's disk, so it is opt-in.

    A node running without the drain's evictor has nothing reclaiming that copy, so
    defaulting this on would silently fill disks that have no owner for the space.
    """
    dual = DualFileSystemPartsStore(str(tmp_path / "ssd"), str(tmp_path / "pool"))
    await _write_part(dual.fallback, part_number=1, chunk=b"pool-only")

    assert await dual.get_chunk(OBJ, 1, 1, 0) == b"pool-only"

    primary = FileSystemPartsStore(str(tmp_path / "ssd"))
    assert await primary.get_chunk(OBJ, 1, 1, 0) is None, "no promotion unless asked for"


@pytest.mark.asyncio
async def test_a_promotion_failure_never_fails_the_read(tmp_path, monkeypatch) -> None:
    """Promotion is a cache warm, not part of serving the request.

    If the local disk is full, read-only, or racing the evictor, the client must still
    get its bytes — the pool copy is authoritative and already in hand.
    """
    dual = DualFileSystemPartsStore(str(tmp_path / "ssd"), str(tmp_path / "pool"), promote=True)
    await _write_part(dual.fallback, part_number=1, chunk=b"pool-only")

    async def _boom(*args, **kwargs):
        raise OSError("no space left on device")

    monkeypatch.setattr(dual, "set_meta", _boom)

    assert await dual.get_chunk(OBJ, 1, 1, 0) == b"pool-only", "the read survives a failed promotion"


@pytest.mark.asyncio
async def test_a_promoted_chunk_records_residency_on_this_node(tmp_path) -> None:
    """Promotion must tell THIS node's evictor about the copy it just made.

    The drain's evictor is scoped to parts a node ingested. A promoted copy was ingested
    elsewhere, so without a residency row for this node nothing would ever reclaim it and
    it would sit on the disk forever.
    """
    recorded: list[tuple[str, int, int, int]] = []

    async def _record(object_id: str, version: int, part_number: int, size_bytes: int) -> bool:
        recorded.append((object_id, version, part_number, size_bytes))
        return True

    dual = DualFileSystemPartsStore(str(tmp_path / "ssd"), str(tmp_path / "pool"), promote=True, on_promote=_record)
    await _write_part(dual.fallback, part_number=1, chunk=b"pool-only")

    await dual.get_chunk(OBJ, 1, 1, 0)

    assert recorded == [(OBJ, 1, 1, len(b"pool-only"))], "the evictor was told what this node now holds"


@pytest.mark.asyncio
async def test_a_primary_hit_never_promotes_or_records(tmp_path) -> None:
    """The steady state costs nothing: no pool read, no copy, no residency write."""
    recorded: list[tuple[str, int, int, int]] = []

    async def _record(*args: object) -> bool:
        recorded.append(args)  # type: ignore[arg-type]
        return True

    dual = DualFileSystemPartsStore(str(tmp_path / "ssd"), str(tmp_path / "pool"), promote=True, on_promote=_record)
    await _write_part(dual, part_number=1, chunk=b"on-ssd")

    assert await dual.get_chunk(OBJ, 1, 1, 0) == b"on-ssd"
    assert recorded == [], "a local hit does not touch the residency table"


@pytest.mark.asyncio
async def test_the_factory_refuses_to_promote_without_a_residency_recorder(tmp_path) -> None:
    """The flag alone is not enough — an unrecorded promotion is an unreclaimable copy.

    Tying the two together in the factory means a config that enables promotion in a
    process with no recorder wired (a worker, say) degrades to plain fallback reads
    instead of quietly filling that node's disk.
    """
    from hippius_s3.cache import create_fs_store

    class _Config:
        object_cache_dir = str(tmp_path / "ssd")
        object_cache_fallback_dir = str(tmp_path / "pool")
        object_cache_promote_on_read = True

    store = create_fs_store(_Config())
    assert isinstance(store, DualFileSystemPartsStore)
    await _write_part(store.fallback, part_number=1, chunk=b"pool-only")
    assert await store.get_chunk(OBJ, 1, 1, 0) == b"pool-only"

    primary = FileSystemPartsStore(str(tmp_path / "ssd"))
    assert await primary.get_chunk(OBJ, 1, 1, 0) is None, "no recorder, so no promotion"


@pytest.mark.asyncio
async def test_a_peer_holding_the_part_is_tried_before_the_pool(tmp_path) -> None:
    """Peer NVMe beats the pool: ~6 ms + ~1 ms network against ~40 ms per chunk.

    Locality is resolved per PART, not per request — measured on prod 2026-08-06, only 2%
    of multi-part objects have every part on one node, so routing a whole GET somewhere
    would leave most parts remote anyway.
    """
    calls: list[tuple[str, int, int, int]] = []

    async def _peer(object_id: str, version: int, part_number: int, chunk_index: int) -> bytes | None:
        calls.append((object_id, version, part_number, chunk_index))
        return b"from-peer"

    dual = DualFileSystemPartsStore(str(tmp_path / "ssd"), str(tmp_path / "pool"), peer_fetch=_peer)
    # Present on the pool too — the peer must win, or the tier is pointless.
    await _write_part(dual.fallback, part_number=1, chunk=b"from-pool")

    assert await dual.get_chunk(OBJ, 1, 1, 0) == b"from-peer"
    assert calls == [(OBJ, 1, 1, 0)]


@pytest.mark.asyncio
async def test_a_local_hit_never_asks_a_peer(tmp_path) -> None:
    """The steady state must not pay a network round trip or a residency lookup."""
    calls: list[object] = []

    async def _peer(*args: object) -> bytes | None:
        calls.append(args)
        return None

    dual = DualFileSystemPartsStore(str(tmp_path / "ssd"), str(tmp_path / "pool"), peer_fetch=_peer)
    await _write_part(dual, part_number=1, chunk=b"on-ssd")

    assert await dual.get_chunk(OBJ, 1, 1, 0) == b"on-ssd"
    assert calls == [], "a local hit short-circuits before the peer tier"


@pytest.mark.asyncio
async def test_a_failing_peer_falls_through_to_the_pool(tmp_path) -> None:
    """A peer is an optimisation. If it is down, slow, or lying, the pool still serves.

    The pool copy is authoritative and always present for a replicated part, so no peer
    failure may ever surface to the client.
    """

    async def _peer(*args: object) -> bytes | None:
        raise OSError("connection refused")

    dual = DualFileSystemPartsStore(str(tmp_path / "ssd"), str(tmp_path / "pool"), peer_fetch=_peer)
    await _write_part(dual.fallback, part_number=1, chunk=b"from-pool")

    assert await dual.get_chunk(OBJ, 1, 1, 0) == b"from-pool"


@pytest.mark.asyncio
async def test_a_peer_miss_falls_through_to_the_pool(tmp_path) -> None:
    """No peer holds it (all evicted, or none ever promoted) — the pool is the answer."""

    async def _peer(*args: object) -> bytes | None:
        return None

    dual = DualFileSystemPartsStore(str(tmp_path / "ssd"), str(tmp_path / "pool"), peer_fetch=_peer)
    await _write_part(dual.fallback, part_number=1, chunk=b"from-pool")

    assert await dual.get_chunk(OBJ, 1, 1, 0) == b"from-pool"


@pytest.mark.asyncio
async def test_read_local_chunk_never_serves_the_fallback(tmp_path) -> None:
    """What a peer serves must come off ITS flash, never off the shared pool.

    If this followed the fallback, a peer fetch would become an extra network hop in front
    of the very pool read the tier exists to avoid — and worse, two nodes each resolving the
    other could bounce a request between them instead of just reading the pool.
    """
    dual = DualFileSystemPartsStore(str(tmp_path / "ssd"), str(tmp_path / "pool"))
    await _write_part(dual.fallback, part_number=1, chunk=b"pool-only")

    assert await dual.get_chunk(OBJ, 1, 1, 0) == b"pool-only", "the normal read path does fall back"
    assert await dual.read_local_chunk(OBJ, 1, 1, 0) is None, "but a peer serves local flash only"


@pytest.mark.asyncio
async def test_read_local_chunk_serves_a_local_part(tmp_path) -> None:
    dual = DualFileSystemPartsStore(str(tmp_path / "ssd"), str(tmp_path / "pool"))
    await _write_part(dual, part_number=1, chunk=b"on-ssd")

    assert await dual.read_local_chunk(OBJ, 1, 1, 0) == b"on-ssd"


@pytest.mark.asyncio
async def test_read_local_chunk_never_consults_the_peer(tmp_path) -> None:
    """The recency-stamping override must keep the base method's tier rule: primary only.

    A peer that asked another peer would let two nodes bounce a request between them; a
    peer that read the pool would put a network hop in front of the read the tier avoids.
    """
    peer_calls: list[tuple] = []

    async def _peer(*args: object) -> bytes | None:
        peer_calls.append(args)
        return b"from-peer"

    dual = DualFileSystemPartsStore(str(tmp_path / "ssd"), str(tmp_path / "pool"), peer_fetch=_peer)
    await _write_part(dual.fallback, part_number=1, chunk=b"pool-only")

    assert await dual.read_local_chunk(OBJ, 1, 1, 0) is None
    assert peer_calls == [], "a peer serve must never fan out to another peer"


@pytest.mark.asyncio
async def test_read_local_chunk_stamps_recency_only_on_a_local_hit(tmp_path) -> None:
    seen: list[tuple] = []

    async def on_local_read(*args: object) -> None:
        seen.append(args)

    dual = DualFileSystemPartsStore(str(tmp_path / "ssd"), str(tmp_path / "pool"), on_local_read=on_local_read)
    await _write_part(dual.fallback, part_number=1, chunk=b"pool-only")
    assert await dual.read_local_chunk(OBJ, 1, 1, 0) is None
    assert seen == []

    await _write_part(dual, part_number=1, chunk=b"on-ssd")
    assert await dual.read_local_chunk(OBJ, 1, 1, 0) == b"on-ssd"
    assert seen == [(OBJ, 1, 1)]


@pytest.mark.asyncio
async def test_read_local_chunk_stamps_recency_even_when_the_recorder_fails(tmp_path) -> None:
    """The stamp is bookkeeping in front of a serve that already has its bytes: a recorder that
    cannot reach its database must lose the sample, never the chunk."""
    from hippius_s3.cache.read_recency import ReadRecencyRecorder

    class DownPool:
        def acquire(self, *, timeout: float | None = None) -> object:  # noqa: ASYNC109
            raise RuntimeError("pool is closing")

    recorder = ReadRecencyRecorder(DownPool(), "node-a")  # type: ignore[arg-type]
    dual = DualFileSystemPartsStore(str(tmp_path / "ssd"), str(tmp_path / "pool"), on_local_read=recorder)
    await _write_part(dual, part_number=1, chunk=b"on-ssd")

    assert await dual.read_local_chunk(OBJ, 1, 1, 0) == b"on-ssd"


@pytest.mark.asyncio
async def test_plain_store_read_local_chunk_is_unchanged(tmp_path) -> None:
    """Without HIPPIUS_OBJECT_CACHE_FALLBACK_DIR the api runs a plain FileSystemPartsStore, which
    has no recency hook: the override lives on the dual store only, and the base must still just
    read its own copy."""
    plain = FileSystemPartsStore(str(tmp_path / "single"))
    assert not hasattr(plain, "_on_local_read")
    assert await plain.read_local_chunk(OBJ, 1, 1, 0) is None

    await _write_part(plain, part_number=1, chunk=b"single-tier")
    assert await plain.read_local_chunk(OBJ, 1, 1, 0) == b"single-tier"
