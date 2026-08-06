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

    async def _record(object_id: str, version: int, part_number: int, size_bytes: int) -> None:
        recorded.append((object_id, version, part_number, size_bytes))

    dual = DualFileSystemPartsStore(str(tmp_path / "ssd"), str(tmp_path / "pool"), promote=True, on_promote=_record)
    await _write_part(dual.fallback, part_number=1, chunk=b"pool-only")

    await dual.get_chunk(OBJ, 1, 1, 0)

    assert recorded == [(OBJ, 1, 1, len(b"pool-only"))], "the evictor was told what this node now holds"


@pytest.mark.asyncio
async def test_a_primary_hit_never_promotes_or_records(tmp_path) -> None:
    """The steady state costs nothing: no pool read, no copy, no residency write."""
    recorded: list[tuple[str, int, int, int]] = []

    async def _record(*args: object) -> None:
        recorded.append(args)  # type: ignore[arg-type]

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
