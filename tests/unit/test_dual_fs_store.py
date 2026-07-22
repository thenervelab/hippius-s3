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
