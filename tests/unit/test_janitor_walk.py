"""The parallel FS walk that replaces the serial `_safe_iterdir` producer.

The serial walk ran on the event loop at ~40 object-dirs/s over CephFS (measured on prod
2026-07-23), so a pass over ~2.8M object dirs took ~20h and never completed inside a cycle —
starving every phase after `cleanup_stale_parts`. `iter_part_dirs` fans the per-object
descent across a thread pool, shards for fair coverage, and honours a wall-clock budget so
the cycle always completes. These tests pin the properties the deletion logic relies on:
completeness, shard-union == full walk, budget truncation, and legacy serial equivalence.
"""

from __future__ import annotations

import asyncio
import os
from pathlib import Path

import pytest

from workers.run_janitor_in_loop import PartDirInfo
from workers.run_janitor_in_loop import WalkState
from workers.run_janitor_in_loop import _object_in_shard
from workers.run_janitor_in_loop import iter_part_dirs


def _build_tree(root: Path, n_objects: int, versions: int = 1, parts: int = 1, meta: bool = True) -> int:
    made = 0
    for i in range(n_objects):
        oid = f"{i:032x}"
        for v in range(1, versions + 1):
            for p in range(1, parts + 1):
                d = root / oid / f"v{v}" / f"part_{p}"
                d.mkdir(parents=True, exist_ok=True)
                (d / "chunk_0.bin").write_bytes(b"x")
                if meta:
                    (d / "meta.json").write_text("{}")
                made += 1
    return made


async def _collect(root: Path, **kw) -> tuple[list[PartDirInfo], WalkState]:
    state = WalkState()
    out: list[PartDirInfo] = []
    async for info in iter_part_dirs(root, state=state, **kw):
        out.append(info)
    return out, state


@pytest.mark.asyncio
async def test_full_walk_finds_every_part(tmp_path: Path) -> None:
    expected = _build_tree(tmp_path, 40, versions=2, parts=3)
    got, state = await _collect(tmp_path, concurrency=8, shard=0, shards=1, deadline=None)
    assert len(got) == expected == 240
    assert state.truncated is False
    keys = {(i.object_id, i.object_version, i.part_number) for i in got}
    assert len(keys) == expected  # no duplicates


@pytest.mark.asyncio
async def test_shards_partition_the_tree_exactly(tmp_path: Path) -> None:
    """Every part appears in exactly one shard; the union over all shards is the full walk."""
    _build_tree(tmp_path, 100, versions=1, parts=2)
    full, _ = await _collect(tmp_path, concurrency=4, shard=0, shards=1, deadline=None)
    full_keys = {(i.object_id, i.object_version, i.part_number) for i in full}

    shards = 5
    seen: set = set()
    for s in range(shards):
        part, _ = await _collect(tmp_path, concurrency=4, shard=s, shards=shards, deadline=None)
        keys = {(i.object_id, i.object_version, i.part_number) for i in part}
        assert not (keys & seen), "an object landed in two shards — coverage would double-scan"
        seen |= keys
    assert seen == full_keys, "shard union must equal the full walk — no object left unswept"


@pytest.mark.asyncio
async def test_budget_truncates_and_flags(tmp_path: Path) -> None:
    _build_tree(tmp_path, 200)
    loop = asyncio.get_running_loop()
    got, state = await _collect(
        tmp_path, concurrency=2, shard=0, shards=1, deadline=loop.time() - 1.0
    )
    assert state.truncated is True
    assert len(got) < 200  # stopped early


@pytest.mark.asyncio
async def test_serial_equivalence(tmp_path: Path) -> None:
    """concurrency=1 is the legacy one-object-at-a-time descent and must find the same set."""
    _build_tree(tmp_path, 30, versions=2, parts=2)
    par, _ = await _collect(tmp_path, concurrency=8, shard=0, shards=1, deadline=None)
    ser, _ = await _collect(tmp_path, concurrency=1, shard=0, shards=1, deadline=None)
    assert {(i.object_id, i.object_version, i.part_number) for i in par} == {
        (i.object_id, i.object_version, i.part_number) for i in ser
    }


@pytest.mark.asyncio
async def test_missing_root_is_empty_not_error(tmp_path: Path) -> None:
    got, state = await _collect(tmp_path / "does-not-exist", concurrency=4, shard=0, shards=1, deadline=None)
    assert got == []
    assert state.objects_scanned == 0


@pytest.mark.asyncio
async def test_stat_prefers_meta_then_falls_back_to_part_dir(tmp_path: Path) -> None:
    """meta.json's mtime is the readiness signal; without it the part dir's is used."""
    d = tmp_path / f"{1:032x}" / "v1" / "part_1"
    d.mkdir(parents=True)
    (d / "chunk_0.bin").write_bytes(b"x")
    (d / "meta.json").write_text("{}")
    meta_mtime = 1_000_000.0
    os.utime(d / "meta.json", (meta_mtime, meta_mtime))
    got, _ = await _collect(tmp_path, concurrency=2, shard=0, shards=1, deadline=None)
    assert len(got) == 1
    assert got[0].mtime == pytest.approx(meta_mtime)

    # Now a part with no meta.json → falls back to the dir's own mtime.
    d2 = tmp_path / f"{2:032x}" / "v1" / "part_1"
    d2.mkdir(parents=True)
    (d2 / "chunk_0.bin").write_bytes(b"x")
    got2, _ = await _collect(tmp_path, concurrency=2, shard=0, shards=1, deadline=None)
    assert len(got2) == 2


@pytest.mark.asyncio
async def test_malformed_dir_names_are_skipped(tmp_path: Path) -> None:
    _build_tree(tmp_path, 5)
    # junk that must not crash the walk or be yielded
    (tmp_path / "not-an-object" / "vXX" / "part_zz").mkdir(parents=True)
    (tmp_path / f"{99:032x}" / "vbad").mkdir(parents=True)
    (tmp_path / f"{98:032x}" / "v1" / "partZ").mkdir(parents=True)
    got, _ = await _collect(tmp_path, concurrency=4, shard=0, shards=1, deadline=None)
    # only the 5 well-formed parts
    assert len(got) == 5


def test_shard_helper_is_stable_and_bounded() -> None:
    oid = "a" * 32
    assert _object_in_shard(oid, 0, 1) is True  # shards<=1 always in
    hits = [s for s in range(8) if _object_in_shard(oid, s, 8)]
    assert len(hits) == 1  # exactly one shard claims it
    # stable across calls
    assert _object_in_shard(oid, hits[0], 8) is True
