"""Exhaustive tests for the janitor Phase 1 (`cleanup_stale_parts`) three-way gate.

This is critical, data-loss-adjacent code. `cleanup_stale_parts` must classify
every stale part dir into exactly one of three outcomes:

  1. No `parts` row in the DB  → ORPHAN → DELETE.
     The object's rows were cascade-removed by the Phase 4 hard-delete
     (objects → object_versions → parts → part_chunks → chunk_backend), or the
     FS dir is a leftover with no record. The mtime>stale gate already rules out
     an in-flight write (chunks land on disk before the `parts` row is created),
     so reaping is safe and necessary — otherwise deleted-object cache files
     leak forever (Phase 2 also skips zero-chunk parts).

  2. `parts` row exists but NOT fully replicated → SKIP (protect).
     A pending / in-flight / aborted upload. Deleting it would lose data that
     isn't on every required backend yet. Never delete this.

  3. `parts` row exists AND fully replicated → DELETE (normal reclaim).

Plus the pre-DB guards: a recently-written part (mtime within the stale window)
and a DLQ-protected object are always skipped before any of the above.

The single discriminating query returns `(uploaded_at > cutoff) AS recent`:
  - row is None      → no parts row → orphan (case 1)
  - row["recent"]    → recently written → skip
  - not row["recent"]→ exists + old → gate on replication (cases 2/3)
"""

from __future__ import annotations

import os
import shutil
import time
from pathlib import Path
from unittest.mock import AsyncMock
from unittest.mock import MagicMock
from unittest.mock import patch

import pytest

from workers import run_janitor_in_loop as janitor


OBJ = "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"
STALE = 86400  # mpu_stale_seconds used throughout


def _make_part(
    fs_root: Path,
    object_id: str,
    version: int,
    part_number: int,
    *,
    mtime_offset: float = 200000,
    atime_offset: float = 200000,
) -> Path:
    """Create a real part dir (chunk + meta) with adjustable timestamps.

    Offsets are seconds *before now*. Defaults make the part "old" (well past
    the 1-day stale window) so it reaches the DB-classification stage.
    """
    part_dir = fs_root / object_id / f"v{version}" / f"part_{part_number}"
    part_dir.mkdir(parents=True, exist_ok=True)
    (part_dir / "chunk_0.bin").write_bytes(b"payload")
    (part_dir / "meta.json").write_text('{"chunk_size": 7, "num_chunks": 1, "size_bytes": 7}')

    now = time.time()
    target = (now - atime_offset, now - mtime_offset)
    os.utime(part_dir / "chunk_0.bin", target)
    os.utime(part_dir / "meta.json", target)
    return part_dir


class _FakeFsStore:
    """fs_store with a real `delete_part` so deletions are observable on disk."""

    def __init__(self, root: Path) -> None:
        self.root = root
        self.deleted: list[tuple[str, int, int]] = []

    async def delete_part(self, object_id: str, object_version: int, part_number: int) -> None:
        self.deleted.append((object_id, int(object_version), int(part_number)))
        p = self.root / object_id / f"v{object_version}" / f"part_{part_number}"
        if p.exists():
            shutil.rmtree(p)


@pytest.fixture
def fs_root(tmp_path: Path) -> Path:
    return tmp_path


@pytest.fixture
def fs_store(fs_root: Path) -> _FakeFsStore:
    return _FakeFsStore(fs_root)


@pytest.fixture
def redis_mock():
    client = MagicMock()
    client.lrange = AsyncMock(return_value=[])  # empty DLQs by default
    return client


@pytest.fixture(autouse=True)
def _config():
    janitor.config.mpu_stale_seconds = STALE
    janitor.config.upload_backends = ["arion", "ovh"]
    janitor.config.backup_backends = []


def _db(recent_row) -> AsyncMock:
    """A db whose discriminating fetchrow returns `recent_row`.

    recent_row: None (no parts row) | {"recent": True} | {"recent": False}.
    """
    db = AsyncMock()
    db.fetchrow = AsyncMock(return_value=recent_row)
    return db


def _exists(fs_root: Path, object_id: str = OBJ, version: int = 1, part: int = 1) -> bool:
    return (fs_root / object_id / f"v{version}" / f"part_{part}").exists()


# ------------------------------------------------------------------ case 1: orphan


@pytest.mark.asyncio
async def test_orphan_no_parts_row_is_reaped(fs_root, fs_store, redis_mock):
    """No `parts` row (hard-deleted object cascade / leftover FS) → DELETE.

    This is the regression guard: a flat replication gate skips these (zero
    chunks ⇒ not 'replicated') and leaks every deleted object's cache forever.
    """
    _make_part(fs_root, OBJ, 1, 1)
    db = _db(None)  # fetchrow finds no parts row

    with patch.object(janitor, "is_replicated_on_all_backends", AsyncMock()) as repl:
        await janitor.cleanup_stale_parts(db, fs_store, redis_mock)

    assert not _exists(fs_root), "orphaned cache dir must be reclaimed"
    assert (OBJ, 1, 1) in fs_store.deleted
    # The replication gate is irrelevant for orphans and must not be consulted.
    repl.assert_not_awaited()


# ------------------------------------------------------------- case 2: pending upload


@pytest.mark.asyncio
async def test_existing_part_not_replicated_is_protected(fs_root, fs_store, redis_mock):
    """`parts` row exists but not fully replicated → SKIP (no data loss)."""
    _make_part(fs_root, OBJ, 1, 1)
    db = _db({"recent": False})

    with patch.object(janitor, "is_replicated_on_all_backends", AsyncMock(return_value=False)) as repl:
        await janitor.cleanup_stale_parts(db, fs_store, redis_mock)

    assert _exists(fs_root), "un-replicated tracked part must NOT be deleted"
    assert fs_store.deleted == []
    repl.assert_awaited_once()


# ----------------------------------------------------------- case 3: replicated reclaim


@pytest.mark.asyncio
async def test_existing_part_fully_replicated_is_reaped(fs_root, fs_store, redis_mock):
    """`parts` row exists AND fully replicated → DELETE (normal reclaim)."""
    _make_part(fs_root, OBJ, 1, 1)
    db = _db({"recent": False})

    with patch.object(janitor, "is_replicated_on_all_backends", AsyncMock(return_value=True)) as repl:
        await janitor.cleanup_stale_parts(db, fs_store, redis_mock)

    assert not _exists(fs_root), "replicated cold part should be reclaimed"
    assert (OBJ, 1, 1) in fs_store.deleted
    repl.assert_awaited_once()


# ------------------------------------------------------------------- pre-DB guards


@pytest.mark.asyncio
async def test_recent_uploaded_at_is_skipped(fs_root, fs_store, redis_mock):
    """Row exists and was written recently (`recent`=True) → SKIP, and the
    replication gate is not even consulted."""
    _make_part(fs_root, OBJ, 1, 1)
    db = _db({"recent": True})

    with patch.object(janitor, "is_replicated_on_all_backends", AsyncMock()) as repl:
        await janitor.cleanup_stale_parts(db, fs_store, redis_mock)

    assert _exists(fs_root)
    assert fs_store.deleted == []
    repl.assert_not_awaited()


@pytest.mark.asyncio
async def test_recent_mtime_is_skipped_before_any_db_call(fs_root, fs_store, redis_mock):
    """A part written within the stale window (in-flight) is skipped by the
    mtime guard BEFORE any DB query — protects fresh uploads whose `parts` row
    may not exist yet."""
    _make_part(fs_root, OBJ, 1, 1, mtime_offset=10, atime_offset=10)  # ~now
    db = _db(None)

    with patch.object(janitor, "is_replicated_on_all_backends", AsyncMock()) as repl:
        await janitor.cleanup_stale_parts(db, fs_store, redis_mock)

    assert _exists(fs_root), "in-flight part must not be touched"
    db.fetchrow.assert_not_awaited()
    repl.assert_not_awaited()


@pytest.mark.asyncio
async def test_dlq_protected_object_is_skipped(fs_root, fs_store, redis_mock):
    """An object present in any DLQ is skipped before the DB classification."""
    import json

    _make_part(fs_root, OBJ, 1, 1)
    redis_mock.lrange = AsyncMock(return_value=[json.dumps({"object_id": OBJ})])
    db = _db(None)

    with patch.object(janitor, "is_replicated_on_all_backends", AsyncMock()) as repl:
        await janitor.cleanup_stale_parts(db, fs_store, redis_mock)

    assert _exists(fs_root), "DLQ-protected object must not be deleted"
    db.fetchrow.assert_not_awaited()
    repl.assert_not_awaited()


# --------------------------------------------------------------- conservative on error


@pytest.mark.asyncio
async def test_fetchrow_error_skips_deletion(fs_root, fs_store, redis_mock):
    """If the discriminating query raises, be conservative and skip."""
    _make_part(fs_root, OBJ, 1, 1)
    db = AsyncMock()
    db.fetchrow = AsyncMock(side_effect=RuntimeError("db down"))

    await janitor.cleanup_stale_parts(db, fs_store, redis_mock)

    assert _exists(fs_root), "must not delete when the DB check fails"
    assert fs_store.deleted == []


@pytest.mark.asyncio
async def test_replication_check_error_skips_deletion(fs_root, fs_store, redis_mock):
    """If the replication gate raises for an existing+old part, skip (never
    delete on uncertainty)."""
    _make_part(fs_root, OBJ, 1, 1)
    db = _db({"recent": False})

    with patch.object(
        janitor, "is_replicated_on_all_backends", AsyncMock(side_effect=RuntimeError("boom"))
    ):
        await janitor.cleanup_stale_parts(db, fs_store, redis_mock)

    assert _exists(fs_root)
    assert fs_store.deleted == []


# --------------------------------------------------------- integration: mixed walk


@pytest.mark.asyncio
async def test_mixed_batch_classified_correctly(fs_root, fs_store, redis_mock):
    """One walk over four parts exercising all branches together — the realistic
    case. Exactly the orphan and the fully-replicated part get reclaimed; the
    pending and recently-written parts survive."""
    orphan = "11111111-1111-1111-1111-111111111111"
    pending = "22222222-2222-2222-2222-222222222222"
    replicated = "33333333-3333-3333-3333-333333333333"
    fresh = "44444444-4444-4444-4444-444444444444"

    _make_part(fs_root, orphan, 1, 1)  # old
    _make_part(fs_root, pending, 1, 1)  # old
    _make_part(fs_root, replicated, 1, 1)  # old
    _make_part(fs_root, fresh, 1, 1, mtime_offset=5, atime_offset=5)  # in-flight

    # fetchrow per object: orphan→None, others→{"recent": False} (fresh is
    # filtered by mtime before fetchrow, so its value is never consumed).
    rows = {
        orphan: None,
        pending: {"recent": False},
        replicated: {"recent": False},
        fresh: {"recent": False},
    }

    async def fake_fetchrow(_sql, object_id, *_a):
        return rows[object_id]

    db = AsyncMock()
    db.fetchrow = AsyncMock(side_effect=fake_fetchrow)

    async def fake_repl(_db, object_id, *_a):
        return object_id == replicated  # only the replicated object passes the gate

    with patch.object(janitor, "is_replicated_on_all_backends", AsyncMock(side_effect=fake_repl)):
        await janitor.cleanup_stale_parts(db, fs_store, redis_mock)

    assert not _exists(fs_root, orphan), "orphan reclaimed"
    assert not _exists(fs_root, replicated), "replicated reclaimed"
    assert _exists(fs_root, pending), "pending upload protected"
    assert _exists(fs_root, fresh), "in-flight protected"
    assert set(fs_store.deleted) == {(orphan, 1, 1), (replicated, 1, 1)}
