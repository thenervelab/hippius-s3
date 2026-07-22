"""Exhaustive tests for the janitor's terminally-abandoned reclaim branch.

SAFETY-CRITICAL. Phase 1 (`cleanup_stale_parts`) protects every part that is NOT
fully replicated — a pending/in-flight/aborted upload must never be deleted. This
suite covers the ONE narrow exception added on top of that gate: a part whose drain
replication row is terminal-`failed` AND whose object version is unservable (an
abandoned/aborted MPU). Those parts never replicate and never will, so their
CephFS-pool bytes leak forever; reclaiming them is safe only because an unservable
version can never be served by a GET.

These tests pin the **wiring** in `handle` (when the abandoned branch fires, when it
must NOT fire, and that it is conservative on error). The SQL predicate itself — the
`failed AND unservable` truth table that makes this safe — is exercised against a real
Postgres in `tests/integration/test_janitor_abandoned_reclaim_sql.py`.
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


class _PoolCtx:
    def __init__(self, conn) -> None:
        self._conn = conn

    async def __aenter__(self):
        return self._conn

    async def __aexit__(self, *exc) -> bool:
        return False


class _FakePool:
    """Minimal asyncpg.Pool stand-in: every acquire() yields the same conn mock."""

    def __init__(self, conn) -> None:
        self._conn = conn

    def acquire(self) -> _PoolCtx:
        return _PoolCtx(self._conn)


def _pool(conn) -> _FakePool:
    return _FakePool(conn)


def _make_part(
    fs_root: Path,
    object_id: str,
    version: int,
    part_number: int,
    *,
    mtime_offset: float = 200000,
    atime_offset: float = 200000,
) -> Path:
    """Create a real part dir (chunk + meta), old enough to reach DB classification."""
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


@pytest.fixture
def abandoned_counter():
    """Replace the OTel counter with a mock so we can assert on reclaim accounting."""
    counter = MagicMock()
    with patch.object(janitor, "_janitor_abandoned_deleted_counter", counter):
        yield counter


def _db(recent_row) -> AsyncMock:
    """A db whose discriminating fetchrow returns `recent_row`."""
    db = AsyncMock()
    db.fetchrow = AsyncMock(return_value=recent_row)
    return db


def _exists(fs_root: Path, object_id: str = OBJ, version: int = 1, part: int = 1) -> bool:
    return (fs_root / object_id / f"v{version}" / f"part_{part}").exists()


# ----------------------------------------------------- the abandoned branch FIRES


@pytest.mark.asyncio
async def test_failed_and_unservable_part_is_reclaimed(fs_root, fs_store, redis_mock, abandoned_counter):
    """parts row exists + NOT replicated + terminally abandoned → DELETE.

    This is the leak fix: an abandoned/aborted MPU part the drain marked 'failed'
    and never replicated, whose version is unservable, gets reclaimed."""
    _make_part(fs_root, OBJ, 1, 1)
    db = _db({"recent": False})

    with (
        patch.object(janitor, "is_replicated_on_all_backends", AsyncMock(return_value=False)) as repl,
        patch.object(janitor, "is_terminally_abandoned", AsyncMock(return_value=True)) as aband,
    ):
        await janitor.cleanup_stale_parts(_pool(db), fs_store, redis_mock)

    assert not _exists(fs_root), "terminally-abandoned part must be reclaimed"
    assert (OBJ, 1, 1) in fs_store.deleted
    repl.assert_awaited_once()
    aband.assert_awaited_once()
    abandoned_counter.add.assert_called_once_with(1)


# ------------------------------------------------- the abandoned branch must NOT fire


@pytest.mark.asyncio
async def test_not_replicated_and_not_abandoned_is_protected(fs_root, fs_store, redis_mock, abandoned_counter):
    """parts row exists + NOT replicated + NOT abandoned → SKIP.

    THE core safety guard: an ordinary pending/in-flight upload (not 'failed', or
    a still-servable version) must still be protected exactly as before."""
    _make_part(fs_root, OBJ, 1, 1)
    db = _db({"recent": False})

    with (
        patch.object(janitor, "is_replicated_on_all_backends", AsyncMock(return_value=False)),
        patch.object(janitor, "is_terminally_abandoned", AsyncMock(return_value=False)) as aband,
    ):
        await janitor.cleanup_stale_parts(_pool(db), fs_store, redis_mock)

    assert _exists(fs_root), "non-abandoned un-replicated part must NOT be deleted"
    assert fs_store.deleted == []
    aband.assert_awaited_once()
    abandoned_counter.add.assert_not_called()


@pytest.mark.asyncio
async def test_replicated_part_never_consults_abandoned(fs_root, fs_store, redis_mock, abandoned_counter):
    """A fully-replicated part is reclaimed by the normal path; the abandoned check
    is never even consulted (replication short-circuits), and its counter stays put."""
    _make_part(fs_root, OBJ, 1, 1)
    db = _db({"recent": False})

    with (
        patch.object(janitor, "is_replicated_on_all_backends", AsyncMock(return_value=True)),
        patch.object(janitor, "is_terminally_abandoned", AsyncMock()) as aband,
    ):
        await janitor.cleanup_stale_parts(_pool(db), fs_store, redis_mock)

    assert not _exists(fs_root)
    assert (OBJ, 1, 1) in fs_store.deleted
    aband.assert_not_awaited()
    abandoned_counter.add.assert_not_called()


@pytest.mark.asyncio
async def test_recent_part_never_consults_abandoned(fs_root, fs_store, redis_mock):
    """A recently-written part is skipped before the replication/abandoned gates."""
    _make_part(fs_root, OBJ, 1, 1)
    db = _db({"recent": True})

    with (
        patch.object(janitor, "is_replicated_on_all_backends", AsyncMock()) as repl,
        patch.object(janitor, "is_terminally_abandoned", AsyncMock()) as aband,
    ):
        await janitor.cleanup_stale_parts(_pool(db), fs_store, redis_mock)

    assert _exists(fs_root)
    assert fs_store.deleted == []
    repl.assert_not_awaited()
    aband.assert_not_awaited()


@pytest.mark.asyncio
async def test_orphan_no_parts_row_never_consults_abandoned(fs_root, fs_store, redis_mock):
    """No `parts` row → orphan reap via the existing path; abandoned not consulted."""
    _make_part(fs_root, OBJ, 1, 1)
    db = _db(None)

    with (
        patch.object(janitor, "is_replicated_on_all_backends", AsyncMock()) as repl,
        patch.object(janitor, "is_terminally_abandoned", AsyncMock()) as aband,
    ):
        await janitor.cleanup_stale_parts(_pool(db), fs_store, redis_mock)

    assert not _exists(fs_root)
    assert (OBJ, 1, 1) in fs_store.deleted
    repl.assert_not_awaited()
    aband.assert_not_awaited()


# ----------------------------------------------------------- conservative on error


@pytest.mark.asyncio
async def test_abandoned_check_error_skips_deletion(fs_root, fs_store, redis_mock):
    """If the abandoned predicate raises, be conservative and SKIP (never delete on
    uncertainty) — same posture as the replication-check error path."""
    _make_part(fs_root, OBJ, 1, 1)
    db = _db({"recent": False})

    with (
        patch.object(janitor, "is_replicated_on_all_backends", AsyncMock(return_value=False)),
        patch.object(janitor, "is_terminally_abandoned", AsyncMock(side_effect=RuntimeError("boom"))),
    ):
        await janitor.cleanup_stale_parts(_pool(db), fs_store, redis_mock)

    assert _exists(fs_root), "must not delete when the abandoned check fails"
    assert fs_store.deleted == []


# -------------------------------------------------------------------- ordering


@pytest.mark.asyncio
async def test_abandoned_consulted_only_after_replication_false(fs_root, fs_store, redis_mock):
    """The abandoned check is the LAST gate — it only runs after replication returns
    False. This keeps the cheap/common replicated path off the extra query."""
    _make_part(fs_root, OBJ, 1, 1)
    db = _db({"recent": False})
    order: list[str] = []

    async def repl(*_a, **_k):
        order.append("repl")
        return False

    async def aband(*_a, **_k):
        order.append("aband")
        return True

    with (
        patch.object(janitor, "is_replicated_on_all_backends", AsyncMock(side_effect=repl)),
        patch.object(janitor, "is_terminally_abandoned", AsyncMock(side_effect=aband)),
    ):
        await janitor.cleanup_stale_parts(_pool(db), fs_store, redis_mock)

    assert order == ["repl", "aband"], "replication gate must run before the abandoned gate"


# ------------------------------------------------------ integration: mixed walk


@pytest.mark.asyncio
async def test_mixed_batch_classified_correctly(fs_root, fs_store, redis_mock, abandoned_counter):
    """One walk over five parts exercising every branch together. Exactly the
    orphan, the replicated, and the terminally-abandoned parts are reclaimed; the
    plain pending part and the fresh part survive."""
    orphan = "11111111-1111-1111-1111-111111111111"
    pending = "22222222-2222-2222-2222-222222222222"
    replicated = "33333333-3333-3333-3333-333333333333"
    abandoned = "44444444-4444-4444-4444-444444444444"
    fresh = "55555555-5555-5555-5555-555555555555"

    _make_part(fs_root, orphan, 1, 1)
    _make_part(fs_root, pending, 1, 1)
    _make_part(fs_root, replicated, 1, 1)
    _make_part(fs_root, abandoned, 1, 1)
    _make_part(fs_root, fresh, 1, 1, mtime_offset=5, atime_offset=5)  # in-flight

    rows = {
        orphan: None,
        pending: {"recent": False},
        replicated: {"recent": False},
        abandoned: {"recent": False},
        fresh: {"recent": False},
    }

    async def fake_fetchrow(_sql, object_id, *_a):
        return rows[object_id]

    db = AsyncMock()
    db.fetchrow = AsyncMock(side_effect=fake_fetchrow)

    async def fake_repl(_db, object_id, *_a):
        return object_id == replicated

    async def fake_aband(_db, object_id, *_a):
        return object_id == abandoned

    with (
        patch.object(janitor, "is_replicated_on_all_backends", AsyncMock(side_effect=fake_repl)),
        patch.object(janitor, "is_terminally_abandoned", AsyncMock(side_effect=fake_aband)),
    ):
        await janitor.cleanup_stale_parts(_pool(db), fs_store, redis_mock)

    assert not _exists(fs_root, orphan), "orphan reclaimed"
    assert not _exists(fs_root, replicated), "replicated reclaimed"
    assert not _exists(fs_root, abandoned), "terminally-abandoned reclaimed"
    assert _exists(fs_root, pending), "plain pending upload protected"
    assert _exists(fs_root, fresh), "in-flight protected"
    assert set(fs_store.deleted) == {(orphan, 1, 1), (replicated, 1, 1), (abandoned, 1, 1)}
    abandoned_counter.add.assert_called_once_with(1)
