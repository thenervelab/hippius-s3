"""The gate, exercised through the real DualFileSystemPartsStore rather than in isolation.

The unit tests above pin the predicate. These pin that `get_chunk` actually HONOURS it — that a
file present on this node's flash is not returned when the node has no claim to it, and that the
read falls through to the lower tier instead of failing.
"""

from __future__ import annotations

import pytest

from hippius_s3.cache.dual_fs_store import DualFileSystemPartsStore


OBJ = "466916c0-d61b-4518-b81b-9576b574270a"
LOCAL = b"local-attempt-bytes"
POOL = b"pool-attempt-bytes!"


class _Gate:
    def __init__(self, allow: bool) -> None:
        self.allow = allow
        self.calls = 0

    async def may_serve_local(self, *_a: object) -> bool:
        self.calls += 1
        return self.allow


async def _seed(store: DualFileSystemPartsStore, primary: bytes | None, fallback: bytes | None) -> None:
    if primary is not None:
        await store.set_chunk(OBJ, 1, 1, 0, primary)
        await store.set_meta(OBJ, 1, 1, chunk_size=len(primary), num_chunks=1, size_bytes=len(primary))
    if fallback is not None:
        await store.fallback.set_chunk(OBJ, 1, 1, 0, fallback)
        await store.fallback.set_meta(OBJ, 1, 1, chunk_size=len(fallback), num_chunks=1, size_bytes=len(fallback))


@pytest.mark.asyncio
async def test_an_unowned_local_copy_is_not_returned_and_the_pool_answers(tmp_path):
    gate = _Gate(allow=False)
    store = DualFileSystemPartsStore(str(tmp_path / "local"), str(tmp_path / "pool"), local_residency=gate)
    await _seed(store, LOCAL, POOL)

    got = await store.get_chunk(OBJ, 1, 1, 0)

    assert got == POOL, "the unowned local copy was served instead of the pool's"
    assert gate.calls == 1


@pytest.mark.asyncio
async def test_an_owned_local_copy_is_returned(tmp_path):
    gate = _Gate(allow=True)
    store = DualFileSystemPartsStore(str(tmp_path / "local"), str(tmp_path / "pool"), local_residency=gate)
    await _seed(store, LOCAL, POOL)

    assert await store.get_chunk(OBJ, 1, 1, 0) == LOCAL


@pytest.mark.asyncio
async def test_no_gate_configured_keeps_the_previous_behaviour(tmp_path):
    """Workers and pre-drain deployments pass None; they must be unaffected."""
    store = DualFileSystemPartsStore(str(tmp_path / "local"), str(tmp_path / "pool"))
    await _seed(store, LOCAL, POOL)

    assert await store.get_chunk(OBJ, 1, 1, 0) == LOCAL


@pytest.mark.asyncio
async def test_the_gate_is_not_consulted_when_there_is_no_local_copy(tmp_path):
    """It answers 'may I serve THIS file'. With no file there is nothing to ask about, and asking
    would put a DB round-trip on every pool read."""
    gate = _Gate(allow=True)
    store = DualFileSystemPartsStore(str(tmp_path / "local"), str(tmp_path / "pool"), local_residency=gate)
    await _seed(store, None, POOL)

    assert await store.get_chunk(OBJ, 1, 1, 0) == POOL
    assert gate.calls == 0


@pytest.mark.asyncio
async def test_a_refused_read_self_heals_the_unowned_copy(tmp_path):
    """Refusing the read also REPAIRS it, which is why no separate reconciler is needed.

    Falling through to the pool reaches `_promote_chunk`, and promotion writes the chunk
    unconditionally — only *meta* is skipped when already present. So the pool's bytes overwrite
    the unowned ones and the promotion claims residency for them. The next read is served
    locally, correctly, from the copy this node now genuinely owns.

    Before the gate this could never happen: `get_chunk` returned the local bytes and the promote
    path was never reached, so the loser's copy was served for as long as the file existed.

    The repair rides on promotion, so it applies where `HIPPIUS_OBJECT_CACHE_PROMOTE_ON_READ` is
    set — staging today, not prod. Without it the gate still refuses correctly; the unowned bytes
    simply stay on the disk unread, and every read of that part costs a pool round-trip until the
    evictor reclaims it. Correct either way; self-repairing only where promotion is on.
    """
    promoted: list[tuple[str, int, int, int]] = []

    async def on_promote(object_id: str, version: int, part: int, size: int) -> bool:
        promoted.append((object_id, version, part, size))
        return True

    gate = _Gate(allow=False)
    store = DualFileSystemPartsStore(
        str(tmp_path / "local"),
        str(tmp_path / "pool"),
        promote=True,
        local_residency=gate,
        on_promote=on_promote,
    )
    await _seed(store, LOCAL, POOL)

    assert await store.get_chunk(OBJ, 1, 1, 0) == POOL
    assert promoted, "the pool read did not promote, so the unowned copy was left in place"

    # The bytes on this node's own flash are now the pool's, not the losing attempt's.
    gate.allow = True
    assert await store.get_chunk(OBJ, 1, 1, 0) == POOL
