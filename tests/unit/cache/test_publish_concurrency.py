"""Two attempts publishing ONE part concurrently must settle on exactly one of them.

Promoting a staged set is N renames with no atomic primitive around them, so two interleaved
promotions leave a part whose chunks come from both attempts. That mixture is not detectable
downstream: nonces are random per chunk (#402) and the AAD binds (bucket, object, part, chunk)
rather than attempt identity, so every chunk in it decrypts under a valid tag and the client is
handed plausible garbage.

The race is same-process on purpose — one uvicorn worker handling a retried `UploadPart` while the
first attempt is still publishing is the cheapest way to reach it, and it is the case
`_PartPublishLocks` exists for. Which attempt wins is not specified; that the winner is whole is.
"""

from __future__ import annotations

import asyncio
import itertools
import json
import time
import uuid
from pathlib import Path

import pytest

from hippius_s3.cache import FileSystemPartsStore
from hippius_s3.cache import fs_store as fs_store_module
from hippius_s3.cache.fs_store import _STAGED_INFIX


ATTEMPT_A = "aaaa1111"
ATTEMPT_B = "bbbb2222"
# Unequal, so the winner is identifiable from meta alone and the loser would leave a visible tail.
COUNT_A = 24
COUNT_B = 16


@pytest.fixture()
def swap_order(monkeypatch) -> list[str]:
    """Hold each rename open for 1 ms and record which attempt made it.

    Left at real speed the race is nearly unobservable in-process: 24 renames take ~100 us, well
    inside one GIL slice, so a thread finishes its whole swap before the other is scheduled and an
    unlocked publish looks correct in ~99% of runs (measured 0 mixtures in 300 unlocked rounds).
    That is an artefact of a tiny part, not of the code being safe — a 14 GB part is 3584 renames
    measured at 326 ms, and a second worker gets to run inside that. Sleeping 1 ms per rename
    reproduces that window with 40 files instead of 7168.

    Returns the attempt id behind each staged-chunk rename, in the order the renames happened.
    """
    original = Path.replace
    order: list[str] = []

    def slow(self: Path, target):  # noqa: ANN001 - mirrors Path.replace's own loose typing
        if _STAGED_INFIX in self.name:
            order.append(self.name.split(_STAGED_INFIX)[1])
        time.sleep(0.001)
        return original(self, target)

    monkeypatch.setattr(Path, "replace", slow)
    return order


def _payload(marker: str, index: int) -> bytes:
    return f"{marker}-{index}".encode()


async def _stage_attempt(store: FileSystemPartsStore, object_id: str, attempt: str, count: int) -> None:
    for i in range(count):
        await store.stage_chunk(object_id, 1, 1, i, _payload(attempt, i), attempt_id=attempt)


async def _stage_both(store: FileSystemPartsStore, object_id: str) -> None:
    await _stage_attempt(store, object_id, ATTEMPT_A, COUNT_A)
    await _stage_attempt(store, object_id, ATTEMPT_B, COUNT_B)


async def _publish_both(store: FileSystemPartsStore, object_id: str) -> None:
    await asyncio.gather(
        store.publish_part(
            object_id, 1, 1, attempt_id=ATTEMPT_A, chunk_size=8, num_chunks=COUNT_A, size_bytes=8 * COUNT_A
        ),
        store.publish_part(
            object_id, 1, 1, attempt_id=ATTEMPT_B, chunk_size=8, num_chunks=COUNT_B, size_bytes=8 * COUNT_B
        ),
    )


@pytest.mark.asyncio
async def test_the_two_swaps_never_interleave(tmp_path, swap_order) -> None:
    """The invariant the locks encode, asserted directly rather than through its consequences.

    Most interleavings an unlocked publish produces happen to land on a legal final state anyway —
    the shorter attempt trims the longer one's tail and writes its meta last — so judging exclusion
    by the settled bytes catches an unlocked swap in only 2 runs out of 20 (measured), while this
    catches it in 20. Two contiguous blocks of renames is the property itself.
    """
    store = FileSystemPartsStore(str(tmp_path))
    object_id = str(uuid.uuid4())
    await _stage_both(store, object_id)

    await _publish_both(store, object_id)

    assert len(swap_order) == COUNT_A + COUNT_B, "both attempts must complete their own swap"
    handovers = sum(1 for before, after in itertools.pairwise(swap_order) if before != after)
    assert handovers == 1, f"the two swaps interleaved: {swap_order}"


@pytest.mark.asyncio
async def test_concurrent_publishes_of_one_part_settle_on_a_single_attempt(tmp_path, swap_order) -> None:
    store = FileSystemPartsStore(str(tmp_path))
    object_id = str(uuid.uuid4())
    await _stage_both(store, object_id)

    await _publish_both(store, object_id)

    part_dir = Path(store.part_path(object_id, 1, 1))
    names = sorted(p.name for p in part_dir.iterdir())
    meta = json.loads((part_dir / "meta.json").read_text())
    winner = meta["num_chunks"]
    assert winner in (COUNT_A, COUNT_B), f"meta claims a set neither attempt staged: {meta}"

    # Exactly {0..num_chunks-1} plus meta: no tail left by the larger attempt — the drain
    # replicates only an exact set and would strand the part — and no staged file left behind.
    assert names == sorted([f"chunk_{i}.bin" for i in range(winner)] + ["meta.json"])

    marker = ATTEMPT_A if winner == COUNT_A else ATTEMPT_B
    mixed = [i for i in range(winner) if (part_dir / f"chunk_{i}.bin").read_bytes() != _payload(marker, i)]
    assert not mixed, f"part mixes both attempts' chunks at indices {mixed} — silently decryptable corruption"


@pytest.mark.asyncio
async def test_the_publish_lock_registry_is_empty_once_no_publish_is_in_flight(tmp_path) -> None:
    """Refcount hygiene. One retained lock per part ever published is an unbounded leak in an api
    worker that lives for weeks; contenders still have to share ONE lock object, which is why the
    refcount is taken before the await rather than inside it."""
    store = FileSystemPartsStore(str(tmp_path))
    object_id = str(uuid.uuid4())
    await _stage_both(store, object_id)

    await _publish_both(store, object_id)

    assert fs_store_module._publish_locks._entries == {}
