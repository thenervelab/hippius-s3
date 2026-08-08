"""WI-10: a chunk whose stored ciphertext fails authentication is dropped from local flash.

Without this, one bad locally-cached chunk is a PERMANENT read failure for that object on
that node: the local tier is checked first, so every retry re-reads the same bad bytes, and
nothing on the read path unlinks them. Read-through promotion is what makes it reachable —
a peer or pool body is copied onto local flash, and a peer on a rolled-back image can plant
one.

These tests use the real store, real AES-GCM and the real `stream_plan`, because the whole
finding is about how those three compose: the decrypter sees the failure but not the tier,
and the store knows the tier but does not decrypt.
"""

from __future__ import annotations

from collections import Counter
from pathlib import Path
from typing import Any
from unittest.mock import MagicMock
from unittest.mock import patch

import pytest
from cryptography.exceptions import InvalidTag
from nacl.exceptions import CryptoError

from hippius_s3.cache.dual_fs_store import DualFileSystemPartsStore
from hippius_s3.cache.fs_store import FileSystemPartsStore
from hippius_s3.cache.notifier import ChunkNotReadyError
from hippius_s3.reader import streamer
from hippius_s3.reader.types import ChunkPlanItem
from hippius_s3.services.crypto_service import CryptoService


OBJ = "466916c0-d61b-4518-b81b-9576b574270a"
BUCKET = "bkt"
KEY = b"\x11" * 32
WRONG_KEY = b"\x22" * 32
SUITE = "hip-enc/aes256gcm"
PLAINTEXT = (b"alpha-chunk", b"beta-chunk")

# A body long enough to hold a nonce + tag, so it fails the AEAD tag check (InvalidTag), and
# one too short to be a ciphertext at all (CryptoError) — the shape a version-skewed peer
# serves. Both mean the same thing: the bytes on this disk are not the chunk.
POISON_TAG = b"poison" * 8
POISON_SHORT = b"short"


@pytest.fixture(autouse=True)
def _per_chunk_wait_path(monkeypatch: pytest.MonkeyPatch) -> None:
    # Pin the wait path so these tests exercise invalidation, not subscription wiring.
    monkeypatch.setattr(streamer, "_single_subscription_enabled", lambda: False)


def _ct(plaintext: bytes, *, part: int, index: int) -> bytes:
    return CryptoService.get_adapter(SUITE).encrypt_chunk(
        plaintext,
        key=KEY,
        bucket_id=BUCKET,
        object_id=OBJ,
        part_number=part,
        chunk_index=index,
        upload_id="",
    )


def _good() -> list[bytes]:
    return [_ct(pt, part=1, index=i) for i, pt in enumerate(PLAINTEXT)]


def _plan() -> list[ChunkPlanItem]:
    return [
        ChunkPlanItem(part_number=1, chunk_index=0),
        ChunkPlanItem(part_number=1, chunk_index=1),
    ]


async def _write_part(store: FileSystemPartsStore, chunks: list[bytes], *, part: int = 1) -> None:
    for index, data in enumerate(chunks):
        await store.set_chunk(OBJ, 1, part, index, data)
    await store.set_meta(
        OBJ,
        1,
        part,
        chunk_size=len(chunks[0]),
        num_chunks=len(chunks),
        size_bytes=sum(len(c) for c in chunks),
    )


async def _claims_residency(*_args: object) -> bool:
    # WI-7: promotion is cancelled by a falsy claim, so a test double must return a real bool.
    return True


class _StoreCache:
    """The slice of the parts-cache surface `stream_plan` uses, over a real store.

    Trips on a runaway retry rather than spinning: with promotion on, an invalidate-and-retry
    LOOP re-warms the copy it just dropped and so never runs out of things to invalidate. That
    is the fleet-wide failure this whole bound exists to prevent, and a test that hangs on it
    reports far worse than one that fails on it.
    """

    RUNAWAY_AFTER = 3  # the original fetch + one retry is 2; a third means it is looping

    def __init__(self, store: FileSystemPartsStore) -> None:
        self.fs = store
        self.fetches: list[tuple[int, int]] = []

    async def wait_for_chunk(
        self,
        object_id: str,
        object_version: int,
        part_number: int,
        chunk_index: int,
        *,
        timeout: float | None = None,  # noqa: ASYNC109
    ) -> bytes:
        key = (int(part_number), int(chunk_index))
        self.fetches.append(key)
        if self.fetches.count(key) >= self.RUNAWAY_AFTER:
            raise AssertionError(f"runaway retry: chunk {key} fetched {self.fetches.count(key)} times")
        data = await self.fs.get_chunk(object_id, int(object_version), int(part_number), int(chunk_index))
        if data is None:
            raise ChunkNotReadyError(f"no chunk {part_number}/{chunk_index}")
        return data


async def _read(cache: _StoreCache, *, prefetch: int, key: bytes = KEY) -> bytes:
    gen = streamer.stream_plan(
        obj_cache=cache,
        object_id=OBJ,
        object_version=1,
        plan=_plan(),
        storage_version=5,
        key_bytes=key,
        suite_id=SUITE,
        bucket_id=BUCKET,
        upload_id="",
        prefetch_chunks=prefetch,
        chunk_timeout=5.0,
    )
    return b"".join([chunk async for chunk in gen])


def _chunk_file(store: FileSystemPartsStore, index: int) -> Path:
    return Path(store.part_path(OBJ, 1, 1)) / f"chunk_{index}.bin"


def _dual(tmp_path: Path, **kwargs: Any) -> DualFileSystemPartsStore:
    return DualFileSystemPartsStore(str(tmp_path / "ssd"), str(tmp_path / "pool"), **kwargs)


@pytest.mark.asyncio
@pytest.mark.parametrize("prefetch", [0, 4])
@pytest.mark.parametrize("poison", [POISON_TAG, POISON_SHORT], ids=["bad_tag", "truncated"])
@pytest.mark.parametrize("promote", [False, True], ids=["no_promote", "promote"])
async def test_a_poisoned_local_chunk_is_invalidated_and_the_read_recovers(
    tmp_path: Path, prefetch: int, poison: bytes, promote: bool
) -> None:
    """The whole point: a bad local copy must not be a permanent failure for this object."""
    dual = _dual(tmp_path, promote=promote, on_promote=_claims_residency if promote else None)
    good = _good()
    await _write_part(dual.fallback, good)
    await _write_part(dual, [poison, good[1]])

    collector = MagicMock()
    cache = _StoreCache(dual)
    with patch("hippius_s3.monitoring.get_metrics_collector", return_value=collector):
        out = await _read(cache, prefetch=prefetch)

    assert out == b"".join(PLAINTEXT), "the next tier served the chunk the local copy could not"
    local = _chunk_file(dual, 0)
    # With promotion on, the retry re-warms local flash — with the GOOD bytes. Either way the
    # poison is gone, which is what makes the failure non-permanent.
    assert not local.exists() or local.read_bytes() == good[0], "the poisoned local copy is gone"
    collector.record_aead_failure.assert_called_once_with("local", "recovered")


@pytest.mark.asyncio
@pytest.mark.parametrize("prefetch", [0, 4])
async def test_a_poisoned_pool_chunk_stays_an_error_and_unlinks_nothing(tmp_path: Path, prefetch: int) -> None:
    """The pool copy is authoritative and the fault may be the DEK, not the bytes.

    Deleting it would destroy the only copy over a fault we cannot attribute, so a pool
    chunk failing AEAD stays a genuine error.
    """
    dual = _dual(tmp_path)  # promotion off: nothing of the pool body lands locally
    await _write_part(dual.fallback, [POISON_TAG, POISON_TAG])

    collector = MagicMock()
    cache = _StoreCache(dual)
    with patch("hippius_s3.monitoring.get_metrics_collector", return_value=collector), pytest.raises(InvalidTag):
        await _read(cache, prefetch=prefetch)

    assert _chunk_file(dual.fallback, 0).read_bytes() == POISON_TAG, "the pool copy is untouched"
    assert cache.fetches.count((1, 0)) == 1, "nothing to invalidate, so nothing to retry"
    collector.record_aead_failure.assert_called_once_with("remote", "unrecovered")


@pytest.mark.asyncio
@pytest.mark.parametrize("prefetch", [0, 4])
@pytest.mark.parametrize("promote", [False, True], ids=["no_promote", "promote"])
async def test_a_wrong_dek_retries_at_most_once_and_then_fails_cleanly(
    tmp_path: Path, prefetch: int, promote: bool
) -> None:
    """The fleet-wide case: a DEK fault fails every chunk of every object.

    Both tiers hold intact ciphertext, so no invalidation can ever help. The bound is what
    keeps this from turning every read in the fleet into a cache wipe plus a pool re-read
    loop — count the fetches, because that is the constraint whose violation is an incident.

    The `promote` arm is the one that can actually catch a loop, and it is prod's config: the
    retry re-warms local flash from the pool, so a looping retry always has something to
    invalidate again and would spin forever.
    """
    dual = _dual(tmp_path, promote=promote, on_promote=_claims_residency if promote else None)
    good = _good()
    await _write_part(dual.fallback, good)
    await _write_part(dual, good)

    collector = MagicMock()
    cache = _StoreCache(dual)
    with patch("hippius_s3.monitoring.get_metrics_collector", return_value=collector), pytest.raises(InvalidTag):
        await _read(cache, prefetch=prefetch, key=WRONG_KEY)

    fetches = Counter(cache.fetches)
    assert fetches[(1, 0)] == 2, "the original fetch plus exactly one retry — never a loop"
    # And the request as a whole invalidates ONCE: a retry that fails too is not a local-corruption
    # fault, and the stream ends there rather than working through the rest of the plan.
    assert collector.record_aead_failure.call_count == 1
    collector.record_aead_failure.assert_called_once_with("local", "unrecovered")


@pytest.mark.asyncio
@pytest.mark.parametrize("prefetch", [0, 4])
async def test_the_healthy_path_never_probes_or_invalidates(tmp_path: Path, prefetch: int) -> None:
    """The steady state pays nothing: one fetch per chunk, no pool stat, no unlink."""
    dual = _dual(tmp_path)
    good = _good()
    await _write_part(dual, good)
    probes: list[object] = []
    real = dual.invalidate_local_chunk

    async def _spy(*args: object) -> bool:
        probes.append(args)
        return await real(*args)  # type: ignore[arg-type]

    dual.invalidate_local_chunk = _spy  # type: ignore[method-assign]

    collector = MagicMock()
    cache = _StoreCache(dual)
    with patch("hippius_s3.monitoring.get_metrics_collector", return_value=collector):
        out = await _read(cache, prefetch=prefetch)

    assert out == b"".join(PLAINTEXT)
    assert cache.fetches == [(1, 0), (1, 1)], "one fetch per chunk"
    assert probes == [], "no invalidation probe on a healthy read"
    collector.record_aead_failure.assert_not_called()


@pytest.mark.asyncio
async def test_a_chunk_the_pool_cannot_reserve_is_never_invalidated(tmp_path: Path) -> None:
    """A freshly ingested part's SSD copy is the ONLY copy until the drain replicates it.

    A DEK fault fails those chunks too, so an unconditional unlink would turn a read failure
    into permanent data loss. The pool-presence gate is what keeps invalidation to copies the
    next tier can re-serve.
    """
    dual = _dual(tmp_path)
    await _write_part(dual, [_ct(PLAINTEXT[0], part=1, index=0)])

    assert await dual.invalidate_local_chunk(OBJ, 1, 1, 0) is False
    assert _chunk_file(dual, 0).exists(), "the only copy of an undrained chunk survives"


@pytest.mark.asyncio
async def test_invalidating_one_chunk_leaves_the_part_readable(tmp_path: Path) -> None:
    """Removing one chunk, not the part: the siblings are good bytes and meta is the readiness gate.

    A part with meta and a hole is exactly the downloader's normal partial-fill state — the
    hole reads as a miss and falls through a tier, while every other chunk still serves.
    """
    dual = _dual(tmp_path)
    good = _good()
    await _write_part(dual.fallback, good)
    await _write_part(dual, good)

    assert await dual.invalidate_local_chunk(OBJ, 1, 1, 0) is True

    primary = FileSystemPartsStore(str(tmp_path / "ssd"))
    assert await primary.get_chunk(OBJ, 1, 1, 0) is None, "the invalidated chunk reads as a miss"
    assert await primary.get_chunk(OBJ, 1, 1, 1) == good[1], "its siblings are untouched"
    assert (Path(dual.part_path(OBJ, 1, 1)) / "meta.json").exists(), "the readiness gate stays"
    assert _chunk_file(dual.fallback, 0).read_bytes() == good[0], "the pool copy is never touched"


def test_a_single_tier_store_has_no_local_invalidation(tmp_path: Path) -> None:
    """Only a store WITH a lower tier may unlink from its primary.

    Without a fallback dir the one store's root is the shared pool itself, so the same call
    would delete the authoritative copy. Keeping the method off the base class means no call
    site can reach it by accident.
    """
    assert not hasattr(FileSystemPartsStore(str(tmp_path / "pool")), "invalidate_local_chunk")


@pytest.mark.asyncio
async def test_a_single_tier_deployment_never_unlinks_its_only_copy(tmp_path: Path) -> None:
    """The same read against a store with no lower tier: an error, and the bytes stay put."""
    store = FileSystemPartsStore(str(tmp_path / "pool"))
    await _write_part(store, [POISON_TAG, POISON_TAG])

    collector = MagicMock()
    cache = _StoreCache(store)
    with patch("hippius_s3.monitoring.get_metrics_collector", return_value=collector), pytest.raises(InvalidTag):
        await _read(cache, prefetch=0)

    assert _chunk_file(store, 0).read_bytes() == POISON_TAG
    collector.record_aead_failure.assert_called_once_with("remote", "unrecovered")


@pytest.mark.asyncio
async def test_a_truncated_pool_chunk_is_still_a_clean_error(tmp_path: Path) -> None:
    """A body too short to be a ciphertext raises CryptoError, not InvalidTag — same handling."""
    dual = _dual(tmp_path)
    await _write_part(dual.fallback, [POISON_SHORT, POISON_SHORT])

    collector = MagicMock()
    with patch("hippius_s3.monitoring.get_metrics_collector", return_value=collector), pytest.raises(CryptoError):
        await _read(_StoreCache(dual), prefetch=0)

    collector.record_aead_failure.assert_called_once_with("remote", "unrecovered")
