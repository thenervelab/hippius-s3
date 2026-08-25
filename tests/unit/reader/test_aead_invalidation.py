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
from uuid import uuid4

import pytest
from cryptography.exceptions import InvalidTag
from hypothesis import HealthCheck
from hypothesis import given
from hypothesis import settings
from hypothesis import strategies as st
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


def _plaintext(index: int) -> bytes:
    # Distinct per chunk, so a chunk served for the wrong index cannot decrypt to the right body.
    return PLAINTEXT[index] if index < len(PLAINTEXT) else f"chunk-{index}-body".encode()


def _good(n: int = len(PLAINTEXT)) -> list[bytes]:
    return [_ct(_plaintext(i), part=1, index=i) for i in range(n)]


def _plan(n: int = len(PLAINTEXT)) -> list[ChunkPlanItem]:
    return [ChunkPlanItem(part_number=1, chunk_index=i) for i in range(n)]


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


async def _read(cache: _StoreCache, *, prefetch: int, key: bytes = KEY, n: int = len(PLAINTEXT)) -> bytes:
    gen = streamer.stream_plan(
        obj_cache=cache,
        object_id=OBJ,
        object_version=1,
        plan=_plan(n),
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


class _SuspectProbe:
    """A replication-status double that records every consultation."""

    def __init__(self, suspect: bool) -> None:
        self.suspect = suspect
        self.calls: list[tuple[str, int, int]] = []

    async def __call__(self, object_id: str, object_version: int, part_number: int) -> bool:
        self.calls.append((object_id, object_version, part_number))
        return self.suspect


@pytest.mark.asyncio
@pytest.mark.parametrize("prefetch", [0, 4])
async def test_a_redriven_part_fails_the_read_and_keeps_the_local_copy(tmp_path: Path, prefetch: int) -> None:
    """The stale-pool window: a redrive flips the part back to 'pending' while the pool still
    holds SUPERSEDED bytes that AEAD-verify under the same DEK/AAD.

    The pool here holds a DIFFERENT plaintext that decrypts cleanly — exactly what a
    superseded copy looks like — so without the status gate the retry would "succeed" with
    silently wrong content. The read must fail with the original decrypt error instead, and
    the local copy must survive: during a redrive it may be the only good one.
    """
    probe = _SuspectProbe(suspect=True)
    dual = _dual(tmp_path, replication_suspect=probe)
    stale = [_ct(b"superseded-bytes", part=1, index=0), _good()[1]]
    await _write_part(dual.fallback, stale)
    await _write_part(dual, [POISON_TAG, _good()[1]])

    collector = MagicMock()
    cache = _StoreCache(dual)
    with patch("hippius_s3.monitoring.get_metrics_collector", return_value=collector), pytest.raises(InvalidTag):
        await _read(cache, prefetch=prefetch)

    assert _chunk_file(dual, 0).read_bytes() == POISON_TAG, "the local copy is NOT unlinked"
    assert cache.fetches.count((1, 0)) == 1, "no retry — a retry would serve the stale pool bytes"
    assert probe.calls == [(OBJ, 1, 1)], "the status is re-checked freshly, once, for this part"
    collector.record_aead_failure.assert_called_once_with("remote", "unrecovered")


@pytest.mark.asyncio
@pytest.mark.parametrize("prefetch", [0, 4])
async def test_a_replicated_part_keeps_the_invalidate_and_retry_behaviour(tmp_path: Path, prefetch: int) -> None:
    """status='replicated' must change nothing: the pre-announcement B-2 window behind that
    status is closed drain-side (#403), so the read path keeps healing local corruption."""
    probe = _SuspectProbe(suspect=False)
    dual = _dual(tmp_path, replication_suspect=probe)
    good = _good()
    await _write_part(dual.fallback, good)
    await _write_part(dual, [POISON_TAG, good[1]])

    collector = MagicMock()
    cache = _StoreCache(dual)
    with patch("hippius_s3.monitoring.get_metrics_collector", return_value=collector):
        out = await _read(cache, prefetch=prefetch)

    assert out == b"".join(PLAINTEXT)
    assert not _chunk_file(dual, 0).exists(), "the poisoned local copy is gone"
    assert probe.calls == [(OBJ, 1, 1)]
    collector.record_aead_failure.assert_called_once_with("local", "recovered")


@pytest.mark.asyncio
async def test_the_probe_costs_nothing_on_a_healthy_read(tmp_path: Path) -> None:
    """The status check rides the AEAD-retry path only — the normal read never pays for it."""
    probe = _SuspectProbe(suspect=True)
    dual = _dual(tmp_path, replication_suspect=probe)
    await _write_part(dual, _good())

    out = await _read(_StoreCache(dual), prefetch=0)

    assert out == b"".join(PLAINTEXT)
    assert probe.calls == [], "no decrypt failure, so the status is never consulted"


@pytest.mark.asyncio
async def test_a_suspect_part_is_not_unlinked_even_when_the_pool_has_it(tmp_path: Path) -> None:
    """Both gates in order: pool presence passes, the fresh status check still refuses."""
    probe = _SuspectProbe(suspect=True)
    dual = _dual(tmp_path, replication_suspect=probe)
    good = _good()
    await _write_part(dual.fallback, good)
    await _write_part(dual, good)

    assert await dual.invalidate_local_chunk(OBJ, 1, 1, 0) is False
    assert _chunk_file(dual, 0).exists(), "the local copy survives a redrive in flight"


@pytest.mark.asyncio
async def test_a_missing_pool_copy_is_refused_before_the_status_is_asked(tmp_path: Path) -> None:
    """The cheap FS gate stays first: an SSD-only part never costs a DB round trip."""
    probe = _SuspectProbe(suspect=False)
    dual = _dual(tmp_path, replication_suspect=probe)
    await _write_part(dual, [_ct(PLAINTEXT[0], part=1, index=0)])

    assert await dual.invalidate_local_chunk(OBJ, 1, 1, 0) is False
    assert probe.calls == [], "no pool copy, so the status answer could not matter"


@pytest.mark.asyncio
async def test_a_truncated_pool_chunk_is_still_a_clean_error(tmp_path: Path) -> None:
    """A body too short to be a ciphertext raises CryptoError, not InvalidTag — same handling."""
    dual = _dual(tmp_path)
    await _write_part(dual.fallback, [POISON_SHORT, POISON_SHORT])

    collector = MagicMock()
    with patch("hippius_s3.monitoring.get_metrics_collector", return_value=collector), pytest.raises(CryptoError):
        await _read(_StoreCache(dual), prefetch=0)

    collector.record_aead_failure.assert_called_once_with("remote", "unrecovered")


class _InvalidationSpy:
    """Counts what a request asked to drop, and what it actually dropped.

    The per-chunk bound is enforced by `_StoreCache.RUNAWAY_AFTER`; this is the per-REQUEST
    side of it, which no fetch count can see: a plan can recover chunk after chunk, and the
    number of unlinks it accumulates while doing so is the size of the hole it leaves in the
    read tier.
    """

    def __init__(self, dual: DualFileSystemPartsStore) -> None:
        self._real = dual.invalidate_local_chunk
        self.probes: list[tuple[int, int]] = []
        self.unlinked: list[tuple[int, int]] = []

    async def __call__(self, object_id: str, object_version: int, part_number: int, chunk_index: int) -> bool:
        key = (int(part_number), int(chunk_index))
        self.probes.append(key)
        removed = bool(await self._real(object_id, object_version, part_number, chunk_index))
        if removed:
            self.unlinked.append(key)
        return removed


@st.composite
def _poisoned_plans(draw: st.DrawFn) -> tuple[int, frozenset[int]]:
    """A plan length and the chunk indices whose LOCAL copy is unreadable."""
    length = draw(st.integers(min_value=1, max_value=8))
    return length, draw(st.frozensets(st.integers(min_value=0, max_value=length - 1), max_size=length))


# Bounded because each example does real AES-GCM over a real store on a real filesystem; the
# input space (length x poison set x prefetch x promote x poison shape) is small enough that a
# couple of hundred examples cover it densely.
_PROPERTY = settings(
    max_examples=200,
    deadline=None,
    # tmp_path and the wait-path monkeypatch are function-scoped and shared across examples. Both
    # are safe here: every example builds its store under a fresh subdirectory of tmp_path, and
    # the patch sets one module attribute to the same value every time.
    suppress_health_check=[HealthCheck.function_scoped_fixture],
)


@pytest.mark.asyncio
@_PROPERTY
@given(
    shape=_poisoned_plans(),
    prefetch=st.sampled_from([0, 4]),
    promote=st.booleans(),
    poison=st.sampled_from([POISON_TAG, POISON_SHORT]),
)
async def test_a_recovering_request_heals_one_chunk_at_a_time_and_never_wipes_the_tier(
    tmp_path: Path, shape: tuple[int, frozenset[int]], prefetch: int, promote: bool, poison: bytes
) -> None:
    """The per-request bound, for any number of poisoned chunks anywhere in the plan.

    The existing bound tests each pin ONE failing chunk, which cannot distinguish "invalidates
    once per corrupt chunk" from "invalidates once per chunk". That difference is the whole
    blast radius: a request that heals as it goes must remove exactly the copies that failed,
    so the hole it leaves in the read tier is the size of the corruption and not the size of
    the read. A range GET over a large object is thousands of chunks.
    """
    length, poisoned = shape
    dual = _dual(tmp_path / uuid4().hex, promote=promote, on_promote=_claims_residency if promote else None)
    good = _good(length)
    await _write_part(dual.fallback, good)
    await _write_part(dual, [poison if i in poisoned else c for i, c in enumerate(good)])

    spy = _InvalidationSpy(dual)
    dual.invalidate_local_chunk = spy  # type: ignore[method-assign]
    collector = MagicMock()
    cache = _StoreCache(dual)
    with patch("hippius_s3.monitoring.get_metrics_collector", return_value=collector):
        out = await _read(cache, prefetch=prefetch, n=length)

    assert out == b"".join(_plaintext(i) for i in range(length)), "every chunk was served, in order"
    assert len(spy.unlinked) <= len(poisoned), "a request never drops more copies than it found corrupt"
    assert len(cache.fetches) <= 2 * length, "at most one retry per chunk across the whole request"
    # Exact, not just bounded: the counts above are len(poisoned) and length + len(poisoned), so a
    # mutation that invalidates a healthy chunk shows up even when the bound still happens to hold.
    assert spy.unlinked == sorted((1, i) for i in poisoned)
    assert len(cache.fetches) == length + len(poisoned)
    assert collector.record_aead_failure.call_count == len(poisoned)


@pytest.mark.asyncio
@_PROPERTY
@given(
    shape=st.integers(min_value=1, max_value=8).flatmap(
        lambda n: st.tuples(st.just(n), st.integers(min_value=0, max_value=n - 1))
    ),
    prefetch=st.sampled_from([0, 4]),
    promote=st.booleans(),
)
async def test_an_unrecoverable_chunk_ends_the_request_after_exactly_one_invalidation(
    tmp_path: Path, shape: tuple[int, int], prefetch: int, promote: bool
) -> None:
    """The other half of the bound: a chunk no tier can serve stops the stream, it does not skip on.

    Both copies of chunk `failing` are unreadable, so the retry fails too. Whatever the plan
    length and wherever the bad chunk sits, the request must invalidate once and end — this is
    the arm that would spin if the straight-line retry ever became a loop, because with
    promotion on the retry re-warms local flash from the pool and so always has something left
    to invalidate.
    """
    length, failing = shape
    dual = _dual(tmp_path / uuid4().hex, promote=promote, on_promote=_claims_residency if promote else None)
    poisoned = [POISON_TAG if i == failing else c for i, c in enumerate(_good(length))]
    await _write_part(dual.fallback, poisoned)
    await _write_part(dual, poisoned)

    spy = _InvalidationSpy(dual)
    dual.invalidate_local_chunk = spy  # type: ignore[method-assign]
    collector = MagicMock()
    cache = _StoreCache(dual)
    with patch("hippius_s3.monitoring.get_metrics_collector", return_value=collector), pytest.raises(InvalidTag):
        await _read(cache, prefetch=prefetch, n=length)

    assert spy.unlinked == [(1, failing)], "one unlink for the one chunk that failed, then the stream ends"
    assert len(cache.fetches) <= 2 * length, "the in-flight prefetch window, plus the single retry"
    assert cache.fetches.count((1, failing)) == 2, "the original fetch plus exactly one retry — never a loop"
    assert collector.record_aead_failure.call_count == 1
    collector.record_aead_failure.assert_called_once_with("local", "unrecovered")
