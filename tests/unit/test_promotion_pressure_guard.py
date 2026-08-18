"""Promotion must yield to ingest on the disk they share.

Read-through promotion is the only unthrottled writer to the ingest SSD, and on an ingest node
`HIPPIUS_OBJECT_CACHE_DIR` is the same mount as the drain agent's `CEPHOR_SSD_ROOT` — which is
also the mount `fs_cache_pressure` measures when it decides to refuse a PUT with 503. So a
read-heavy period could fill the disk that writes depend on, with promotion failures silently
swallowed the whole way down.

These tests pin the two halves of the fix: promotion backs off under pressure without ever
affecting the read, and the threshold sits where eviction can actually restore it.
"""

from __future__ import annotations

import logging
from unittest import mock

import pytest

from hippius_s3.cache import create_fs_store
from hippius_s3.cache.dual_fs_store import DualFileSystemPartsStore
from hippius_s3.fs_pressure import FreeSpaceGate
from hippius_s3.fs_pressure import PromotionBandError
from hippius_s3.fs_pressure import validate_promotion_band


# The values shipped in hippius_s3/config.py, restated so this file tests the CONTRACT rather
# than whatever the ambient environment resolves to. `.env` overrides both in local dev.
DEFAULT_PROMOTE_MIN_FREE_RATIO = 0.175
DEFAULT_FS_CACHE_MIN_FREE_RATIO = 0.08


class FakeProbe:
    """A scripted free-space reading, counting how often it was consulted."""

    def __init__(self, ratio: float, raises: BaseException | None = None) -> None:
        self.ratio = ratio
        self.raises = raises
        self.calls = 0

    def __call__(self, _path: str) -> float:
        self.calls += 1
        if self.raises is not None:
            raise self.raises
        return self.ratio


def gate(ratio: float, floor: float = 0.175, **kw: object) -> FreeSpaceGate:
    probe = FakeProbe(ratio)
    made = FreeSpaceGate("/nonexistent", floor, probe=probe, **kw)  # type: ignore[arg-type]
    made.probe = probe  # type: ignore[attr-defined]
    return made


def test_the_shipped_defaults_form_a_recoverable_band() -> None:
    """The shipped promote floor must sit inside the evictor's hysteresis band.

    Asserted on the CODE DEFAULT, not on `get_config()`. An earlier version of this test read
    the resolved config and was therefore validating whatever the developer's `.env` happened
    to say — `.env` sets `HIPPIUS_FS_CACHE_MIN_FREE_RATIO=0.01`, so a mutation of the shipped
    default changed nothing and the test passed regardless. A contract test must not depend on
    ambient environment.
    """
    validate_promotion_band(
        promote_min_free_ratio=DEFAULT_PROMOTE_MIN_FREE_RATIO,
        fs_cache_min_free_ratio=DEFAULT_FS_CACHE_MIN_FREE_RATIO,
    )


def test_a_floor_equal_to_the_evict_target_is_rejected() -> None:
    """The first attempt at this threshold. 0.20 IS the evictor's target, so promotion would be
    live only in the instant a pass completes — chattering at the boundary with no hysteresis."""
    with pytest.raises(PromotionBandError, match="below the evictor's target"):
        validate_promotion_band(promote_min_free_ratio=0.20, fs_cache_min_free_ratio=0.08)


def test_a_floor_above_the_evict_target_is_rejected() -> None:
    """The second attempt, and the more dangerous one: a deadlock rather than a wobble.

    Promotion stops below 0.25 and the evictor never frees past 0.20, so nothing can ever
    restore enough for promotion to resume. Staging sits at 23% free, so this would have
    switched the read tier off permanently the moment it deployed — silently.
    """
    with pytest.raises(PromotionBandError, match="can never be restored"):
        validate_promotion_band(promote_min_free_ratio=0.25, fs_cache_min_free_ratio=0.08)


def test_a_floor_below_the_evict_reserve_is_rejected() -> None:
    """Promotion must yield BEFORE eviction arms, or the cheap lever is pulled second."""
    with pytest.raises(PromotionBandError, match="above the evictor's reserve"):
        validate_promotion_band(promote_min_free_ratio=0.10, fs_cache_min_free_ratio=0.08)


def test_a_put_refusal_threshold_above_the_evict_reserve_is_rejected() -> None:
    """Writes must never be refused before the evictor has had a chance to reclaim."""
    with pytest.raises(PromotionBandError, match="below the evictor's reserve"):
        validate_promotion_band(promote_min_free_ratio=0.175, fs_cache_min_free_ratio=0.30)


@pytest.mark.asyncio
async def test_a_disk_with_room_allows_promotion() -> None:
    assert await gate(0.50).allows() is True


@pytest.mark.asyncio
async def test_a_disk_under_the_floor_refuses_promotion() -> None:
    assert await gate(0.10).allows() is False


@pytest.mark.asyncio
async def test_exactly_at_the_floor_refuses() -> None:
    """The floor is a floor. Strict `>` keeps the boundary off the allowed side."""
    assert await gate(0.175).allows() is False


@pytest.mark.asyncio
async def test_the_reading_is_memoised_so_a_chunk_read_is_not_a_syscall() -> None:
    """Promotion is asked per CHUNK, so an unmemoised probe is a syscall per chunk.

    64 reads inside one TTL window must cost one probe; crossing the window costs a second.
    """
    probe = FakeProbe(0.50)
    g = FreeSpaceGate("/nonexistent", 0.175, ttl_seconds=5.0, probe=probe)

    for _ in range(64):
        assert await g.allows(now=100.0) is True
    assert probe.calls == 1, "one probe per TTL window"

    await g.allows(now=106.0)
    assert probe.calls == 2, "the window expired, so it re-probed"


@pytest.mark.asyncio
async def test_a_failing_probe_leaves_the_previous_verdict_rather_than_disabling_the_tier() -> None:
    """Fails open. A statvfs blip must not silently switch the read tier off.

    A genuinely full disk still stops the write with ENOSPC, which `_promote_chunk` already
    swallows — so failing open costs a wasted write attempt, while failing closed would cost
    the whole cache-warming path with nothing logging that it happened.
    """
    probe = FakeProbe(0.50)
    g = FreeSpaceGate("/nonexistent", 0.175, ttl_seconds=1.0, probe=probe)
    assert await g.allows(now=0.0) is True

    probe.raises = OSError("statvfs failed")
    assert await g.allows(now=10.0) is True, "kept the last good verdict"


# --------------------------------------------------------------------------------------------
# The published floor. The static one above is only the fallback: the evictor the gate has to
# stay inside obeys a per-node reserve the allocator moves at runtime, so the live floor is
# published by the drain agent and this side must track it.


class FakeFloor:
    """A scripted published-floor source, counting how often it was consulted."""

    def __init__(self, ratio: float | None, raises: BaseException | None = None) -> None:
        self.ratio = ratio
        self.raises = raises
        self.calls = 0

    async def __call__(self) -> float | None:
        self.calls += 1
        if self.raises is not None:
            raise self.raises
        return self.ratio


@pytest.mark.asyncio
async def test_a_published_floor_refuses_a_promotion_the_static_floor_would_allow() -> None:
    """The finding, as one assertion.

    At the allocator's maximum reserve (400 permille) the evictor holds 40% free, so its
    published floor is 425 permille. A node with 30% free is BELOW that floor and above the
    static 0.175 — pre-fix the gate consulted only the static number and kept promoting into an
    armed evictor. That is the inverted ordering, and it happens exactly when the allocator has
    decided the drain is in trouble.
    """
    g = FreeSpaceGate("/nonexistent", 0.175, probe=FakeProbe(0.30), floor_source=FakeFloor(0.425))
    assert await g.allows() is False

    static_only = FreeSpaceGate("/nonexistent", 0.175, probe=FakeProbe(0.30))
    assert await static_only.allows() is True, "the static floor alone would have allowed it"


@pytest.mark.asyncio
async def test_no_published_key_behaves_exactly_as_the_static_gate() -> None:
    """Fail OPEN to today's behaviour. An agent that has never published — a fresh node, a
    pre-rollout agent, an evictor with its kill-switch on — must not disable the read tier."""
    for ratio in (0.30, 0.10):
        published = FreeSpaceGate("/nonexistent", 0.175, probe=FakeProbe(ratio), floor_source=FakeFloor(None))
        static = FreeSpaceGate("/nonexistent", 0.175, probe=FakeProbe(ratio))
        assert await published.allows() is await static.allows(), f"{ratio} must decide identically"


@pytest.mark.asyncio
async def test_a_floor_source_that_raises_falls_back_instead_of_failing_the_read() -> None:
    """Promotion is awaited inline in `_promote_chunk`, so an exception here would surface on a
    GET. A Redis outage must cost the published floor, not the read."""
    g = FreeSpaceGate(
        "/nonexistent", 0.175, probe=FakeProbe(0.30), floor_source=FakeFloor(None, raises=RuntimeError("redis down"))
    )
    assert await g.allows() is True, "fell back to the static floor"


@pytest.mark.asyncio
async def test_the_published_floor_is_looked_up_once_per_memo_window_not_once_per_chunk() -> None:
    """Promotion asks per CHUNK. An unmemoised lookup would be a Redis round-trip per chunk on
    the read path — strictly worse than the syscall the memo already exists to avoid."""
    floor = FakeFloor(0.425)
    g = FreeSpaceGate("/nonexistent", 0.175, ttl_seconds=5.0, probe=FakeProbe(0.90), floor_source=floor)

    for _ in range(64):
        await g.allows(now=100.0)
    assert floor.calls == 1, "one lookup per TTL window"

    await g.allows(now=106.0)
    assert floor.calls == 2, "the window expired, so it re-read the published floor"


@pytest.mark.asyncio
async def test_a_floor_matching_the_configured_one_is_not_reported_as_a_divergence() -> None:
    """The steady state. An agent on the default policy publishes 175 permille, which is the
    shipped `HIPPIUS_PROMOTE_MIN_FREE_RATIO` exactly — so the common case must be silent, or
    the divergence signal is noise and nobody will look at it when it matters."""
    counted: list[str] = []
    g = FreeSpaceGate("/nonexistent", 0.175, probe=FakeProbe(0.90), floor_source=FakeFloor(175 / 1000.0))
    with mock.patch("hippius_s3.fs_pressure._record_floor_divergence", counted.append):
        assert await g.allows() is True
    assert counted == []


@pytest.mark.asyncio
async def test_a_divergent_floor_is_counted_every_window_and_logged_once_per_change(caplog) -> None:
    """Both directions of the silent failure this change exists to remove.

    A published floor that differs means the evictor is holding a band this process was
    configured for the wrong side of. Counting every decision gives the rate; logging on the
    TRANSITION keeps a multi-hour stall from emitting a line every 5 seconds per pod.
    """
    counted: list[str] = []
    floor = FakeFloor(0.425)
    g = FreeSpaceGate("/nonexistent", 0.175, ttl_seconds=5.0, probe=FakeProbe(0.90), floor_source=floor)

    with mock.patch("hippius_s3.fs_pressure._record_floor_divergence", counted.append):
        with caplog.at_level(logging.WARNING, logger="hippius_s3.fs_pressure"):
            await g.allows(now=0.0)
            await g.allows(now=10.0)
            floor.ratio = 0.375
            await g.allows(now=20.0)

    assert counted == ["stricter", "stricter", "stricter"], "every decision on a divergent floor is counted"
    warnings = [r for r in caplog.records if "promote floor" in r.getMessage()]
    assert len(warnings) == 2, "logged when the divergence appeared and again when it moved, not every window"


@pytest.mark.asyncio
async def test_the_api_wires_the_published_floor_into_the_gate_it_builds(tmp_path) -> None:
    """The wiring, end to end through the factory.

    Everything above tests a gate constructed by hand. This one pins that `create_fs_store` —
    the single place the api builds a promoting store — actually threads the published source
    through, since a gate built without it fails silently in exactly the invisible way the
    original bug did.
    """

    class FakeConfig:
        object_cache_dir = str(tmp_path / "local")
        object_cache_fallback_dir = str(tmp_path / "pool")
        object_cache_promote_on_read = True
        promote_min_free_ratio = 0.175
        fs_cache_min_free_ratio = 0.08

    async def on_promote(*_args: object) -> None:
        return None

    store = create_fs_store(FakeConfig(), on_promote=on_promote, published_floor=FakeFloor(0.425))
    gate_built = store._space_gate  # type: ignore[attr-defined]
    assert gate_built is not None
    # Probe a disk with 30% free: allowed under the static 0.175, refused under the published
    # 0.425. The verdict is the only externally visible proof the source was threaded through.
    gate_built._probe = FakeProbe(0.30)  # type: ignore[attr-defined]
    assert await gate_built.allows() is False


@pytest.mark.asyncio
async def test_a_chunk_is_still_served_when_promotion_is_gated(tmp_path, monkeypatch) -> None:
    """The point of the whole change: backpressure costs a cache warm, never a read.

    The chunk comes back byte-identical from the pool while nothing lands on the local tier,
    and the skip is counted so the backoff is visible before `fs_cache_shed` starts firing.
    """
    primary = tmp_path / "local"
    fallback = tmp_path / "pool"
    primary.mkdir()
    fallback.mkdir()

    payload = b"ciphertext-chunk"
    pool = DualFileSystemPartsStore(str(primary), str(fallback))
    await pool.fallback.set_meta(
        "a" * 8 + "-1111-2222-3333-444444444444", 1, 1, chunk_size=16, num_chunks=1, size_bytes=16
    )
    await pool.fallback.set_chunk("a" * 8 + "-1111-2222-3333-444444444444", 1, 1, 0, payload)

    recorded: list[str] = []
    monkeypatch.setattr(
        "hippius_s3.cache.dual_fs_store._record_promotion_skipped",
        recorded.append,
    )

    promoted: list[tuple] = []

    async def on_promote(*args: object) -> bool:
        promoted.append(args)
        return True

    store = DualFileSystemPartsStore(
        str(primary),
        str(fallback),
        promote=True,
        on_promote=on_promote,
        space_gate=FreeSpaceGate("/nonexistent", 0.175, probe=FakeProbe(0.02)),
    )

    oid = "a" * 8 + "-1111-2222-3333-444444444444"
    got = await store.get_chunk(oid, 1, 1, 0)

    assert got == payload, "the read is served regardless of disk pressure"
    assert promoted == [], "nothing was promoted onto the pressured disk"
    assert recorded == ["disk_pressure"], "the backoff is counted under its own reason"
    assert not (primary / oid).exists(), "no local copy was written"


@pytest.mark.asyncio
async def test_promotion_still_happens_when_the_disk_has_room(tmp_path) -> None:
    """The no-regression half: the gate must not quietly disable the tier it protects."""
    primary = tmp_path / "local"
    fallback = tmp_path / "pool"
    primary.mkdir()
    fallback.mkdir()

    oid = "b" * 8 + "-1111-2222-3333-444444444444"
    payload = b"ciphertext-chunk"
    seed = DualFileSystemPartsStore(str(primary), str(fallback))
    await seed.fallback.set_meta(oid, 1, 1, chunk_size=16, num_chunks=1, size_bytes=16)
    await seed.fallback.set_chunk(oid, 1, 1, 0, payload)

    promoted: list[tuple] = []

    async def on_promote(*args: object) -> bool:
        promoted.append(args)
        return True

    store = DualFileSystemPartsStore(
        str(primary),
        str(fallback),
        promote=True,
        on_promote=on_promote,
        space_gate=FreeSpaceGate("/nonexistent", 0.175, probe=FakeProbe(0.90)),
    )

    assert await store.get_chunk(oid, 1, 1, 0) == payload
    assert promoted, "a healthy disk still warms the local tier"
    assert await store.read_local_chunk(oid, 1, 1, 0) == payload
