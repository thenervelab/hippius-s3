"""DualFileSystemPartsStore read-fallback coverage for the batch existence check.

Under the drain-direct model the drain copies a part to the shared pool (the fallback
tier) and unlinks the node-local SSD copy (the primary). The read path decides
cache-vs-pipeline via `chunks_exist_batch`; if that batch check ignores the fallback,
a part that is durably present on the pool is wrongly fetched through the download
pipeline (observed as x-hippius-source=pipeline in the append e2e tests).
"""

from __future__ import annotations

import asyncio

import pytest

from hippius_s3.cache.dual_fs_store import DualFileSystemPartsStore
from hippius_s3.cache.dual_fs_store import _stream_tiers
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

    async def _record(object_id: str, version: int, part_number: int, size_bytes: int) -> bool:
        recorded.append((object_id, version, part_number, size_bytes))
        return True

    dual = DualFileSystemPartsStore(str(tmp_path / "ssd"), str(tmp_path / "pool"), promote=True, on_promote=_record)
    await _write_part(dual.fallback, part_number=1, chunk=b"pool-only")

    await dual.get_chunk(OBJ, 1, 1, 0)

    assert recorded == [(OBJ, 1, 1, len(b"pool-only"))], "the evictor was told what this node now holds"


@pytest.mark.asyncio
async def test_a_primary_hit_never_promotes_or_records(tmp_path) -> None:
    """The steady state costs nothing: no pool read, no copy, no residency write."""
    recorded: list[tuple[str, int, int, int]] = []

    async def _record(*args: object) -> bool:
        recorded.append(args)  # type: ignore[arg-type]
        return True

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


@pytest.mark.asyncio
async def test_a_peer_holding_the_part_is_tried_before_the_pool(tmp_path) -> None:
    """Peer NVMe beats the pool: ~6 ms + ~1 ms network against ~40 ms per chunk.

    Locality is resolved per PART, not per request — measured on prod 2026-08-06, only 2%
    of multi-part objects have every part on one node, so routing a whole GET somewhere
    would leave most parts remote anyway.
    """
    calls: list[tuple[str, int, int, int]] = []

    async def _peer(object_id: str, version: int, part_number: int, chunk_index: int) -> bytes | None:
        calls.append((object_id, version, part_number, chunk_index))
        return b"from-peer"

    dual = DualFileSystemPartsStore(str(tmp_path / "ssd"), str(tmp_path / "pool"), peer_fetch=_peer)
    # Present on the pool too — the peer must win, or the tier is pointless.
    await _write_part(dual.fallback, part_number=1, chunk=b"from-pool")

    assert await dual.get_chunk(OBJ, 1, 1, 0) == b"from-peer"
    assert calls == [(OBJ, 1, 1, 0)]


@pytest.mark.asyncio
async def test_a_local_hit_never_asks_a_peer(tmp_path) -> None:
    """The steady state must not pay a network round trip or a residency lookup."""
    calls: list[object] = []

    async def _peer(*args: object) -> bytes | None:
        calls.append(args)
        return None

    dual = DualFileSystemPartsStore(str(tmp_path / "ssd"), str(tmp_path / "pool"), peer_fetch=_peer)
    await _write_part(dual, part_number=1, chunk=b"on-ssd")

    assert await dual.get_chunk(OBJ, 1, 1, 0) == b"on-ssd"
    assert calls == [], "a local hit short-circuits before the peer tier"


@pytest.mark.asyncio
async def test_a_failing_peer_falls_through_to_the_pool(tmp_path) -> None:
    """A peer is an optimisation. If it is down, slow, or lying, the pool still serves.

    The pool copy is authoritative and always present for a replicated part, so no peer
    failure may ever surface to the client.
    """

    async def _peer(*args: object) -> bytes | None:
        raise OSError("connection refused")

    dual = DualFileSystemPartsStore(str(tmp_path / "ssd"), str(tmp_path / "pool"), peer_fetch=_peer)
    await _write_part(dual.fallback, part_number=1, chunk=b"from-pool")

    assert await dual.get_chunk(OBJ, 1, 1, 0) == b"from-pool"


@pytest.mark.asyncio
async def test_a_peer_miss_falls_through_to_the_pool(tmp_path) -> None:
    """No peer holds it (all evicted, or none ever promoted) — the pool is the answer."""

    async def _peer(*args: object) -> bytes | None:
        return None

    dual = DualFileSystemPartsStore(str(tmp_path / "ssd"), str(tmp_path / "pool"), peer_fetch=_peer)
    await _write_part(dual.fallback, part_number=1, chunk=b"from-pool")

    assert await dual.get_chunk(OBJ, 1, 1, 0) == b"from-pool"


@pytest.mark.asyncio
async def test_read_local_chunk_never_serves_the_fallback(tmp_path) -> None:
    """What a peer serves must come off ITS flash, never off the shared pool.

    If this followed the fallback, a peer fetch would become an extra network hop in front
    of the very pool read the tier exists to avoid — and worse, two nodes each resolving the
    other could bounce a request between them instead of just reading the pool.
    """
    dual = DualFileSystemPartsStore(str(tmp_path / "ssd"), str(tmp_path / "pool"))
    await _write_part(dual.fallback, part_number=1, chunk=b"pool-only")

    assert await dual.get_chunk(OBJ, 1, 1, 0) == b"pool-only", "the normal read path does fall back"
    assert await dual.read_local_chunk(OBJ, 1, 1, 0) is None, "but a peer serves local flash only"


@pytest.mark.asyncio
async def test_read_local_chunk_serves_a_local_part(tmp_path) -> None:
    dual = DualFileSystemPartsStore(str(tmp_path / "ssd"), str(tmp_path / "pool"))
    await _write_part(dual, part_number=1, chunk=b"on-ssd")

    assert await dual.read_local_chunk(OBJ, 1, 1, 0) == b"on-ssd"


@pytest.mark.asyncio
async def test_the_tier_counter_counts_only_inside_a_stream(tmp_path) -> None:
    """Outside a stream the ContextVar is unset and reads count into metrics alone."""
    dual = DualFileSystemPartsStore(str(tmp_path / "ssd"), str(tmp_path / "pool"))
    await _write_part(dual.fallback, part_number=1, chunk=b"pool-only")
    await _write_part(dual, part_number=2, chunk=b"on-ssd")

    assert _stream_tiers.get() is None
    assert await dual.get_chunk(OBJ, 1, 1, 0) == b"pool-only"
    assert _stream_tiers.get() is None, "a read never sets the counter on its own"

    counts: dict[str, int] = {}
    token = _stream_tiers.set(counts)
    try:
        await dual.get_chunk(OBJ, 1, 1, 0)
        await dual.get_chunk(OBJ, 1, 2, 0)
        # The streamer fetches from spawned tasks; they inherit the dict by reference.
        await asyncio.create_task(dual.get_chunk(OBJ, 1, 2, 0))
    finally:
        _stream_tiers.reset(token)
    assert counts == {"pool": 1, "local": 2}

    await dual.get_chunk(OBJ, 1, 2, 0)
    assert counts == {"pool": 1, "local": 2}, "nothing counts once the stream's counter is gone"


@pytest.mark.asyncio
async def test_a_promoted_chunk_announces_itself(tmp_path) -> None:
    """A reader parked in wait_for_chunk on this chunk wakes only if something publishes for it.

    The downloader publishes for what it lands; nothing published for a promotion, so a reader
    that fell to an empty pool while a sibling promoted the chunk slept out the chunk timeout.
    """
    announced: list[tuple[str, int, int, int]] = []

    async def _hook(object_id: str, version: int, part_number: int, chunk_index: int) -> None:
        announced.append((object_id, version, part_number, chunk_index))

    dual = DualFileSystemPartsStore(str(tmp_path / "ssd"), str(tmp_path / "pool"), promote=True, on_promoted=_hook)
    await _write_part(dual.fallback, part_number=1, chunk=b"pool-only")

    assert await dual.get_chunk(OBJ, 1, 1, 0) == b"pool-only"
    assert announced == [(OBJ, 1, 1, 0)]

    assert await dual.get_chunk(OBJ, 1, 1, 0) == b"pool-only"
    assert announced == [(OBJ, 1, 1, 0)], "a local hit promotes nothing and announces nothing"


@pytest.mark.asyncio
async def test_a_failed_promotion_announces_nothing(tmp_path, monkeypatch) -> None:
    announced: list[object] = []

    async def _hook(*args: object) -> None:
        announced.append(args)

    async def _boom(*args, **kwargs):
        raise OSError("no space left on device")

    dual = DualFileSystemPartsStore(str(tmp_path / "ssd"), str(tmp_path / "pool"), promote=True, on_promoted=_hook)
    await _write_part(dual.fallback, part_number=1, chunk=b"pool-only")
    monkeypatch.setattr(dual, "set_chunk", _boom)

    assert await dual.get_chunk(OBJ, 1, 1, 0) == b"pool-only"
    assert announced == [], "no copy landed, so there is nothing to wake a waiter for"


@pytest.mark.asyncio
async def test_peer_locate_without_a_fetcher_reports_no_owner(tmp_path) -> None:
    dual = DualFileSystemPartsStore(str(tmp_path / "ssd"), str(tmp_path / "pool"))

    assert await dual.peer_locate(OBJ, 1, 1) == (None, False)
    assert dual.peer_last_owner(OBJ, 1, 1) is None


class _LocatingFetcher:
    def __init__(self, owner: str | None, unreplicated: bool) -> None:
        self._owner = owner
        self._unreplicated = unreplicated
        self.located: list[tuple[str, int, int]] = []

    async def __call__(self, object_id: str, version: int, part_number: int, chunk_index: int) -> bytes | None:
        return None

    async def locate(self, object_id: str, version: int, part_number: int) -> tuple[str | None, bool]:
        self.located.append((object_id, version, part_number))
        return self._owner, self._unreplicated

    def last_owner(self, object_id: str, version: int, part_number: int) -> str | None:
        return self._owner


@pytest.mark.asyncio
async def test_peer_locate_passes_through_to_the_fetcher(tmp_path) -> None:
    fetcher = _LocatingFetcher("node-b", True)
    dual = DualFileSystemPartsStore(str(tmp_path / "ssd"), str(tmp_path / "pool"), peer_fetch=fetcher)

    assert await dual.peer_locate(OBJ, 1, 1) == ("node-b", True)
    assert fetcher.located == [(OBJ, 1, 1)]
    assert dual.peer_last_owner(OBJ, 1, 1) == "node-b"


@pytest.mark.asyncio
async def test_a_failing_peer_locate_never_fails_the_read_path(tmp_path) -> None:
    class _Broken(_LocatingFetcher):
        async def locate(self, *args: object) -> tuple[str | None, bool]:
            raise OSError("postgres down")

    dual = DualFileSystemPartsStore(str(tmp_path / "ssd"), str(tmp_path / "pool"), peer_fetch=_Broken("node-b", True))

    assert await dual.peer_locate(OBJ, 1, 1) == (None, False)


@pytest.mark.asyncio
async def test_a_raising_promoted_hook_fails_neither_the_promotion_nor_the_read(tmp_path) -> None:
    """The hook is a wakeup for OTHER readers; this reader already has its bytes, and the copy
    that landed stays landed."""

    async def _hook(*args: object) -> None:
        raise ConnectionError("redis down")

    dual = DualFileSystemPartsStore(str(tmp_path / "ssd"), str(tmp_path / "pool"), promote=True, on_promoted=_hook)
    await _write_part(dual.fallback, part_number=1, chunk=b"pool-only")

    assert await dual.get_chunk(OBJ, 1, 1, 0) == b"pool-only"
    assert await FileSystemPartsStore.get_chunk(dual, OBJ, 1, 1, 0) == b"pool-only", (
        "the promoted copy is on local flash"
    )


@pytest.mark.asyncio
async def test_two_concurrent_streams_count_into_their_own_tier_dicts(tmp_path) -> None:
    """One stream's counter must never see another's reads, however interleaved their fetches are."""
    dual = DualFileSystemPartsStore(str(tmp_path / "ssd"), str(tmp_path / "pool"))
    await _write_part(dual.fallback, part_number=1, chunk=b"pool-only")
    await _write_part(dual, part_number=2, chunk=b"on-ssd")

    gate = asyncio.Event()

    async def _stream(counts: dict[str, int], part_number: int, reads: int) -> None:
        _stream_tiers.set(counts)
        await gate.wait()
        # Spawned fetches, as the streamer does it: each inherits this stream's dict by reference.
        await asyncio.gather(*(asyncio.create_task(dual.get_chunk(OBJ, 1, part_number, 0)) for _ in range(reads)))

    pool_counts: dict[str, int] = {}
    local_counts: dict[str, int] = {}
    streams = [
        asyncio.create_task(_stream(pool_counts, 1, 3)),
        asyncio.create_task(_stream(local_counts, 2, 5)),
    ]
    await asyncio.sleep(0)
    gate.set()
    await asyncio.gather(*streams)

    assert pool_counts == {"pool": 3}
    assert local_counts == {"local": 5}
    assert _stream_tiers.get() is None, "neither stream's counter leaked into the spawning task"
