"""Peer discovery and per-part chunk fetching.

Discovery is self-registration through Redis rather than a map of node IPs, because k8s node
names do not resolve in cluster DNS, pod IPs change on every restart, and a `hostPort`
address would reach the peer from the node IP (192.168.x) once SNAT'd — which the api's
`ip_whitelist` middleware rejects, admitting only 10.x/172.x.
"""

from __future__ import annotations

import asyncio
import json
import time
from typing import Any
from typing import Optional

import httpx
import pytest

from hippius_s3.cache.peers import PEER_PORT
from hippius_s3.cache.peers import PeerChunkFetcher
from hippius_s3.cache.peers import PeerRegistry
from hippius_s3.cache.peers import effective_max_inflight
from hippius_s3.cache.peers import encode_fresh_part
from hippius_s3.cache.peers import fresh_part_key
from hippius_s3.cache.peers import peer_key


SECRET = "a" * 64
OBJ = "466916c0-d61b-4518-b81b-9576b574270a"

# A registered peer, and this pod. Both must satisfy the address allow-list, or the tests
# would be asserting against URLs the resolver refuses to hand out.
PEER_URL = f"http://10.42.2.9:{PEER_PORT}"
SELF_URL = f"http://10.42.1.5:{PEER_PORT}"


class FakeRedis:
    def __init__(self) -> None:
        self.store: dict[str, str] = {}
        self.ttls: dict[str, int] = {}
        self.fail = False

    async def set(self, key: str, value: str, ex: int | None = None) -> None:
        if self.fail:
            raise ConnectionError("redis down")
        self.store[key] = value
        if ex is not None:
            self.ttls[key] = ex

    async def get(self, key: str) -> Optional[str]:
        if self.fail:
            raise ConnectionError("redis down")
        return self.store.get(key)


def residency_row(node_id: str = "node-b", *, chunk_size: int = 10, num_chunks: int = 128) -> dict[str, Any]:
    """One peer-resolution row: who holds the part, and its exact per-chunk ciphertext sizes.

    Both facts come back from a single query because both are per-PART and the read path asks
    per chunk — see `test_the_chunk_sizes_arrive_with_the_owner_in_one_query`.
    """
    return {
        "node_id": node_id,
        "chunk_indexes": list(range(num_chunks)),
        "cipher_sizes": [chunk_size] * num_chunks,
    }


def sized_row(sizes: list[int], node_id: str = "node-b") -> dict[str, Any]:
    """A row for a part whose chunks are not all the same size — i.e. a real part."""
    return {"node_id": node_id, "chunk_indexes": list(range(len(sizes))), "cipher_sizes": list(sizes)}


class FakeConn:
    def __init__(self, row: Optional[dict[str, Any]]) -> None:
        self._row = row
        self.queries: list[tuple[Any, ...]] = []

    async def fetchrow(self, _sql: str, *args: Any) -> Optional[dict[str, Any]]:
        self.queries.append(args)
        return self._row


class FakePool:
    def __init__(self, row: Optional[dict[str, Any]] = None) -> None:
        self.conn = FakeConn(row)

    def acquire(self) -> Any:
        conn = self.conn

        class _Ctx:
            async def __aenter__(self) -> FakeConn:
                return conn

            async def __aexit__(self, *_: Any) -> None:
                return None

        return _Ctx()


class ScriptedPool:
    """Returns fetchrow results in order. Owner miss then sizes, for the fresh-part path."""

    def __init__(self, rows: list[Optional[dict[str, Any]]]) -> None:
        self._rows = list(rows)
        self.queries: list[tuple[Any, ...]] = []

    def acquire(self) -> Any:
        pool = self

        class _Conn:
            async def fetchrow(self, _sql: str, *args: Any) -> Optional[dict[str, Any]]:
                pool.queries.append(args)
                if not pool._rows:
                    return None
                return pool._rows.pop(0)

        class _Ctx:
            async def __aenter__(self) -> _Conn:
                return _Conn()

            async def __aexit__(self, *_: Any) -> None:
                return None

        return _Ctx()


class FakeStream:
    """One peer response, delivered the way httpx delivers one: a body read in pieces.

    `piece_size` and `delay` are what let a test be a slow-drip peer — the case an inter-chunk
    read timeout cannot bound, because every individual gap is inside it.
    """

    def __init__(self, status_code: int, body: bytes = b"", piece_size: int = 0, delay: float = 0.0) -> None:
        self.status_code = status_code
        self._body = body
        self._piece = piece_size or max(len(body), 1)
        self._delay = delay
        self.closed = False
        self.pieces_read = 0

    async def aiter_bytes(self):  # noqa: ANN201
        for start in range(0, len(self._body), self._piece):
            if self._delay:
                await asyncio.sleep(self._delay)
            self.pieces_read += 1
            yield self._body[start : start + self._piece]


class _StreamCtx:
    def __init__(self, stream: FakeStream) -> None:
        self._stream = stream

    async def __aenter__(self) -> FakeStream:
        return self._stream

    async def __aexit__(self, *_: Any) -> None:
        self._stream.closed = True


class FakeHttp:
    """Records every URL it was asked for, so "no request was made" is assertable."""

    def __init__(self, status_code: int = 200, body: bytes = b"", piece_size: int = 0, delay: float = 0.0) -> None:
        self._spec = (status_code, body, piece_size, delay)
        self.urls: list[str] = []
        self.streams: list[FakeStream] = []

    def stream(self, method: str, url: str, headers: dict[str, str] | None = None) -> Any:
        assert method == "GET"
        self.urls.append(url)
        stream = FakeStream(*self._spec)
        self.streams.append(stream)
        return _StreamCtx(stream)


class Flaky:
    """A peer that answers `status` once and then serves the chunk normally.

    This is the shape that separates a transient refusal from a memo poison: if the first
    answer took the whole part off the peer tier, the second request never reaches here.
    """

    def __init__(self, status: int) -> None:
        self._status = status
        self.calls = 0

    def stream(self, method: str, url: str, headers: dict[str, str] | None = None) -> Any:
        assert method == "GET"
        self.calls += 1
        return _StreamCtx(FakeStream(self._status) if self.calls == 1 else FakeStream(200, b"peer-bytes"))


@pytest.mark.asyncio
async def test_registration_publishes_this_pods_address_with_a_ttl() -> None:
    """The TTL IS the liveness signal — a pod that stops refreshing stops being a peer."""
    redis = FakeRedis()
    registry = PeerRegistry(redis, "k8s-v3-node2", SELF_URL, 90)

    await registry.register()

    assert json.loads(redis.store[peer_key("k8s-v3-node2")])["url"] == SELF_URL
    assert redis.ttls[peer_key("k8s-v3-node2")] == 90


@pytest.mark.asyncio
async def test_a_redis_outage_never_breaks_registration_or_lookup() -> None:
    """Peer discovery is an optimisation; losing it costs pool reads, not requests."""
    redis = FakeRedis()
    redis.fail = True
    registry = PeerRegistry(redis, "k8s-v3-node2", SELF_URL, 90)

    await registry.register()  # must not raise
    assert await registry.resolve("k8s-v3-node3") is None


@pytest.mark.asyncio
async def test_an_unregistered_peer_resolves_to_none() -> None:
    registry = PeerRegistry(FakeRedis(), "k8s-v3-node2", SELF_URL, 90)
    assert await registry.resolve("k8s-v3-node3") is None


@pytest.mark.asyncio
async def test_a_chunk_is_fetched_from_the_peer_that_holds_it() -> None:
    redis = FakeRedis()
    registry = PeerRegistry(redis, "node-a", SELF_URL, 90)
    await PeerRegistry(redis, "node-b", PEER_URL, 90).register()
    http = FakeHttp(200, b"peer-bytes")
    fetcher = PeerChunkFetcher(FakePool(residency_row()), registry, "node-a", http, auth_secret=SECRET)

    assert await fetcher(OBJ, 1, 3, 2) == b"peer-bytes"
    assert http.urls == [f"{PEER_URL}/internal/parts/{OBJ}/1/3/chunks/2"]


@pytest.mark.asyncio
async def test_the_residency_lookup_excludes_this_node() -> None:
    """Asking ourselves is pointless — the local tier already missed on this chunk.

    The exclusion lives in the query so a node that holds the part but has it evicted
    mid-read cannot resolve to itself and burn a network round trip on its own miss.
    """
    pool = FakePool(residency_row())
    registry = PeerRegistry(FakeRedis(), "node-a", SELF_URL, 90)
    fetcher = PeerChunkFetcher(pool, registry, "node-a", FakeHttp(404), auth_secret=SECRET)

    await fetcher(OBJ, 1, 3, 2)

    assert pool.conn.queries[0] == (OBJ, 1, 3, "node-a"), "this node is bound out of the lookup"


@pytest.mark.asyncio
async def test_no_peer_holds_the_part_so_the_pool_is_used() -> None:
    registry = PeerRegistry(FakeRedis(), "node-a", SELF_URL, 90)
    fetcher = PeerChunkFetcher(FakePool(None), registry, "node-a", FakeHttp(200, b"x"), auth_secret=SECRET)

    assert await fetcher(OBJ, 1, 3, 2) is None


@pytest.mark.asyncio
async def test_pool_only_chunks_of_one_part_share_the_negative_memo() -> None:
    """A miss is still per-PART: eight chunks must not issue eight postgres lookups."""
    registry = PeerRegistry(FakeRedis(), "node-a", SELF_URL, 90)
    pool = FakePool(None)
    fetcher = PeerChunkFetcher(pool, registry, "node-a", FakeHttp(200, b"x"), auth_secret=SECRET)

    for chunk in range(8):
        assert await fetcher(OBJ, 1, 3, chunk) is None

    assert len(pool.conn.queries) == 1, f"8 chunks issued {len(pool.conn.queries)} owner lookups"


@pytest.mark.asyncio
async def test_a_peer_that_evicted_the_chunk_returns_none() -> None:
    """404 is routine: the evictor unlinked it between the residency read and the fetch."""
    redis = FakeRedis()
    await PeerRegistry(redis, "node-b", PEER_URL, 90).register()
    registry = PeerRegistry(redis, "node-a", SELF_URL, 90)
    fetcher = PeerChunkFetcher(FakePool(residency_row()), registry, "node-a", FakeHttp(404), auth_secret=SECRET)

    assert await fetcher(OBJ, 1, 3, 2) is None


@pytest.mark.asyncio
async def test_a_resolved_but_unregistered_peer_is_skipped() -> None:
    """Residency says node-b holds it, but node-b's pod has not registered (or aged out)."""
    registry = PeerRegistry(FakeRedis(), "node-a", SELF_URL, 90)
    http = FakeHttp(200, b"never")
    fetcher = PeerChunkFetcher(FakePool(residency_row()), registry, "node-a", http, auth_secret=SECRET)

    assert await fetcher(OBJ, 1, 3, 2) is None
    assert http.urls == [], "no address, so no request was attempted"


@pytest.mark.asyncio
async def test_the_owner_lookup_is_per_part_not_per_chunk() -> None:
    """Which peer holds a part is a per-PART fact, but the read path asks per chunk.

    Unmemoised, a 64-chunk part costs 64 Postgres round-trips plus 64 Redis GETs on the read
    path — self-defeating on a tier that exists to save ~34 ms per chunk.
    """
    redis = FakeRedis()
    await PeerRegistry(redis, "node-b", PEER_URL, 90).register()
    registry = PeerRegistry(redis, "node-a", SELF_URL, 90)
    pool = FakePool(residency_row())
    http = FakeHttp(200, b"peer-bytes")
    fetcher = PeerChunkFetcher(pool, registry, "node-a", http, auth_secret=SECRET)

    for chunk in range(64):
        assert await fetcher(OBJ, 1, 3, chunk) == b"peer-bytes"

    assert len(pool.conn.queries) == 1, f"64 chunks issued {len(pool.conn.queries)} residency lookups"
    assert len(http.urls) == 64, "but every chunk is still fetched"


@pytest.mark.asyncio
async def test_a_different_part_is_looked_up_separately() -> None:
    """The memo is keyed by part — parts of one object can live on different nodes."""
    redis = FakeRedis()
    await PeerRegistry(redis, "node-b", PEER_URL, 90).register()
    registry = PeerRegistry(redis, "node-a", SELF_URL, 90)
    pool = FakePool(residency_row(chunk_size=1))
    fetcher = PeerChunkFetcher(pool, registry, "node-a", FakeHttp(200, b"x"), auth_secret=SECRET)

    await fetcher(OBJ, 1, 3, 0)
    await fetcher(OBJ, 1, 4, 0)

    assert len(pool.conn.queries) == 2, "part 3 and part 4 are resolved independently"


class _RaisingCtx:
    async def __aenter__(self) -> Any:
        raise httpx.ConnectError("connection refused")

    async def __aexit__(self, *_: Any) -> None:
        return None


class FailingHttp:
    """A peer that is registered but unreachable — a cordoned or drained node."""

    def __init__(self) -> None:
        self.attempts = 0

    def stream(self, method: str, url: str, headers: dict[str, str] | None = None) -> Any:
        self.attempts += 1
        return _RaisingCtx()


@pytest.mark.asyncio
async def test_a_dead_peer_is_tried_once_not_once_per_chunk() -> None:
    """A registered-but-dead peer must not make reads SLOWER than having no peer tier.

    Its Redis registration stays resolvable for up to the 90 s TTL if no replacement pod
    re-registers under the same node name — a drained node, not a rolling restart. Retrying
    it per chunk pays the full fetch timeout every time before falling through to the pool,
    so for up to a minute and a half that node's shard reads worse than with the tier off.
    """
    redis = FakeRedis()
    await PeerRegistry(redis, "node-b", PEER_URL, 90).register()
    registry = PeerRegistry(redis, "node-a", SELF_URL, 90)
    http = FailingHttp()
    fetcher = PeerChunkFetcher(FakePool(residency_row()), registry, "node-a", http, auth_secret=SECRET)

    for chunk in range(32):
        assert await fetcher(OBJ, 1, 3, chunk) is None, "every chunk falls through to the pool"

    assert http.attempts == 1, f"a dead peer was retried {http.attempts} times"


class _BlockingCtx:
    def __init__(self, http: BlockingHttp) -> None:
        self._http = http

    async def __aenter__(self) -> FakeStream:
        self._http.started.set()
        await self._http.release.wait()
        return FakeStream(200, self._http.body)

    async def __aexit__(self, *_: Any) -> None:
        return None


class BlockingHttp:
    """A peer whose responses hang until released, so in-flight state is observable."""

    def __init__(self, body: bytes = b"peer-bytes") -> None:
        self.release = asyncio.Event()
        self.started = asyncio.Event()
        self.attempts = 0
        self.body = body

    def stream(self, method: str, url: str, headers: dict[str, str] | None = None) -> Any:
        self.attempts += 1
        return _BlockingCtx(self)


@pytest.mark.asyncio
async def test_a_saturated_peer_is_skipped_rather_than_queued() -> None:
    """Owl caps per-peer fanout; without one, a hot part turns one node into a hotspot.

    Every other node's reads converge on whichever pod holds the popular data — the same
    uvicorn serving that node's own ingest. The cap must SKIP to the pool rather than queue:
    waiting behind a saturated peer would add its latency on top of the pool read that
    follows, which is the opposite of what this tier is for.
    """
    redis = FakeRedis()
    await PeerRegistry(redis, "node-b", PEER_URL, 90).register()
    registry = PeerRegistry(redis, "node-a", SELF_URL, 90)
    http = BlockingHttp()
    fetcher = PeerChunkFetcher(FakePool(residency_row()), registry, "node-a", http, max_inflight=1, auth_secret=SECRET)

    first = asyncio.create_task(fetcher(OBJ, 1, 3, 0))
    await asyncio.wait_for(http.started.wait(), timeout=1)

    # The peer's one slot is taken, so this must fall through immediately.
    assert await fetcher(OBJ, 1, 3, 1) is None, "a saturated peer is skipped, not queued"
    assert http.attempts == 1, "and no second request was even attempted"

    http.release.set()
    assert await first == b"peer-bytes"


@pytest.mark.asyncio
async def test_a_busy_peer_503_falls_through_without_poisoning_the_memo() -> None:
    """503 means "busy right now", not "gone" — unlike a connect failure.

    Poisoning the memo on transient load would keep sending the whole part to the pool long
    after the peer recovered, converting a brief spike into a sustained cold read.
    """
    redis = FakeRedis()
    await PeerRegistry(redis, "node-b", PEER_URL, 90).register()
    registry = PeerRegistry(redis, "node-a", SELF_URL, 90)

    fetcher = PeerChunkFetcher(FakePool(residency_row()), registry, "node-a", Flaky(503), auth_secret=SECRET)

    assert await fetcher(OBJ, 1, 3, 0) is None, "shed, so this chunk reads from the pool"
    assert await fetcher(OBJ, 1, 3, 1) == b"peer-bytes", "the peer is retried once it recovers"


@pytest.mark.asyncio
async def test_both_shed_paths_are_recorded_with_their_reason(monkeypatch) -> None:
    """A shed must be observable, and the two causes must stay distinguishable.

    Without this, `chunk_reads_by_tier_total` shows the peer tier underperforming with no way
    to say why. The two reasons call for opposite responses: `server_busy` is real contention
    at the peer, while `client_cap` can simply be this reader out-pacing its own per-peer cap
    — the read path prefetches `HTTP_STREAM_PREFETCH_CHUNKS` chunks and the chunks of one part
    all resolve to the same peer, so a part with more chunks than `peer_fetch_max_inflight`
    sheds its own excess. Reading the second as saturation would send someone tuning the peer
    when the cap is what needs raising.
    """
    recorded: list[str] = []
    monkeypatch.setattr("hippius_s3.cache.peers._record_shed", recorded.append)

    redis = FakeRedis()
    await PeerRegistry(redis, "node-b", PEER_URL, 90).register()
    registry = PeerRegistry(redis, "node-a", SELF_URL, 90)

    # Client cap: one slot, held by a request that has not come back yet.
    blocking = BlockingHttp()
    capped = PeerChunkFetcher(
        FakePool(residency_row()), registry, "node-a", blocking, max_inflight=1, auth_secret=SECRET
    )
    first = asyncio.create_task(capped(OBJ, 1, 3, 0))
    await asyncio.wait_for(blocking.started.wait(), timeout=1)
    assert await capped(OBJ, 1, 3, 1) is None
    blocking.release.set()
    await first

    # Server cap: the peer sheds it back with a 503.
    busy = PeerChunkFetcher(
        FakePool(residency_row()), registry, "node-a", FakeHttp(503), max_inflight=8, auth_secret=SECRET
    )
    assert await busy(OBJ, 1, 4, 0) is None

    assert recorded == ["client_cap", "server_busy"], (
        "each shed is counted once, under the reason that actually caused it"
    )


# ----------------------------------------------------------------- per-peer cap vs prefetch


def test_the_cap_is_floored_at_the_prefetch_depth() -> None:
    """A cap below the prefetch depth makes a reader compete with itself.

    Every chunk of one PART resolves to the same peer, and the streamer keeps
    HTTP_STREAM_PREFETCH_CHUNKS fetches in flight, so with the shipped 8-vs-16 pairing half of
    every part over 32 MiB went to CephFS while the peer sat idle.
    """
    assert effective_max_inflight(8, 16) == 16


def test_a_cap_already_at_or_above_the_prefetch_depth_is_left_alone() -> None:
    """Deliberate over-provisioning is an operator decision; only the broken case is corrected."""
    assert effective_max_inflight(32, 16) == 32
    assert effective_max_inflight(16, 16) == 16


@pytest.mark.asyncio
async def test_a_single_readers_prefetch_window_never_sheds_against_an_idle_peer() -> None:
    """THE Phase G regression test: it fails on the shipped 8-vs-16 defaults.

    One reader, one part, a full prefetch window in flight, and a peer that is doing nothing
    else. Every chunk must reach the peer. Before this fix the 9th through 16th were shed to
    the pool and booked as `client_cap`, which reads on a dashboard as peer contention while
    the peer is idle — and understates `chunk_reads_by_tier_total{tier=peer}` for the whole
    soak, making "the peer tier is not helping" unfalsifiable.
    """
    prefetch = 16
    redis = FakeRedis()
    await PeerRegistry(redis, "node-b", PEER_URL, 90).register()
    registry = PeerRegistry(redis, "node-a", SELF_URL, 90)
    http = BlockingHttp()
    fetcher = PeerChunkFetcher(
        FakePool(residency_row()),
        registry,
        "node-a",
        http,
        auth_secret=SECRET,
        max_inflight=effective_max_inflight(8, prefetch),
    )

    # One part, so every chunk resolves to the same peer — the case the cap used to break.
    inflight = [asyncio.create_task(fetcher(OBJ, 1, 3, i)) for i in range(prefetch)]
    await asyncio.sleep(0)  # let the owner lookup and the acquires run
    await asyncio.sleep(0)

    http.release.set()
    results = await asyncio.gather(*inflight)

    assert http.attempts == prefetch, f"only {http.attempts} of {prefetch} chunks reached an idle peer"
    assert all(r == b"peer-bytes" for r in results), "every chunk came from the peer, none from the pool"


@pytest.mark.asyncio
async def test_the_cap_still_sheds_when_readers_genuinely_contend() -> None:
    """The Owl property must survive the floor: real contention still skips to the pool.

    Note what this does NOT claim. The semaphore is per (pod, peer) and shared across readers,
    so raising it does not add peer capacity — under concurrency it moves shedding from
    `client_cap` to the peer's own `server_busy`. Aggregate demand is `(pods - 1) x cap`
    against one serve cap, oversubscribed by design, with Ceph as the intended overflow valve.
    """
    redis = FakeRedis()
    await PeerRegistry(redis, "node-b", PEER_URL, 90).register()
    registry = PeerRegistry(redis, "node-a", SELF_URL, 90)
    http = BlockingHttp()
    cap = 4
    fetcher = PeerChunkFetcher(
        FakePool(residency_row()), registry, "node-a", http, max_inflight=cap, auth_secret=SECRET
    )

    saturating = [asyncio.create_task(fetcher(OBJ, 1, 3, i)) for i in range(cap)]
    await asyncio.wait_for(http.started.wait(), timeout=1)
    await asyncio.sleep(0)

    # The cap is now taken; a further reader must skip to the pool rather than queue behind it.
    # `wait_for` is the assertion, not a safety net: without the skip this call blocks on the
    # semaphore until the peer replies, which is the failure mode — the reader would pay the
    # saturated peer's full latency and THEN still read the pool. A plain await would hang the
    # suite instead of failing it.
    shed = await asyncio.wait_for(fetcher(OBJ, 1, 3, 99), timeout=1)
    assert shed is None, "a saturated peer is skipped, not queued"
    assert http.attempts == cap, "and the extra request was never even attempted"

    http.release.set()
    await asyncio.gather(*saturating)


# --------------------------------------------------------------- the peer address allow-list


async def _publish_raw(redis: FakeRedis, node: str, url: str) -> None:
    """Put an arbitrary string where a peer's registration goes, bypassing `register`."""
    await redis.set(peer_key(node), json.dumps({"url": url}))


@pytest.mark.parametrize(
    ("url", "why"),
    [
        ("http://169.254.169.254/", "the cloud metadata endpoint"),
        (f"http://169.254.169.254:{PEER_PORT}", "metadata on the api port — `is_private` alone admits this"),
        ("file:///etc/passwd", "a non-http scheme"),
        ("https://evil.example", "a name, not an address, and the wrong scheme"),
        (f"http://evil.example:{PEER_PORT}", "a DNS name, whose resolution we do not control"),
        (f"http://8.8.8.8:{PEER_PORT}", "a public address"),
        ("http://10.1.2.3:9999", "the pod network but the wrong port"),
        ("http://10.1.2.3", "no port at all"),
        (f"http://127.0.0.1:{PEER_PORT}", "this pod itself, which already missed locally"),
        (f"http://192.168.1.9:{PEER_PORT}", "a NODE address — the SNAT case the pod IP exists to avoid"),
        (f"http://10.1.2.3:{PEER_PORT}/../admin", "a path, so the fetch URL is no longer ours to build"),
        (f"http://10.1.2.3:{PEER_PORT}?x=1", "a query string"),
        (f"http://user:pw@10.1.2.3:{PEER_PORT}", "embedded credentials"),
        ("", "an empty registration"),
    ],
)
@pytest.mark.asyncio
async def test_a_peer_address_outside_the_pod_network_is_refused(url: str, why: str) -> None:
    """Whatever sits in Redis is input, and `resolve` is the one place it can be refused."""
    redis = FakeRedis()
    await _publish_raw(redis, "node-b", url)
    registry = PeerRegistry(redis, "node-a", SELF_URL, 90)

    assert await registry.resolve("node-b") is None, f"accepted {url!r} ({why})"


@pytest.mark.asyncio
async def test_a_pod_network_address_on_the_api_port_is_accepted() -> None:
    """The allow-list has to admit the real thing, or the peer tier is off in production."""
    redis = FakeRedis()
    await _publish_raw(redis, "node-b", f"http://10.1.2.3:{PEER_PORT}")
    registry = PeerRegistry(redis, "node-a", SELF_URL, 90)

    assert await registry.resolve("node-b") == f"http://10.1.2.3:{PEER_PORT}"


@pytest.mark.asyncio
async def test_the_node_network_is_admitted_only_under_the_hostnetwork_flag(monkeypatch) -> None:
    """Under hostNetwork, POD_IP is the node address, so registrations are 192.168.x by
    construction; HIPPIUS_PEER_ALLOW_NODE_NETWORK is what keeps the tier lit there."""
    from hippius_s3.config import get_config

    monkeypatch.setattr(get_config(), "peer_allow_node_network", True)
    redis = FakeRedis()
    await _publish_raw(redis, "node-b", f"http://192.168.1.200:{PEER_PORT}")
    registry = PeerRegistry(redis, "node-a", SELF_URL, 90)

    assert await registry.resolve("node-b") == f"http://192.168.1.200:{PEER_PORT}"


@pytest.mark.parametrize(
    ("url", "why"),
    [
        ("http://169.254.169.254/", "the metadata endpoint is not the node network"),
        (f"http://8.8.8.8:{PEER_PORT}", "a public address stays refused"),
        (f"http://127.0.0.1:{PEER_PORT}", "loopback stays refused"),
        ("http://192.168.1.200:9999", "the node network but the wrong port"),
        (f"http://192.168.1.200:{PEER_PORT}/../admin", "a path stays refused on the node network too"),
    ],
)
@pytest.mark.asyncio
async def test_the_hostnetwork_flag_widens_nothing_but_the_node_network(monkeypatch, url: str, why: str) -> None:
    from hippius_s3.config import get_config

    monkeypatch.setattr(get_config(), "peer_allow_node_network", True)
    redis = FakeRedis()
    await _publish_raw(redis, "node-b", url)
    registry = PeerRegistry(redis, "node-a", SELF_URL, 90)

    assert await registry.resolve("node-b") is None, f"accepted {url!r} ({why})"


@pytest.mark.asyncio
async def test_a_refused_peer_address_reaches_no_http_client_at_all() -> None:
    """The point of refusing in `resolve` is that nothing downstream ever sees the URL.

    Asserting only on the return value would pass even if the fetcher had already issued the
    request and thrown the response away — which is the whole of the SSRF.
    """
    redis = FakeRedis()
    await _publish_raw(redis, "node-b", "http://169.254.169.254/")
    registry = PeerRegistry(redis, "node-a", SELF_URL, 90)
    http = FakeHttp(200, b"metadata")
    fetcher = PeerChunkFetcher(FakePool(residency_row()), registry, "node-a", http, auth_secret=SECRET)

    assert await fetcher(OBJ, 1, 3, 2) is None
    assert http.urls == [], f"a refused address still produced {len(http.urls)} request(s)"


@pytest.mark.asyncio
async def test_a_refused_peer_address_is_counted_and_warned(monkeypatch, caplog) -> None:
    """A rejection is never routine, so it logs at warning and counts under its own reason."""
    recorded: list[str] = []
    monkeypatch.setattr("hippius_s3.cache.peers._record_shed", recorded.append)

    redis = FakeRedis()
    await _publish_raw(redis, "node-b", f"http://8.8.8.8:{PEER_PORT}")
    registry = PeerRegistry(redis, "node-a", SELF_URL, 90)

    with caplog.at_level("WARNING", logger="hippius_s3.cache.peers"):
        assert await registry.resolve("node-b") is None

    assert recorded == ["bad_peer_url"]
    assert any(record.levelname == "WARNING" for record in caplog.records)


# ------------------------------------------------------------- the exact per-chunk size check


@pytest.mark.asyncio
async def test_a_short_body_legal_for_the_part_but_wrong_for_this_chunk_is_rejected() -> None:
    """THE test the upper-bound design fails.

    A peer on a rolled-back or half-deployed image answers 200 with a truncated body. 40 bytes
    is a perfectly legal chunk length for this part — it is exactly what the last chunk is — so
    `len(data) <= chunk_size + overhead` accepts it. Only the size recorded for THIS chunk
    index separates a truncated chunk 1 from a genuine chunk 3.

    Accepting it is not a transient error: the body would be promoted onto local flash and then
    win the local-tier read on every subsequent request for that chunk, AEAD-failing each time,
    until the evictor happens to unlink the part.
    """
    redis = FakeRedis()
    await PeerRegistry(redis, "node-b", PEER_URL, 90).register()
    registry = PeerRegistry(redis, "node-a", SELF_URL, 90)
    pool = FakePool(sized_row([100, 100, 100, 40]))
    fetcher = PeerChunkFetcher(pool, registry, "node-a", FakeHttp(200, b"t" * 40), auth_secret=SECRET)

    assert await fetcher(OBJ, 1, 3, 1) is None, "a 40-byte body was accepted for a 100-byte chunk"


@pytest.mark.asyncio
async def test_a_genuinely_short_final_chunk_is_still_served() -> None:
    """The guard against overcorrecting into "reject anything short".

    The last chunk of a part is legitimately shorter than the rest. A check that rejected it
    would take the peer tier out for the tail of every single part.
    """
    redis = FakeRedis()
    await PeerRegistry(redis, "node-b", PEER_URL, 90).register()
    registry = PeerRegistry(redis, "node-a", SELF_URL, 90)
    pool = FakePool(sized_row([100, 100, 100, 40]))
    fetcher = PeerChunkFetcher(pool, registry, "node-a", FakeHttp(200, b"t" * 40), auth_secret=SECRET)

    assert await fetcher(OBJ, 1, 3, 3) == b"t" * 40, "the real final chunk must still be served"


@pytest.mark.asyncio
async def test_an_oversized_body_is_abandoned_without_being_read_to_the_end() -> None:
    """The memory bound has to bite while the body arrives, not after it has been buffered.

    `await client.get()` has already paid for the whole body by the time anything could
    measure it, so the abort is what makes an over-long body cost bounded memory rather than
    whatever the peer chose to send.
    """
    redis = FakeRedis()
    await PeerRegistry(redis, "node-b", PEER_URL, 90).register()
    registry = PeerRegistry(redis, "node-a", SELF_URL, 90)
    pool = FakePool(sized_row([100]))
    http = FakeHttp(200, b"t" * 10_000, piece_size=10)
    fetcher = PeerChunkFetcher(pool, registry, "node-a", http, auth_secret=SECRET)

    assert await fetcher(OBJ, 1, 3, 0) is None
    stream = http.streams[0]
    assert stream.pieces_read <= 11, f"read {stream.pieces_read} pieces past a 100-byte bound"
    assert stream.closed, "the response was left open"


@pytest.mark.asyncio
async def test_a_chunk_with_no_recorded_size_is_not_fetched_from_a_peer() -> None:
    """No recorded size means no way to check the body, and an unchecked body is the finding.

    The pool copy is authoritative and always present for a replicated part, so declining
    costs one pool read. It is counted rather than silent precisely because it should never
    happen — `part_chunks` rows are written for every chunk index at part creation.
    """
    redis = FakeRedis()
    await PeerRegistry(redis, "node-b", PEER_URL, 90).register()
    registry = PeerRegistry(redis, "node-a", SELF_URL, 90)
    pool = FakePool(sized_row([100, 100]))
    http = FakeHttp(200, b"t" * 100)
    fetcher = PeerChunkFetcher(pool, registry, "node-a", http, auth_secret=SECRET)

    assert await fetcher(OBJ, 1, 3, 7) is None, "chunk 7 has no recorded size"
    assert http.urls == [], "and no request was made on an unverifiable chunk"


@pytest.mark.asyncio
async def test_a_wrong_length_is_counted_under_its_own_reason(monkeypatch) -> None:
    """`bad_length` must be distinguishable from contention, because it means a broken peer.

    A rise in `client_cap`/`server_busy` is a capacity signal. A rise in `bad_length` is a
    version-skewed or corrupt peer, and the response is to find that pod, not to tune a cap.
    """
    recorded: list[str] = []
    monkeypatch.setattr("hippius_s3.cache.peers._record_shed", recorded.append)

    redis = FakeRedis()
    await PeerRegistry(redis, "node-b", PEER_URL, 90).register()
    registry = PeerRegistry(redis, "node-a", SELF_URL, 90)
    fetcher = PeerChunkFetcher(
        FakePool(sized_row([100, 100])), registry, "node-a", FakeHttp(200, b"t" * 40), auth_secret=SECRET
    )

    assert await fetcher(OBJ, 1, 3, 0) is None

    assert recorded == ["bad_length"]


# ------------------------------------------------- the peer's own answer, when it is not 200


async def _registered_peer() -> PeerRegistry:
    """A registry in which node-b is a resolvable peer of node-a."""
    redis = FakeRedis()
    await PeerRegistry(redis, "node-b", PEER_URL, 90).register()
    return PeerRegistry(redis, "node-a", SELF_URL, 90)


@pytest.mark.asyncio
async def test_a_peer_404_is_counted_rather_than_leaving_only_a_gap_in_the_tier_split(monkeypatch) -> None:
    """A 404 is routine, and counting it is what makes an eviction storm visible.

    Uncounted, the whole peer tier can go away — every node evicting between the residency read
    and the fetch — and the only trace is a DROP in `chunk_reads_by_tier_total{tier=peer}`. That
    is a negative-space signal: it looks identical to the feature simply being idle, so nothing
    can alert on it.
    """
    recorded: list[str] = []
    monkeypatch.setattr("hippius_s3.cache.peers._record_shed", recorded.append)
    fetcher = PeerChunkFetcher(
        FakePool(residency_row()), await _registered_peer(), "node-a", FakeHttp(404), auth_secret=SECRET
    )

    assert await fetcher(OBJ, 1, 3, 2) is None, "the chunk still falls through to the pool"

    assert recorded == ["peer_miss"]


@pytest.mark.asyncio
async def test_a_peer_5xx_is_counted_apart_from_a_miss(monkeypatch) -> None:
    """A broken peer and an evicting one call for opposite responses.

    A rise in `peer_miss` is the eviction cursor moving faster than reads, and settles on its
    own. A rise in `peer_error` is a peer serving errors — a half-rolled deploy — and the
    response is to find that pod. Collapsed into one counter, the second hides inside the first,
    which on this fleet is the routine one.
    """
    recorded: list[str] = []
    monkeypatch.setattr("hippius_s3.cache.peers._record_shed", recorded.append)
    fetcher = PeerChunkFetcher(
        FakePool(residency_row()), await _registered_peer(), "node-a", FakeHttp(500), auth_secret=SECRET
    )

    assert await fetcher(OBJ, 1, 3, 2) is None

    assert recorded == ["peer_error"]


@pytest.mark.asyncio
async def test_neither_a_404_nor_a_5xx_poisons_the_part_memo() -> None:
    """Only an unreachable peer earns the memo poison; an answering one is retried.

    A connect failure is memoised off because retrying a dead peer costs the full deadline on
    every chunk. Neither of these is that: the peer answered promptly. Poisoning on a 404 would
    push a whole 64-chunk part to the pool for 30 s over one evicted chunk, and on a 5xx it
    would outlast the bad pod's rollback.
    """
    for status in (404, 500):
        http = Flaky(status)
        fetcher = PeerChunkFetcher(
            FakePool(residency_row()), await _registered_peer(), "node-a", http, auth_secret=SECRET
        )

        assert await fetcher(OBJ, 1, 3, 0) is None, f"the {status} chunk reads from the pool"
        assert await fetcher(OBJ, 1, 3, 1) == b"peer-bytes", f"a {status} kept the part off the peer tier"


@pytest.mark.asyncio
async def test_the_chunk_sizes_arrive_with_the_owner_in_one_query() -> None:
    """Both facts are per-PART, so both are paid for once per part, in one round trip.

    A second query for the size would put a Postgres round trip back on the per-chunk path —
    exactly the cost the owner memo exists to remove, and self-defeating on a tier whose whole
    justification is saving ~34 ms per chunk.
    """
    redis = FakeRedis()
    await PeerRegistry(redis, "node-b", PEER_URL, 90).register()
    registry = PeerRegistry(redis, "node-a", SELF_URL, 90)
    pool = FakePool(residency_row())
    fetcher = PeerChunkFetcher(pool, registry, "node-a", FakeHttp(200, b"peer-bytes"), auth_secret=SECRET)

    for chunk in range(64):
        assert await fetcher(OBJ, 1, 3, chunk) == b"peer-bytes"

    assert len(pool.conn.queries) == 1, f"64 verified chunks issued {len(pool.conn.queries)} queries"


# --------------------------------------------------------------------- the overall deadline


@pytest.mark.asyncio
async def test_a_drip_feeding_peer_is_cut_off_at_the_overall_deadline() -> None:
    """httpx's `read` timeout bounds the gap BETWEEN body pieces, never the whole response.

    Verified against the installed httpx (0.28.1): `Timeout` carries only
    connect/read/write/pool. So a peer sending one piece just inside the inter-chunk timeout
    holds the connection and the growing buffer for as long as it likes. The overall deadline
    is the only thing that ends it.

    Asserted against the wall clock, because the failure mode is elapsed time — a mocked clock
    would pass against the very code that hangs.
    """
    redis = FakeRedis()
    await PeerRegistry(redis, "node-b", PEER_URL, 90).register()
    registry = PeerRegistry(redis, "node-a", SELF_URL, 90)
    # 200 pieces, 50 ms apart: ~10 s of dripping, every individual gap well inside a 0.5 s
    # inter-chunk read timeout.
    http = FakeHttp(200, b"t" * 200, piece_size=1, delay=0.05)
    fetcher = PeerChunkFetcher(
        FakePool(sized_row([200])),
        registry,
        "node-a",
        http,
        auth_secret=SECRET,
        deadline_seconds=0.3,
    )

    started = time.monotonic()
    assert await fetcher(OBJ, 1, 3, 0) is None, "the drip-feeding peer was not cut off"
    elapsed = time.monotonic() - started

    assert elapsed < 2.0, f"the fetch ran {elapsed:.2f}s against a 0.3s deadline"
    assert http.streams[0].closed, "the aborted response was left open"


@pytest.mark.asyncio
async def test_the_deadline_covers_connecting_not_just_the_body() -> None:
    """A peer that accepts the connection and then never responds must also be cut off.

    Bounding only the body read would leave the case where nothing arrives at all — which is
    what a wedged pod looks like — outside the deadline entirely.
    """
    redis = FakeRedis()
    await PeerRegistry(redis, "node-b", PEER_URL, 90).register()
    registry = PeerRegistry(redis, "node-a", SELF_URL, 90)
    http = BlockingHttp()  # hangs in __aenter__, i.e. before any response exists
    fetcher = PeerChunkFetcher(
        FakePool(residency_row()),
        registry,
        "node-a",
        http,
        auth_secret=SECRET,
        deadline_seconds=0.2,
    )

    started = time.monotonic()
    assert await fetcher(OBJ, 1, 3, 0) is None
    assert time.monotonic() - started < 2.0, "a peer that never responded was waited on"


@pytest.mark.asyncio
async def test_a_timed_out_peer_is_not_retried_for_every_chunk_of_the_part() -> None:
    """A drip-feeding peer is as useless as a dead one, and must be memoised off the same way.

    Without this, every chunk of the part pays the full deadline before falling through — so
    the peer tier makes that part read many times WORSE than having no peer tier at all.
    """
    redis = FakeRedis()
    await PeerRegistry(redis, "node-b", PEER_URL, 90).register()
    registry = PeerRegistry(redis, "node-a", SELF_URL, 90)
    http = FakeHttp(200, b"t" * 200, piece_size=1, delay=0.05)
    fetcher = PeerChunkFetcher(
        FakePool(residency_row(chunk_size=200)),
        registry,
        "node-a",
        http,
        auth_secret=SECRET,
        deadline_seconds=0.2,
    )

    for chunk in range(8):
        assert await fetcher(OBJ, 1, 3, chunk) is None

    assert len(http.urls) == 1, f"a timed-out peer was retried {len(http.urls)} times"


# ------------------------------------------------- fresh-part hint (read-after-write)


@pytest.mark.asyncio
async def test_remember_part_is_visible_to_the_other_node() -> None:
    redis = FakeRedis()
    writer = PeerRegistry(redis, "node-b", PEER_URL, 90)
    reader = PeerRegistry(redis, "node-a", SELF_URL, 90)

    await writer.remember_part(OBJ, 1, 3)

    assert redis.store[fresh_part_key(OBJ, 1, 3)] == "node-b"
    assert redis.ttls[fresh_part_key(OBJ, 1, 3)] == 60
    assert await reader.lookup_fresh_part(OBJ, 1, 3) == ("node-b", {})
    assert await writer.lookup_fresh_part(OBJ, 1, 3) is None, "the ingest node does not fetch itself"


@pytest.mark.asyncio
async def test_remember_part_carries_cipher_sizes_so_the_reader_skips_postgres() -> None:
    redis = FakeRedis()
    writer = PeerRegistry(redis, "node-b", PEER_URL, 90)
    reader = PeerRegistry(redis, "node-a", SELF_URL, 90)

    await writer.remember_part(OBJ, 1, 3, cipher_sizes=[36])

    assert redis.store[fresh_part_key(OBJ, 1, 3)] == encode_fresh_part("node-b", [36])
    assert await reader.lookup_fresh_part(OBJ, 1, 3) == ("node-b", {0: 36})


@pytest.mark.asyncio
async def test_remember_part_never_raises() -> None:
    redis = FakeRedis()
    redis.fail = True
    await PeerRegistry(redis, "node-b", PEER_URL, 90).remember_part(OBJ, 1, 3)


@pytest.mark.asyncio
async def test_a_just_written_part_is_fetched_from_the_ingest_node_before_drain_claims() -> None:
    """Harbor blob-commit GETs startedat on the other api-local before drain has claimed.

    Postgres resolve is empty. The fresh-part Redis hint is what finds the ingest node
    instead of waiting 5-9s for the pool copy.
    """
    redis = FakeRedis()
    writer = PeerRegistry(redis, "node-b", PEER_URL, 90)
    await writer.register()
    await writer.remember_part(OBJ, 1, 3, cipher_sizes=[len(b"peer-bytes")])
    reader = PeerRegistry(redis, "node-a", SELF_URL, 90)
    body = b"peer-bytes"
    pool = ScriptedPool([None])
    http = FakeHttp(200, body)
    fetcher = PeerChunkFetcher(pool, reader, "node-a", http, auth_secret=SECRET)

    assert await fetcher(OBJ, 1, 3, 0) == body
    assert http.urls == [f"{PEER_URL}/internal/parts/{OBJ}/1/3/chunks/0"]
    assert len(pool.queries) == 1, "sizes came from the hint; postgres was only asked for the owner"


@pytest.mark.asyncio
async def test_a_legacy_hint_without_sizes_still_fetches_via_postgres() -> None:
    """Writers on the #448 payload (bare node name) must keep working during the roll."""
    redis = FakeRedis()
    writer = PeerRegistry(redis, "node-b", PEER_URL, 90)
    await writer.register()
    await writer.remember_part(OBJ, 1, 3)
    reader = PeerRegistry(redis, "node-a", SELF_URL, 90)
    body = b"peer-bytes"
    pool = ScriptedPool(
        [
            None,
            {"chunk_indexes": [0], "cipher_sizes": [len(body)]},
        ]
    )
    http = FakeHttp(200, body)
    fetcher = PeerChunkFetcher(pool, reader, "node-a", http, auth_secret=SECRET)

    assert await fetcher(OBJ, 1, 3, 0) == body
    assert len(pool.queries) == 2, "owner miss, then sizes for the legacy hint"


@pytest.mark.asyncio
async def test_empty_hint_sizes_are_not_memoised_for_30s(monkeypatch: pytest.MonkeyPatch) -> None:
    """Postgres replica lag: hint finds the node, part_chunks are not visible yet.

    A 30s positive memo of empty sizes made Harbor startedat wait out drain-to-pool.
    """
    clock = {"t": 1_000.0}
    monkeypatch.setattr("hippius_s3.cache.part_memo.time.monotonic", lambda: clock["t"])

    redis = FakeRedis()
    writer = PeerRegistry(redis, "node-b", PEER_URL, 90)
    await writer.register()
    await writer.remember_part(OBJ, 1, 3)
    reader = PeerRegistry(redis, "node-a", SELF_URL, 90)
    body = b"peer-bytes"
    pool = ScriptedPool(
        [
            None,
            {"chunk_indexes": [], "cipher_sizes": []},
            None,
            {"chunk_indexes": [0], "cipher_sizes": [len(body)]},
        ]
    )
    http = FakeHttp(200, body)
    fetcher = PeerChunkFetcher(pool, reader, "node-a", http, auth_secret=SECRET)

    assert await fetcher(OBJ, 1, 3, 0) is None
    assert http.urls == []

    clock["t"] = 1_000.3
    assert await fetcher(OBJ, 1, 3, 0) == body
    assert http.urls == [f"{PEER_URL}/internal/parts/{OBJ}/1/3/chunks/0"]


class _TimeoutStream:
    async def __aenter__(self) -> None:
        raise asyncio.TimeoutError()

    async def __aexit__(self, *_: Any) -> None:
        return None


class TimeoutHttp:
    def __init__(self) -> None:
        self.urls: list[str] = []

    def stream(self, method: str, url: str, headers: dict[str, str] | None = None) -> Any:
        del method, headers
        self.urls.append(url)
        return _TimeoutStream()


@pytest.mark.asyncio
async def test_a_timed_out_fresh_hint_is_retried_after_the_short_ttl(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The peer SSD is the only copy until drain. 30s poison waited ~14s for the pool."""
    clock = {"t": 1_000.0}
    monkeypatch.setattr("hippius_s3.cache.part_memo.time.monotonic", lambda: clock["t"])

    redis = FakeRedis()
    writer = PeerRegistry(redis, "node-b", PEER_URL, 90)
    await writer.register()
    body = b"peer-bytes"
    await writer.remember_part(OBJ, 1, 3, cipher_sizes=[len(body)])
    reader = PeerRegistry(redis, "node-a", SELF_URL, 90)
    boom = TimeoutHttp()
    fetcher = PeerChunkFetcher(ScriptedPool([None]), reader, "node-a", boom, auth_secret=SECRET)

    assert await fetcher(OBJ, 1, 3, 0) is None
    assert len(boom.urls) == 1

    clock["t"] = 1_000.1
    assert await fetcher(OBJ, 1, 3, 0) is None, "still inside the short poison TTL"
    assert len(boom.urls) == 1

    ok = FakeHttp(200, body)
    fetcher._client = ok
    clock["t"] = 1_000.3
    assert await fetcher(OBJ, 1, 3, 0) == body
    assert ok.urls == [f"{PEER_URL}/internal/parts/{OBJ}/1/3/chunks/0"]


@pytest.mark.asyncio
async def test_a_negative_owner_memo_expires_so_a_late_drain_claim_is_seen(monkeypatch: pytest.MonkeyPatch) -> None:
    """A 30s negative memo made wait_for_chunk ignore a drain claim that landed 1s later."""
    clock = {"t": 1_000.0}
    monkeypatch.setattr("hippius_s3.cache.part_memo.time.monotonic", lambda: clock["t"])

    redis = FakeRedis()
    await PeerRegistry(redis, "node-b", PEER_URL, 90).register()
    registry = PeerRegistry(redis, "node-a", SELF_URL, 90)
    pool = FakePool(None)
    http = FakeHttp(200, b"peer-bytes")
    fetcher = PeerChunkFetcher(pool, registry, "node-a", http, auth_secret=SECRET)

    assert await fetcher(OBJ, 1, 3, 0) is None
    assert len(pool.conn.queries) == 1
    assert http.urls == []

    pool.conn._row = residency_row()
    clock["t"] = 1_000.1
    assert await fetcher(OBJ, 1, 3, 0) is None, "still inside the short negative TTL"
    assert len(pool.conn.queries) == 1

    clock["t"] = 1_000.3
    assert await fetcher(OBJ, 1, 3, 0) == b"peer-bytes"
    assert len(pool.conn.queries) == 2, "the late claim was re-resolved after the negative TTL"
