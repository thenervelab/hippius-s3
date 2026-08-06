"""Peer discovery and per-part chunk fetching.

Discovery is self-registration through Redis rather than a map of node IPs, because k8s node
names do not resolve in cluster DNS, pod IPs change on every restart, and a `hostPort`
address would reach the peer from the node IP (192.168.x) once SNAT'd — which the api's
`ip_whitelist` middleware rejects, admitting only 10.x/172.x.
"""

from __future__ import annotations

import json
from typing import Any
from typing import Optional

import httpx
import pytest

from hippius_s3.cache.peers import PeerChunkFetcher
from hippius_s3.cache.peers import PeerRegistry
from hippius_s3.cache.peers import peer_key


OBJ = "466916c0-d61b-4518-b81b-9576b574270a"


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


class FakeResponse:
    def __init__(self, status_code: int, content: bytes = b"") -> None:
        self.status_code = status_code
        self.content = content


class FakeHttp:
    def __init__(self, response: FakeResponse) -> None:
        self._response = response
        self.urls: list[str] = []

    async def get(self, url: str) -> FakeResponse:
        self.urls.append(url)
        return self._response


@pytest.mark.asyncio
async def test_registration_publishes_this_pods_address_with_a_ttl() -> None:
    """The TTL IS the liveness signal — a pod that stops refreshing stops being a peer."""
    redis = FakeRedis()
    registry = PeerRegistry(redis, "k8s-v3-node2", "http://10.42.1.5:8000", 90)

    await registry.register()

    assert json.loads(redis.store[peer_key("k8s-v3-node2")])["url"] == "http://10.42.1.5:8000"
    assert redis.ttls[peer_key("k8s-v3-node2")] == 90


@pytest.mark.asyncio
async def test_a_redis_outage_never_breaks_registration_or_lookup() -> None:
    """Peer discovery is an optimisation; losing it costs pool reads, not requests."""
    redis = FakeRedis()
    redis.fail = True
    registry = PeerRegistry(redis, "k8s-v3-node2", "http://10.42.1.5:8000", 90)

    await registry.register()  # must not raise
    assert await registry.resolve("k8s-v3-node3") is None


@pytest.mark.asyncio
async def test_an_unregistered_peer_resolves_to_none() -> None:
    registry = PeerRegistry(FakeRedis(), "k8s-v3-node2", "http://10.42.1.5:8000", 90)
    assert await registry.resolve("k8s-v3-node3") is None


@pytest.mark.asyncio
async def test_a_chunk_is_fetched_from_the_peer_that_holds_it() -> None:
    redis = FakeRedis()
    registry = PeerRegistry(redis, "node-a", "http://10.42.1.5:8000", 90)
    await PeerRegistry(redis, "node-b", "http://10.42.2.9:8000", 90).register()
    http = FakeHttp(FakeResponse(200, b"peer-bytes"))
    fetcher = PeerChunkFetcher(FakePool({"node_id": "node-b"}), registry, "node-a", http)

    assert await fetcher(OBJ, 1, 3, 2) == b"peer-bytes"
    assert http.urls == [f"http://10.42.2.9:8000/internal/parts/{OBJ}/1/3/chunks/2"]


@pytest.mark.asyncio
async def test_the_residency_lookup_excludes_this_node() -> None:
    """Asking ourselves is pointless — the local tier already missed on this chunk.

    The exclusion lives in the query so a node that holds the part but has it evicted
    mid-read cannot resolve to itself and burn a network round trip on its own miss.
    """
    pool = FakePool({"node_id": "node-b"})
    registry = PeerRegistry(FakeRedis(), "node-a", "http://10.42.1.5:8000", 90)
    fetcher = PeerChunkFetcher(pool, registry, "node-a", FakeHttp(FakeResponse(404)))

    await fetcher(OBJ, 1, 3, 2)

    assert pool.conn.queries[0] == (OBJ, 1, 3, "node-a"), "this node is bound out of the lookup"


@pytest.mark.asyncio
async def test_no_peer_holds_the_part_so_the_pool_is_used() -> None:
    registry = PeerRegistry(FakeRedis(), "node-a", "http://10.42.1.5:8000", 90)
    fetcher = PeerChunkFetcher(FakePool(None), registry, "node-a", FakeHttp(FakeResponse(200, b"x")))

    assert await fetcher(OBJ, 1, 3, 2) is None


@pytest.mark.asyncio
async def test_a_peer_that_evicted_the_chunk_returns_none() -> None:
    """404 is routine: the evictor unlinked it between the residency read and the fetch."""
    redis = FakeRedis()
    await PeerRegistry(redis, "node-b", "http://10.42.2.9:8000", 90).register()
    registry = PeerRegistry(redis, "node-a", "http://10.42.1.5:8000", 90)
    fetcher = PeerChunkFetcher(FakePool({"node_id": "node-b"}), registry, "node-a", FakeHttp(FakeResponse(404)))

    assert await fetcher(OBJ, 1, 3, 2) is None


@pytest.mark.asyncio
async def test_a_resolved_but_unregistered_peer_is_skipped() -> None:
    """Residency says node-b holds it, but node-b's pod has not registered (or aged out)."""
    registry = PeerRegistry(FakeRedis(), "node-a", "http://10.42.1.5:8000", 90)
    http = FakeHttp(FakeResponse(200, b"never"))
    fetcher = PeerChunkFetcher(FakePool({"node_id": "node-b"}), registry, "node-a", http)

    assert await fetcher(OBJ, 1, 3, 2) is None
    assert http.urls == [], "no address, so no request was attempted"


@pytest.mark.asyncio
async def test_the_owner_lookup_is_per_part_not_per_chunk() -> None:
    """Which peer holds a part is a per-PART fact, but the read path asks per chunk.

    Unmemoised, a 64-chunk part costs 64 Postgres round-trips plus 64 Redis GETs on the read
    path — self-defeating on a tier that exists to save ~34 ms per chunk.
    """
    redis = FakeRedis()
    await PeerRegistry(redis, "node-b", "http://10.42.2.9:8000", 90).register()
    registry = PeerRegistry(redis, "node-a", "http://10.42.1.5:8000", 90)
    pool = FakePool({"node_id": "node-b"})
    http = FakeHttp(FakeResponse(200, b"peer-bytes"))
    fetcher = PeerChunkFetcher(pool, registry, "node-a", http)

    for chunk in range(64):
        assert await fetcher(OBJ, 1, 3, chunk) == b"peer-bytes"

    assert len(pool.conn.queries) == 1, f"64 chunks issued {len(pool.conn.queries)} residency lookups"
    assert len(http.urls) == 64, "but every chunk is still fetched"


@pytest.mark.asyncio
async def test_a_different_part_is_looked_up_separately() -> None:
    """The memo is keyed by part — parts of one object can live on different nodes."""
    redis = FakeRedis()
    await PeerRegistry(redis, "node-b", "http://10.42.2.9:8000", 90).register()
    registry = PeerRegistry(redis, "node-a", "http://10.42.1.5:8000", 90)
    pool = FakePool({"node_id": "node-b"})
    fetcher = PeerChunkFetcher(pool, registry, "node-a", FakeHttp(FakeResponse(200, b"x")))

    await fetcher(OBJ, 1, 3, 0)
    await fetcher(OBJ, 1, 4, 0)

    assert len(pool.conn.queries) == 2, "part 3 and part 4 are resolved independently"


class FailingHttp:
    """A peer that is registered but unreachable — a cordoned or drained node."""

    def __init__(self) -> None:
        self.attempts = 0

    async def get(self, url: str):  # noqa: ANN201
        self.attempts += 1
        raise httpx.ConnectError("connection refused")


@pytest.mark.asyncio
async def test_a_dead_peer_is_tried_once_not_once_per_chunk() -> None:
    """A registered-but-dead peer must not make reads SLOWER than having no peer tier.

    Its Redis registration stays resolvable for up to the 90 s TTL if no replacement pod
    re-registers under the same node name — a drained node, not a rolling restart. Retrying
    it per chunk pays the full fetch timeout every time before falling through to the pool,
    so for up to a minute and a half that node's shard reads worse than with the tier off.
    """
    redis = FakeRedis()
    await PeerRegistry(redis, "node-b", "http://10.42.2.9:8000", 90).register()
    registry = PeerRegistry(redis, "node-a", "http://10.42.1.5:8000", 90)
    http = FailingHttp()
    fetcher = PeerChunkFetcher(FakePool({"node_id": "node-b"}), registry, "node-a", http)

    for chunk in range(32):
        assert await fetcher(OBJ, 1, 3, chunk) is None, "every chunk falls through to the pool"

    assert http.attempts == 1, f"a dead peer was retried {http.attempts} times"
