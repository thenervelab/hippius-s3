"""Peer discovery and per-part chunk fetching across ingest nodes.

The drain retains each part on the node that ingested it, so a chunk is usually on SOME
node's NVMe even when it is not on this one. Reading it from that peer (~6 ms + ~1 ms
network) beats falling through to the CephFS pool (~40 ms, measured on node1 2026-08-06).

Locality is resolved per PART, never per request: on prod 2026-08-06 only 48 of 2,214
sampled multi-part object versions (2%) had every part on one node, while 684 (31%) spanned
all five, because each `UploadPart` is handled by whichever `api-local` pod the round-robin
Service picks. There is no single node a whole GET could usefully be routed to.

Discovery is self-registration through Redis rather than a ConfigMap of node IPs:

- k8s node names do not resolve in cluster DNS, so peers need an explicit address.
- A `hostPort` address would arrive at the peer from the node IP (192.168.x) once SNAT'd,
  which the api's `ip_whitelist` middleware rejects — it admits 10.x/172.x only. Registering
  the POD IP keeps peer traffic on the pod network and inside that whitelist.
- Pod IPs change on every restart, so a hand-maintained map would be wrong within a day.

Each api pod publishes `{prefix}{node_name} -> {"url": ...}` with a TTL and refreshes it; a
pod that dies ages out and stops being offered as a peer.
"""

from __future__ import annotations

import asyncio
import ipaddress
import json
import logging
from typing import Optional
from urllib.parse import urlsplit

import asyncpg
import httpx
import redis.asyncio as async_redis

from hippius_s3.cache.part_memo import PartMemo
from hippius_s3.monitoring import PeerShedReason
from hippius_s3.peer_auth import PEER_AUTH_HEADER


logger = logging.getLogger(__name__)

_PEER_KEY_PREFIX = "hippius:peer:"

# The internal api's port. Every registration is a pod's own `http://{POD_IP}:{PEER_PORT}`, so
# pinning the port here is what makes "this address is one we wrote" checkable. `main.py` builds
# `self_url` from this same constant, so the pin and the registration cannot drift apart.
PEER_PORT = 8000

# The pod network, and the whole of what a peer address may be.
#
# Deliberately NOT `ipaddress.is_private`, which is wrong for this job in both directions that
# matter: it returns True for 169.254.0.0/16 — the cloud metadata range — and for 127.0.0.0/8
# and 192.168.0.0/16. 192.168.x is the NODE network here, and a node address is precisely the
# SNAT case the pod-IP registration exists to avoid (see the module docstring), so excluding it
# is a correctness property and not merely hardening.
_PEER_NETWORKS = (
    ipaddress.ip_network("10.0.0.0/8"),
    ipaddress.ip_network("172.16.0.0/12"),
)


def _is_peer_address(url: str) -> bool:
    """Whether `url` is one this cluster could have registered.

    A peer URL crosses a process boundary through Redis, so it is input rather than a value we
    computed. It has to be an `http` address on the pod network at the api port, with nothing
    after the authority — the fetch path appends its own path, so a registration carrying one
    would be choosing what this pod requests.

    The host must be a literal IP, never a name. A name would put DNS between the address and
    this allow-list: whatever it resolves to today is not what it resolves to tomorrow, and the
    resolution happens inside httpx where nothing can re-check it.
    """
    try:
        parts = urlsplit(url)
        port = parts.port
    except ValueError:
        # urlsplit raises on a malformed authority, and `.port` raises on a non-numeric or
        # out-of-range port. Both mean the same thing here: not an address we wrote.
        return False
    if parts.scheme != "http" or port != PEER_PORT:
        return False
    if parts.path or parts.query or parts.fragment or parts.username or parts.password:
        return False
    host = parts.hostname
    if host is None:
        return False
    try:
        ip = ipaddress.ip_address(host)
    except ValueError:
        return False
    return any(ip in network for network in _PEER_NETWORKS)


# How long a resolved peer address for a part is reused. Which node holds a part is a per-PART
# fact, but the read path asks per CHUNK, so without this a 64-chunk part costs 64 Postgres
# round-trips plus 64 Redis GETs — self-defeating on a tier that exists to save ~34 ms/chunk.
#
# TTL'd, not permanent: the evictor deletes residency rows underneath us, so a stale answer
# must expire. A wrong answer is cheap anyway — the peer 404s and the read falls to the pool.
#
# The memo can also outlive a redrive flipping the part's replication status back to 'pending'
# (the resolve joins on status='replicated', but only at resolve time). Deliberately NOT
# re-checked at fetch time and not shortened: what a peer serves is its SSD copy, which during
# a redrive is the SOURCE the drain re-copies from — current bytes, not stale ones. The
# stale-bytes hazard a redrive opens is the POOL tier, and the AEAD-retry path re-checks the
# status freshly there (`ReplicationSuspectProbe`); paying a per-part Postgres round trip here
# would guard against nothing.
_OWNER_MEMO_TTL_SECONDS = 30.0
_OWNER_MEMO_ENTRIES = 50_000

# Concurrent peer fetches this pod will have in flight to any ONE peer node.
#
# Owl selects a peer subject to per-peer fanout and bandwidth constraints, and the reason is
# the failure this prevents: a part that is hot and resident on a single node draws every
# other node's reads onto that node's api pod — the same uvicorn serving its own ingest. The
# cap SKIPS to the pool rather than queueing, because waiting behind a saturated peer would
# add its latency on top of the pool read that follows.
#
# Must be >= HTTP_STREAM_PREFETCH_CHUNKS or a single reader sheds its own prefetch window —
# see `effective_max_inflight`, which enforces that floor at wiring time regardless of config.
_DEFAULT_MAX_INFLIGHT_PER_PEER = 16

# Overall bound on one peer fetch, connect included. Distinct from the httpx timeout, which is
# per socket operation — its `read` bounds the gap BETWEEN body pieces, so a peer dripping one
# piece just inside it holds the connection and the growing buffer indefinitely. Verified
# against httpx 0.28.1: `Timeout` carries connect/read/write/pool and no total-response field.
#
# Must stay well ABOVE the inter-chunk timeout rather than reusing it. The two mean different
# things: 0.5s of silence is a dead peer, but 0.5s to deliver a whole 4 MiB chunk is a peer
# doing its job. Reusing the loss-cut here would abort healthy large-chunk fetches.
_DEFAULT_DEADLINE_SECONDS = 2.0


def effective_max_inflight(configured: int, prefetch_chunks: int) -> int:
    """The per-peer cap, raised to at least the read path's prefetch depth.

    Every chunk of one PART resolves to the same peer, and the streamer keeps up to
    `HTTP_STREAM_PREFETCH_CHUNKS` chunk fetches in flight. So whenever the cap is below the
    prefetch depth, a **single reader of a single part sheds its own excess to the pool** and
    books it as `client_cap` — contention that does not exist. Shipped defaults were 8 against
    a prefetch of 16, so half of every part over 32 MiB (8 chunks at 4 MiB) went to CephFS
    while the peer sat idle.

    Deriving the floor rather than validating it is deliberate: an invariant that must hold
    between two independently-tuned knobs is one somebody eventually breaks, and the failure is
    silent — reads still succeed, just slower and against the wrong tier. Raising is safe
    because the *serving* side has its own cap and sheds with 503, so the peer stays protected
    however high a client goes.

    **What this does not fix.** The semaphore is per (pod, peer) and shared across every reader
    on the pod, so under concurrent readers the cap still binds — it just moves the shedding
    from `client_cap` to `server_busy` at the peer. Aggregate client demand is
    `(pods - 1) x cap` against one `HIPPIUS_PEER_SERVE_MAX_INFLIGHT`, which on a 5-node fleet is
    oversubscribed by design: Ceph is the intended overflow valve. This only removes the case
    where a reader competes with *itself*.
    """
    if configured >= prefetch_chunks:
        return configured
    logger.warning(
        "raising HIPPIUS_PEER_FETCH_MAX_INFLIGHT from %d to %d to match HTTP_STREAM_PREFETCH_CHUNKS: "
        "a cap below the prefetch depth makes a single reader shed its own chunks to the pool",
        configured,
        prefetch_chunks,
    )
    return prefetch_chunks


def _record_shed(reason: PeerShedReason) -> None:
    """Count a peer fetch we declined to make (or that the peer declined). Never fatal."""
    try:
        from hippius_s3.monitoring import get_metrics_collector

        collector = get_metrics_collector()
        if collector is not None:
            collector.record_peer_fetch_shed(reason)
    except Exception:  # noqa: BLE001 - observability must not fail a read
        pass


def peer_key(node_name: str) -> str:
    return f"{_PEER_KEY_PREFIX}{node_name}"


class PeerRegistry:
    """Publishes this pod's address and resolves peers' addresses."""

    def __init__(
        self,
        redis_client: async_redis.Redis | async_redis.RedisCluster,
        node_name: str,
        self_url: str,
        ttl_seconds: int,
    ) -> None:
        self._redis = redis_client
        self._node_name = node_name
        self._self_url = self_url
        self._ttl = ttl_seconds

    async def register(self) -> None:
        """Publish this pod's address with a fresh TTL. Best-effort.

        The TTL IS the liveness signal — a pod that stops refreshing ages out and peers stop
        routing to it — so a failed refresh degrades to "this node is not offered as a peer",
        which costs pool reads and nothing else.
        """
        try:
            await self._redis.set(
                peer_key(self._node_name),
                json.dumps({"url": self._self_url}),
                ex=self._ttl,
            )
        except Exception as exc:  # noqa: BLE001 - registration must never break the api
            logger.debug("peer registration failed for %s: %s", self._node_name, exc)

    async def run_refresh(self, interval_seconds: int) -> None:
        """Re-publish this pod's address forever, so its TTL never lapses while it is alive.

        Without this the entry expires once and the node silently stops being offered as a
        peer for the rest of the pod's life — the fleet degrades to pool reads with nothing
        logging that it happened.
        """
        while True:
            await asyncio.sleep(interval_seconds)
            await self.register()

    async def resolve(self, node_name: str) -> Optional[str]:
        """The peer's base URL, or None when it has not registered, aged out, or is not ours.

        Validation lives HERE rather than at the fetch, because this is the single point every
        peer address passes through. A check at the call site would be one a future caller could
        skip; a check here means no code path can hand httpx an address that did not pass it.
        """
        try:
            raw = await self._redis.get(peer_key(node_name))
        except Exception as exc:  # noqa: BLE001
            logger.debug("peer lookup failed for %s: %s", node_name, exc)
            return None
        if not raw:
            return None
        try:
            url = str(json.loads(raw)["url"])
        except (ValueError, KeyError, TypeError):
            return None
        if not _is_peer_address(url):
            # Never routine: a pod's own registration always passes, so anything here was put in
            # Redis by something that is not an api pod. WARNING so it is visible the first time
            # rather than after someone goes looking for it.
            _record_shed("bad_peer_url")
            logger.warning(
                "refusing peer address %r registered for node %s: not an http pod-network address on port %d",
                url,
                node_name,
                PEER_PORT,
            )
            return None
        return url


class PeerChunkFetcher:
    """Fetches a chunk from whichever peer node currently holds the part on flash."""

    def __init__(
        self,
        pool: asyncpg.Pool,
        registry: PeerRegistry,
        node_name: str,
        client: httpx.AsyncClient,
        *,
        auth_secret: str,
        max_inflight: int = _DEFAULT_MAX_INFLIGHT_PER_PEER,
        deadline_seconds: float = _DEFAULT_DEADLINE_SECONDS,
    ) -> None:
        self._pool = pool
        self._registry = registry
        self._node_name = node_name
        self._client = client
        # Required, with no default: the peer refuses a request that does not carry it, so a
        # default would only let a caller build a fetcher that silently never fetches.
        self._auth_secret = auth_secret
        self._deadline = deadline_seconds
        # Caches the resolved base URL and the part's per-chunk ciphertext sizes, so the one
        # query that yields both is paid once per part rather than once per chunk. A `None`
        # result is cached too — "no peer has this" is just as per-part, and re-asking for every
        # chunk of a pool-only part is the common cold-read case.
        #
        # The sizes are kept ONLY when a peer actually resolved. They are the expensive half of
        # the entry (one int per chunk, so ~1280 for a 5 GiB part), and a pool-only part — the
        # common case, and the one that would otherwise dominate this memo — never needs them.
        self._owner_url: PartMemo[tuple[str, int, int], tuple[Optional[str], dict[int, int]]] = PartMemo(
            _OWNER_MEMO_TTL_SECONDS, _OWNER_MEMO_ENTRIES
        )
        self._max_inflight = max_inflight
        # One slot pool per peer, keyed by its base URL (one URL per node). This bounds what
        # THIS pod sends; the serving side has its own limiter, because five pods each within
        # their own cap still add up at the peer.
        self._inflight: dict[str, asyncio.Semaphore] = {}

    async def _resolve_part(
        self, object_id: str, object_version: int, part_number: int
    ) -> tuple[Optional[str], dict[int, int]]:
        """Who holds the part on flash, and the exact ciphertext size of each of its chunks.

        Residency — not `cephor_replication_status.node_id` — is the ground truth for "who
        has it on flash right now": the ingest node may have evicted its copy, and a
        promotion may have put one somewhere else entirely.

        Several nodes can hold one part once promotion has run, so the `LIMIT 1` needs an
        ORDER BY to be a CHOICE rather than whatever the plan emits first. Unordered it is not
        merely arbitrary but unstable across replans, and the `PartMemo` hides that locally
        while leaving the fleet's peer traffic distributed by the planner — which is precisely
        what the serve-side fanout cap has no way to account for. Ordering on `resident_at`
        prefers the copy that has already survived eviction passes over one a read created
        moments ago. The candidate set is bounded by the node count (5 in prod), so the sort
        is free.

        Both facts come back together because both are per-PART while the read path asks per
        CHUNK, so the memo above amortises one round trip over the whole part. Splitting the
        sizes into a second query would put a Postgres round trip back on the per-chunk path,
        which is the cost the memo exists to remove.

        The sizes hang off the owner rather than sitting beside it: with no owner there are no
        rows to join against, so the `part_chunks` lookup is never evaluated and a pool-only
        part — the common cold read — costs exactly what it did before.
        """
        async with self._pool.acquire() as conn:
            row = await conn.fetchrow(
                """
                WITH owner AS (
                    SELECT r.node_id
                    FROM cephor_ssd_residency r
                    JOIN cephor_replication_status s
                      ON s.object_id = r.object_id AND s.version = r.version AND s.part_number = r.part_number
                    WHERE r.object_id = $1 AND r.version = $2 AND r.part_number = $3
                      AND r.node_id <> $4
                      AND s.status = 'replicated'
                    ORDER BY r.resident_at
                    LIMIT 1
                )
                SELECT o.node_id, sizes.chunk_indexes, sizes.cipher_sizes
                FROM owner o
                LEFT JOIN LATERAL (
                    SELECT
                        array_agg(pc.chunk_index ORDER BY pc.chunk_index) AS chunk_indexes,
                        array_agg(pc.cipher_size_bytes ORDER BY pc.chunk_index) AS cipher_sizes
                    FROM parts p
                    JOIN part_chunks pc ON pc.part_id = p.part_id
                    WHERE p.object_id = $1::uuid AND p.object_version = $2 AND p.part_number = $3
                ) sizes ON TRUE
                """,
                str(object_id),
                int(object_version),
                int(part_number),
                self._node_name,
            )
        if row is None:
            return None, {}
        indexes = row["chunk_indexes"] or []
        sizes = row["cipher_sizes"] or []
        return str(row["node_id"]), {int(i): int(s) for i, s in zip(indexes, sizes, strict=False)}

    async def __call__(
        self, object_id: str, object_version: int, part_number: int, chunk_index: int
    ) -> Optional[bytes]:
        """The chunk from a peer, or None to fall through to the pool.

        Returns None rather than raising on every failure path. The caller treats a peer as
        an optimisation over an authoritative pool copy, so "no peer answered" and "the peer
        errored" are the same outcome: read the pool.
        """
        part_key = (str(object_id), int(object_version), int(part_number))
        cached = self._owner_url.get(part_key)
        if cached is not None:
            base, sizes = cached
        else:
            owner, sizes = await self._resolve_part(object_id, object_version, part_number)
            base = await self._registry.resolve(owner) if owner is not None else None
            if base is None:
                # No peer to fetch from, so the sizes are dead weight in a memo bounded by
                # entries rather than by bytes. Dropping them keeps a pool-only part's entry as
                # cheap as it was before the size check existed.
                sizes = {}
            # Paired with the base so a cached "no peer" (None) is distinguishable from a cache
            # miss — otherwise every pool-only part would re-query on every chunk.
            self._owner_url.put(part_key, (base, sizes))
        if base is None:
            return None

        expected = sizes.get(int(chunk_index))
        if expected is None:
            # No recorded size means no way to tell a correct body from a truncated one, and an
            # unverified body that gets promoted onto local flash is a PERMANENT read failure
            # for that chunk: nothing on the read path invalidates a cached chunk on AEAD
            # failure, so it wins the local tier and fails every subsequent read until the
            # evictor happens to unlink the part. The pool copy is authoritative, so declining
            # costs one pool read. Counted rather than silent because it should never happen —
            # `part_chunks` rows are written for every chunk index when the part is created.
            _record_shed("unknown_size")
            return None

        slots = self._inflight.setdefault(base, asyncio.Semaphore(self._max_inflight))
        # Best-effort fast path, not a hard gate: a slot can free between this check and the
        # acquire below, in which case the request proceeds rather than shedding. The cap
        # itself is the semaphore; this only avoids QUEUEING behind a saturated peer, since
        # waiting would stack that peer's backlog on top of the pool read that follows anyway.
        #
        # This cap is per (pod, peer) and SHARED across every reader on the pod, so a shed here
        # means genuine contention — several readers converging on one peer. It used to mean
        # something weaker as well: with the cap below `HTTP_STREAM_PREFETCH_CHUNKS`, one reader
        # of one part shed its own prefetch window, because every chunk of a part resolves to
        # the same peer. `effective_max_inflight` now floors the cap at the prefetch depth, so
        # that case is gone and a `client_cap` count is trustworthy as a contention signal.
        if slots.locked():
            _record_shed("client_cap")
            return None

        url = f"{base}/internal/parts/{object_id}/{object_version}/{part_number}/chunks/{chunk_index}"
        try:
            async with slots:
                return await asyncio.wait_for(self._fetch_verified(url, expected), self._deadline)
        except (httpx.HTTPError, OSError, asyncio.TimeoutError) as exc:
            # A registered-but-unreachable peer — a drained or cordoned node whose replacement
            # has not re-registered under the same key — stays resolvable until its TTL lapses.
            # Retrying it per chunk pays the full fetch timeout every time before falling
            # through to the pool, which makes that shard read WORSE than with no peer tier at
            # all. Poison the memo so the rest of this part goes straight to the pool; the
            # entry expires on its own, so a peer that comes back is picked up again.
            #
            # A deadline expiry is poisoned on the same grounds: a peer that drips is as useless
            # as one that refuses, and paying the full deadline once per chunk is the worst of
            # both tiers.
            self._owner_url.put(part_key, (None, {}))
            logger.debug("peer fetch to %s failed, falling through to the pool: %s", base, exc)
            return None

    async def _fetch_verified(self, url: str, expected: int) -> Optional[bytes]:
        """One chunk from a peer, returned only if its body is exactly `expected` bytes.

        Streamed rather than read through `client.get`, because the size bound has to apply
        WHILE the body arrives: by the time a buffered response could be measured, the memory
        it costs has already been spent, and a peer would be choosing how much that is.

        The exact length is the check. An upper bound of `chunk_size + AEAD overhead` accepts
        precisely the failure this exists to catch — a peer on a rolled-back or half-deployed
        image answers 200 with a SHORT body, and short bodies are legal, because the last chunk
        of a part is short.
        """
        # The auth header rides on the STREAM call, not just the buffered one it replaced. The
        # peer refuses an unauthenticated request with 404, which this method reads as a routine
        # miss — so dropping the header here would not fail loudly, it would shed the entire peer
        # tier to the pool and look like an eviction storm. Pinned by test_peer_handshake.py.
        async with self._client.stream("GET", url, headers={PEER_AUTH_HEADER: self._auth_secret}) as response:
            if response.status_code == 503:
                # The peer shed this request to protect its own ingest. Transient, so it must
                # NOT poison the memo the way a connect failure does — that would keep the whole
                # part on the pool long after a brief spike passed.
                _record_shed("server_busy")
                return None
            if response.status_code == 404:
                # Routine — the peer evicted the part between the residency read and now — and
                # counted for exactly that reason. Under an eviction storm this is the entire
                # peer tier going away, and the only other trace of it is a DROP in
                # `chunk_reads_by_tier_total{tier=peer}`, which reads the same as a tier nobody
                # is using. Not memo-poisoned: the peer answered, and promptly.
                _record_shed("peer_miss")
                return None
            if response.status_code != 200:
                # Anything else is a peer that is broken rather than busy or empty — a half-rolled
                # deploy answering 500. Kept apart from `peer_miss` because the response differs:
                # find the pod, rather than wait for the eviction cursor to settle.
                _record_shed("peer_error")
                return None
            body = bytearray()
            async for piece in response.aiter_bytes():
                body += piece
                if len(body) > expected:
                    # Abandon mid-body rather than read to the end. The body is already wrong,
                    # and this is what keeps an over-long one bounded by the chunk we asked for.
                    _record_shed("bad_length")
                    return None
            if len(body) != expected:
                _record_shed("bad_length")
                return None
            return bytes(body)
