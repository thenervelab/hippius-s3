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
import json
import logging
from typing import Optional

import asyncpg
import httpx
import redis.asyncio as async_redis

from hippius_s3.cache.part_memo import PartMemo


logger = logging.getLogger(__name__)

_PEER_KEY_PREFIX = "hippius:peer:"

# How long a resolved peer address for a part is reused. Which node holds a part is a per-PART
# fact, but the read path asks per CHUNK, so without this a 64-chunk part costs 64 Postgres
# round-trips plus 64 Redis GETs — self-defeating on a tier that exists to save ~34 ms/chunk.
#
# TTL'd, not permanent: the evictor deletes residency rows underneath us, so a stale answer
# must expire. A wrong answer is cheap anyway — the peer 404s and the read falls to the pool.
_OWNER_MEMO_TTL_SECONDS = 30.0
_OWNER_MEMO_ENTRIES = 50_000


def peer_key(node_name: str) -> str:
    return f"{_PEER_KEY_PREFIX}{node_name}"


class PeerRegistry:
    """Publishes this pod's address and resolves peers' addresses."""

    def __init__(self, redis_client: async_redis.Redis, node_name: str, self_url: str, ttl_seconds: int) -> None:
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
        """The peer's base URL, or None when it has not registered (or has aged out)."""
        try:
            raw = await self._redis.get(peer_key(node_name))
        except Exception as exc:  # noqa: BLE001
            logger.debug("peer lookup failed for %s: %s", node_name, exc)
            return None
        if not raw:
            return None
        try:
            url = json.loads(raw)["url"]
        except (ValueError, KeyError, TypeError):
            return None
        return str(url) or None


class PeerChunkFetcher:
    """Fetches a chunk from whichever peer node currently holds the part on flash."""

    def __init__(
        self,
        pool: asyncpg.Pool,
        registry: PeerRegistry,
        node_name: str,
        client: httpx.AsyncClient,
    ) -> None:
        self._pool = pool
        self._registry = registry
        self._node_name = node_name
        self._client = client
        # Caches the resolved base URL per part, so both the residency query and the peer
        # address lookup are paid once per part rather than once per chunk. A `None` result is
        # cached too — "no peer has this" is just as per-part, and re-asking for every chunk
        # of a pool-only part is the common cold-read case.
        self._owner_url: PartMemo[tuple[str, int, int], tuple[Optional[str]]] = PartMemo(
            _OWNER_MEMO_TTL_SECONDS, _OWNER_MEMO_ENTRIES
        )

    async def _owner(self, object_id: str, object_version: int, part_number: int) -> Optional[str]:
        """A node other than this one that holds the part, per the residency table.

        Residency — not `cephor_replication_status.node_id` — is the ground truth for "who
        has it on flash right now": the ingest node may have evicted its copy, and a
        promotion may have put one somewhere else entirely.
        """
        async with self._pool.acquire() as conn:
            row = await conn.fetchrow(
                """
                SELECT r.node_id
                FROM cephor_ssd_residency r
                JOIN cephor_replication_status s
                  ON s.object_id = r.object_id AND s.version = r.version AND s.part_number = r.part_number
                WHERE r.object_id = $1 AND r.version = $2 AND r.part_number = $3
                  AND r.node_id <> $4
                  AND s.status = 'replicated'
                LIMIT 1
                """,
                str(object_id),
                int(object_version),
                int(part_number),
                self._node_name,
            )
        return str(row["node_id"]) if row else None

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
            base = cached[0]
        else:
            owner = await self._owner(object_id, object_version, part_number)
            base = await self._registry.resolve(owner) if owner is not None else None
            # Wrapped in a 1-tuple so a cached "no peer" (None) is distinguishable from a
            # cache miss — otherwise every pool-only part would re-query on every chunk.
            self._owner_url.put(part_key, (base,))
        if base is None:
            return None
        url = f"{base}/internal/parts/{object_id}/{object_version}/{part_number}/chunks/{chunk_index}"
        response = await self._client.get(url)
        if response.status_code != 200:
            # 404 is routine: the peer evicted the part between the residency read and now.
            return None
        return response.content
