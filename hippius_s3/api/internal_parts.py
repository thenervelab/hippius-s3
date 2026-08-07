"""Internal endpoint serving a single cached chunk from this node's local flash.

The peer tier of the read path (`hippius_s3/cache/peers.py`) calls this on a sibling
`api-local` pod when that node holds a part this one does not. It exists so a cache miss
costs a peer NVMe read (~6 ms + ~1 ms network) instead of a CephFS pool read (~40 ms).

Two properties bound the BLAST RADIUS of what it hands over:

- It reads the LOCAL tier only, never the pool and never another peer. A peer that could
  proxy onward would let a lookup race turn into a fetch loop between nodes; and serving the
  pool from here would be pure overhead, since the caller can read the pool itself.
- It returns ciphertext exactly as stored. Chunks are AES-256-GCM encrypted under a
  per-object-version DEK that never leaves the KMS path, so these bytes are useless without
  the envelope — this endpoint grants no read access the caller does not already have.

Neither is an authorization argument, and the api's `ip_whitelist` middleware is not one
either: the gateway is a pod on that same 10.x network and it proxies arbitrary paths straight
off the internet, so "only pods can reach it" reduced to "anyone can" — a 200-vs-404 existence
oracle and unmetered NVMe load on a pod that is also serving ingest. What actually bounds this
route is a shared secret every peer presents (`hippius_s3/peer_auth.py`), which is sufficient
only because the gateway strips every inbound `x-hippius-*` header before forwarding — so a
client cannot supply the one this checks. The route is not mounted at all unless both
`HIPPIUS_PEER_SERVE_ENABLED` and a secret are set, because an "authentication disabled" mode
would be indistinguishable from the defect that made this paragraph necessary.

The durable fix is a second uvicorn port for internal routes that the gateway has no route to,
which would make the secret a second line rather than the only one. Not done here: it changes
the Service and probe topology, so it is deployment work, not a code change.

Peers still address each other by POD IP rather than through a `hostPort` on the node IP, so
that the traffic stays inside the ip_whitelist as well.
"""

from __future__ import annotations

import logging

from fastapi import APIRouter
from fastapi import Request
from fastapi import Response

from hippius_s3.peer_auth import PEER_AUTH_HEADER
from hippius_s3.peer_auth import peer_auth_matches


logger = logging.getLogger(__name__)

router = APIRouter()


@router.get("/internal/parts/{object_id}/{object_version}/{part_number}/chunks/{chunk_index}")
async def get_local_chunk(
    object_id: str,
    object_version: int,
    part_number: int,
    chunk_index: int,
    request: Request,
) -> Response:
    """One chunk from this node's local tier, or 404.

    404 is the routine answer, not an error: the caller resolved this node from the residency
    table, and the evictor may have unlinked the part in between. The caller falls through to
    the pool on any non-200, so this never needs to distinguish "evicted" from "never had it".

    An unauthenticated caller gets that same 404 — not a 403, which would confirm the route
    exists and that they named a real (object, version, part). That existence oracle is most
    of what this endpoint was worth to an attacker, so the refusal must be indistinguishable
    from a miss, and it runs before any filesystem work so the timing cannot separate them
    either.
    """
    expected = getattr(request.app.state, "peer_auth_secret", "")
    if not peer_auth_matches(request.headers.get(PEER_AUTH_HEADER), expected):
        return Response(status_code=404)

    fs_store = getattr(request.app.state, "fs_store", None)
    if fs_store is None:
        return Response(status_code=404)

    # `super()`-style local-only read: on a DualFileSystemPartsStore this MUST NOT be the
    # fallback-aware `get_chunk`, or a peer would happily serve the pool copy — turning a
    # tier meant to avoid pool reads into an extra hop in front of one.
    local = getattr(fs_store, "read_local_chunk", None)
    if local is None:
        return Response(status_code=404)

    # Shed over the in-flight cap. This pod is also serving its own ingest and reads, so a
    # part that is hot and resident only here would otherwise let every other node's fetches
    # crowd out PUTs on the same uvicorn. 503 is the right answer rather than queueing: the
    # caller treats any non-200 as "read the pool", so shedding costs it a fallback, while
    # queueing would add this pod's saturation to the pool read that follows anyway.
    # Best-effort fast path, not a hard gate: a slot can free between this check and the
    # acquire below, in which case the request proceeds instead of shedding. The semaphore is
    # the actual cap; this check only avoids queueing, which would add this pod's saturation
    # to the pool read the caller falls back to anyway.
    limiter = getattr(request.app.state, "peer_serve_limiter", None)
    if limiter is not None and limiter.locked():
        return Response(status_code=503)

    try:
        if limiter is None:
            data = await local(object_id, object_version, part_number, chunk_index)
        else:
            async with limiter:
                data = await local(object_id, object_version, part_number, chunk_index)
    except (OSError, ValueError) as exc:
        logger.debug(
            "local chunk read failed for %s v%s part %s chunk %s: %s",
            object_id,
            object_version,
            part_number,
            chunk_index,
            exc,
        )
        return Response(status_code=404)

    if data is None:
        return Response(status_code=404)
    return Response(content=data, media_type="application/octet-stream")
