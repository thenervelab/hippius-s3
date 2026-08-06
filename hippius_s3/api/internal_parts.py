"""Internal endpoint serving a single cached chunk from this node's local flash.

The peer tier of the read path (`hippius_s3/cache/peers.py`) calls this on a sibling
`api-local` pod when that node holds a part this one does not. It exists so a cache miss
costs a peer NVMe read (~6 ms + ~1 ms network) instead of a CephFS pool read (~40 ms).

Two properties keep it safe to expose:

- It reads the LOCAL tier only, never the pool and never another peer. A peer that could
  proxy onward would let a lookup race turn into a fetch loop between nodes; and serving the
  pool from here would be pure overhead, since the caller can read the pool itself.
- It returns ciphertext exactly as stored. Chunks are AES-256-GCM encrypted under a
  per-object-version DEK that never leaves the KMS path, so these bytes are useless without
  the envelope — this endpoint grants no read access the caller does not already have.

It sits behind the api's `ip_whitelist` middleware (10.x/172.x pod network only), which is
also why peers address each other by POD IP rather than through a `hostPort` on the node IP.
"""

from __future__ import annotations

import logging

from fastapi import APIRouter
from fastapi import Request
from fastapi import Response


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
    """
    fs_store = getattr(request.app.state, "fs_store", None)
    if fs_store is None:
        return Response(status_code=404)

    # `super()`-style local-only read: on a DualFileSystemPartsStore this MUST NOT be the
    # fallback-aware `get_chunk`, or a peer would happily serve the pool copy — turning a
    # tier meant to avoid pool reads into an extra hop in front of one.
    local = getattr(fs_store, "read_local_chunk", None)
    if local is None:
        return Response(status_code=404)

    try:
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
