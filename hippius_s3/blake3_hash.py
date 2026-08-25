"""BLAKE3 of the object plaintext — the digest the console shows as Arion hash.

Computed in flight on the write pipeline (same framed buffers as the rolling
MD5), not after the async Arion hop, and not by re-reading ciphertext from disk.
"""

from __future__ import annotations

from typing import Any

import blake3

from hippius_s3.utils_core import get_query


def hex_of(data: bytes) -> str:
    return blake3.blake3(data, max_threads=1).hexdigest()


def new_hasher() -> blake3.blake3:
    # max_threads=1: updates already run on the single-worker etag-hash FIFO.
    # Inner rayon would oversubscribe every PUT for a few microseconds of gain.
    return blake3.blake3(max_threads=1)


async def persist_version_hash(
    db: Any,
    *,
    object_id: str,
    object_version: int,
    digest: str,
) -> None:
    """Write the file hash onto the version so listings can surface it.

    Its own column, deliberately — NOT ipfs_cid/cid_id. Those are read back as real CIDs by the
    purge/unpin scripts (`COALESCE(c.cid, ov.ipfs_cid)`, guarded only against ''/'pending'/NULL),
    so a 64-hex digest parked there would enter the unpin worklist as though it were a pin.
    """
    await db.execute(
        get_query("update_object_version_body_blake3"),
        digest,
        object_id,
        int(object_version),
    )
