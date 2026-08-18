"""BLAKE3 of the object body — the id Arion uses for a file.

Computed at PUT / Complete time from the plaintext the client sent, so the
console can show the hash immediately instead of waiting for the async
Arion hop (which hashes encrypted chunks, not the original file).
"""

from __future__ import annotations

from typing import Any

import blake3

from hippius_s3.utils_core import upsert_cid_and_get_id


def hex_of(data: bytes) -> str:
    return blake3.blake3(data).hexdigest()


def new_hasher() -> blake3.blake3:
    return blake3.blake3()


async def persist_version_hash(
    db: Any,
    *,
    object_id: str,
    object_version: int,
    digest: str,
) -> None:
    """Write the file hash onto the version so listings can surface it."""
    cid_id = await upsert_cid_and_get_id(db, digest)
    await db.execute(
        """
        UPDATE object_versions
           SET ipfs_cid = $1,
               cid_id = $2::uuid
         WHERE object_id = $3
           AND object_version = $4
        """,
        digest,
        cid_id,
        object_id,
        int(object_version),
    )
