"""The drain's content digest of a part, reproduced byte-for-byte on the uploader side.

The drain records `cephor_replication_status.content_sha256` when it hands a part over: the
per-chunk sha256 of every chunk file, folded in index order with a version tag and explicit
lengths (crates/hippius-drain-core/src/redrive.rs `part_digest`). The uploader hashes the bytes it
actually sends and compares the fold to the row before it writes any `chunk_backend` row — so an
upload that raced an `UploadPart` retry (some chunks old, some new) or a request that outlived a
re-drive can never become the backend's copy of a part whose acknowledged bytes are different.

KEEP IN SYNC with the Rust fold: `tests/unit/test_part_digest.py` pins both sides to one golden.
"""

from __future__ import annotations

import hashlib
from collections.abc import Iterable


_TAG = b"hippius-drain/part-digest/v1\n"


def chunk_hash(data: bytes) -> str:
    """Lowercase hex sha256 of one chunk's ciphertext — the agent's `chunk_hash`."""
    return hashlib.sha256(data).hexdigest()


def part_digest(chunk_hashes: Iterable[str]) -> str:
    """Fold per-chunk hashes (in chunk-index order) into the part digest the drain stores."""
    hashes = list(chunk_hashes)
    fold = hashlib.sha256()
    fold.update(_TAG)
    fold.update(len(hashes).to_bytes(8, "little"))
    for value in hashes:
        raw = value.encode("ascii")
        fold.update(len(raw).to_bytes(4, "little"))
        fold.update(raw)
    return fold.hexdigest()
