"""BLAKE3 of object plaintext — the digest the console shows as Arion hash.

KATs match hippius-migrator `copy.rs` and the console `useApiObjects.test.ts`.
"""

from __future__ import annotations

import uuid

import pytest

from hippius_s3.blake3_hash import hex_of
from hippius_s3.blake3_hash import new_hasher
from hippius_s3.blake3_hash import persist_version_hash
from tests.unit._fake_pool import make_fake_pool


ABC_HASH = "6437b3ac38465133ffb63b75273a8db548c558465d79db03fd359c6cd5bd9d85"
EMPTY_HASH = "af1349b9f5f9a1a6a0404dea36dcc9499bcb25c9adc112b7cc9a93cae41f3262"


def test_hex_of_matches_published_abc_vector() -> None:
    assert hex_of(b"abc") == ABC_HASH


def test_empty_input_matches_published_vector() -> None:
    assert hex_of(b"") == EMPTY_HASH


def test_incremental_hasher_matches_one_shot() -> None:
    hasher = new_hasher()
    hasher.update(b"hel")
    hasher.update(b"lo")
    assert hasher.hexdigest() == hex_of(b"hello")


@pytest.mark.asyncio
async def test_persist_writes_body_blake3_on_the_version() -> None:
    pool = make_fake_pool()
    digest = ABC_HASH
    object_id = str(uuid.uuid4())

    await persist_version_hash(
        pool,
        object_id=object_id,
        object_version=3,
        digest=digest,
    )

    updates = [e for e in pool.calls("execute") if e["query"] and "body_blake3" in e["query"]]
    assert len(updates) == 1
    assert updates[0]["args"] == (digest, object_id, 3)


@pytest.mark.asyncio
async def test_persist_never_touches_the_legacy_cid_columns() -> None:
    """ipfs_cid/cid_id are read back as REAL CIDs by the purge+unpin scripts.

    `nuke_user.py`, `purge_buckets.py`, `purge_source_versions.py`,
    `cleanup_migration_versions.py` and `export_legacy_unpin_worklist.py` all select
    `COALESCE(c.cid, ov.ipfs_cid)` guarded only against NULL/''/'pending' — a 64-hex BLAKE3
    digest passes every one of those, so parking the digest there would feed plaintext hashes
    into the unpin worklist as though they were pins.
    """
    pool = make_fake_pool()

    await persist_version_hash(pool, object_id=str(uuid.uuid4()), object_version=1, digest=ABC_HASH)

    for call in pool.calls():
        query = call.get("query") or ""
        assert "ipfs_cid" not in query
        assert "cid_id" not in query
        assert "INSERT INTO cids" not in query
