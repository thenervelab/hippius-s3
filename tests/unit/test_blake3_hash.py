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
async def test_persist_writes_ipfs_cid_and_cid_id_on_the_version() -> None:
    cid_id = uuid.uuid4()

    def router(method: str, query: str | None, args: tuple) -> object:
        if method == "fetchrow":
            return {"id": cid_id}
        return None

    pool = make_fake_pool(router)
    digest = ABC_HASH
    object_id = str(uuid.uuid4())

    await persist_version_hash(
        pool,
        object_id=object_id,
        object_version=3,
        digest=digest,
    )

    fetch = pool.calls("fetchrow")
    assert len(fetch) == 1
    assert fetch[0]["args"] == (digest,)

    updates = [
        e for e in pool.calls("execute") if e["query"] and "ipfs_cid" in e["query"]
    ]
    assert len(updates) == 1
    assert updates[0]["args"] == (digest, str(cid_id), object_id, 3)
