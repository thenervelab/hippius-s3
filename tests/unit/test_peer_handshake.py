"""The two halves of the peer handshake, driven against each other.

Every other test in this area exercises one side: `test_peer_fetch.py` asserts what the
fetcher sends against a fake HTTP client, and `test_internal_parts_endpoint.py` asserts what
the endpoint accepts against hand-built headers. Both keep passing if the two sides disagree
about the header's name or casing, because each agrees with itself.

That disagreement takes the whole peer tier dark in production and shows up only as
`chunk_reads_by_tier_total{tier=peer}` going flat while pool reads climb — reads still
succeed, so nothing pages. So this file builds NO headers of its own: the real
`PeerChunkFetcher` calls the really-mounted router, and the only way it can succeed is if
both sides derive the header from the same constant.
"""

from __future__ import annotations

import pytest
from fastapi import FastAPI
from httpx import ASGITransport
from httpx import AsyncClient

from hippius_s3.api.internal_parts import router
from hippius_s3.cache.dual_fs_store import DualFileSystemPartsStore
from hippius_s3.cache.peers import PeerChunkFetcher
from hippius_s3.cache.peers import PeerRegistry
from tests.unit.test_peer_fetch import FakePool
from tests.unit.test_peer_fetch import FakeRedis
from tests.unit.test_peer_fetch import residency_row


OBJ = "466916c0-d61b-4518-b81b-9576b574270a"
PEER_URL = "http://10.42.2.9:8000"


def _serving_app(fs_store: object, secret: str) -> FastAPI:
    app = FastAPI()
    app.include_router(router)
    app.state.fs_store = fs_store
    app.state.peer_auth_secret = secret
    return app


async def _fetcher_against(app: FastAPI, secret: str) -> tuple[PeerChunkFetcher, AsyncClient]:
    """A fetcher on node-a whose HTTP client speaks straight into the peer's ASGI app."""
    redis = FakeRedis()
    await PeerRegistry(redis, "node-b", PEER_URL, 90).register()
    registry = PeerRegistry(redis, "node-a", "http://10.42.1.5:8000", 90)
    client = AsyncClient(transport=ASGITransport(app=app))
    fetcher = PeerChunkFetcher(
        # The resolver now returns the part's per-chunk ciphertext sizes alongside the owner, and
        # the fetcher rejects a body that is not exactly the expected length — so this row has to
        # declare 10 bytes to match the `b"peer-bytes"` the peer serves. A bare {"node_id": ...}
        # resolves an owner and then fails every fetch on an unverifiable size.
        FakePool(residency_row("node-b", chunk_size=10)),
        registry,
        "node-a",
        client,
        auth_secret=secret,
    )
    return fetcher, client


@pytest.mark.asyncio
async def test_a_fetcher_and_a_peer_sharing_a_secret_exchange_the_chunk(tmp_path) -> None:
    secret = "the-one-configured-secret"
    store = DualFileSystemPartsStore(str(tmp_path / "ssd"), str(tmp_path / "pool"))
    await store.set_chunk(OBJ, 1, 3, 2, b"peer-bytes")
    await store.set_meta(OBJ, 1, 3, chunk_size=10, num_chunks=1, size_bytes=10)

    fetcher, client = await _fetcher_against(_serving_app(store, secret), secret)
    async with client:
        assert await fetcher(OBJ, 1, 3, 2) == b"peer-bytes"


@pytest.mark.asyncio
async def test_a_fetcher_with_the_wrong_secret_falls_through_to_the_pool(tmp_path) -> None:
    """Mismatched secrets must read as an ordinary miss, not an error the read path handles."""
    store = DualFileSystemPartsStore(str(tmp_path / "ssd"), str(tmp_path / "pool"))
    await store.set_chunk(OBJ, 1, 3, 2, b"peer-bytes")
    await store.set_meta(OBJ, 1, 3, chunk_size=10, num_chunks=1, size_bytes=10)

    fetcher, client = await _fetcher_against(_serving_app(store, "what-the-peer-expects"), "what-this-pod-sends")
    async with client:
        assert await fetcher(OBJ, 1, 3, 2) is None
