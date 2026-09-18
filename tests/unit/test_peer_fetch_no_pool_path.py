"""Cross-node GET with no shared cache must peer-fetch, not 503.

Production/staging unmounted HIPPIUS_OBJECT_CACHE_FALLBACK_DIR. If create_fs_store
goes back to returning a bare FileSystemPartsStore, PeerChunkFetcher is discarded and
a GET on the non-ingest node waits 10s then SlowDown — the overnight smoke
IncompleteRead / Object-not-ready failures.

This file is the end-to-end pin of that path without a second docker node:
two node-local stores, empty fallback, real /internal/parts serve, real PeerChunkFetcher
over ASGI. It does not need docker_services.
"""

from __future__ import annotations

import pytest
from fastapi import FastAPI
from httpx import ASGITransport
from httpx import AsyncClient

from hippius_s3.api import internal_parts
from hippius_s3.cache import create_fs_store
from hippius_s3.cache.fs_store import FileSystemPartsStore
from hippius_s3.cache.peers import PEER_PORT
from hippius_s3.cache.peers import PeerChunkFetcher
from hippius_s3.cache.peers import PeerRegistry
from tests.unit.test_peer_fetch import FakePool
from tests.unit.test_peer_fetch import FakeRedis
from tests.unit.test_peer_fetch import sized_row


SECRET = "b" * 64
OBJ = "466916c0-d61b-4518-b81b-9576b574270a"
INGEST = "node-ingest"
READER = "node-reader"
PEER_URL = f"http://10.42.2.9:{PEER_PORT}"
SELF_URL = f"http://10.42.1.5:{PEER_PORT}"


class _NoPool:
    object_cache_dir = ""
    object_cache_fallback_dir = ""
    object_cache_promote_on_read = True


async def _write_part(store: FileSystemPartsStore, *, part: int, chunks: list[bytes]) -> None:
    for i, body in enumerate(chunks):
        await store.set_chunk(OBJ, 1, part, i, body)
    await store.set_meta(
        OBJ,
        1,
        part,
        chunk_size=len(chunks[0]),
        num_chunks=len(chunks),
        size_bytes=sum(len(c) for c in chunks),
    )


def _ingest_app(store: FileSystemPartsStore) -> FastAPI:
    app = FastAPI()
    app.include_router(internal_parts.router)
    app.state.fs_store = store
    app.state.peer_auth_secret = SECRET
    return app


async def _reader_store(tmp_path, ingest_app: FastAPI, chunks: list[bytes]):
    redis = FakeRedis()
    await PeerRegistry(redis, INGEST, PEER_URL, 90).register()
    registry = PeerRegistry(redis, READER, SELF_URL, 90)
    transport = ASGITransport(app=ingest_app)
    http = AsyncClient(transport=transport, base_url=PEER_URL)
    sizes = [len(c) for c in chunks]
    fetcher = PeerChunkFetcher(
        FakePool(sized_row(sizes, node_id=INGEST)),
        registry,
        READER,
        http,
        auth_secret=SECRET,
    )
    cfg = _NoPool()
    cfg.object_cache_dir = str(tmp_path / "reader-ssd")
    return create_fs_store(cfg, peer_fetch=fetcher), http


@pytest.mark.asyncio
async def test_reader_node_with_no_pool_serves_an_uploading_part_from_the_ingest_peer(tmp_path) -> None:
    """The production GET after unmount: local miss, no CephFS, peer has the only copy."""
    body = b"cipher-chunk-0----"
    ingest = FileSystemPartsStore(str(tmp_path / "ingest-ssd"))
    await _write_part(ingest, part=1, chunks=[body])
    reader, http = await _reader_store(tmp_path, _ingest_app(ingest), [body])
    try:
        got = await reader.get_chunk(OBJ, 1, 1, 0)
        assert got == body, (
            "no-pool Dual must peer-fetch; a bare FileSystemPartsStore returns None here "
            "and the streamer 503 SlowDowns after 10s"
        )
        assert await FileSystemPartsStore.get_chunk(reader, OBJ, 1, 1, 0) is None, (
            "the reader node must not already hold the bytes — that would skip the peer tier"
        )
    finally:
        await http.aclose()


@pytest.mark.asyncio
async def test_a_five_part_mpu_is_read_entirely_from_the_ingest_peer_with_no_pool(tmp_path) -> None:
    """Smoke MPU shape: 5 parts, GET on a node that ingested none of them.

    Overnight test_04 died after the first 5 MiB (one part) because later parts lived
    on other nodes. All five must come from the peer when fallback_dir is empty.
    """
    chunks = [f"part-{i}-bytes-xx".encode() for i in range(5)]
    ingest = FileSystemPartsStore(str(tmp_path / "ingest-ssd"))
    for part_number, chunk in enumerate(chunks, start=1):
        await _write_part(ingest, part=part_number, chunks=[chunk])

    redis = FakeRedis()
    await PeerRegistry(redis, INGEST, PEER_URL, 90).register()
    registry = PeerRegistry(redis, READER, SELF_URL, 90)
    transport = ASGITransport(app=_ingest_app(ingest))
    http = AsyncClient(transport=transport, base_url=PEER_URL)
    try:
        cfg = _NoPool()
        cfg.object_cache_dir = str(tmp_path / "reader-ssd")

        async def _fetch(object_id: str, version: int, part_number: int, chunk_index: int) -> bytes | None:
            pool = FakePool(sized_row([len(chunks[part_number - 1])], node_id=INGEST))
            fetcher = PeerChunkFetcher(pool, registry, READER, http, auth_secret=SECRET)
            return await fetcher(object_id, version, part_number, chunk_index)

        reader = create_fs_store(cfg, peer_fetch=_fetch)
        assembled = []
        for part_number, expected in enumerate(chunks, start=1):
            got = await reader.get_chunk(OBJ, 1, part_number, 0)
            assert got == expected, f"part {part_number} was not peer-fetched (got {got!r})"
            assembled.append(got)
        assert b"".join(assembled) == b"".join(chunks)
    finally:
        await http.aclose()


@pytest.mark.asyncio
async def test_dropping_peer_fetch_at_the_factory_makes_the_cross_node_read_miss(tmp_path) -> None:
    """The exact regression: empty FALLBACK_DIR and no peer_fetch → local miss → 503."""
    body = b"only-on-ingest-node"
    ingest = FileSystemPartsStore(str(tmp_path / "ingest-ssd"))
    await _write_part(ingest, part=1, chunks=[body])

    cfg = _NoPool()
    cfg.object_cache_dir = str(tmp_path / "reader-ssd")
    broken = create_fs_store(cfg)
    assert type(broken) is FileSystemPartsStore
    assert await broken.get_chunk(OBJ, 1, 1, 0) is None
