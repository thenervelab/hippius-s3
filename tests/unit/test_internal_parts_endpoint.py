"""The peer-fetch endpoint: serve one chunk from THIS node's flash, or 404.

It is the server side of the read path's peer tier. Two safety properties are under test.

It never reaches past its own local tier — not to the CephFS pool, not to another peer —
because a peer that could do either would put a network hop in front of the pool read the
tier exists to avoid, and two nodes resolving each other could bounce a request between
them instead of either just reading the pool.

And it answers nobody who cannot present the shared secret. The route is mounted on the same
app the gateway proxies arbitrary paths into, so "only pods can reach it" was never true.
Every case below therefore passes the header: the secret is incidental to what each test
means, but a request without it is not a request this endpoint ever sees the body of.
"""

from __future__ import annotations

import asyncio

import pytest
from fastapi import FastAPI
from httpx import ASGITransport
from httpx import AsyncClient

from hippius_s3.api.internal_parts import router
from hippius_s3.cache.dual_fs_store import DualFileSystemPartsStore
from hippius_s3.peer_auth import PEER_AUTH_HEADER


OBJ = "466916c0-d61b-4518-b81b-9576b574270a"
SECRET = "peer-secret-under-test"
AUTH = {PEER_AUTH_HEADER: SECRET}


def _app(fs_store: object | None) -> FastAPI:
    app = FastAPI()
    app.include_router(router)
    app.state.fs_store = fs_store
    return app


def _app_with_secret(fs_store: object | None, secret: str) -> FastAPI:
    app = _app(fs_store)
    app.state.peer_auth_secret = secret
    return app


async def _client(app: FastAPI) -> AsyncClient:
    return AsyncClient(transport=ASGITransport(app=app), base_url="http://peer")


async def _write_part(store: object, *, part_number: int, chunk: bytes) -> None:
    await store.set_chunk(OBJ, 1, part_number, 0, chunk)  # type: ignore[attr-defined]
    await store.set_meta(OBJ, 1, part_number, chunk_size=len(chunk), num_chunks=1, size_bytes=len(chunk))  # type: ignore[attr-defined]


@pytest.mark.asyncio
async def test_a_locally_held_chunk_is_served(tmp_path) -> None:
    store = DualFileSystemPartsStore(str(tmp_path / "ssd"), str(tmp_path / "pool"))
    await _write_part(store, part_number=1, chunk=b"local-bytes")

    async with await _client(_app_with_secret(store, SECRET)) as client:
        response = await client.get(f"/internal/parts/{OBJ}/1/1/chunks/0", headers=AUTH)

    assert response.status_code == 200
    assert response.content == b"local-bytes"


@pytest.mark.asyncio
async def test_a_pool_only_chunk_is_a_404_not_a_pool_read(tmp_path) -> None:
    """The whole point of the tier: a peer answers from flash or not at all."""
    store = DualFileSystemPartsStore(str(tmp_path / "ssd"), str(tmp_path / "pool"))
    await _write_part(store.fallback, part_number=1, chunk=b"pool-bytes")

    async with await _client(_app_with_secret(store, SECRET)) as client:
        response = await client.get(f"/internal/parts/{OBJ}/1/1/chunks/0", headers=AUTH)

    assert response.status_code == 404, "the peer must not proxy the pool copy"


@pytest.mark.asyncio
async def test_an_evicted_chunk_is_a_routine_404(tmp_path) -> None:
    """The caller resolved this node from the residency table, then the evictor unlinked it.

    That race is expected under disk pressure, so it is an ordinary 404 the caller absorbs
    by reading the pool — not an error either side needs to reason about.
    """
    store = DualFileSystemPartsStore(str(tmp_path / "ssd"), str(tmp_path / "pool"))

    async with await _client(_app_with_secret(store, SECRET)) as client:
        response = await client.get(f"/internal/parts/{OBJ}/1/1/chunks/0", headers=AUTH)

    assert response.status_code == 404


@pytest.mark.asyncio
async def test_a_malformed_object_id_is_rejected_without_touching_the_filesystem(tmp_path) -> None:
    """Path traversal guard: object ids are UUIDs and the store validates them.

    This endpoint takes an id straight off the URL, so a non-UUID must fail closed rather
    than resolve into a path outside the cache root.
    """
    store = DualFileSystemPartsStore(str(tmp_path / "ssd"), str(tmp_path / "pool"))

    async with await _client(_app_with_secret(store, SECRET)) as client:
        response = await client.get("/internal/parts/..%2F..%2Fetc/1/1/chunks/0", headers=AUTH)

    assert response.status_code == 404


@pytest.mark.asyncio
async def test_a_store_without_a_local_read_is_a_404() -> None:
    """A process whose store predates the peer tier must refuse rather than crash."""

    class _Legacy:
        pass

    async with await _client(_app_with_secret(_Legacy(), SECRET)) as client:
        response = await client.get(f"/internal/parts/{OBJ}/1/1/chunks/0", headers=AUTH)

    assert response.status_code == 404


@pytest.mark.asyncio
async def test_the_peer_endpoint_sheds_over_its_in_flight_cap(tmp_path) -> None:
    """Serving peers must never crowd out this node's own ingest.

    This pod runs the api for its own clients too. A part that is hot and resident only here
    would otherwise draw every other node's fetches onto the same uvicorn as its PUTs. 503 is
    correct rather than queueing: the caller treats any non-200 as "read the pool", so
    shedding costs it one fallback, while queueing would add this pod's saturation on top of
    that pool read anyway.
    """
    store = DualFileSystemPartsStore(str(tmp_path / "ssd"), str(tmp_path / "pool"))
    await _write_part(store, part_number=1, chunk=b"local-bytes")

    app = _app_with_secret(store, SECRET)
    app.state.peer_serve_limiter = asyncio.Semaphore(1)
    await app.state.peer_serve_limiter.acquire()  # the one slot is already taken

    async with await _client(app) as client:
        response = await client.get(f"/internal/parts/{OBJ}/1/1/chunks/0", headers=AUTH)

    assert response.status_code == 503, "shed rather than queue behind a saturated pod"


@pytest.mark.asyncio
async def test_the_peer_endpoint_serves_normally_under_its_cap(tmp_path) -> None:
    store = DualFileSystemPartsStore(str(tmp_path / "ssd"), str(tmp_path / "pool"))
    await _write_part(store, part_number=1, chunk=b"local-bytes")

    app = _app_with_secret(store, SECRET)
    app.state.peer_serve_limiter = asyncio.Semaphore(4)

    async with await _client(app) as client:
        response = await client.get(f"/internal/parts/{OBJ}/1/1/chunks/0", headers=AUTH)

    assert response.status_code == 200
    assert response.content == b"local-bytes"


# ------------------------------------------------------------------------------- peer auth


@pytest.mark.asyncio
async def test_an_unauthenticated_request_is_refused_and_gets_no_bytes(tmp_path) -> None:
    """The case that made this endpoint an internet-facing oracle.

    404 rather than 401/403 on purpose: a 403 would confirm both that the route exists and
    that the caller named a real (object, version, part), which is most of what the oracle
    was worth. An unauthenticated caller must not be able to tell a wrong secret from a
    chunk this node does not hold.
    """
    store = DualFileSystemPartsStore(str(tmp_path / "ssd"), str(tmp_path / "pool"))
    await _write_part(store, part_number=1, chunk=b"local-bytes")

    async with await _client(_app_with_secret(store, SECRET)) as client:
        response = await client.get(f"/internal/parts/{OBJ}/1/1/chunks/0")

    assert response.status_code == 404
    assert response.content != b"local-bytes"


@pytest.mark.asyncio
async def test_a_wrong_secret_is_refused(tmp_path) -> None:
    store = DualFileSystemPartsStore(str(tmp_path / "ssd"), str(tmp_path / "pool"))
    await _write_part(store, part_number=1, chunk=b"local-bytes")

    async with await _client(_app_with_secret(store, SECRET)) as client:
        response = await client.get(
            f"/internal/parts/{OBJ}/1/1/chunks/0",
            headers={PEER_AUTH_HEADER: SECRET + "x"},
        )

    assert response.status_code == 404
    assert response.content != b"local-bytes"


@pytest.mark.asyncio
async def test_the_configured_secret_is_served(tmp_path) -> None:
    store = DualFileSystemPartsStore(str(tmp_path / "ssd"), str(tmp_path / "pool"))
    await _write_part(store, part_number=1, chunk=b"local-bytes")

    async with await _client(_app_with_secret(store, SECRET)) as client:
        response = await client.get(f"/internal/parts/{OBJ}/1/1/chunks/0", headers=AUTH)

    assert response.status_code == 200
    assert response.content == b"local-bytes"


@pytest.mark.asyncio
async def test_an_unset_secret_refuses_everyone_rather_than_disabling_the_check(tmp_path) -> None:
    """"No secret configured" must never mean "no authentication required".

    That degradation is how a fail-closed handshake becomes a fail-open one during a bad
    rollout: the config lands empty, the check silently stops applying, and the endpoint is
    back to the state this whole change exists to leave. An empty secret must not even match
    an empty presented header.
    """
    store = DualFileSystemPartsStore(str(tmp_path / "ssd"), str(tmp_path / "pool"))
    await _write_part(store, part_number=1, chunk=b"local-bytes")

    app = _app_with_secret(store, "")
    async with await _client(app) as client:
        no_header = await client.get(f"/internal/parts/{OBJ}/1/1/chunks/0")
        empty_header = await client.get(f"/internal/parts/{OBJ}/1/1/chunks/0", headers={PEER_AUTH_HEADER: ""})
        any_header = await client.get(f"/internal/parts/{OBJ}/1/1/chunks/0", headers=AUTH)

    assert [r.status_code for r in (no_header, empty_header, any_header)] == [404, 404, 404]
