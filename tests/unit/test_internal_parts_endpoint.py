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


async def _raw_get(app: FastAPI, path: bytes, *, auth: bytes | None = None) -> int | str:
    """Drive the ASGI app directly, so the request can carry bytes a client would not send.

    httpx ASCII-encodes header values and would raise before sending, so the whole class of
    "what does an adversarial byte do to the auth comparison" is unreachable through
    `_client`. A test written with the client passes vacuously. Returns the status, or a
    description of whatever escaped the app — an unhandled exception IS the failure here.
    """
    headers = [(b"host", b"peer")]
    if auth is not None:
        headers.append((PEER_AUTH_HEADER.lower().encode(), auth))
    scope = {
        "type": "http",
        "method": "GET",
        "http_version": "1.1",
        "path": path.decode("utf-8", "surrogateescape"),
        "raw_path": path,
        "root_path": "",
        "scheme": "http",
        "query_string": b"",
        "headers": headers,
        "client": ("10.42.1.5", 1234),
        "server": ("10.42.2.9", 8000),
    }
    sent: list[dict] = []

    async def receive() -> dict:
        return {"type": "http.request", "body": b"", "more_body": False}

    async def send(message: dict) -> None:
        sent.append(message)

    try:
        await app(scope, receive, send)
    except Exception as exc:  # noqa: BLE001 - an escaping exception is the thing under test
        return f"unhandled {type(exc).__name__}: {exc}"
    return int(sent[0]["status"])


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
    """ "No secret configured" must never mean "no authentication required".

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


# ------------------------------------------------- every refusal is the same 404, or it is an oracle
#
# The 404 is chosen over a 403 so a caller cannot tell "wrong secret" from "no such chunk". Any
# OTHER status this route can be made to emit before it authenticates undoes that: it does not
# leak which objects exist, but it does confirm the route is mounted, which is the first thing
# an attacker needs and the thing an unmounted route denies.


@pytest.mark.asyncio
async def test_a_non_ascii_secret_is_refused_as_a_miss_not_a_server_error(tmp_path) -> None:
    """A refusal must stay indistinguishable from a miss even for bytes no client should send.

    `hmac.compare_digest` supports only ASCII when handed `str`, and Starlette decodes header
    values as latin-1, so any byte >= 0x80 arrives as a non-ASCII `str` and raises TypeError.
    That surfaces as a 500 — which tells the caller this route is mounted, defeating the exact
    existence oracle the 404 above exists to deny, and burns an unhandled exception per request
    on the pod that is also serving ingest.

    The header is injected as RAW BYTES through the ASGI scope on purpose: httpx ascii-encodes
    header values and raises before the request is ever sent, so the same assertion written
    against the test client passes vacuously and proves nothing.
    """
    store = DualFileSystemPartsStore(str(tmp_path / "ssd"), str(tmp_path / "pool"))
    await _write_part(store, part_number=1, chunk=b"local-bytes")
    app = _app_with_secret(store, SECRET)
    path = f"/internal/parts/{OBJ}/1/1/chunks/0".encode()

    assert await _raw_get(app, path, auth=b"\xff" * 8) == 404
    # The whole latin-1 range, not just one byte, and a UTF-8 multi-byte sequence.
    assert await _raw_get(app, path, auth=bytes(range(128, 256))) == 404
    assert await _raw_get(app, path, auth="sécret".encode()) == 404
    # A correct secret still works, so the encoding fix did not break the comparison itself.
    assert await _raw_get(app, path, auth=SECRET.encode()) == 200


@pytest.mark.asyncio
async def test_a_malformed_path_is_refused_as_a_miss_not_a_validation_error(tmp_path) -> None:
    """Unparseable path segments must 404 like everything else, not 422.

    FastAPI validates `int` path params BEFORE the handler body, so declaring them as ints put
    a 422 in front of the auth check — announcing the route to an unauthenticated caller
    exactly as loudly as the 500 did. The segments are therefore taken as strings and parsed
    after authenticating.
    """
    store = DualFileSystemPartsStore(str(tmp_path / "ssd"), str(tmp_path / "pool"))
    await _write_part(store, part_number=1, chunk=b"local-bytes")
    app = _app_with_secret(store, SECRET)

    for path in (
        f"/internal/parts/{OBJ}/1/1/chunks/abc".encode(),
        f"/internal/parts/{OBJ}/1/zz/chunks/0".encode(),
        f"/internal/parts/{OBJ}/x/1/chunks/0".encode(),
        b"/internal/parts/\xff\xff/1/1/chunks/0",
    ):
        assert await _raw_get(app, path, auth=b"wrong-secret") == 404, path
        # Authenticating does not make a malformed path any more parseable.
        assert await _raw_get(app, path, auth=SECRET.encode()) == 404, path


@pytest.mark.asyncio
async def test_each_segment_addresses_the_part_it_names(tmp_path) -> None:
    """Taking the segments as strings must not stop them addressing the right chunk.

    Part 1 and part 2 hold different bytes, so this catches a handler that mixed up or
    hardcoded a segment — every other case in this file reads part 1 and would not notice.

    It does NOT pin the `int()` parse itself, and cannot: `FileSystemPartsStore` coerces its
    own arguments (`f"part_{int(part_number)}"`), so passing the raw strings through behaves
    identically. Verified by mutation — dropping the parse keeps this green. The parse earns
    its place by moving the failure to a 404 instead of FastAPI's 422, which is pinned by
    test_a_malformed_path_is_refused_as_a_miss_not_a_validation_error.
    """
    store = DualFileSystemPartsStore(str(tmp_path / "ssd"), str(tmp_path / "pool"))
    await _write_part(store, part_number=1, chunk=b"part-one")
    await _write_part(store, part_number=2, chunk=b"part-two")
    app = _app_with_secret(store, SECRET)

    async with await _client(app) as client:
        first = await client.get(f"/internal/parts/{OBJ}/1/1/chunks/0", headers=AUTH)
        second = await client.get(f"/internal/parts/{OBJ}/1/2/chunks/0", headers=AUTH)

    assert (first.content, second.content) == (b"part-one", b"part-two")
