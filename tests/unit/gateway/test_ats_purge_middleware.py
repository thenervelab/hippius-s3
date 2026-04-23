"""Tests for ats_purge_middleware — checks that PURGE calls are dispatched correctly per HTTP verb."""

from typing import Any

import pytest
from fastapi import FastAPI
from fastapi import Request
from fastapi import Response
from httpx import ASGITransport
from httpx import AsyncClient

from gateway.middlewares.ats_purge import ats_purge_middleware


@pytest.fixture  # type: ignore[misc]
def captured_purges(monkeypatch: pytest.MonkeyPatch) -> list[tuple[str, str]]:
    calls: list[tuple[str, str]] = []

    def fake_schedule_purge(host: str, key: str) -> None:
        calls.append((host, key))

    monkeypatch.setattr("gateway.middlewares.ats_purge.schedule_purge", fake_schedule_purge)
    return calls


@pytest.fixture  # type: ignore[misc]
def app(captured_purges: list[tuple[str, str]]) -> Any:
    app = FastAPI()
    app.middleware("http")(ats_purge_middleware)

    @app.api_route("/{path:path}", methods=["GET", "POST", "PUT", "DELETE", "HEAD", "PATCH"])
    async def catch_all(request: Request) -> Response:
        status = int(request.headers.get("x-test-status", "200"))
        return Response(status_code=status, content=b"ok")

    return app


@pytest.mark.asyncio
async def test_put_object_purges_key(app: Any, captured_purges: list[tuple[str, str]]) -> None:
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://s3.hippius.com") as client:
        r = await client.put("/mybucket/my/key.bin", content=b"data")
    assert r.status_code == 200
    assert captured_purges == [("s3.hippius.com", "mybucket/my/key.bin")]


@pytest.mark.asyncio
async def test_delete_object_purges_key(app: Any, captured_purges: list[tuple[str, str]]) -> None:
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://s3.hippius.com") as client:
        r = await client.delete("/mybucket/foo.txt")
    assert r.status_code == 200
    assert captured_purges == [("s3.hippius.com", "mybucket/foo.txt")]


@pytest.mark.asyncio
async def test_copy_object_purges_both_source_and_destination(app: Any, captured_purges: list[tuple[str, str]]) -> None:
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://s3.hippius.com") as client:
        r = await client.put(
            "/destbucket/dest/key",
            content=b"",
            headers={"x-amz-copy-source": "/srcbucket/src/key"},
        )
    assert r.status_code == 200
    assert ("s3.hippius.com", "destbucket/dest/key") in captured_purges
    assert ("s3.hippius.com", "srcbucket/src/key") in captured_purges
    assert len(captured_purges) == 2


@pytest.mark.asyncio
async def test_complete_multipart_upload_purges_key(app: Any, captured_purges: list[tuple[str, str]]) -> None:
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://s3.hippius.com") as client:
        r = await client.post("/mybucket/big/obj?uploadId=abc123", content=b"<CompleteMultipartUpload/>")
    assert r.status_code == 200
    assert captured_purges == [("s3.hippius.com", "mybucket/big/obj")]


@pytest.mark.asyncio
async def test_part_upload_does_not_purge(app: Any, captured_purges: list[tuple[str, str]]) -> None:
    """UploadPart (PUT with uploadId + partNumber) is invisible until CompleteMultipartUpload."""
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://s3.hippius.com") as client:
        r = await client.put("/mybucket/big/obj?uploadId=abc&partNumber=1", content=b"part-data")
    assert r.status_code == 200
    assert captured_purges == []


@pytest.mark.asyncio
async def test_post_with_partNumber_does_not_purge(app: Any, captured_purges: list[tuple[str, str]]) -> None:
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://s3.hippius.com") as client:
        r = await client.post("/mybucket/big/obj?uploadId=abc&partNumber=2", content=b"")
    assert r.status_code == 200
    assert captured_purges == []


@pytest.mark.asyncio
async def test_batch_delete_does_not_purge_in_v1(app: Any, captured_purges: list[tuple[str, str]]) -> None:
    """`POST /{bucket}?delete` is deliberately skipped in v1 — see plan."""
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://s3.hippius.com") as client:
        r = await client.post("/mybucket?delete", content=b"<Delete/>")
    assert r.status_code == 200
    assert captured_purges == []


@pytest.mark.asyncio
async def test_bucket_acl_flip_purges_wildcard(app: Any, captured_purges: list[tuple[str, str]]) -> None:
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://s3.hippius.com") as client:
        r = await client.put("/mybucket?acl", content=b"<AccessControlPolicy/>")
    assert r.status_code == 200
    assert captured_purges == [("s3.hippius.com", "mybucket/*")]


@pytest.mark.asyncio
async def test_bucket_delete_purges_wildcard(app: Any, captured_purges: list[tuple[str, str]]) -> None:
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://s3.hippius.com") as client:
        r = await client.delete("/mybucket")
    assert r.status_code == 200
    assert captured_purges == [("s3.hippius.com", "mybucket/*")]


@pytest.mark.asyncio
async def test_failed_write_does_not_purge(app: Any, captured_purges: list[tuple[str, str]]) -> None:
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://s3.hippius.com") as client:
        r = await client.put("/mybucket/k", content=b"x", headers={"x-test-status": "500"})
    assert r.status_code == 500
    assert captured_purges == []


@pytest.mark.asyncio
async def test_client_error_does_not_purge(app: Any, captured_purges: list[tuple[str, str]]) -> None:
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://s3.hippius.com") as client:
        r = await client.delete("/mybucket/k", headers={"x-test-status": "403"})
    assert r.status_code == 403
    assert captured_purges == []


@pytest.mark.asyncio
async def test_get_does_not_purge(app: Any, captured_purges: list[tuple[str, str]]) -> None:
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://s3.hippius.com") as client:
        r = await client.get("/mybucket/k")
    assert r.status_code == 200
    assert captured_purges == []


@pytest.mark.asyncio
async def test_head_does_not_purge(app: Any, captured_purges: list[tuple[str, str]]) -> None:
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://s3.hippius.com") as client:
        r = await client.head("/mybucket/k")
    assert r.status_code == 200
    assert captured_purges == []


@pytest.mark.asyncio
async def test_root_path_does_not_purge(app: Any, captured_purges: list[tuple[str, str]]) -> None:
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://s3.hippius.com") as client:
        r = await client.put("/", content=b"")
    assert r.status_code == 200
    assert captured_purges == []


@pytest.mark.asyncio
async def test_host_header_propagated(app: Any, captured_purges: list[tuple[str, str]]) -> None:
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://s3-staging.hippius.com") as client:
        r = await client.put("/mybucket/k", content=b"x")
    assert r.status_code == 200
    assert captured_purges == [("s3-staging.hippius.com", "mybucket/k")]
