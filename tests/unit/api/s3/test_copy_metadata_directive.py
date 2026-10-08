"""CopyObject MetadataDirective=REPLACE must store the request's Content-Type and user metadata.

The shipped endpoint is the entry point. Collaborators that touch the database, the
source byte stream, and the address write are stubbed. The metadata choice and
put_simple_stream_full's arguments are the real copy path.
"""

from __future__ import annotations

from types import SimpleNamespace
from typing import Any

import pytest
from starlette.datastructures import Headers

from hippius_s3.api.s3 import copy_helpers
from hippius_s3.api.s3.objects import copy_object_endpoint as mod
from hippius_s3.writer.types import PutResult
from tests.unit._fake_pool import make_fake_pool


BUCKET_ID = "11111111-1111-1111-1111-111111111111"
SRC_ID = "22222222-2222-2222-2222-222222222222"
OTHER_ID = "33333333-3333-3333-3333-333333333333"
BUCKET = "b"
SRC_KEY = "page.html"
SOURCE_TYPE = "application/octet-stream"
SOURCE_META = {"original": "keep-me", "role": "source"}
REQUEST_TYPE = "text/html; charset=utf-8"
REQUEST_META = {"replaced": "yes", "role": "dest"}


def _source() -> dict[str, Any]:
    return {
        "object_id": SRC_ID,
        "bucket_id": BUCKET_ID,
        "object_key": SRC_KEY,
        "md5_hash": "78415af0c3864e4bb17651b120a14ec8",
        "storage_version": 5,
        "multipart": False,
        "object_version": 1,
        "metadata": dict(SOURCE_META),
        "content_type": SOURCE_TYPE,
        "enc_chunk_size_bytes": None,
        "enc_suite_id": None,
        "encryption_version": None,
        "kek_id": None,
        "wrapped_dek": None,
        "is_delete_marker": False,
        "size_bytes": 40,
    }


def _request(
    dest_key: str,
    *,
    directive: str | None,
    content_type: str | None,
    metadata: dict[str, str] | None,
) -> Any:
    raw: dict[str, str] = {"x-amz-copy-source": f"/{BUCKET}/{SRC_KEY}"}
    if directive is not None:
        raw["x-amz-metadata-directive"] = directive
    if content_type is not None:
        raw["content-type"] = content_type
    for key, value in (metadata or {}).items():
        raw[f"x-amz-meta-{key}"] = value
    return SimpleNamespace(
        headers=Headers(raw),
        state=SimpleNamespace(main_account_id="acct-main", ray_id="ray-1"),
        app=SimpleNamespace(state=SimpleNamespace(fs_store=SimpleNamespace(), obj_cache=SimpleNamespace())),
    )


def _router(live: dict[str, str] | None) -> Any:
    def route(method: str, query: str | None, _args: tuple) -> Any:
        if method != "fetchrow" or not query:
            return None
        if "INSERT INTO object_names" in query:
            return {"object_id": SRC_ID}
        if "FROM objects" in query:
            return live
        return None

    return route


async def _drive(
    monkeypatch: pytest.MonkeyPatch,
    *,
    dest_key: str,
    directive: str | None,
    content_type: str | None,
    metadata: dict[str, str] | None,
    live: dict[str, str] | None,
    existing_object_id: str | None,
    resolved_object_key: str | None = None,
) -> tuple[Any, list[dict[str, Any]]]:
    """Run the real CopyObject endpoint. `live` is what the alias lookup would see."""
    source = _source()
    bucket = {"bucket_id": BUCKET_ID, "bucket_name": BUCKET, "is_public": False, "main_account_id": "acct-main"}
    puts: list[dict[str, Any]] = []

    async def _resolve(**_kw: Any) -> Any:
        return ({"main_account_id": "acct-main"}, bucket, bucket, source)

    class _Repo:
        def __init__(self, _db: Any) -> None:
            return None

        async def get_by_path(self, _bucket_id: str, key: str) -> Any:
            if existing_object_id is None:
                return None
            # Real get_by_path returns the primary's object_key, which is not the alias name.
            return {"object_id": existing_object_id, "object_key": resolved_object_key or key}

    async def _stream(*_a: Any, **_k: Any) -> Any:
        async def _gen() -> Any:
            yield b"<html>hi</html>"

        return _gen()

    async def _put(_self: Any, **kwargs: Any) -> PutResult:
        puts.append(kwargs)
        return PutResult(
            object_id=str(kwargs.get("object_id")),
            etag="dest-etag",
            size_bytes=15,
            upload_id="44444444-4444-4444-4444-444444444444",
            object_version=2,
        )

    async def _address(*_a: Any, **_k: Any) -> None:
        return None

    monkeypatch.setattr(mod, "resolve_copy_resources", _resolve)
    monkeypatch.setattr(mod, "ObjectRepository", _Repo)
    monkeypatch.setattr(copy_helpers, "stream_object", _stream)
    monkeypatch.setattr(copy_helpers.ObjectWriter, "put_simple_stream_full", _put)
    monkeypatch.setattr(copy_helpers, "set_object_version_address", _address)

    response = await mod.handle_copy_object(
        BUCKET,
        dest_key,
        _request(dest_key, directive=directive, content_type=content_type, metadata=metadata),
        make_fake_pool(_router(live)),
        SimpleNamespace(),
    )
    return response, puts


@pytest.mark.asyncio
async def test_inplace_replace_writes_request_content_type_and_metadata(monkeypatch: pytest.MonkeyPatch) -> None:
    response, puts = await _drive(
        monkeypatch,
        dest_key=SRC_KEY,
        directive="REPLACE",
        content_type=REQUEST_TYPE,
        metadata=REQUEST_META,
        live={"object_id": SRC_ID},
        existing_object_id=SRC_ID,
    )

    assert response.status_code == 200
    assert len(puts) == 1
    written = puts[0]
    assert written["object_key"] == SRC_KEY
    assert written["object_id"] == SRC_ID
    assert written["content_type"] == REQUEST_TYPE
    assert written["metadata"] == REQUEST_META


@pytest.mark.asyncio
async def test_different_key_replace_writes_request_metadata_not_source(monkeypatch: pytest.MonkeyPatch) -> None:
    response, puts = await _drive(
        monkeypatch,
        dest_key="other.html",
        directive="REPLACE",
        content_type=REQUEST_TYPE,
        metadata=REQUEST_META,
        live=None,
        existing_object_id=None,
    )

    assert response.status_code == 200
    assert len(puts) == 1
    written = puts[0]
    assert written["object_key"] == "other.html"
    assert written["content_type"] == REQUEST_TYPE
    assert written["metadata"] == REQUEST_META
    assert written["metadata"] != SOURCE_META
    assert "original" not in written["metadata"]


@pytest.mark.asyncio
async def test_replace_onto_an_alias_does_not_version_the_source(monkeypatch: pytest.MonkeyPatch) -> None:
    """get_by_path follows the alias back to the source object. REPLACE must not version that id."""
    _response, puts = await _drive(
        monkeypatch,
        dest_key="other.html",
        directive="REPLACE",
        content_type=REQUEST_TYPE,
        metadata=REQUEST_META,
        live={"object_id": SRC_ID},
        existing_object_id=SRC_ID,
        resolved_object_key=SRC_KEY,
    )

    assert len(puts) == 1
    assert puts[0]["object_key"] == "other.html"
    assert puts[0]["object_id"] != SRC_ID
    assert puts[0]["content_type"] == REQUEST_TYPE
    assert puts[0]["metadata"] == REQUEST_META


@pytest.mark.asyncio
async def test_replace_without_content_type_uses_put_default(monkeypatch: pytest.MonkeyPatch) -> None:
    _response, puts = await _drive(
        monkeypatch,
        dest_key=SRC_KEY,
        directive="REPLACE",
        content_type=None,
        metadata={"role": "dest"},
        live={"object_id": SRC_ID},
        existing_object_id=SRC_ID,
    )

    assert len(puts) == 1
    assert puts[0]["content_type"] == "application/octet-stream"
    assert puts[0]["metadata"] == {"role": "dest"}


@pytest.mark.asyncio
@pytest.mark.parametrize("directive", ["COPY", None])
async def test_inplace_copy_does_not_apply_request_metadata(
    monkeypatch: pytest.MonkeyPatch, directive: str | None
) -> None:
    response, puts = await _drive(
        monkeypatch,
        dest_key=SRC_KEY,
        directive=directive,
        content_type=REQUEST_TYPE,
        metadata=REQUEST_META,
        live={"object_id": SRC_ID},
        existing_object_id=SRC_ID,
    )

    assert response.status_code == 200
    assert puts == []


@pytest.mark.asyncio
async def test_different_key_copy_still_aliases(monkeypatch: pytest.MonkeyPatch) -> None:
    response, puts = await _drive(
        monkeypatch,
        dest_key="other.html",
        directive="COPY",
        content_type=REQUEST_TYPE,
        metadata=REQUEST_META,
        live=None,
        existing_object_id=None,
    )

    assert response.status_code == 200
    assert puts == []


@pytest.mark.asyncio
@pytest.mark.parametrize("directive", ["COPY", None])
async def test_streaming_copy_keeps_source_metadata(
    monkeypatch: pytest.MonkeyPatch, directive: str | None
) -> None:
    """Dest is already a different object, so the copy byte-writes. Request headers stay unused."""
    response, puts = await _drive(
        monkeypatch,
        dest_key="other.html",
        directive=directive,
        content_type=REQUEST_TYPE,
        metadata=REQUEST_META,
        live={"object_id": OTHER_ID},
        existing_object_id=OTHER_ID,
    )

    assert response.status_code == 200
    assert len(puts) == 1
    assert puts[0]["content_type"] == SOURCE_TYPE
    assert puts[0]["metadata"] == SOURCE_META
    assert puts[0]["object_key"] == "other.html"
