"""A versioned CopyObject must copy bytes, never attach an alias.

Same-bucket CopyObject normally attaches a second S3 name to the source's `object_id` instead of
re-encrypting 64 MiB of ciphertext — the v5 AAD binds the id, so a real copy is expensive and that
optimisation is what makes Harbor's blob commit fast.

An alias, though, resolves through `objects.current_object_version`. It shows whatever the source
is NOW, so it cannot express `CopySource={"VersionId": N}`: the caller asked for an older version
and would receive the current one, and the "copy" would keep tracking the source afterwards, which
no S3 copy does. Both features are correct alone; only together do they produce silently wrong
bytes, which is exactly what these tests pin.
"""

from __future__ import annotations

from types import SimpleNamespace
from typing import Any

import pytest
from fastapi import Response

from hippius_s3.api.s3.objects import copy_object_endpoint as mod


BUCKET_ID = "11111111-1111-1111-1111-111111111111"


def _request(copy_source: str) -> Any:
    return SimpleNamespace(
        state=SimpleNamespace(main_account_id="acct-main", ray_id="ray-1"),
        headers={"x-amz-copy-source": copy_source, "Host": "h"},
        query_params={},
    )


@pytest.fixture
def wiring(monkeypatch: pytest.MonkeyPatch) -> dict[str, Any]:
    """Both copy strategies stubbed, so a test can see which one the endpoint chose."""
    calls: dict[str, list[Any]] = {"alias": [], "stream": []}

    bucket = {"bucket_id": BUCKET_ID, "bucket_name": "b", "main_account_id": "acct-main"}
    source_object = {
        "object_id": "22222222-2222-2222-2222-222222222222",
        "bucket_id": BUCKET_ID,
        "object_key": "src.txt",
        "md5_hash": "abc",
        "storage_version": 5,
        "multipart": False,
        "object_version": 1,
        "metadata": None,
        "enc_chunk_size_bytes": None,
        "is_delete_marker": False,
    }

    async def _resolve(**_kw: Any) -> Any:
        return ({"main_account_id": "acct-main"}, bucket, bucket, source_object)

    async def _alias(*_a: Any, **_kw: Any) -> Response:
        calls["alias"].append(True)
        return Response(status_code=200, content=b"<aliased/>", media_type="application/xml")

    async def _stream(*_a: Any, **_kw: Any) -> Response:
        calls["stream"].append(True)
        return Response(status_code=200, content=b"<streamed/>", media_type="application/xml")

    class _FakeObjectRepo:
        def __init__(self, _db: Any) -> None: ...

        async def get_by_path(self, _bucket_id: str, _key: str) -> Any:
            return None

    monkeypatch.setattr(mod, "resolve_copy_resources", _resolve)
    monkeypatch.setattr(mod, "handle_same_bucket_copy", _alias)
    monkeypatch.setattr(mod, "handle_streaming_copy", _stream)
    monkeypatch.setattr(mod, "ObjectRepository", _FakeObjectRepo)
    return calls


@pytest.mark.asyncio
async def test_unversioned_same_bucket_copy_still_aliases(wiring: dict[str, list[Any]]) -> None:
    """The Harbor fast path is untouched when no version is named."""
    resp = await mod.handle_copy_object("b", "dst.txt", _request("/b/src.txt"), None, None)

    assert resp.body == b"<aliased/>"
    assert wiring["alias"] and not wiring["stream"]


@pytest.mark.asyncio
async def test_versioned_same_bucket_copy_streams_instead(wiring: dict[str, list[Any]]) -> None:
    """THE guard: an alias would hand back the CURRENT version, not the one asked for."""
    resp = await mod.handle_copy_object("b", "dst.txt", _request("/b/src.txt?versionId=1"), None, None)

    assert resp.body == b"<streamed/>"
    assert wiring["stream"] and not wiring["alias"]


@pytest.mark.asyncio
async def test_version_null_is_treated_as_unversioned(wiring: dict[str, list[Any]]) -> None:
    """ "null" is AWS's id for a pre-versioning object, i.e. "current" — aliasing stays valid."""
    resp = await mod.handle_copy_object("b", "dst.txt", _request("/b/src.txt?versionId=null"), None, None)

    assert resp.body == b"<aliased/>"
    assert wiring["alias"] and not wiring["stream"]
