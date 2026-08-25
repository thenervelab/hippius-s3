"""Regression pin for the state.account split (commit b42246d6).

Chunks are stored under the bucket OWNER's Arion namespace. The download address
handle_get_object hands to read_response must therefore be the owner even when the
caller is a different account (a cross-account ACL grantee, or a signed caller reading
someone else's public bucket). Before the fix this read the un-rebound caller's
account.main_account, so a cross-account read missed the cache and fetched from the
caller's empty namespace -> 404 -> indefinite 503.
"""

from types import SimpleNamespace
from typing import Any

import pytest
from fastapi import Response

from hippius_s3.api.s3.objects import get_object_endpoint
from hippius_s3.models.account import HippiusAccount


OWNER = "5OwnerMainAccountSS58"
CALLER = "5CallerSubAccountSS58"


class _Conn:
    async def fetchrow(self, query: str, *args: Any) -> Any:
        if query == "get_or_create_user_by_main_account":
            return {"id": "user-1"}
        if query == "get_object_for_download_with_permissions":
            return {
                "object_id": "obj-1",
                "bucket_id": "bkt-1",
                "bucket_name": "someone-elses-bucket",
                "object_version": 1,
                "storage_version": 3,
                "size_bytes": 10,
                "multipart": False,
                "download_chunks": None,
                "content_type": "application/octet-stream",
                "created_at": "2026-01-01T00:00:00Z",
                "md5_hash": "d41d8cd98f00b204e9800998ecf8427e",
                "metadata": None,
                "bucket_owner_id": OWNER,
                "encryption_version": None,
                "enc_suite_id": None,
                "enc_chunk_size_bytes": None,
                "kek_id": None,
                "wrapped_dek": None,
            }
        return None

    async def fetchval(self, *_: Any) -> Any:
        return True


class _Pool:
    def __init__(self) -> None:
        self._conn = _Conn()

    async def acquire(self) -> _Conn:
        return self._conn

    async def release(self, _conn: Any) -> None:
        return None


def _request() -> Any:
    # The caller (state.account) differs from the bucket owner (state.main_account_id) —
    # exactly the cross-account grantee case request_context now models with two keys.
    return SimpleNamespace(
        state=SimpleNamespace(
            account=HippiusAccount(id=CALLER, main_account=CALLER, has_credits=True, upload=False, delete=False),
            main_account_id=OWNER,
            ray_id="ray-1",
        ),
        query_params={},
        headers={},
        app=SimpleNamespace(state=SimpleNamespace(redis_client=object(), obj_cache=object())),
    )


@pytest.mark.asyncio
async def test_cross_account_get_resolves_download_address_to_bucket_owner(monkeypatch: Any) -> None:
    monkeypatch.setattr(get_object_endpoint, "get_query", lambda name: name)
    monkeypatch.setattr(get_object_endpoint, "require_supported_storage_version", lambda v: v)

    captured: dict[str, Any] = {}

    async def _fake_read_response(**kwargs: Any) -> Response:
        captured["address"] = kwargs["address"]
        return Response(status_code=200)

    monkeypatch.setattr("hippius_s3.services.object_reader.read_response", _fake_read_response)

    response = await get_object_endpoint.handle_get_object(
        "someone-elses-bucket", "key.txt", _request(), _Pool(), redis_client=object()
    )

    assert response.status_code == 200
    # The owner's namespace, never the caller's — this is the whole point of the split.
    assert captured["address"] == OWNER
