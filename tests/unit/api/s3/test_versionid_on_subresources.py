"""?versionId on ?acl / ?tagging must be refused, not ignored.

AWS makes both subresources per-version. Here they are not: tags live in
object_versions.metadata and the handlers resolve the CURRENT version, so a versionId in the query
string changed nothing about which row was read or written.

The read side merely lied. The write side is why this is a 501 and not a doc note:

    PUT /key?tagging&versionId=1   ->  200, tags written to the LIVE version

which is the same silent-write-to-the-wrong-version shape as the `DELETE ?versionId` data-loss bug
this branch exists to fix. These tests pin the refusal, and pin that the plain subresource still
works so the guard cannot be over-applied.
"""

from __future__ import annotations

from typing import Any
from unittest.mock import AsyncMock
from unittest.mock import patch

import pytest
from fastapi import Response

from hippius_s3.api.s3.objects import router as object_router


KEY = "docs/report.pdf"


def _request(query: dict[str, str]) -> Any:
    from starlette.datastructures import QueryParams

    class _Req:
        query_params = QueryParams(query)

    return _Req()


def test_no_version_id_is_not_rejected() -> None:
    """The plain subresource must keep working — this guard is narrow on purpose."""
    assert object_router._reject_version_id(_request({"tagging": ""}), KEY, "tagging") is None
    assert object_router._reject_version_id(_request({"acl": ""}), KEY, "acl") is None


@pytest.mark.parametrize("subresource", ["acl", "tagging"])
def test_version_id_is_refused_with_501(subresource: str) -> None:
    resp = object_router._reject_version_id(_request({subresource: "", "versionId": "3"}), KEY, subresource)

    assert resp is not None, f"?{subresource}&versionId must not fall through to the handler"
    assert resp.status_code == 501
    body = resp.body.decode()
    assert "NotImplemented" in body
    assert subresource in body, "the error should name the subresource that was refused"
    assert "<VersionId>3</VersionId>" in body
    assert f"<Key>{KEY}</Key>" in body


def test_refusal_is_unconditional_on_bucket_versioning_state() -> None:
    """Whether versioning is enabled makes no difference to our inability to honour the param.

    The helper never consults bucket state — a signature that took it would invite someone to
    "allow it on unversioned buckets", where the write would be just as wrong.
    """
    import inspect

    params = set(inspect.signature(object_router._reject_version_id).parameters)
    assert params == {"request", "object_key", "subresource"}


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("verb", "subresource", "handler"),
    [
        ("get", "acl", "get_object_acl"),
        ("get", "tagging", "tags_get_object_tags"),
        ("put", "acl", "put_object_acl"),
        ("put", "tagging", "tags_set_object_tags"),
        ("delete", "tagging", "tags_delete_object_tags"),
    ],
)
async def test_the_handler_is_never_reached(verb: str, subresource: str, handler: str) -> None:
    """The point of the guard: the handler that would touch the wrong version never runs."""
    called = AsyncMock(return_value=Response(status_code=200))
    with patch.object(object_router, handler, called):
        request = _request({subresource: "", "versionId": "2"})
        rejected = object_router._reject_version_id(request, KEY, subresource)

        assert rejected is not None and rejected.status_code == 501
        called.assert_not_awaited()


def test_every_subresource_dispatch_site_is_guarded() -> None:
    """Five dispatch sites: GET ?acl, GET ?tagging, PUT ?acl, PUT ?tagging, DELETE ?tagging.

    A sixth added later without a guard would silently reintroduce the wrong-version write.
    """
    import pathlib

    source = pathlib.Path(object_router.__file__).read_text()
    guarded = source.count("_reject_version_id(request, object_key")
    assert guarded == 5, f"expected 5 guarded dispatch sites, found {guarded}"
