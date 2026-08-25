"""The copy SOURCE must be authorised, and judged by the same parser the handlers use.

`x-amz-copy-source` names a bucket that appears nowhere in the request path, so every
permission check derived from the path describes the destination only. Two independent things
have to hold, and each is a separate class of bug:

  1. the source is authorised at all — otherwise a write grant on a bucket you control reads
     any object in any bucket;
  2. the bucket this middleware authorises is the bucket the handler will resolve — otherwise
     the check passes against one resource while another is read.

(2) is the same split-view class as the path-traversal bug, one layer up, and it is easy to
reintroduce: `copy_helpers.parse_copy_source` and the inline parse in `multipart.upload_part`
both percent-decode BEFORE splitting, so any parser here that decodes afterwards disagrees with
them on a header like `victim%2Fkey`.
"""

from typing import Any
from unittest.mock import AsyncMock
from urllib.parse import unquote

import pytest
from fastapi import FastAPI
from httpx import ASGITransport
from httpx import AsyncClient

from hippius_s3.gateway.middlewares.acl import acl_middleware
from hippius_s3.gateway.middlewares.acl import parse_copy_source
from hippius_s3.gateway.services.acl_service import BucketLookup
from hippius_s3.models.acl import Permission
from tests.unit.gateway._suspension_fakes import install_no_suspension_state


OWNER = "5EvT2ccmmY6t3q1U3PXwjzwFBjE2KzvWdC6mMsCvBbiBDs55"
ATTACKER = "5GrwvaEF5zXb26Fz9rcQpDWS57CtERHpNehXCPcNoHGKutQY"


def _handler_parse(header: str) -> str | None:
    """Reproduction of the handlers' own bucket derivation (multipart.py / copy_helpers.py)."""
    src = unquote(header.strip())
    src = src[1:] if src.startswith("/") else src
    return src.split("/", 1)[0] if "/" in src else None


def _make_app(buckets: dict[str, str], permit: dict[tuple[str, str | None], bool], caller: str) -> Any:
    """`buckets` maps name -> owner; `permit` maps (bucket, key) -> check_permission result."""
    service = AsyncMock()
    service.get_bucket_owner_and_id = AsyncMock(
        side_effect=lambda name: (
            BucketLookup(owner_id=buckets[name], bucket_id=f"{name}-id", is_cache_warm=False)
            if name in buckets
            else None
        )
    )
    service.check_permission = AsyncMock(side_effect=lambda **kw: permit.get((kw["bucket"], kw["key"]), False))

    app = FastAPI()
    app.state.acl_service = service
    install_no_suspension_state(app)

    @app.put("/{bucket}/{key:path}")
    async def put_object(bucket: str, key: str) -> dict[str, str]:
        return {"bucket": bucket, "key": key}

    async def _stamp(request: Any, call_next: Any) -> Any:
        request.state.account_id = caller
        return await call_next(request)

    app.middleware("http")(acl_middleware)
    app.middleware("http")(_stamp)
    return app, service


class TestSourceIsAuthorised:
    @pytest.mark.asyncio
    async def test_cross_account_source_is_denied(self) -> None:
        """The confirmed exploit: write to a bucket you own, read from one you do not."""
        app, service = _make_app(
            buckets={"attacker-bucket": ATTACKER, "victim-bucket": OWNER},
            permit={("attacker-bucket", "loot"): True},
            caller=ATTACKER,
        )
        async with AsyncClient(transport=ASGITransport(app=app), base_url="http://t") as c:
            r = await c.put("/attacker-bucket/loot", headers={"x-amz-copy-source": "/victim-bucket/secrets.csv"})

        assert r.status_code == 403
        assert any(
            call.kwargs.get("bucket") == "victim-bucket" and call.kwargs.get("permission") == Permission.READ
            for call in service.check_permission.await_args_list
        ), "the source bucket must have been checked for READ"

    @pytest.mark.asyncio
    async def test_permitted_source_is_allowed(self) -> None:
        app, _ = _make_app(
            buckets={"mine": OWNER, "also-mine": OWNER},
            permit={("mine", "dst"): True, ("also-mine", "src"): True},
            caller=OWNER,
        )
        async with AsyncClient(transport=ASGITransport(app=app), base_url="http://t") as c:
            r = await c.put("/mine/dst", headers={"x-amz-copy-source": "/also-mine/src"})
        assert r.status_code == 200

    @pytest.mark.asyncio
    async def test_destination_grant_alone_is_not_enough(self) -> None:
        """Write on the destination, no read on the source, same owner: still denied."""
        app, _ = _make_app(
            buckets={"dst": OWNER, "src": OWNER},
            permit={("dst", "k"): True},
            caller=OWNER,
        )
        async with AsyncClient(transport=ASGITransport(app=app), base_url="http://t") as c:
            r = await c.put("/dst/k", headers={"x-amz-copy-source": "/src/secret"})
        assert r.status_code == 403

    @pytest.mark.asyncio
    async def test_object_level_key_is_passed_to_the_permission_check(self) -> None:
        """The source KEY must reach check_permission, or object-scoped ACLs are skipped."""
        app, service = _make_app(
            buckets={"dst": OWNER, "src": OWNER},
            permit={("dst", "k"): True, ("src", "dir/secret.txt"): True},
            caller=OWNER,
        )
        async with AsyncClient(transport=ASGITransport(app=app), base_url="http://t") as c:
            r = await c.put("/dst/k", headers={"x-amz-copy-source": "/src/dir/secret.txt"})
        assert r.status_code == 200
        src_calls = [x for x in service.check_permission.await_args_list if x.kwargs.get("bucket") == "src"]
        assert src_calls and src_calls[0].kwargs["key"] == "dir/secret.txt"

    @pytest.mark.asyncio
    async def test_no_copy_source_header_is_untouched(self) -> None:
        """An ordinary PutObject must not pay for any of this."""
        app, service = _make_app(buckets={"dst": OWNER}, permit={("dst", "k"): True}, caller=OWNER)
        async with AsyncClient(transport=ASGITransport(app=app), base_url="http://t") as c:
            r = await c.put("/dst/k")
        assert r.status_code == 200
        assert all(x.kwargs.get("bucket") == "dst" for x in service.check_permission.await_args_list)

    @pytest.mark.asyncio
    async def test_nonexistent_source_falls_through_to_the_handler(self) -> None:
        """Preserves the handler's NoSuchBucket rather than masking it with a 403."""
        app, _ = _make_app(buckets={"dst": OWNER}, permit={("dst", "k"): True}, caller=OWNER)
        async with AsyncClient(transport=ASGITransport(app=app), base_url="http://t") as c:
            r = await c.put("/dst/k", headers={"x-amz-copy-source": "/ghost-bucket/key"})
        assert r.status_code == 200

    @pytest.mark.asyncio
    async def test_anonymous_cannot_copy_from_a_private_source(self) -> None:
        app, _ = _make_app(buckets={"dst": OWNER, "src": OWNER}, permit={("dst", "k"): True}, caller="anonymous")
        async with AsyncClient(transport=ASGITransport(app=app), base_url="http://t") as c:
            r = await c.put("/dst/k", headers={"x-amz-copy-source": "/src/secret"})
        assert r.status_code == 403


class TestParserAgreesWithTheHandlers:
    """Every spelling must yield the bucket the handler will actually resolve."""

    @pytest.mark.parametrize(
        "header",
        [
            "/victim/key",
            "victim/key",
            "/victim/key?versionId=1",
            "victim%2Fkey",
            "/victim%2Fkey",
            "%2Fvictim%2Fkey",
            "/victim/dir/sub/key.txt",
            "  /victim/key  ",
            "/victim/key%20with%20spaces",
        ],
    )
    def test_bucket_matches_handler_derivation(self, header: str) -> None:
        assert parse_copy_source(header)[0] == _handler_parse(header)

    @pytest.mark.parametrize("header", ["", "justbucket", "/", "//", "/onlybucket"])
    def test_unauthorisable_headers_yield_no_bucket(self, header: str) -> None:
        """No bucket parsed means the middleware cannot authorise it, so it must refuse."""
        assert parse_copy_source(header)[0] is None

    def test_encoded_slash_does_not_hide_the_real_bucket(self) -> None:
        """The specific bypass: decode-last reads `victim%2Fkey`, the handlers read `victim`."""
        assert parse_copy_source("victim%2Fkey") == ("victim", "key")

    def test_arn_form_is_not_special_cased(self) -> None:
        """Neither handler recognises ARNs, so neither may this — see the module docstring."""
        assert parse_copy_source("arn:aws:s3:::other/key")[0] == _handler_parse("arn:aws:s3:::other/key")


class TestEncodedSourceIsAuthorisedAsTheHandlerSeesIt:
    @pytest.mark.asyncio
    async def test_encoded_slash_source_is_denied(self) -> None:
        """`victim%2Fkey` must be checked as bucket `victim`, not waved through as unparseable."""
        app, service = _make_app(
            buckets={"attacker-bucket": ATTACKER, "victim": OWNER},
            permit={("attacker-bucket", "loot"): True},
            caller=ATTACKER,
        )
        async with AsyncClient(transport=ASGITransport(app=app), base_url="http://t") as c:
            r = await c.put("/attacker-bucket/loot", headers={"x-amz-copy-source": "victim%2Fkey"})

        assert r.status_code == 403
        assert any(x.kwargs.get("bucket") == "victim" for x in service.check_permission.await_args_list)
