"""CopyObject refuses an If-None-Match rather than ignoring it.

A copy is a write to the destination key, so `If-None-Match: *` carries the same create-only intent
PutObject honours. None of the three copy paths (alias, v5 fast path, streaming) goes through the
conditional reserve/finalize the PUT writer uses, so create-only is not implemented here — and
silently ignoring the header is exactly how a write-once client ends up with an overwritten object,
which is the bug this branch exists to fix. 501 for every value, before any copy work starts.
"""

from __future__ import annotations

from types import SimpleNamespace
from typing import Any

import pytest
from starlette.datastructures import Headers

from hippius_s3.api.s3.objects import copy_object_endpoint as mod


def _request(headers: dict[str, str]) -> Any:
    return SimpleNamespace(
        headers=Headers({"x-amz-copy-source": "/src-bkt/src-key", **headers}),
        state=SimpleNamespace(main_account_id="acct-main"),
    )


def _exploding_pool() -> Any:
    async def boom(*_a: Any, **_kw: Any) -> Any:
        raise AssertionError("the copy must be refused before any DB work")

    return SimpleNamespace(fetchrow=boom, fetch=boom, execute=boom, acquire=boom)


@pytest.mark.asyncio
@pytest.mark.parametrize("value", ["*", '"5d41402abc4b2a76b9719d911017c592"', ""])
async def test_if_none_match_on_a_copy_is_not_implemented(value: str) -> None:
    resp = await mod.handle_copy_object(
        "dst-bkt", "audit.log", _request({"If-None-Match": value}), _exploding_pool(), SimpleNamespace()
    )
    assert resp.status_code == 501
    assert b"<Code>NotImplemented</Code>" in resp.body
    assert b"<Header>If-None-Match</Header>" in resp.body


@pytest.mark.asyncio
async def test_a_copy_without_the_header_reaches_the_copy_machinery() -> None:
    # The negative control: with no header the guard must not fire, so the request gets far enough to
    # trip the sentinel pool. Without this, a guard that refused everything would still pass above.
    with pytest.raises(AssertionError, match="before any DB work"):
        await mod.handle_copy_object("dst-bkt", "audit.log", _request({}), _exploding_pool(), SimpleNamespace())
