"""A percent-encoded `?` or `#` in the path is refused rather than reinterpreted downstream.

`ForwardService` interpolates the decoded path into a URL *string*, which httpx re-parses. Both
characters are delimiters there, so a client that percent-encodes one controls where the api
thinks the path ends — and, for `?`, hands the forwarded request a query string that the gateway
itself never saw, because `request.query_params` is built from `scope["query_string"]`.

Layers that key off the query then judge a different operation from the one the api performs:
`acl.py`'s CreateBucket shape is `len(query_params) == 0`, and `get_required_permission` maps
subresources like `acl`/`tagging` to WRITE_ACP/READ_ACP. Refusing the character up front is
cheaper than teaching each layer to model the rewrite.

Nothing legitimate is lost: uvicorn has already split the real query at the first raw `?`, so a
literal one in `scope["path"]` can only have arrived percent-encoded, and such a key is silently
truncated today rather than stored intact.
"""

from __future__ import annotations

from typing import Any

import pytest
from fastapi import FastAPI
from fastapi import Request
from fastapi import Response
from httpx import ASGITransport
from httpx import AsyncClient

from hippius_s3.gateway.middlewares.input_validation import input_validation_middleware


def _app() -> tuple[FastAPI, list[str]]:
    """A gateway stand-in that records whatever gets past validation."""
    reached: list[str] = []
    app = FastAPI()
    app.middleware("http")(input_validation_middleware)

    @app.api_route("/{full_path:path}", methods=["GET", "PUT", "HEAD", "DELETE"])
    async def catch_all(full_path: str, request: Request) -> Response:  # pragma: no cover - trivial
        reached.append(request.scope["path"])
        return Response(status_code=200)

    return app, reached


async def _send(method: str, raw_path: str) -> tuple[int, list[str]]:
    app, reached = _app()
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://gw") as client:
        resp = await client.request(method, raw_path)
    return resp.status_code, reached


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "path",
    [
        "/victimbucket%3Fpolicy",
        "/victimbucket%3Ftagging",
        "/victimbucket%3Facl",
        "/victimbucket%23x",
    ],
)
async def test_a_bucket_only_path_with_a_delimiter_is_refused(path: str) -> None:
    """ONLY the path check covers these, which is why they are their own test.

    `OBJECT_KEY_AVOID_CHARS` runs only when a key segment exists (`len(key_parts) >= 2`), so a
    single-segment path was never judged by it at all. That is the privilege-confusion shape:
    `PUT /victimbucket%3Fpolicy` reaches the api as `PUT /victimbucket?policy` — PutBucketPolicy —
    while the gateway authorizes it as CreateBucket, whose branch performs no permission check.
    """
    status, reached = await _send("PUT", path)

    assert status == 400, f"{path} was accepted"
    assert reached == [], f"{path} reached the backend"


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "path",
    ["/bucket/key%3Facl", "/bucket/report%23v1.txt"],
)
async def test_a_key_with_a_delimiter_is_refused(path: str) -> None:
    """Two layers can refuse these, and the assertion is deliberately on the OUTCOME.

    Both the path check and `OBJECT_KEY_AVOID_CHARS` catch a key-segment case, so this test
    passes with either one removed — it does not isolate the new code, and is not meant to. What
    it pins is the property a client depends on: a key containing a delimiter is refused rather
    than silently truncated onto another object. The layers are pinned separately, by
    `test_a_bucket_only_path_with_a_delimiter_is_refused` above and
    `test_the_key_level_guard_still_lists_both_delimiters` below.
    """
    status, reached = await _send("PUT", path)

    assert status == 400
    assert reached == []


@pytest.mark.asyncio
async def test_two_keys_differing_only_after_the_delimiter_cannot_collide() -> None:
    """The data-loss shape: both forward as key `report`, so one silently overwrites the other.

    Also an outcome assertion covered by both layers — it survives removing either one alone. It
    earns its place by naming the user-visible consequence, not by isolating a layer.
    """
    first, reached_first = await _send("PUT", "/bucket/report%3Fv1.txt")
    second, reached_second = await _send("PUT", "/bucket/report%3Fv2.txt")

    assert (first, second) == (400, 400)
    assert reached_first == [] and reached_second == []


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "path",
    [
        "/bucket",
        "/bucket/key.txt",
        "/bucket/nested/key.txt",
        "/bucket/key%20with%20spaces.txt",
        "/bucket/key-with-dashes_and_underscores.txt",
        "/health",
    ],
)
async def test_ordinary_paths_are_untouched(path: str) -> None:
    """The refusal must not widen: normal keys, encoded spaces and gateway routes all pass."""
    status, reached = await _send("GET", path)

    assert status == 200, f"{path} was rejected"
    assert reached, f"{path} did not reach the backend"


@pytest.mark.asyncio
async def test_a_real_query_string_is_still_fine() -> None:
    """A genuine `?` is split off by the server into scope['query_string'] and never seen here."""
    status, reached = await _send("PUT", "/bucket?tagging")

    assert status == 200
    assert reached == ["/bucket"]


def test_the_key_level_guard_still_lists_both_delimiters() -> None:
    """Defence in depth, and pinned directly because nothing else pins it any more.

    The path-level check above preempts `OBJECT_KEY_AVOID_CHARS` for every request, so removing
    `#` and `?` from that list changes no observable behaviour and the whole suite stays green —
    verified. Before this change the sole thing holding `#` in the list was one assertion in
    test_input_validation_internal.py, which moved to the path check when its error code changed.

    The list is still the guard if the path check is ever narrowed, reordered behind the
    SKIP_PREFIXES bypass, or fed a `decoded_path` that has already been truncated (its
    `raw_path`-missing fallback returns `request.url.path`, which truncates). Asserting the
    constant is the only way to pin a layer that a stricter layer in front of it makes invisible.
    """
    from hippius_s3.gateway.middlewares.input_validation import OBJECT_KEY_AVOID_CHARS

    assert "#" in OBJECT_KEY_AVOID_CHARS
    assert "?" in OBJECT_KEY_AVOID_CHARS


@pytest.mark.asyncio
async def test_the_path_check_is_what_covers_a_bucket_only_path() -> None:
    """The genuinely new coverage, isolated from the key-level guard.

    `OBJECT_KEY_AVOID_CHARS` only runs when a key segment exists (`len(key_parts) >= 2`), so a
    single-segment path was never judged by it. That is the privilege-confusion case:
    `PUT /victimbucket%3Fpolicy` reached the api as `PUT /victimbucket?policy` — PutBucketPolicy —
    while the gateway authorized it as CreateBucket, whose ACL branch performs no permission
    check at all. Only the path check covers this shape.
    """
    status, reached = await _send("PUT", "/victimbucket%3Fpolicy")

    assert status == 400
    assert reached == []
