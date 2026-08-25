"""The parity claim of `forwarded_path`, asserted over generated paths instead of a list.

`test_collapse_dot_segments.py` pins the same claim against a hand-written list of shapes. That
list is what let the `#` divergence through: every entry exercised `.`/`..`, so a universal claim
("the path exactly as the api will receive it") was verified on one axis only, and
`/internal%23x/parts/1` sailed past the reserved-name denylist while reaching the api as
`GET /internal`. Generating the paths is what makes the claim falsifiable — this property fails on
that input without anyone having thought to write it down.

httpx is the oracle: `ForwardService` builds `f"{backend_url}{scope['path']}"` and hands the string
to httpx, so whatever `httpx.URL(...).path` says is what the api's own `scope["path"]` becomes
(uvicorn derives it the same way, by unquoting the request target).
"""

from __future__ import annotations

import asyncio
from typing import Any
from urllib.parse import quote
from urllib.parse import unquote

import httpx
import pytest
from fastapi import Request
from fastapi import Response
from hypothesis import HealthCheck
from hypothesis import given
from hypothesis import settings
from hypothesis import strategies as st

from hippius_s3.gateway.middlewares.input_validation import input_validation_middleware
from hippius_s3.gateway.utils.paths import forwarded_path


# Segments chosen to collide with everything the forwarder rewrites or a security check reads:
# empty (`//`), both dot spellings, near-misses that must NOT collapse (`...`, `a..b`), the two
# delimiters that truncate the request target, and the reserved names the denylist polices.
#
# Deliberately no `%`: httpx forwards percent-escapes verbatim and the api decodes them a second
# time, so `/%69nternal` arrives as `/internal`. That divergence is real but cannot be fixed inside
# `forwarded_path`, and it is pinned as a strict xfail in
# `test_collapse_dot_segments.py::test_percent_escapes_are_a_known_parity_gap`. Generating `%` here
# would just re-fail that known gap on every run instead of guarding this one.
SEGMENTS = st.sampled_from(
    ["", ".", "..", "...", "a..b", "a", "b", "key.txt", "internal", "docs", "a#b", "a?b", "#", "?"]
)

PATHS = st.lists(SEGMENTS, min_size=0, max_size=6).map(lambda parts: "/" + "/".join(parts))

# The escapes that survive the gateway's single decode and are read again by the api: spellings of
# `internal` and of `..`, plus a bare `%`. These belong only to the security property below, whose
# oracle is httpx rather than `forwarded_path`.
ESCAPED_SEGMENTS = st.sampled_from(
    ["%69nternal", "int%65rnal", "interna%6C", "%2e%2e", "%2E%2E", "%2e", "%", "%25", "%23internal"]
)

ESCAPED_PATHS = st.lists(st.one_of(SEGMENTS, ESCAPED_SEGMENTS), min_size=0, max_size=5).map(
    lambda parts: "/" + "/".join(parts)
)


@settings(max_examples=200, deadline=None)
@given(path=PATHS)
def test_forwarded_path_matches_httpx_for_generated_paths(path: str) -> None:
    assert forwarded_path(path) == httpx.URL(f"http://api{path}").path


def _run_middleware(path: str) -> tuple[Response, list[Request]]:
    """Drive the middleware over the scope uvicorn would build for a client sending `path` encoded.

    `path` is the DECODED scope path, so `raw_path` is it re-quoted: `quote` escapes `%`, `#` and
    `?`, which is exactly what a client must send for those characters to survive as literals, and
    `decoded_path` unquotes it back to `path`.
    """
    forwarded: list[Request] = []

    async def call_next(request: Request) -> Response:
        forwarded.append(request)
        return Response(status_code=200)

    scope: dict[str, Any] = {
        "type": "http",
        "method": "GET",
        "path": path,
        "raw_path": quote(path, safe="/").encode(),
        "root_path": "",
        "headers": [],
        "query_string": b"",
    }
    response = asyncio.run(input_validation_middleware(Request(scope), call_next))
    return response, forwarded


def _api_first_segment(path: str) -> str:
    """The first segment the api will route on, derived WITHOUT the code under test.

    httpx decides the request target and the api's uvicorn unquotes it, so `httpx.URL(...).path`
    put through one more unquote is the api's own `scope["path"]`. Using this rather than
    `forwarded_path` as the precondition is what keeps the property honest: a bug in
    `forwarded_path` cannot make the precondition stop holding and quietly excuse the middleware.
    """
    return unquote(httpx.URL(f"http://api{path}").path).strip("/").split("/")[0]


# Constructing a Request is cheap but not free; the health check being suppressed is about
# function-scoped fixtures, of which this uses none.
@settings(max_examples=200, deadline=None, suppress_health_check=[HealthCheck.function_scoped_fixture])
@given(path=ESCAPED_PATHS)
def test_no_generated_path_reaches_the_api_as_internal(path: str) -> None:
    """The guarantee, not the mechanism: if the api would route it to `internal`, we rejected it.

    Parity is only one way this is achieved — dot segments and `#`/`?` are modelled in
    `forwarded_path`, while the percent double-decode is refused outright instead. Stating the claim
    over the api's own view covers both, and covers a third rewrite nobody has thought of yet.
    """
    if _api_first_segment(path) != "internal":
        return

    response, forwarded = _run_middleware(path)

    assert response.status_code == 400, f"{path!r} reaches the api as `internal`"
    assert forwarded == []


@pytest.mark.parametrize(
    "path",
    [
        "/internal#x/parts/1",
        "/internal?x/parts/1",
        "/anybucket/../internal/parts/1",
        "/%69nternal/parts/1",
        "/int%65rnal/parts/1",
    ],
)
def test_the_property_above_is_not_vacuous(path: str) -> None:
    """Every bypass shape in the generated space really does trip the precondition.

    A property whose precondition never holds passes trivially, and this one is a live risk: the
    precondition went silently vacuous for the `#` shapes when `forwarded_path` was the oracle and
    had no truncation. One entry per rewrite the api performs.
    """
    assert _api_first_segment(path) == "internal"
    response, forwarded = _run_middleware(path)
    assert response.status_code == 400
    assert forwarded == []


@pytest.mark.parametrize("path", ["/%2e%2e/internal/parts/1", "/%2E%2E/internal/parts/1"])
def test_encoded_dot_segments_do_not_reach_internal_at_all(path: str) -> None:
    """A near-miss that looks like a bypass and is not — worth pinning so nobody "fixes" it.

    httpx removes dot segments from the still-ENCODED path, so `%2e%2e` is not a dot segment to it
    and survives; the api then decodes it to a literal `..` and never collapses (neither uvicorn nor
    Starlette does). So the api routes on `/../internal/parts/1`, which matches no internal route.
    The traversal only works with LITERAL dots, which `forwarded_path` collapses.
    """
    assert _api_first_segment(path) == ".."
