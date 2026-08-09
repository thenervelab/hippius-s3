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

import httpx
import pytest
from fastapi import Request
from fastapi import Response
from hypothesis import HealthCheck
from hypothesis import given
from hypothesis import settings
from hypothesis import strategies as st

from gateway.middlewares.input_validation import input_validation_middleware
from gateway.utils.paths import forwarded_path


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


@settings(max_examples=200, deadline=None)
@given(path=PATHS)
def test_forwarded_path_matches_httpx_for_generated_paths(path: str) -> None:
    assert forwarded_path(path) == httpx.URL(f"http://api{path}").path


def _run_middleware(path: str) -> tuple[Response, list[Request]]:
    forwarded: list[Request] = []

    async def call_next(request: Request) -> Response:
        forwarded.append(request)
        return Response(status_code=200)

    scope: dict[str, Any] = {
        "type": "http",
        "method": "GET",
        "path": path,
        # No `%` in the generated pool, so unquoting this is the identity and `decoded_path`
        # returns exactly `path` — the same scope uvicorn would build for these characters sent
        # percent-encoded.
        "raw_path": path.encode(),
        "root_path": "",
        "headers": [],
        "query_string": b"",
    }
    response = asyncio.run(input_validation_middleware(Request(scope), call_next))
    return response, forwarded


# The generated paths deliberately include `#`/`?`, and constructing a Request is cheap but not
# free; function-scoped fixtures are what the health check guards against and none are used here.
@settings(max_examples=200, deadline=None, suppress_health_check=[HealthCheck.function_scoped_fixture])
@given(path=PATHS)
def test_no_generated_path_reaches_the_api_as_internal(path: str) -> None:
    """The guarantee, not the mechanism: if the api would route it to `internal`, we rejected it.

    Parity is only how this is achieved. Stating the security claim directly means a future change
    that stops using `forwarded_path` in the middleware — or adds a third rewrite httpx performs —
    fails here even if the parity property is still satisfied.
    """
    if forwarded_path(path).strip("/").split("/")[0] != "internal":
        return

    response, forwarded = _run_middleware(path)

    assert response.status_code == 400, f"{path!r} reaches the api as `internal`"
    assert forwarded == []


@pytest.mark.parametrize("path", ["/internal#x/parts/1", "/anybucket/../internal/parts/1"])
def test_the_property_above_is_not_vacuous(path: str) -> None:
    """Both bypass shapes really are in the generated space, and really are rejected.

    A property whose precondition never holds passes trivially. These are the two shapes the
    generator can produce that must trip it.
    """
    assert forwarded_path(path).strip("/").split("/")[0] == "internal"
    response, forwarded = _run_middleware(path)
    assert response.status_code == 400
    assert forwarded == []
