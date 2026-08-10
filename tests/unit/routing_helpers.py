"""Read the api's route table the same way on every FastAPI version.

FastAPI 0.141 changed the shape of `app.routes`: each `include_router()` call now leaves a
`fastapi.routing._IncludedRouter` in the list instead of splicing the included routes in. That
wrapper carries neither `.name` nor `.path`, so any test walking `app.routes` both misses every
included route and raises `AttributeError: '_IncludedRouter' object has no attribute 'name'`.

That is how this suite went red with no behaviour change: CI installs with
`uv pip install -e ".[dev]"`, which resolves fresh from `pyproject.toml` rather than `uv.lock`, so
it picked up FastAPI 0.141 while `uv.lock` still pins 0.136. Routing itself was verified
unaffected — with the flag and secret set, `/internal/parts/...` answers 404 with no header, 404
with a wrong one, and 200 with the right one, on both versions.

The wrapper exposes `effective_route_contexts()`, whose entries carry the FULLY-PREFIXED path plus
`name`, `methods` and `matches` — i.e. everything the callers need. Reaching for
`original_router.routes` instead looks equivalent and is not: those paths are missing the
`include_router(prefix=...)` prefix, so `/user/list_buckets` reads as `/list_buckets` and any
assertion about path segments silently changes meaning.

Flattening here rather than in each test keeps the three call sites asserting the same thing and
keeps the version quirk in one place with the reason attached.
"""

from __future__ import annotations

from typing import Any


def leaf_routes(app: Any) -> list[Any]:
    """Every real route, in registration order, with `_IncludedRouter` wrappers expanded.

    Order is load-bearing: two callers assert route PRECEDENCE, which on this app is registration
    order, so each included router's routes are spliced in at the position its wrapper occupied
    rather than appended.

    Entries are either real `Route`/`APIRoute` objects or FastAPI's `_EffectiveRouteContext`. Both
    expose `name`, `path`, `methods` and `matches`, which is the whole surface used here.
    """
    out: list[Any] = []

    def walk(routes: list[Any]) -> None:
        for route in routes:
            contexts = getattr(route, "effective_route_contexts", None)
            if callable(contexts):
                # FastAPI >= 0.141 wrapper. These carry the fully-prefixed path.
                walk(list(contexts()))
                continue
            out.append(route)

    walk(list(getattr(app, "routes", [])))
    return out


def route_names(app: Any) -> list[str]:
    """Names of every real route, in registration order."""
    return [n for n in (getattr(r, "name", None) for r in leaf_routes(app)) if n]
