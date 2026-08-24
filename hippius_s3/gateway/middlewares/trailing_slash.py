from __future__ import annotations

from typing import Awaitable
from typing import Callable

from fastapi import Request
from fastapi import Response


async def trailing_slash_normalizer(
    request: Request,
    call_next: Callable[[Request], Awaitable[Response]],
) -> Response:
    """Normalize trailing slashes without redirecting.

    Removes a trailing slash from the URL path (except for "/") so both
    with/without trailing slash resolve to the same route.

    Two things here are load-bearing, both for the same reason: this middleware is the
    only layer that rewrites the path AFTER `path_normalization` has established the
    routing view, and `account`/`acl` run inner to it.

    - `raw_path` is edited as BYTES. Re-encoding `request.url.path` (already decoded once)
      would hand `routing_path` a value it decodes a second time, so a key sent as
      `a%2541.txt` would be judged as `aA.txt` — a decoded view no client asked for.
    - The memoized routing view is dropped, so every inner security layer recomputes the
      path the router will actually serve. Leaving it stale made `PUT /bucket/` parse as
      bucket + EMPTY key: `key is None` was False, so `is_create_bucket` never fired and
      the sub-token branch fell through to `call_next` with no scope check at all.
    """
    path = request.url.path
    if path != "/" and path.endswith("/"):
        request.scope["path"] = path.rstrip("/")

        raw = request.scope.get("raw_path")
        if raw is not None:
            stripped = raw.rstrip(b"/")
            request.scope["raw_path"] = stripped or b"/"

        request.scope.pop("_hippius_routing_path", None)

    return await call_next(request)
