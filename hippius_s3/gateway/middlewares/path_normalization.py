"""One path view for every layer — set once, outermost.

Pre-merge, security middlewares judged `routing_path()` (dot-collapsed, truncated at
`#`/`?` — the rewrites httpx applied in ForwardService) while the api's router received
the httpx-rewritten path, so the two views agreed by construction. With the forward hop
gone nothing rewrites the path anymore, so the router would act on the RAW path while
security judges the collapsed one — `PUT /victim/../other/key` would be permission-checked
as `other/key` and stored under `victim`. This middleware realizes the one-view rule the
old gateway TODO called for: `scope["path"]` becomes the routing view before any layer
reads it.

`raw_path` is deliberately left untouched: SigV4 canonicalizes the path exactly as the
client signed it (S3 does not normalize for signing), and that verification reads
`raw_path`. The old system had the same split — signatures verified on the original path,
routing/storage on the collapsed one.
"""

from typing import Awaitable
from typing import Callable

from fastapi import Request
from fastapi import Response

from hippius_s3.gateway.utils.paths import routing_path


async def path_normalization_middleware(
    request: Request,
    call_next: Callable[[Request], Awaitable[Response]],
) -> Response:
    request.scope["path"] = routing_path(request)
    return await call_next(request)
