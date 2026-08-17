"""Derive the handler-facing request context from the auth middlewares' state.

Before the gateway/api merge, this translation happened over HTTP: the gateway
serialized its request.state into X-Hippius-* headers (forward_service) and the
api parsed them back (parse_internal_headers). The two services are one app now,
so the same mapping is a direct state-to-state step — but the mapping itself is
load-bearing and must stay exactly what the header round-trip produced:

- ``request_user_id`` / ``account_id``: the authenticated caller (empty for
  anonymous public reads).
- ``bucket_owner_id``: the bucket owner resolved by the ACL middleware, falling
  back to the caller — the same fallback forward_service applied when building
  X-Hippius-Bucket-Owner.
- ``account.main_account``: the bucket owner, NOT the caller's own main account.
  S3 handlers attribute storage to the bucket owner's account; the caller's
  HippiusAccount (set by the account middleware) only contributes the
  credit/upload/delete flags.
"""

from typing import Awaitable
from typing import Callable

from fastapi import Request
from fastapi import Response

from hippius_s3.models.account import HippiusAccount


async def request_context_middleware(
    request: Request,
    call_next: Callable[[Request], Awaitable[Response]],
) -> Response:
    account_id = getattr(request.state, "account_id", "") or ""
    request.state.account_id = account_id
    request.state.request_user_id = account_id

    bucket_owner = getattr(request.state, "bucket_owner_id", "") or account_id
    request.state.bucket_owner_id = bucket_owner

    request.state.bucket_id = str(getattr(request.state, "bucket_id", "") or "")

    caller = getattr(request.state, "account", None)
    request.state.account = HippiusAccount(
        id=account_id,
        main_account=bucket_owner,
        has_credits=bool(caller is not None and caller.has_credits),
        upload=bool(caller is not None and caller.upload),
        delete=bool(caller is not None and caller.delete),
    )

    return await call_next(request)
