"""Derive the handler-facing request context from the auth middlewares' state.

Before the gateway/api merge, this translation happened over HTTP: the gateway
serialized its request.state into X-Hippius-* headers (forward_service) and the
api parsed them back (parse_internal_headers). The two services are one app now,
so the same mapping is a direct state-to-state step. Two DISTINCT identities come
out of it, under two distinct names — the old header round-trip fused them into
one object and every reader had to know which half it was looking at:

- ``request.state.account`` — the CALLER's ``HippiusAccount`` exactly as the
  account middleware built it (empty stand-in for anonymous). Its id and
  credit/upload/delete flags describe who is making the request. Never rebound.
- ``request.state.main_account_id`` — the STORAGE-ATTRIBUTION account: the
  bucket owner resolved by the ACL middleware, falling back to the caller — the
  same fallback forward_service applied when building X-Hippius-Main-Account.
  S3 handlers bill and query against this, never against the caller's own main
  account.
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
    request.state.main_account_id = bucket_owner

    request.state.bucket_id = str(getattr(request.state, "bucket_id", "") or "")

    if getattr(request.state, "account", None) is None:
        request.state.account = HippiusAccount(
            id=account_id,
            main_account=account_id,
            has_credits=False,
            upload=False,
            delete=False,
        )

    return await call_next(request)
