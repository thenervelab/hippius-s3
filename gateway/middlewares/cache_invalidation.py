from __future__ import annotations

import logging
from typing import Awaitable
from typing import Callable

from fastapi import Request
from fastapi import Response

from gateway.repositories.cached_acl_repository import CachedACLRepository
from gateway.services.acl_service import ACLService


logger = logging.getLogger(__name__)


async def cache_invalidation_middleware(
    request: Request,
    call_next: Callable[[Request], Awaitable[Response]],
) -> Response:
    """Purge gateway-side caches after operations that change bucket state.

    Today: invalidates the ACL cache (`redis-acl`, TTL 600s) on a successful
    DeleteBucket. Without this, cached public-bucket grants would keep
    authorizing anonymous reads against a soft-deleted bucket for up to the
    cache TTL — a real authz hole.
    """
    response = await call_next(request)

    if not _is_successful_bucket_delete(request, response):
        return response

    bucket_name = _bucket_from_path(request.url.path)
    if not bucket_name:
        return response

    acl_service = getattr(request.app.state, "acl_service", None)
    if acl_service is None:
        return response

    await _invalidate_bucket_acl_cache(acl_service, bucket_name)
    return response


def _is_successful_bucket_delete(request: Request, response: Response) -> bool:
    if request.method != "DELETE":
        return False
    if response.status_code != 204:
        return False
    # DELETE /<bucket>?tagging removes only tags; bucket itself stays.
    return "tagging" not in request.query_params


def _bucket_from_path(path: str) -> str | None:
    """Return the bucket name iff `path` is exactly `/<bucket>` (no key)."""
    stripped = path.strip("/")
    if not stripped or "/" in stripped:
        return None
    return stripped


async def _invalidate_bucket_acl_cache(acl_service: ACLService, bucket_name: str) -> None:
    if not isinstance(acl_service.acl_repo, CachedACLRepository):
        return
    cached = acl_service.acl_repo
    await cached.invalidate_bucket_acl(bucket_name)
    await cached.invalidate_all_bucket_objects(bucket_name)
