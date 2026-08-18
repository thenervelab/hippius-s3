from __future__ import annotations

import logging
from typing import Awaitable
from typing import Callable

from fastapi import Request
from fastapi import Response

from hippius_s3.gateway.repositories.cached_acl_repository import CachedACLRepository
from hippius_s3.gateway.services.acl_service import ACLService
from hippius_s3.gateway.utils.paths import decoded_path


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

    Same call also purges the bucket-meta entry (owner_id + bucket_id, keyed by
    bucket NAME). Soft-delete frees the name for any account immediately, so a
    surviving entry would resolve the previous owner against the next account's
    bucket of that name — granting them the master-token ownership bypass and
    the "private" canned-ACL owner match, while 403'ing the rightful owner.

    Every cache key here is keyed by bucket NAME, and a name outlives the bucket
    that held it, so a successful CreateBucket purges too. Delete-side purging
    alone is not sufficient: (1) `get_bucket_owner_and_id` is a plain read-through
    with no write guard, so a request that read the row before the soft-delete
    committed can SETEX it back *after* this middleware purged, resurrecting the
    old owner for a full TTL; (2) buckets also disappear out of band —
    `scripts/purge_buckets.py` hard-DELETEs rows with no gateway in the path.
    CreateBucket is the moment the name→owner mapping actually changes and is the
    only point where a stale entry can do harm, so purging there closes both.
    """
    response = await call_next(request)

    if not (_is_successful_bucket_delete(request, response) or _is_successful_bucket_create(request, response)):
        return response

    bucket_name = _bucket_from_path(decoded_path(request))
    if not bucket_name:
        return response

    acl_service = getattr(request.app.state, "acl_service", None)
    if acl_service is None:
        return response

    # Best-effort: a redis-acl outage at delete time must not turn a successful
    # 204 into a 500 (the upstream API has already committed the soft-delete).
    # Cache TTL (600s) bounds staleness if invalidation fails.
    try:
        await _invalidate_bucket_acl_cache(acl_service, bucket_name)
    except Exception:
        logger.exception(f"Failed to invalidate ACL cache for soft-deleted bucket {bucket_name}")
    return response


def _is_successful_bucket_delete(request: Request, response: Response) -> bool:
    if request.method != "DELETE":
        return False
    if response.status_code != 204:
        return False
    # DELETE /<bucket>?tagging removes only tags; bucket itself stays.
    return "tagging" not in request.query_params


def _is_successful_bucket_create(request: Request, response: Response) -> bool:
    if request.method != "PUT" or response.status_code != 200:
        return False
    # Any query param makes this a sub-resource write (?acl, ?tagging, ?lifecycle, ?policy,
    # ?cors) — those mutate bucket config, not the name→owner mapping this cache holds.
    return not request.query_params


def _bucket_from_path(path: str) -> str | None:
    """Return the bucket name iff `path` is exactly `/<bucket>` (no key)."""
    stripped = path.strip("/")
    if not stripped or "/" in stripped:
        return None
    return stripped


async def _invalidate_bucket_acl_cache(acl_service: ACLService, bucket_name: str) -> None:
    # The bucket-meta entry (owner_id + bucket_id, keyed by NAME) lives on ACLService's own Redis
    # handle rather than the repo's, so purge it before the isinstance gate below.
    await acl_service.invalidate_bucket_meta(bucket_name)
    if not isinstance(acl_service.acl_repo, CachedACLRepository):
        return
    cached = acl_service.acl_repo
    await cached.invalidate_bucket_acl(bucket_name)
    await cached.invalidate_all_bucket_objects(bucket_name)
