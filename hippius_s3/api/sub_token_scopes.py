"""Sub-token scope management endpoints.

Exposed under `/user/sub-tokens/` on the internal API. The gateway protects
`/user/*` with a shared HMAC secret (`X-HMAC-Signature`, computed as
HMAC-SHA256(FRONTEND_HMAC_SECRET, METHOD + PATH[+?QUERY])), so callers of
these endpoints are the console/frontend, not end users.

The scope record lives in the gateway's Postgres (`sub_token_scopes` table)
and is consulted on every S3 request made with the sub-token's access key
(see `gateway/middlewares/acl.py`). Writes here also invalidate the 60s
Redis cache so scope changes take effect immediately.
"""

from __future__ import annotations

import asyncio
import logging
from typing import NoReturn

from fastapi import APIRouter
from fastapi import Depends
from fastapi import HTTPException
from fastapi import Path as FPath
from fastapi import Query
from fastapi import Request
from fastapi import status
from fastapi.responses import Response
from pydantic import BaseModel
from pydantic import Field

from gateway.services.sub_token_scope_cache import scope_cache_key
from hippius_s3.dependencies import DBConnection
from hippius_s3.dependencies import get_postgres
from hippius_s3.models.sub_token import ACCESS_KEY_PATTERN
from hippius_s3.models.sub_token import SS58_PATTERN
from hippius_s3.models.sub_token import BucketScope
from hippius_s3.models.sub_token import Permission
from hippius_s3.repositories.sub_token_scope_repository import SubTokenScopeRepository
from hippius_s3.services.hippius_api_service import HippiusApiClient
from hippius_s3.utils import get_query


logger = logging.getLogger(__name__)
router = APIRouter(tags=["sub-token-scopes"])


# --- request / response models ---------------------------------------------


class ScopePutBody(BaseModel):
    """Request body for PUT /user/sub-tokens/{access_key_id}/scope."""

    account_id: str = Field(
        ...,
        description=(
            "Substrate SS58 address of the account that owns the sub-token. "
            "Buckets listed in `buckets` must be owned by this account."
        ),
        examples=["5FHneW46xGXgs5mUiveU4sbTyGBzmstUspZC92UhjJM694ty"],
    )
    permission: Permission = Field(..., description="R2-style permission tier.")
    bucket_scope: BucketScope = Field(
        ...,
        description="'all' for every bucket owned by account_id, or 'specific' for the buckets listed in `buckets`.",
    )
    buckets: list[str] = Field(
        default_factory=list,
        description="Bucket names (not UUIDs). Required when bucket_scope='specific'; must be empty when bucket_scope='all'.",
        examples=[["logs-bucket", "reports-bucket"]],
    )


class ScopeResponse(BaseModel):
    """Current scope record for a sub-token."""

    access_key_id: str = Field(..., description="Sub-token access key ID (hip_*).")
    account_id: str = Field(..., description="SS58 of the account that owns the sub-token.")
    permission: Permission
    bucket_scope: BucketScope
    buckets: list[str] = Field(
        ...,
        description="Bucket names the scope applies to (empty when bucket_scope='all').",
    )
    stale_bucket_ids: list[str] = Field(
        default_factory=list,
        description=(
            "Bucket UUIDs referenced by the stored scope but no longer present in the `buckets` table "
            "(e.g. the bucket was deleted). Empty in the normal case."
        ),
    )


class ErrorDetail(BaseModel):
    code: str
    message: str


# --- helpers ---------------------------------------------------------------


def _raise(code: str, message: str, http_status: int) -> NoReturn:
    raise HTTPException(status_code=http_status, detail={"code": code, "message": message})


def _validate_access_key(access_key_id: str) -> None:
    if not ACCESS_KEY_PATTERN.match(access_key_id):
        _raise("InvalidArgument", f"Invalid access_key_id format: {access_key_id}", status.HTTP_400_BAD_REQUEST)


def _validate_account_id(account_id: str) -> None:
    if not SS58_PATTERN.match(account_id):
        _raise("InvalidArgument", f"Invalid account_id (must be SS58): {account_id}", status.HTTP_400_BAD_REQUEST)


async def _resolve_bucket_ids(db: DBConnection, account_id: str, bucket_names: list[str]) -> list[str]:
    """Resolve bucket names to UUIDs; every bucket must be owned by account_id.

    Reports *all* missing and/or wrongly-owned buckets in a single error, not
    just the first one — spares the caller N round-trips to fix a typo'd list.
    """
    if not bucket_names:
        return []
    rows = await db.fetch(get_query("resolve_buckets_by_names"), list(bucket_names))
    by_name = {r["bucket_name"]: r for r in rows}

    missing = [n for n in bucket_names if n not in by_name]
    if missing:
        _raise("NoSuchBucket", f"Buckets do not exist: {missing}", status.HTTP_404_NOT_FOUND)

    unauthorized = [n for n in bucket_names if by_name[n]["main_account_id"] != account_id]
    if unauthorized:
        _raise("AccessDenied", f"Buckets not owned by account_id: {unauthorized}", status.HTTP_403_FORBIDDEN)

    return [str(by_name[n]["bucket_id"]) for n in bucket_names]


async def _verify_access_key_owned_by_account(access_key_id: str, account_id: str) -> None:
    """Confirm the target access_key_id is an active sub-token of `account_id`.

    Calls the account API directly (no caching — this is a write path, not the
    hot read path). Keeps the API layer free of a dependency on gateway services.
    """
    async with HippiusApiClient() as client:
        token_response = await client.auth(access_key_id)
    if not token_response.valid:
        _raise("InvalidArgument", "Target access key is not valid", status.HTTP_400_BAD_REQUEST)
    if token_response.token_type != "sub":
        _raise(
            "InvalidArgument",
            "Target access key is not a sub-token (scope can only be installed on sub-tokens)",
            status.HTTP_400_BAD_REQUEST,
        )
    if token_response.account_address != account_id:
        _raise(
            "AccessDenied",
            "Target sub-token belongs to a different account than the one supplied in body.account_id",
            status.HTTP_403_FORBIDDEN,
        )


# --- endpoints --------------------------------------------------------------


@router.get(
    "/{access_key_id}/scope",
    response_model=ScopeResponse,
    responses={
        404: {"model": ErrorDetail, "description": "No scope set for this access key."},
        403: {"model": ErrorDetail, "description": "Scope exists but account_id does not match."},
        400: {"model": ErrorDetail, "description": "Invalid access_key_id or account_id format."},
    },
    summary="Get sub-token scope",
    description=(
        "Return the current R2-style scope record for a sub-token's access key.\n\n"
        "Returns **404** if no scope has been set (sub-tokens default-deny until scope is installed). "
        "Returns **403** if a scope exists but is stored under a different account_id.\n\n"
        "**Auth**: protected by the frontend HMAC layer (`X-HMAC-Signature`)."
    ),
)
async def get_scope(
    request: Request,
    access_key_id: str = FPath(..., description="Sub-token access key ID, e.g. `hip_abcdef012345...`."),
    account_id: str = Query(..., description="SS58 of the owning account — must match the stored record."),
    db: DBConnection = Depends(get_postgres),
) -> ScopeResponse:
    _validate_access_key(access_key_id)
    _validate_account_id(account_id)

    row = await db.fetchrow(get_query("get_sub_token_scope_with_buckets"), access_key_id)
    if row is None:
        _raise("NoSuchScope", "Scope not set for this access key", status.HTTP_404_NOT_FOUND)

    if row["account_id"] != account_id:
        logger.warning(
            f"get_scope account_id mismatch: access_key={access_key_id[:8]}***, "
            f"requested={account_id}, stored={row['account_id']}"
        )
        _raise(
            "AccessDenied",
            "Scope exists but does not belong to the provided account_id",
            status.HTTP_403_FORBIDDEN,
        )

    stale_bucket_ids = list(row["stale_bucket_ids"] or [])
    if stale_bucket_ids:
        logger.warning(f"get_scope found stale bucket_ids: access_key={access_key_id[:8]}***, stale={stale_bucket_ids}")
    return ScopeResponse(
        access_key_id=row["access_key_id"],
        account_id=row["account_id"],
        permission=Permission(row["permission"]),
        bucket_scope=BucketScope(row["bucket_scope"]),
        buckets=list(row["live_bucket_names"] or []),
        stale_bucket_ids=stale_bucket_ids,
    )


@router.put(
    "/{access_key_id}/scope",
    response_model=ScopeResponse,
    responses={
        400: {"model": ErrorDetail, "description": "Invalid body or target access key is not a sub-token."},
        403: {"model": ErrorDetail, "description": "Target access key or a bucket is not owned by account_id."},
        404: {"model": ErrorDetail, "description": "One or more buckets in `buckets` do not exist."},
    },
    summary="Set (upsert) sub-token scope",
    description=(
        "Install or replace the scope for a sub-token.\n\n"
        "Upsert: if a scope already exists for `access_key_id` it's replaced; otherwise inserted. "
        "Takes effect on the next S3 request — the gateway's 60s scope cache is invalidated on success.\n\n"
        "**Rules**\n"
        "- `access_key_id` must start with `hip_` and be an active sub-token belonging to `account_id`.\n"
        "- Every bucket in `buckets` must exist and be owned by `account_id`.\n"
        "- `bucket_scope='specific'` requires a non-empty `buckets` list; `bucket_scope='all'` requires empty.\n\n"
        "**Auth**: protected by the frontend HMAC layer (`X-HMAC-Signature`)."
    ),
)
async def put_scope(
    request: Request,
    body: ScopePutBody,
    access_key_id: str = FPath(..., description="Sub-token access key ID, e.g. `hip_abcdef012345...`."),
    db: DBConnection = Depends(get_postgres),
) -> ScopeResponse:
    _validate_access_key(access_key_id)
    _validate_account_id(body.account_id)

    if body.bucket_scope is BucketScope.specific and not body.buckets:
        _raise("InvalidArgument", "bucket_scope='specific' requires non-empty buckets", status.HTTP_400_BAD_REQUEST)
    if body.bucket_scope is BucketScope.all and body.buckets:
        _raise("InvalidArgument", "bucket_scope='all' must not specify buckets", status.HTTP_400_BAD_REQUEST)

    # Run the token validation and bucket resolution concurrently — they're
    # independent and the slow path is the account-API call.
    _, bucket_ids = await asyncio.gather(
        _verify_access_key_owned_by_account(access_key_id, body.account_id),
        _resolve_bucket_ids(db, body.account_id, body.buckets),
    )

    repo: SubTokenScopeRepository = request.app.state.sub_token_scope_repo
    redis_client = request.app.state.redis_client

    # Upsert and cache-invalidate in parallel.
    scope, _ = await asyncio.gather(
        repo.upsert(
            access_key_id=access_key_id,
            account_id=body.account_id,
            permission=body.permission,
            bucket_scope=body.bucket_scope,
            bucket_ids=bucket_ids,
        ),
        redis_client.delete(scope_cache_key(access_key_id)),
    )

    logger.info(
        f"Sub-token scope upserted: access_key={access_key_id[:8]}***, "
        f"account={body.account_id}, permission={body.permission.value}, "
        f"bucket_scope={body.bucket_scope.value}, buckets={len(body.buckets)}"
    )
    return ScopeResponse(
        access_key_id=scope.access_key_id,
        account_id=scope.account_id,
        permission=scope.permission,
        bucket_scope=scope.bucket_scope,
        buckets=list(body.buckets),
        stale_bucket_ids=[],
    )


@router.delete(
    "/{access_key_id}/scope",
    status_code=status.HTTP_204_NO_CONTENT,
    responses={
        400: {"model": ErrorDetail, "description": "Invalid access_key_id format."},
    },
    summary="Revoke sub-token scope",
    description=(
        "Delete the scope row for a sub-token. Idempotent: returns 204 whether or not a row existed. "
        "The 60s cache is invalidated so the sub-token default-denies everything on its next request.\n\n"
        "**Auth**: protected by the frontend HMAC layer (`X-HMAC-Signature`)."
    ),
)
async def delete_scope(
    request: Request,
    access_key_id: str = FPath(..., description="Sub-token access key ID, e.g. `hip_abcdef012345...`."),
) -> Response:
    _validate_access_key(access_key_id)

    repo: SubTokenScopeRepository = request.app.state.sub_token_scope_repo
    redis_client = request.app.state.redis_client

    deleted, _ = await asyncio.gather(
        repo.delete(access_key_id),
        redis_client.delete(scope_cache_key(access_key_id)),
    )

    logger.info(f"Sub-token scope deleted: access_key={access_key_id[:8]}***, existed={deleted}")
    return Response(status_code=status.HTTP_204_NO_CONTENT)
