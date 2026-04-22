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

import logging
import re
from enum import Enum

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

from hippius_s3.dependencies import DBConnection
from hippius_s3.dependencies import get_postgres
from hippius_s3.repositories.sub_token_scope_repository import SubTokenScopeRepository
from hippius_s3.repositories.sub_token_scope_repository import scope_cache_key


logger = logging.getLogger(__name__)
router = APIRouter(tags=["sub-token-scopes"])

ACCESS_KEY_PATTERN = re.compile(r"^hip_[a-zA-Z0-9_-]{1,240}$")
SS58_PATTERN = re.compile(r"^[1-9A-HJ-NP-Za-km-z]{47,48}$")


# --- enums ------------------------------------------------------------------


class Permission(str, Enum):
    """R2-style permission tiers. Each tier maps to a fixed set of S3 operations:

    - `admin_read_write` — all S3 ops on in-scope buckets; CreateBucket / DeleteBucket only when `bucket_scope=all`
    - `admin_read` — GetObject, HeadObject, ListBucket, ListBuckets, GetBucketAcl/Location/Tagging
    - `object_read_write` — GetObject, HeadObject, PutObject, DeleteObject, ListBucket, multipart ops
    - `object_read` — GetObject, HeadObject, ListBucket
    """

    admin_read_write = "admin_read_write"
    admin_read = "admin_read"
    object_read_write = "object_read_write"
    object_read = "object_read"


class BucketScope(str, Enum):
    """Which buckets the sub-token is permitted on:

    - `all` — every bucket owned by `account_id`. Required for CreateBucket / ListBuckets.
    - `specific` — only the buckets listed in `buckets` (by name). Must be non-empty.
    """

    all = "all"
    specific = "specific"


# --- request / response models ---------------------------------------------


class ScopePutBody(BaseModel):
    """Request body for PUT /user/sub-tokens/{access_key_id}/scope."""

    account_id: str = Field(
        ...,
        description="Substrate SS58 address of the account that owns the sub-token. Buckets listed in `buckets` must be owned by this account.",
        examples=["5FHneW46xGXgs5mUiveU4sbTyGBzmstUspZC92UhjJM694ty"],
    )
    permission: Permission = Field(
        ...,
        description="R2-style permission tier.",
    )
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


class ErrorDetail(BaseModel):
    code: str
    message: str


# --- helpers ---------------------------------------------------------------


def _raise(code: str, message: str, http_status: int) -> None:
    raise HTTPException(status_code=http_status, detail={"code": code, "message": message})


def _validate_access_key(access_key_id: str) -> None:
    if not ACCESS_KEY_PATTERN.match(access_key_id):
        _raise("InvalidArgument", f"Invalid access_key_id format: {access_key_id}", status.HTTP_400_BAD_REQUEST)


def _validate_account_id(account_id: str) -> None:
    if not SS58_PATTERN.match(account_id):
        _raise("InvalidArgument", f"Invalid account_id (must be SS58): {account_id}", status.HTTP_400_BAD_REQUEST)


async def _resolve_bucket_ids(db: DBConnection, account_id: str, bucket_names: list[str]) -> list[str]:
    """Resolve bucket names to UUIDs; every bucket must be owned by account_id."""
    if not bucket_names:
        return []
    rows = await db.fetch(
        "SELECT bucket_name, bucket_id, main_account_id FROM buckets WHERE bucket_name = ANY($1)",
        list(bucket_names),
    )
    by_name = {r["bucket_name"]: r for r in rows}
    bucket_ids: list[str] = []
    for name in bucket_names:
        row = by_name.get(name)
        if row is None:
            _raise("NoSuchBucket", f"Bucket does not exist: {name}", status.HTTP_404_NOT_FOUND)
            return []  # unreachable, for type-checker
        if row["main_account_id"] != account_id:
            _raise(
                "AccessDenied",
                f"Bucket not owned by account_id: {name}",
                status.HTTP_403_FORBIDDEN,
            )
            return []  # unreachable
        bucket_ids.append(str(row["bucket_id"]))
    return bucket_ids


# --- endpoints --------------------------------------------------------------


@router.get(
    "/{access_key_id}/scope",
    response_model=ScopeResponse,
    responses={
        404: {"model": ErrorDetail, "description": "No scope set for this access key."},
        400: {"model": ErrorDetail, "description": "Invalid access_key_id format."},
    },
    summary="Get sub-token scope",
    description=(
        "Return the current R2-style scope record for a sub-token's access key.\n\n"
        "Returns **404** if no scope has been set (sub-tokens default-deny until scope is installed).\n\n"
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

    repo = SubTokenScopeRepository(db.pool)
    scope = await repo.get(access_key_id)
    if scope is None or scope.account_id != account_id:
        _raise("NoSuchScope", "Scope not set for this access key", status.HTTP_404_NOT_FOUND)
        raise AssertionError  # unreachable; helps mypy

    bucket_rows = await db.fetch(
        "SELECT bucket_id, bucket_name FROM buckets WHERE bucket_id = ANY($1::uuid[])",
        list(scope.bucket_ids),
    )
    names_by_id = {str(r["bucket_id"]): r["bucket_name"] for r in bucket_rows}
    return ScopeResponse(
        access_key_id=scope.access_key_id,
        account_id=scope.account_id,
        permission=Permission(scope.permission),
        bucket_scope=BucketScope(scope.bucket_scope),
        buckets=[names_by_id[b] for b in scope.bucket_ids if b in names_by_id],
    )


@router.put(
    "/{access_key_id}/scope",
    response_model=ScopeResponse,
    responses={
        400: {
            "model": ErrorDetail,
            "description": "Invalid body (bad permission, bucket_scope, missing buckets, etc.).",
        },
        403: {"model": ErrorDetail, "description": "A bucket in `buckets` is not owned by `account_id`."},
        404: {"model": ErrorDetail, "description": "A bucket in `buckets` does not exist."},
    },
    summary="Set (upsert) sub-token scope",
    description=(
        "Install or replace the scope for a sub-token.\n\n"
        "This is an upsert: if a scope already exists for `access_key_id` it's replaced; "
        "otherwise it's inserted. Takes effect on the next S3 request from the sub-token "
        "(the gateway's 60s scope cache is invalidated on success).\n\n"
        "**Rules**\n"
        "- Every bucket in `buckets` must exist and be owned by `account_id`.\n"
        "- `bucket_scope='specific'` requires a non-empty `buckets` list.\n"
        "- `bucket_scope='all'` requires `buckets` to be empty.\n"
        "- `access_key_id` must start with `hip_`.\n\n"
        "**Common patterns**\n"
        "- Read-only access to a single bucket: `permission=object_read`, `bucket_scope=specific`, `buckets=[A]`.\n"
        "- Read+write on two buckets: `permission=object_read_write`, `bucket_scope=specific`, `buckets=[A,B]`.\n"
        "- Account-wide admin (can create new buckets): `permission=admin_read_write`, `bucket_scope=all`, `buckets=[]`.\n\n"
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

    if body.bucket_scope == BucketScope.specific and not body.buckets:
        _raise("InvalidArgument", "bucket_scope='specific' requires non-empty buckets", status.HTTP_400_BAD_REQUEST)
    if body.bucket_scope == BucketScope.all and body.buckets:
        _raise("InvalidArgument", "bucket_scope='all' must not specify buckets", status.HTTP_400_BAD_REQUEST)

    bucket_ids = await _resolve_bucket_ids(db, body.account_id, body.buckets)

    repo = SubTokenScopeRepository(db.pool)
    scope = await repo.upsert(
        access_key_id=access_key_id,
        account_id=body.account_id,
        permission=body.permission.value,
        bucket_scope=body.bucket_scope.value,
        bucket_ids=bucket_ids,
    )

    redis_client = request.app.state.redis_client
    await redis_client.delete(scope_cache_key(access_key_id))
    logger.info(
        f"Sub-token scope upserted: access_key={access_key_id[:8]}***, "
        f"account={body.account_id}, permission={body.permission.value}, "
        f"bucket_scope={body.bucket_scope.value}, buckets={len(body.buckets)}"
    )

    return ScopeResponse(
        access_key_id=scope.access_key_id,
        account_id=scope.account_id,
        permission=Permission(scope.permission),
        bucket_scope=BucketScope(scope.bucket_scope),
        buckets=list(body.buckets),
    )


@router.delete(
    "/{access_key_id}/scope",
    status_code=status.HTTP_204_NO_CONTENT,
    responses={
        400: {"model": ErrorDetail, "description": "Invalid access_key_id format."},
    },
    summary="Revoke sub-token scope",
    description=(
        "Delete the scope row for a sub-token. The sub-token immediately default-denies everything "
        "on the next request (after the 60s cache is invalidated).\n\n"
        "Idempotent: returns 204 whether or not a row existed.\n\n"
        "**Auth**: protected by the frontend HMAC layer (`X-HMAC-Signature`)."
    ),
)
async def delete_scope(
    request: Request,
    access_key_id: str = FPath(..., description="Sub-token access key ID, e.g. `hip_abcdef012345...`."),
    db: DBConnection = Depends(get_postgres),
) -> Response:
    _validate_access_key(access_key_id)

    repo = SubTokenScopeRepository(db.pool)
    deleted = await repo.delete(access_key_id)

    redis_client = request.app.state.redis_client
    await redis_client.delete(scope_cache_key(access_key_id))
    logger.info(f"Sub-token scope deleted: access_key={access_key_id[:8]}***, existed={deleted}")

    return Response(status_code=status.HTTP_204_NO_CONTENT)
