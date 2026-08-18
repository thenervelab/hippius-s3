"""Admin account-management endpoints (issue #421).

Exposed under `/admin/` on the internal API. The gateway protects `/admin/*` with a
dedicated HMAC secret (`X-HMAC-Signature`, HMAC-SHA256(HIPPIUS_ADMIN_HMAC_SECRET,
METHOD + PATH[+?QUERY])) — see gateway/middlewares/admin_hmac.py. Callers are the
billing backend / staff cockpit, never end users. The target account travels in the
signed path, never the unsigned body.

Suspension state lives in `account_suspensions` (row present = suspended) and is
enforced by gateway/middlewares/suspension.py plus the bucket-owner check in
gateway/middlewares/acl.py. Writes here go through the same Redis keys the gateway
reads, so state changes take effect on every pod immediately.

Purge jobs are rows in `purge_jobs` consumed by the purger worker
(hippius_s3/workers/purger.py). "done" means all rows are soft-deleted and unpin
requests enqueued — backend deletion and disk reclaim trail asynchronously via the
unpinner and janitor.
"""

from __future__ import annotations

import asyncio
import logging
import uuid
from typing import Any
from typing import Literal
from typing import NoReturn

from fastapi import APIRouter
from fastapi import Depends
from fastapi import HTTPException
from fastapi import Path as FPath
from fastapi import Request
from fastapi import status
from fastapi.responses import JSONResponse
from pydantic import BaseModel
from pydantic import Field
from redis.exceptions import RedisError

from gateway.services.suspension import SUSPENSION_CACHE_TTL_SECONDS
from gateway.services.suspension import suspension_cache_key
from hippius_s3.dependencies import DBConnection
from hippius_s3.dependencies import get_postgres
from hippius_s3.models.sub_token import SS58_PATTERN
from hippius_s3.utils import get_query


logger = logging.getLogger(__name__)
# default_response_class=JSONResponse is load-bearing: the app is built with
# default_response_class=Response (hippius_s3/main.py), whose render() calls
# content.encode() and 500s on a dict/model. Every model-returning endpoint here relies
# on this router default (mirrors the explicit JSONResponse on the sub-token endpoints).
router = APIRouter(tags=["admin"], default_response_class=JSONResponse)

# Bound on the status endpoint's bucket/bytes aggregate — a 10+ TB account can push the
# SUM into tens of seconds; the endpoint degrades to null counts instead of 500ing.
STATS_QUERY_TIMEOUT_SECONDS = 15.0


# --- request / response models ----------------------------------------------


class SuspendBody(BaseModel):
    mode: Literal["full", "read_only"] = Field(
        default="full",
        description="'full' blocks all access; 'read_only' allows downloads but blocks writes.",
    )


class AccountStateResponse(BaseModel):
    account_id: str
    state: Literal["active", "suspended", "read_only"]


class AccountStatusResponse(BaseModel):
    account_id: str
    state: Literal["active", "suspended", "read_only"]
    buckets: int | None = Field(..., description="Live bucket count; null if the aggregate timed out.")
    bytes: int | None = Field(
        ...,
        description="Logical bytes across current object versions; null if the aggregate timed out.",
    )


class PurgeAcceptedResponse(BaseModel):
    job_id: str


class PurgeJobResponse(BaseModel):
    job_id: str
    account_id: str
    state: Literal["queued", "running", "done", "failed"]
    deleted_objects: int
    deleted_bytes: int = Field(
        ...,
        description="Logical bytes purged (sum of version sizes) — disk reclaim trails via unpinner+janitor.",
    )
    error: str | None = None


class ErrorDetail(BaseModel):
    code: str
    message: str


# --- helpers ---------------------------------------------------------------


def _raise(code: str, message: str, http_status: int) -> NoReturn:
    raise HTTPException(status_code=http_status, detail={"code": code, "message": message})


def _validate_account_id(account_id: str) -> None:
    if not SS58_PATTERN.match(account_id):
        _raise("InvalidArgument", f"Invalid account_id (must be SS58): {account_id}", status.HTTP_400_BAD_REQUEST)


def _mode_to_state(mode: str | None) -> Literal["active", "suspended", "read_only"]:
    if mode is None:
        return "active"
    return "suspended" if mode == "full" else "read_only"


async def _write_suspension_cache(redis_client: Any, account_id: str, mode: str | None) -> None:
    # Write-through so every gateway pod sees the change immediately (they all read the
    # same Redis). Best-effort: the DB row is the source of truth and a stale cache
    # entry expires within SUSPENSION_CACHE_TTL_SECONDS.
    try:
        if mode is None:
            await redis_client.delete(suspension_cache_key(account_id))
        else:
            await redis_client.setex(suspension_cache_key(account_id), SUSPENSION_CACHE_TTL_SECONDS, mode)
    except RedisError as exc:
        logger.warning(
            f"suspension cache: write-through failed for {account_id} "
            f"(stale entry expires in <={SUSPENSION_CACHE_TTL_SECONDS}s): {exc}"
        )


# --- endpoints --------------------------------------------------------------


@router.post(
    "/accounts/{account_id}/suspend",
    response_model=AccountStateResponse,
    responses={400: {"model": ErrorDetail, "description": "Invalid account_id."}},
    summary="Suspend an account",
    description=(
        "Immediately blocks access for EVERY credential of the account (master token, all "
        "sub-tokens, presigned URLs, bearer), regardless of installed scopes.\n\n"
        "`mode='full'` blocks everything, including anonymous reads of the account's public "
        "buckets. `mode='read_only'` allows downloads but blocks all writes.\n\n"
        "Idempotent; repeat calls (including mode changes) return the resulting state.\n\n"
        "**Auth**: admin HMAC layer (`X-HMAC-Signature` with the admin secret)."
    ),
)
async def suspend_account(
    request: Request,
    body: SuspendBody,
    account_id: str = FPath(..., description="Substrate SS58 address of the account."),
    db: DBConnection = Depends(get_postgres),
) -> AccountStateResponse:
    _validate_account_id(account_id)

    row = await db.fetchrow(get_query("upsert_account_suspension"), account_id, body.mode)
    await _write_suspension_cache(request.app.state.redis_client, account_id, body.mode)

    logger.info(f"Account suspended: account={account_id}, mode={body.mode}")
    return AccountStateResponse(account_id=account_id, state=_mode_to_state(row["mode"]))


@router.post(
    "/accounts/{account_id}/reactivate",
    response_model=AccountStateResponse,
    responses={
        400: {"model": ErrorDetail, "description": "Invalid account_id."},
        409: {"model": ErrorDetail, "description": "A purge job is queued/running for this account."},
    },
    summary="Reactivate a suspended account",
    description=(
        "Lifts the suspension. Existing sub-token scopes were never touched by the "
        "suspension and resume exactly as they were — no re-push needed.\n\n"
        "Idempotent: returns `state='active'` even if the account was not suspended. "
        "Rejected with **409** while a purge job is queued or running.\n\n"
        "**Auth**: admin HMAC layer (`X-HMAC-Signature` with the admin secret)."
    ),
)
async def reactivate_account(
    request: Request,
    account_id: str = FPath(..., description="Substrate SS58 address of the account."),
    db: DBConnection = Depends(get_postgres),
) -> AccountStateResponse:
    _validate_account_id(account_id)

    active_job = await db.fetchrow(get_query("get_active_purge_job"), account_id)
    if active_job is not None:
        _raise(
            "PurgeInProgress",
            f"Cannot reactivate: purge job {active_job['job_id']} is {active_job['state']}",
            status.HTTP_409_CONFLICT,
        )

    await db.fetchrow(get_query("delete_account_suspension"), account_id)
    await _write_suspension_cache(request.app.state.redis_client, account_id, None)

    logger.info(f"Account reactivated: account={account_id}")
    return AccountStateResponse(account_id=account_id, state="active")


@router.get(
    "/accounts/{account_id}/status",
    response_model=AccountStatusResponse,
    responses={400: {"model": ErrorDetail, "description": "Invalid account_id."}},
    summary="Account suspension state + storage stats",
    description=(
        "Authoritative suspension state for the staff cockpit, plus live bucket count and "
        "logical bytes (current object versions). On very large accounts the aggregate can "
        "exceed its query timeout — `buckets`/`bytes` come back null rather than failing.\n\n"
        "**Auth**: admin HMAC layer (`X-HMAC-Signature` with the admin secret)."
    ),
)
async def account_status(
    account_id: str = FPath(..., description="Substrate SS58 address of the account."),
    db: DBConnection = Depends(get_postgres),
) -> AccountStatusResponse:
    _validate_account_id(account_id)

    suspension = await db.fetchrow(get_query("get_account_suspension"), account_id)

    buckets: int | None
    total_bytes: int | None
    try:
        stats = await db.fetchrow(get_query("get_admin_account_stats"), account_id, timeout=STATS_QUERY_TIMEOUT_SECONDS)
        buckets = int(stats["buckets"])
        total_bytes = int(stats["bytes"])
    except asyncio.TimeoutError:
        logger.warning(f"admin status: stats aggregate timed out for {account_id}, returning null counts")
        buckets = None
        total_bytes = None

    return AccountStatusResponse(
        account_id=account_id,
        state=_mode_to_state(suspension["mode"] if suspension else None),
        buckets=buckets,
        bytes=total_bytes,
    )


@router.delete(
    "/accounts/{account_id}/data",
    response_model=PurgeAcceptedResponse,
    status_code=status.HTTP_202_ACCEPTED,
    responses={400: {"model": ErrorDetail, "description": "Invalid account_id."}},
    summary="Purge ALL account data (async)",
    description=(
        "Queues server-side deletion of every bucket and object owned by the account and "
        "returns immediately with a job id to poll on `GET /admin/purge-jobs/{job_id}`.\n\n"
        "Implies a `full` suspension (upserted before the job is created) — reactivation is "
        "blocked until the job finishes. Idempotent: while a job is queued/running, repeat "
        "calls return the same job_id. Sub-token scope rows for the account are deleted as "
        "part of the purge; the backend re-provisions credentials if the user returns.\n\n"
        "**Auth**: admin HMAC layer (`X-HMAC-Signature` with the admin secret)."
    ),
)
async def purge_account_data(
    request: Request,
    account_id: str = FPath(..., description="Substrate SS58 address of the account."),
    db: DBConnection = Depends(get_postgres),
) -> PurgeAcceptedResponse:
    _validate_account_id(account_id)

    await db.fetchrow(get_query("upsert_account_suspension"), account_id, "full")
    await _write_suspension_cache(request.app.state.redis_client, account_id, "full")

    job_id = str(uuid.uuid4())
    inserted = await db.fetchrow(get_query("insert_purge_job"), job_id, account_id)
    if inserted is None:
        # Lost the race (or a job already exists) — the partial unique index guarantees
        # exactly one live job per account; return it.
        existing = await db.fetchrow(get_query("get_active_purge_job"), account_id)
        job_id = str(existing["job_id"])
        logger.info(f"Purge already in flight: account={account_id}, job={job_id}")
        return PurgeAcceptedResponse(job_id=job_id)

    logger.info(f"Purge job created: account={account_id}, job={job_id}")
    return PurgeAcceptedResponse(job_id=job_id)


@router.get(
    "/purge-jobs/{job_id}",
    response_model=PurgeJobResponse,
    responses={
        400: {"model": ErrorDetail, "description": "Invalid job_id."},
        404: {"model": ErrorDetail, "description": "Unknown job_id."},
    },
    summary="Purge job status",
    description=(
        "Progress of an account purge. `deleted_bytes` is logical bytes purged; physical "
        "backend deletion and disk reclaim continue asynchronously after `state='done'`.\n\n"
        "**Auth**: admin HMAC layer (`X-HMAC-Signature` with the admin secret)."
    ),
)
async def purge_job_status(
    job_id: str = FPath(..., description="Job id returned by DELETE /admin/accounts/{account_id}/data."),
    db: DBConnection = Depends(get_postgres),
) -> PurgeJobResponse:
    try:
        parsed_job_id = uuid.UUID(job_id)
    except ValueError:
        _raise("InvalidArgument", f"Invalid job_id (must be a UUID): {job_id}", status.HTTP_400_BAD_REQUEST)

    row = await db.fetchrow(get_query("get_purge_job"), parsed_job_id)
    if row is None:
        _raise("NoSuchJob", f"Unknown purge job: {job_id}", status.HTTP_404_NOT_FOUND)

    return PurgeJobResponse(
        job_id=str(row["job_id"]),
        account_id=row["account_id"],
        state=row["state"],
        deleted_objects=int(row["deleted_objects"]),
        deleted_bytes=int(row["deleted_bytes"]),
        error=row["error"],
    )
