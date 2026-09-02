from typing import Awaitable
from typing import Callable
from urllib.parse import unquote

from fastapi import Request
from fastapi import Response

from hippius_s3.config import get_config
from hippius_s3.gateway.middlewares.auth_probe import is_valid_auth_probe
from hippius_s3.gateway.services.sub_token_scope import OP_LIST_BUCKETS
from hippius_s3.gateway.services.sub_token_scope import OP_READ_OBJECT
from hippius_s3.gateway.services.sub_token_scope import bucket_in_scope
from hippius_s3.gateway.services.sub_token_scope import evaluate as evaluate_sub_token_scope
from hippius_s3.gateway.services.sub_token_scope import permission_allows
from hippius_s3.gateway.services.sub_token_scope_cache import get_cached_sub_token_scope
from hippius_s3.gateway.services.suspension import get_account_suspension
from hippius_s3.gateway.services.suspension import suspension_blocks
from hippius_s3.gateway.utils.accounts import is_sentinel_account_id
from hippius_s3.gateway.utils.errors import s3_error_response
from hippius_s3.gateway.utils.paths import routing_path
from hippius_s3.models.acl import Permission
from hippius_s3.peer_auth import is_authorized_peer_fetch
from hippius_s3.services.ray_id_service import get_logger_with_ray_id


def _parse_copy_source_bucket(header_value: str) -> str | None:
    """Extract the source bucket from an `x-amz-copy-source` header.

    Accepted formats (per AWS S3 SDKs):
      - `/sourcebucket/sourcekey`
      - `sourcebucket/sourcekey`
      - `sourcebucket/sourcekey?versionId=v1`

    The ARN form (`arn:aws:s3:::bucket/key`) is not supported and returns None.
    The bucket portion is never URL-encoded in real-world traffic — only the
    key — so we don't need to decode.
    """
    if not header_value or header_value.startswith("arn:"):
        return None
    val = header_value.lstrip("/").split("?", 1)[0]
    parts = val.split("/", 1)
    if not parts or not parts[0]:
        return None
    return parts[0]


def parse_copy_source(header_value: str) -> tuple[str | None, str | None]:
    """Split `x-amz-copy-source` into (bucket, key), exactly as the HANDLERS split it.

    This must mirror `copy_helpers.parse_copy_source` and the inline parse in
    `multipart.upload_part`: **percent-decode first, then strip the leading slash, then split.**
    Deriving the bucket any other way reintroduces the split-view class one layer up — a header
    of `victim%2Fkey` reads as the (nonexistent) bucket `victim%2Fkey` under a decode-last
    parser while both handlers read it as bucket `victim`, so the authorised resource and the
    accessed resource are different objects.

    Note the ARN form is deliberately NOT special-cased here: neither handler recognises it, so
    treating it as unparseable while they treat it as a literal bucket name is itself a
    disagreement. Let it through as the name they will use.
    """
    if not header_value:
        return None, None
    src = unquote(header_value.strip()).lstrip("/")
    bucket, sep, key = src.partition("/")
    if not sep or not bucket:
        return None, None
    return bucket, (key.split("?", 1)[0] or None)


def parse_s3_path(path: str) -> tuple[str | None, str | None]:
    """
    Parse S3 path into bucket and key components.

    Returns:
        (bucket, key) tuple where:
        - (None, None) for root path /
        - (bucket, None) for bucket-only paths
        - (bucket, key) for object paths
    """
    if path == "/" or path == "":
        return None, None

    path_stripped = path.lstrip("/")
    if not path_stripped:
        return None, None

    parts = path_stripped.split("/", 1)
    bucket = parts[0] if parts else None
    key = parts[1] if len(parts) > 1 else None

    # A trailing slash is not an object key. Every caller asks `key is not None` to mean
    # "this is an object operation", so an empty key would make `/bucket/` evaluate as one:
    # `is_create_bucket` stops firing and bucket ops map to object ops in the sub-token
    # scope check. Belt to trailing_slash_normalizer's braces — either alone closes it.
    if key is not None and key.strip("/") == "":
        key = None

    return bucket, key


# Query params that select a bucket SUBRESOURCE on PUT — i.e. make the request something other
# than CreateBucket. Must stay in step with the dispatch in api/s3/buckets/router.py: judging
# CreateBucket as "no query params at all" while the router treats any unrecognised param as a
# create meant `PUT /new?x=1` skipped this middleware's CreateBucket branch entirely.
#
# Adding a name here without a matching router branch is NOT safe: the name stops being a create
# shape, so `PUT /nonexistent?<name>` skips the CreateBucket guards (the sentinel-account check)
# and still lands on handle_create_bucket. The two lists move together, in that order, and
# test_subresource_set_covers_every_branch_the_put_router_dispatches pins them.
#
# `retention` and `legal-hold` are the deliberate exception to that rule, and safe only because
# maybe_object_lock_not_implemented_response is the FIRST statement in create_or_modify_bucket:
# they 501 before the create fallthrough, so they can never reach handle_create_bucket with its
# guards skipped. They are listed because grading them (below) closes only half the gate —
# is_create_bucket_shape is the other half, and a param missing from THIS set takes the
# CreateBucket bypass and never reaches the permission check at all. That asymmetry is exactly how
# ?versioning shipped ungated: it WAS graded, and still bypassed. Listing them now means the Tier 2
# surface cannot land ungated whichever half a future change forgets, and it stops
# `PUT /someone-elses-bucket?retention` distinguishing 501-from-403 as a bucket-existence oracle.
BUCKET_PUT_SUBRESOURCES = frozenset(
    {"acl", "tagging", "lifecycle", "policy", "cors", "versioning", "object-lock", "retention", "legal-hold"}
)


def is_create_bucket_shape(method: str, key: str | None, query_params: dict) -> bool:
    return method == "PUT" and key is None and not (BUCKET_PUT_SUBRESOURCES & query_params.keys())


def get_required_permission(
    method: str,
    query_params: dict,
    has_key: bool,
) -> Permission:
    """
    Determine required permission from HTTP method and query parameters.

    Args:
        method: HTTP method (GET, PUT, POST, DELETE, HEAD)
        query_params: Query parameters dict
        has_key: Whether request has an object key

    Returns:
        Required Permission enum value
    """
    if "acl" in query_params:
        return Permission.READ_ACP if method == "GET" else Permission.WRITE_ACP

    # `policy` is an access-control operation, not a data one: PUT ?policy replaces the bucket
    # ACL (see bucket_policy_endpoint.set_bucket_policy). Falling through to the method-only
    # mapping below graded it WRITE, so a write-only grantee could publish a bucket to AllUsers.
    if "policy" in query_params:
        return Permission.READ_ACP if method in ("GET", "HEAD") else Permission.WRITE_ACP

    if "tagging" in query_params:
        return Permission.READ_ACP if method in ["GET", "HEAD"] else Permission.WRITE_ACP

    # `versioning` is bucket CONFIGURATION, not data. AWS has no ACL grant that confers it — the
    # bucket ACL vocabulary is only READ / WRITE / READ_ACP / WRITE_ACP / FULL_CONTROL, and
    # s3:PutBucketVersioning has to come from an IAM or bucket policy. Grading it by the
    # method-only mapping below would say WRITE, i.e. "may create objects", and let anyone who can
    # upload turn versioning on. Enabling it is also irreversible here (Suspended is a 501), and it
    # changes DELETE semantics for every key in the bucket, so it belongs with the other _ACP ops.
    if "versioning" in query_params:
        return Permission.READ_ACP if method in ("GET", "HEAD") else Permission.WRITE_ACP

    # Object Lock is a WORM control: turning it on makes objects undeletable for the duration of a
    # retention period, so mis-grading it is a durability/ransom problem, not just a config leak.
    # AWS gates the bucket configuration behind s3:PutBucketObjectLockConfiguration and the
    # per-object surface behind s3:PutObjectRetention / s3:PutObjectLegalHold — all distinct from
    # s3:PutObject, i.e. "may upload" must never imply "may set retention".
    #
    # `retention` / `legal-hold` are Tier 2 and answered 501 by object_lock_guard today. They are
    # graded here anyway so the gate is already correct when that surface lands, rather than
    # defaulting to WRITE the moment a handler appears — which is exactly how ?versioning shipped
    # ungated.
    if "object-lock" in query_params or "retention" in query_params or "legal-hold" in query_params:
        return Permission.READ_ACP if method in ("GET", "HEAD") else Permission.WRITE_ACP

    if "uploads" in query_params or "uploadId" in query_params:
        return Permission.WRITE

    if method in ["GET", "HEAD"]:
        return Permission.READ

    if method in ["PUT", "POST", "DELETE"]:
        return Permission.WRITE

    raise ValueError(f"Unknown HTTP method: {method}")


def _access_denied() -> Response:
    return s3_error_response(code="AccessDenied", message="Access Denied", status_code=403)


async def acl_middleware(
    request: Request,
    call_next: Callable[[Request], Awaitable[Response]],
) -> Response:
    """
    ACL enforcement middleware that checks S3 permissions before forwarding requests.

    Blocks unauthorized requests with 403 AccessDenied.
    Allows requests that pass ACL checks to continue to backend.
    """
    ray_id = getattr(request.state, "ray_id", "no-ray-id")
    logger = get_logger_with_ray_id(__name__, ray_id)

    # The bucket and key the api will ACT on. `request.url.path` is the wrong lens twice over: it is
    # uncollapsed, so `/docs/../anybucket/key` made this evaluate permissions for the bucket `docs`
    # (ownerless in prod) while the api served `anybucket`; and it is fragment-truncated, so it
    # disagrees with the key `input_validation` judged.
    path = routing_path(request)

    # `/health` is a real route, but it is also a valid `/{bucket_name}` shape, and the S3
    # routers bind DELETE/POST on that shape while the health route is GET-only. Bypassing the
    # ACL check for every method therefore handed the destructive bucket handlers — which carry
    # no ownership predicate of their own — an unauthenticated caller. `/user/` and `/admin/`
    # are prefix-matched and sit behind their own HMAC middlewares; `/health` has no such
    # backstop, so it is scoped to the methods its route actually serves.
    if (request.method in ("GET", "HEAD") and path == "/health") or path.startswith(("/user/", "/admin/")):
        return await call_next(request)

    # Secret-authenticated peer chunk fetches carry no S3 permission model
    # (see input_validation for the rationale).
    if is_authorized_peer_fetch(request):
        return await call_next(request)

    if request.method == "OPTIONS":
        return await call_next(request)

    # PURGE from gateway → ATS authproxy → here. The probe secret is the
    # trust boundary. acl can't compute a required permission for PURGE
    # anyway (get_required_permission raises). Skip; auth_probe (innermost)
    # returns 200 so ATS proceeds with the actual cache invalidation.
    if request.method == "PURGE" and is_valid_auth_probe(request):
        return await call_next(request)

    bucket, key = parse_s3_path(path)
    request.state.s3_bucket = bucket
    request.state.s3_key = key
    query_params = dict(request.query_params)

    auth_method = getattr(request.state, "auth_method", None)
    token_type = getattr(request.state, "token_type", None)
    account_id = getattr(request.state, "account_id", None)
    access_key = (
        getattr(request.state, "access_key", None) if auth_method in ("access_key", "bearer_access_key") else None
    )

    acl_service = request.app.state.acl_service

    # Resolve bucket ownership / id / warm-cache flag in a single query when a bucket is in play.
    bucket_owner_id: str | None = None
    bucket_id: str | None = None
    if bucket is not None:
        lookup = await acl_service.get_bucket_owner_and_id(bucket)
        if lookup is not None:
            bucket_owner_id = lookup.owner_id
            bucket_id = lookup.bucket_id
            request.state.bucket_is_cache_warm = lookup.is_cache_warm
            request.state.bucket_owner_id = bucket_owner_id
            # Forwarded to the API so it can skip its own get_bucket_by_name lookup.
            request.state.bucket_id = bucket_id
        else:
            request.state.bucket_is_cache_warm = False

    # Bucket-owner suspension check (issue #421). The suspension_middleware already
    # covers requests authenticated AS the suspended account, so skip the lookup when the
    # requester IS the owner; this branch exists only for everyone ELSE touching the
    # suspended owner's buckets — anonymous public reads and cross-account (contractor)
    # access — which carry a different (or no) identity. 'full' blocks all access to the
    # owner's data; 'read_only' still blocks writes so a delinquent account's storage
    # cannot keep growing via bucket-ACL grants.
    if bucket_owner_id is not None and bucket_owner_id != account_id:
        owner_suspension = await get_account_suspension(
            bucket_owner_id,
            request.app.state.postgres_pool,
            request.app.state.redis_client,
        )
        if owner_suspension is not None and suspension_blocks(
            owner_suspension,
            method=request.method,
            query_params=query_params,
            has_key=key is not None,
        ):
            logger.info(
                f"Blocked request on suspended owner's bucket: owner={bucket_owner_id}, "
                f"bucket={bucket}, mode={owner_suspension}"
            )
            return _access_denied()

    # CopyObject and UploadPartCopy name their SOURCE in a header. Every permission check in
    # this middleware is derived from the request PATH, which describes only the destination —
    # so the source was never authorised at all, and the handlers resolve it by name with no
    # ownership predicate (multipart.py) or scoped to the DESTINATION owner (copy_helpers.py).
    # A write grant on a bucket you control was therefore enough to read any object in any
    # bucket in the system. This runs ahead of every branch below, including the sub-token and
    # master-token bypasses, because each of those returns early on the destination alone.
    if key is not None:
        copy_source = request.headers.get("x-amz-copy-source")
        if copy_source:
            src_bucket, src_key = parse_copy_source(copy_source)
            if src_bucket is None:
                logger.info(f"Rejecting unparseable x-amz-copy-source: {copy_source!r}")
                return _access_denied()

            src_lookup = await acl_service.get_bucket_owner_and_id(src_bucket)
            if src_lookup is not None:
                src_allowed = await acl_service.check_permission(
                    account_id=account_id,
                    bucket=src_bucket,
                    key=src_key,
                    permission=Permission.READ,
                    access_key=access_key,
                    bucket_owner_id=src_lookup.owner_id,
                )
                if not src_allowed:
                    logger.info(
                        f"Copy source denied: account={account_id}, source={src_bucket}/{src_key}, dest={bucket}/{key}"
                    )
                    return _access_denied()
            # A source bucket that does not exist is left to the handler's NoSuchBucket.

    # -------------------------------------------------------------------------
    # Sub-token branch (R2-style): authoritative for intra-account requests,
    # falls through to bucket ACL grants for cross-account (contractor) access.
    # -------------------------------------------------------------------------
    if auth_method in ("access_key", "bearer_access_key") and token_type == "sub" and access_key:
        is_cross_account = bucket_owner_id is not None and bucket_owner_id != account_id
        if not is_cross_account:
            repo = request.app.state.sub_token_scope_repo
            redis_client = request.app.state.redis_client
            scope = await get_cached_sub_token_scope(access_key, repo, redis_client)

            # ListBuckets: no bucket in play.
            if bucket is None:
                if scope is None or not permission_allows(scope.permission, OP_LIST_BUCKETS):
                    logger.info(
                        f"Sub-token ListBuckets denied: account={account_id}, "
                        f"permission={scope.permission if scope else 'none'}"
                    )
                    return _access_denied()
                logger.info(f"Sub-token ListBuckets allowed: account={account_id}, permission={scope.permission}")
                return await call_next(request)

            # Bucket does not exist yet AND this is CreateBucket (PUT /bucket, no key, no query).
            # evaluate_sub_token_scope handles the OP_CREATE_BUCKET check including scope='all'.
            if bucket_owner_id is None:
                is_create_bucket = is_create_bucket_shape(request.method, key, query_params)
                if not is_create_bucket:
                    # Non-create op on a nonexistent bucket: pass through so backend returns NoSuchBucket.
                    return await call_next(request)

            allowed, reason = evaluate_sub_token_scope(
                scope=scope,
                bucket_id=bucket_id,
                method=request.method,
                has_key=key is not None,
                query_params=query_params,
            )
            logger.info(
                f"Sub-token scope check: account={account_id}, bucket={bucket}, key={key or 'None'}, "
                f"method={request.method}, result={'GRANTED' if allowed else 'DENIED'}"
                f"{'' if allowed else f' ({reason})'}"
            )
            if not allowed:
                return _access_denied()

            # CopyObject / UploadPartCopy: scope must also cover the source
            # bucket. The destination check above only validates write access
            # to the destination — without this second check, a sub-token
            # scoped to bucket B could copy data from bucket A (which it
            # cannot read) into B. Cross-account sources fall through to the
            # backend / bucket-ACL flow (existing contractor pattern).
            copy_source = request.headers.get("x-amz-copy-source")
            if copy_source and key is not None and scope is not None:
                src_bucket_name = _parse_copy_source_bucket(copy_source)
                if src_bucket_name:
                    src_lookup = await acl_service.get_bucket_owner_and_id(src_bucket_name)
                    src_owner_id = src_lookup.owner_id if src_lookup else None
                    src_bucket_id = src_lookup.bucket_id if src_lookup else None
                    if src_owner_id == account_id:
                        src_allowed = bucket_in_scope(src_bucket_id, scope) and permission_allows(
                            scope.permission, OP_READ_OBJECT
                        )
                        if not src_allowed:
                            logger.info(
                                f"Sub-token copy denied: source bucket {src_bucket_name} "
                                f"(id={src_bucket_id}) not in scope for account={account_id}"
                            )
                            return _access_denied()

            request.state.is_anonymous_access = False
            return await call_next(request)
        # cross-account sub-token falls through to check_permission below.

    # -------------------------------------------------------------------------
    # Master token + all other auth paths below here.
    # -------------------------------------------------------------------------
    if bucket is None:
        return await call_next(request)

    is_create_bucket = is_create_bucket_shape(request.method, key, query_params)
    if is_create_bucket:
        # Bypassing the ACL check here is correct — the bucket does not exist, so there is nothing
        # to authorise against. But "nothing to authorise against" is not "no identity required",
        # and a bucket stamped with a sentinel owner is owned by nobody in a way the ACL layer
        # reads as owned by anybody. AWS requires SigV4 on CreateBucket for the same reason: a
        # bucket always has a real owner. auth_orchestrator and bucket_create_endpoint already
        # refuse this; this is the earliest layer that can see it.
        if is_sentinel_account_id(account_id):
            logger.info(f"Rejecting unauthenticated CreateBucket for bucket: {bucket}")
            return _access_denied()

        # AWS S3 default: BucketOwnerEnforced enabled, ACLs disabled (since April 2023)
        x_amz_acl = request.headers.get("x-amz-acl")
        if x_amz_acl:
            logger.info(f"Rejecting CreateBucket with ACL header for bucket: {bucket}")
            return s3_error_response(
                code="InvalidBucketAclWithObjectOwnership",
                message="Bucket cannot be created with ACLs. Object Ownership is set to BucketOwnerEnforced.",
                status_code=400,
            )
        logger.info(f"Bypassing ACL check for CreateBucket: {bucket}")
        return await call_next(request)

    if bucket_owner_id is None:
        logger.info(f"Bucket not found in ACL check: {bucket}, passing through to backend for proper S3 error")
        return await call_next(request)

    request.state.bucket_owner_id = bucket_owner_id

    # Compute anonymous_read_allowed once, before any auth-bypass paths, so
    # master-token and presigned-URL reads of public objects also populate ATS cache.
    # Gated on ATS being active to avoid a Redis round-trip when there's no consumer.
    request.state.anonymous_read_allowed = False
    if get_config().ats_cache_endpoints and request.method in ("GET", "HEAD") and key is not None:
        request.state.anonymous_read_allowed = await acl_service.check_permission(
            account_id=None,
            bucket=bucket,
            key=key,
            permission=Permission.READ,
            access_key=None,
            bucket_owner_id=bucket_owner_id,
        )

    if (
        auth_method == "access_key"
        and token_type == "master"
        and not is_sentinel_account_id(account_id)
        and bucket_owner_id == account_id
    ):
        logger.info(f"Master token bypass for account {account_id} on bucket {bucket}")
        request.state.is_anonymous_access = False
        return await call_next(request)

    permission = get_required_permission(
        method=request.method,
        query_params=query_params,
        has_key=key is not None,
    )

    try:
        has_permission = await acl_service.check_permission(
            account_id=account_id,
            bucket=bucket,
            key=key,
            permission=permission,
            access_key=access_key,
            bucket_owner_id=bucket_owner_id,
        )
    except ValueError as e:
        if "Bucket not found" in str(e):
            logger.info(f"Bucket not found in ACL check: {bucket}, passing through to backend for proper S3 error")
            return await call_next(request)
        raise

    if not has_permission:
        logger.info(f"Access denied: account={account_id}, bucket={bucket}, key={key}, permission={permission.value}")
        return _access_denied()

    is_anonymous = account_id is None or account_id == "anonymous"
    request.state.is_anonymous_access = is_anonymous

    response = await call_next(request)

    if is_anonymous:
        response.headers["x-hippius-access-mode"] = "anon"

    return response
