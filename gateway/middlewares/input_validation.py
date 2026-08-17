"""Input validation middleware for S3 operations.

Validates bucket names, object keys, and metadata headers at the gateway
level before body streaming begins, saving bandwidth on invalid requests.
"""

import logging
import re
from typing import Awaitable
from typing import Callable

from fastapi import Request
from fastapi import Response
from substrateinterface.utils.ss58 import is_valid_ss58_address

from gateway.utils.errors import s3_error_response
from gateway.utils.paths import collapse_dot_segments
from gateway.utils.paths import decoded_path
from gateway.utils.paths import forwarded_path
from hippius_s3.config import get_config
from hippius_s3.peer_auth import is_authorized_peer_fetch
from hippius_s3.reserved_bucket_names import RESERVED_BUCKET_SEGMENTS


logger = logging.getLogger(__name__)
config = get_config()


# Moved to gateway/utils/paths.py so every middleware that keys a security decision off the
# first path segment shares one decoder. Aliased rather than renamed: the call sites below and
# the tests reference `_decoded_path`.
_decoded_path = decoded_path


# S3 bucket name validation (AWS S3 compatible)
# Must be 3-63 characters, lowercase letters/numbers/dots/hyphens only
# Must start and end with letter or number, no adjacent dots, no IP format
BUCKET_NAME_PATTERN = re.compile(r"^[a-z0-9][a-z0-9\.\-]*[a-z0-9]$")
IP_ADDRESS_PATTERN = re.compile(r"^\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}$")

# Object key characters to avoid (non-printable ASCII and problematic chars).
#
# `#` and `?` are here for the same concrete reason rather than as style: `ForwardService`
# interpolates the decoded path into a URL *string*, which httpx then re-parses, and both are
# delimiters there. A key sent as `report%3Fv1.txt` arrives at the api as `report` — so
# `report%3Fv1.txt` and `report%3Fv2.txt` are two distinct keys that both answer 200 and land on
# one object. `#` was already covered; `?` behaves identically and was not.
OBJECT_KEY_AVOID_CHARS = (
    ["\\", "{", "}", "^", "%", "`", "[", "]", '"', "<", ">", "~", "#", "?", "|"]
    + [chr(i) for i in range(0, 32)]
    + [chr(127)]
)

# Prohibited bucket name prefixes and suffixes (AWS S3 standard)
PROHIBITED_BUCKET_PREFIXES = ["xn--", "sthree-", "amzn-s3-demo-"]
PROHIBITED_BUCKET_SUFFIXES = ["-s3alias", "--ol-s3", ".mrap", "--x-s3", "--table-s3"]

SKIP_PREFIXES = {"health", "user", "docs", "robots.txt", "openapi.json"}

# Re-exported so this module keeps reading as the place bucket-name policy is enforced. The set
# itself is defined in hippius_s3/reserved_bucket_names.py — the audit script needs it too, and a
# second copy drifting is the exact failure this rejection exists to prevent.
#
# Deliberately NOT in it: `acl` (acl_router mounts /{bucket} at the ROOT — there is no /acl path)
# and `static` (the gateway has no StaticFiles mount, and the api's is registered AFTER
# s3_router_new, so the S3 catch-all shadows it rather than the reverse).
#
# auth_router.ALL_EXEMPT_SEGMENTS must stay a subset — enforced by
# test_every_auth_exempt_segment_is_a_reserved_bucket_name.
__all__ = ["RESERVED_BUCKET_SEGMENTS", "input_validation_middleware"]


async def input_validation_middleware(
    request: Request,
    call_next: Callable[[Request], Awaitable[Response]],
) -> Response:
    # Peer chunk fetches are the one legitimate consumer of the reserved `internal`
    # segment. Pre-merge they reached the api directly, bypassing the gateway; in the
    # merged app they present the peer secret and skip the S3 pipeline. Fail-closed:
    # anything without the valid secret faces the `internal` rejection below unchanged.
    if is_authorized_peer_fetch(request):
        return await call_next(request)

    """Validate S3 inputs for security and AWS compatibility."""

    decoded = _decoded_path(request)

    # ROUTING view: the path the api will SEE. httpx both collapses dot segments and truncates at
    # `#`/`?` when the forwarder builds the outgoing URL, so `/anybucket/../internal/...` and
    # `/internal%23x/parts/1` both reach the api as an `internal` first segment while their
    # as-sent first segment is an innocuous bucket name. Every bucket-name and reserved-name
    # decision below keys off the first segment, so all of them must use this.
    path_parts = forwarded_path(decoded).strip("/").split("/")

    # A literal `?` or `#` in the DECODED path means the client percent-encoded one, because
    # uvicorn has already split the real query off at the first raw `?`. Both are delimiters to
    # the URL string `ForwardService` builds, so httpx re-reads them: the path the api routes on
    # is truncated there, and everything after a `?` becomes a QUERY on the forwarded request.
    #
    # That second half is the reason this check is here rather than only on the key. The gateway's
    # own `request.query_params` comes from `scope["query_string"]` and stays empty, so a
    # subresource smuggled this way is invisible to every layer that keys off the query — which
    # includes `acl.py`'s CreateBucket shape (`len(query_params) == 0`) and `get_required_permission`,
    # both of which then judge a different operation from the one the api performs.
    #
    # Refusing the whole class up front is much cheaper to reason about than teaching each of
    # those layers to model the rewrite, and it costs nothing legitimate: no gateway route and no
    # creatable bucket name contains either character, and an object key that does is already
    # silently truncated today (see OBJECT_KEY_AVOID_CHARS) rather than stored intact — so this
    # turns a quiet overwrite into a 400.
    for delimiter in ("?", "#"):
        if delimiter in decoded:
            logger.warning("Request path rejected at gateway: contains %r", delimiter)
            return s3_error_response(
                code="InvalidURI",
                message=f"Request path contains an invalid character: {delimiter!r}",
                status_code=400,
            )

    # Validate bucket name only on CreateBucket (PUT /{bucket} with no object key and no
    # tagging/lifecycle/policy query params). Existing buckets with non-compliant names
    # (uppercase, SS58 addresses, etc.) must remain accessible for all other operations.
    is_create_bucket = (
        request.method == "PUT"
        and len(path_parts) == 1
        and path_parts[0]
        and "tagging" not in request.query_params
        and "lifecycle" not in request.query_params
        and "policy" not in request.query_params
    )

    # Reserved-name rejection must run BEFORE the SKIP_PREFIXES bypass: PUT /docs is
    # CreateBucket-shaped, and skipping it is exactly how the ownerless "docs" bucket
    # got written.
    if is_create_bucket and path_parts[0] in RESERVED_BUCKET_SEGMENTS:
        return s3_error_response(
            code="InvalidBucketName",
            message=f"Bucket name '{path_parts[0]}' is reserved for gateway routes",
            status_code=400,
        )

    # `internal` is refused on EVERY method, not just the CreateBucket shape above. The api
    # mounts /internal/parts/... ahead of its S3 catch-all, and the request that reached it was
    # a plain GET: anonymous auth succeeds, and the ACL middleware passes through on a bucket
    # nobody owns. So the write-shaped check above never saw it.
    #
    # Defence in depth, not the boundary — the api requires a shared secret on that route, and
    # this is a blocklist, so it protects exactly the one prefix somebody thought to add and
    # will rot the moment another internal route lands. The durable fix is a second port for
    # internal routes with no gateway route to it; until then, anything mounted on the api app
    # outside the S3 namespace needs a line here.
    if path_parts[0] == "internal":
        return s3_error_response(
            code="InvalidBucketName",
            message="Bucket name 'internal' is reserved for gateway routes",
            status_code=400,
        )

    # The api decodes the forwarded target a SECOND time, so an escape that survives the gateway's
    # single decode disappears at the api: a client's `/%2569nternal/parts/1` is judged here as
    # first segment `%69nternal` (not `internal`, so the denylist above passes it), httpx puts
    # `%69nternal` on the wire untouched, and the api's uvicorn unquotes it to `internal` — this
    # time reaching the peer-serve route, not just the S3 catch-all. `int%65rnal` does it from the
    # middle, so no prefix or spelling check closes this; only refusing `%` outright does.
    #
    # Refusing it costs nothing legitimate. A bucket name is `[a-z0-9.-]` or an SS58 address, and
    # no gateway route contains a `%` — a first segment with one could not have been created
    # through BUCKET_NAME_PATTERN below. `%` stays allowed in an object key exactly as before
    # (OBJECT_KEY_AVOID_CHARS rejects it there, on the key view).
    #
    # Modelling the double-decode inside `forwarded_path` was the alternative and is worse: it
    # would decode object keys twice too, turning a key sent as `a%252Fb` — rejected today for
    # containing `%` — into the accepted key `a/b`.
    if "%" in path_parts[0]:
        return s3_error_response(
            code="InvalidBucketName",
            message="Bucket name cannot contain '%'",
            status_code=400,
        )

    # Skip validation for non-S3 endpoints
    if path_parts[0] in SKIP_PREFIXES:
        return await call_next(request)

    if is_create_bucket:
        bucket_name = path_parts[0]

        # Length check
        if len(bucket_name) < config.min_bucket_name_length:
            return s3_error_response(
                code="InvalidBucketName",
                message=f"Bucket name too short (minimum {config.min_bucket_name_length} characters)",
                status_code=400,
            )
        if len(bucket_name) > config.max_bucket_name_length:
            return s3_error_response(
                code="InvalidBucketName",
                message=f"Bucket name too long (maximum {config.max_bucket_name_length} characters)",
                status_code=400,
            )

        # SS58 addresses bypass format checks — ownership is verified downstream
        # in bucket_create_endpoint.py
        if not is_valid_ss58_address(bucket_name):
            # Character and format validation
            if not BUCKET_NAME_PATTERN.match(bucket_name):
                return s3_error_response(
                    code="InvalidBucketName",
                    message="Bucket name contains invalid characters or format",
                    status_code=400,
                )

            # Check for adjacent periods
            if ".." in bucket_name:
                return s3_error_response(
                    code="InvalidBucketName",
                    message="Bucket name cannot contain adjacent periods",
                    status_code=400,
                )

            # Check if formatted like IP address
            if IP_ADDRESS_PATTERN.match(bucket_name):
                return s3_error_response(
                    code="InvalidBucketName",
                    message="Bucket name cannot be formatted like an IP address",
                    status_code=400,
                )

            # Check prohibited prefixes
            for prefix in PROHIBITED_BUCKET_PREFIXES:
                if bucket_name.startswith(prefix):
                    return s3_error_response(
                        code="InvalidBucketName",
                        message=f"Bucket name cannot start with '{prefix}'",
                        status_code=400,
                    )

            # Check prohibited suffixes
            for suffix in PROHIBITED_BUCKET_SUFFIXES:
                if bucket_name.endswith(suffix):
                    return s3_error_response(
                        code="InvalidBucketName",
                        message=f"Bucket name cannot end with '{suffix}'",
                        status_code=400,
                    )

    # KEY view: deliberately NOT `path_parts`. Truncation at `#` is the data loss this check
    # exists to prevent — `report%23v1.txt` and `report%23v2.txt` both forward as `report`, so one
    # silently overwrites the other. Judging the truncated key would see no `#` and wave both
    # through. Dot segments still collapse, because that rewrite decides the key that lands.
    key_parts = collapse_dot_segments(decoded).strip("/").split("/")

    # Validate object key if present
    if len(key_parts) >= 2:
        object_key = "/".join(key_parts[1:])

        # Length check (max bytes for UTF-8)
        key_bytes = len(object_key.encode("utf-8"))
        if key_bytes > config.max_object_key_length:
            logger.warning(
                f"Object key rejected at gateway: {key_bytes} bytes exceeds limit of {config.max_object_key_length}"
            )
            return s3_error_response(
                code="KeyTooLongError",
                message=f"Object key too long (maximum {config.max_object_key_length} bytes)",
                status_code=400,
            )

        # Check for characters to avoid (strongly discouraged by AWS)
        for char in OBJECT_KEY_AVOID_CHARS:
            if char in object_key:
                char_desc = repr(char) if ord(char) >= 32 else f"ASCII-{ord(char)}"
                logger.warning(f"Object key rejected at gateway: contains discouraged character {char_desc}")
                return s3_error_response(
                    code="InvalidArgument",
                    message=f"Object key contains discouraged character: {char_desc}",
                    status_code=400,
                )

    # Validate metadata headers
    for header_name, header_value in request.headers.items():
        if header_name.lower().startswith("x-amz-meta-") and len(header_value) > config.max_metadata_size:
            return s3_error_response(
                code="MetadataTooLarge",
                message="Metadata value too large",
                status_code=400,
            )

    return await call_next(request)
