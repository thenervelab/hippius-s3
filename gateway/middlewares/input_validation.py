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

from gateway.config import get_config
from gateway.utils.errors import s3_error_response
from gateway.utils.paths import collapse_dot_segments
from gateway.utils.paths import decoded_path
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

# Object key characters to avoid (non-printable ASCII and problematic chars)
OBJECT_KEY_AVOID_CHARS = (
    ["\\", "{", "}", "^", "%", "`", "[", "]", '"', "<", ">", "~", "#", "|"]
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
    """Validate S3 inputs for security and AWS compatibility."""

    # Validate the path the api will SEE, not the one the client sent. httpx collapses dot
    # segments when the forwarder builds the outgoing URL, so `/anybucket/../internal/...`
    # reaches the api as `/internal/...` — checked uncollapsed, its first segment is a bucket
    # name and every check below judges the wrong request. Collapsing is identity for any path
    # without `.`/`..` segments, and for paths with them it matches what forwarding already
    # does, so no key that survives the proxy is judged differently.
    path_parts = collapse_dot_segments(_decoded_path(request)).strip("/").split("/")

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

    # Validate object key if present
    if len(path_parts) >= 2:
        object_key = "/".join(path_parts[1:])

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
