"""Input validation middleware for S3 operations."""

import re
from typing import Awaitable
from typing import Callable

from fastapi import Request
from fastapi import Response

from hippius_s3.api.s3.errors import s3_error_response
from hippius_s3.config import get_config


config = get_config()

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
)  # Non-printable ASCII

# Prohibited bucket name prefixes and suffixes (AWS S3 standard)
PROHIBITED_BUCKET_PREFIXES = ["xn--", "sthree-", "amzn-s3-demo-"]
PROHIBITED_BUCKET_SUFFIXES = ["-s3alias", "--ol-s3", ".mrap", "--x-s3", "--table-s3"]


async def input_validation_middleware(
    request: Request,
    call_next: Callable[[Request], Awaitable[Response]],
) -> Response:
    """Validate S3 inputs for security and AWS compatibility."""

    path_parts = request.url.path.strip("/").split("/")

    # Skip validation for non-S3 endpoints
    if path_parts[0] in [
        "user",
        "health",
        "robots.txt",
        "docs",
        "openapi.json",
    ]:
        return await call_next(request)

    # Validate bucket name if present
    if len(path_parts) >= 1 and path_parts[0]:
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

        # Length check (max 1024 bytes for UTF-8)
        if len(object_key.encode("utf-8")) > config.max_object_key_length:
            return s3_error_response(
                code="KeyTooLongError",
                message="Object key too long (maximum 1024 bytes)",
                status_code=400,
            )

        # Check for characters to avoid (strongly discouraged by AWS)
        for char in OBJECT_KEY_AVOID_CHARS:
            if char in object_key:
                char_desc = repr(char) if ord(char) >= 32 else f"ASCII-{ord(char)}"
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
