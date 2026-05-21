"""Tier 0 Object Lock guard.

S3 Object Lock is not implemented (see specs/s3-object-lock.md). Any request that touches
the Object Lock surface — bucket / object subresources, or per-object/per-bucket
`x-amz-object-lock-*` headers — must return `501 NotImplemented` rather than silently
falling through to CreateBucket / ListObjects / PutObject, which yields misleading
behavior (the prod gateway today returns `409 BucketAlreadyExists` for
`PUT /bucket?object-lock` because the query string is dropped on the floor).

This helper centralises the detection and the error response so every entry point can
make a single one-line call.
"""

from __future__ import annotations

from typing import Final

from fastapi import Request
from fastapi import Response

from hippius_s3.api.s3 import errors


_NOT_IMPLEMENTED_MESSAGE: Final[str] = (
    "S3 Object Lock is not supported by this implementation. See specs/s3-object-lock.md for the planned roadmap."
)

_QUERY_SUBRESOURCES: Final[frozenset[str]] = frozenset({"object-lock", "retention", "legal-hold"})
_OBJECT_LOCK_HEADERS: Final[frozenset[str]] = frozenset(
    {
        "x-amz-object-lock-mode",
        "x-amz-object-lock-retain-until-date",
        "x-amz-object-lock-legal-hold",
    }
)
_BUCKET_LOCK_ENABLED_HEADER: Final[str] = "x-amz-bucket-object-lock-enabled"


def maybe_object_lock_not_implemented_response(request: Request) -> Response | None:
    """Return a 501 NotImplemented S3 XML error if the request touches Object Lock.

    Detected signals:
    - Query subresources: `?object-lock`, `?retention`, `?legal-hold`.
    - Per-object headers: `x-amz-object-lock-mode`, `x-amz-object-lock-retain-until-date`,
      `x-amz-object-lock-legal-hold`.
    - Per-bucket header: `x-amz-bucket-object-lock-enabled: true` (value
      compared case-insensitively). boto3 exposes this as
      `CreateBucket(ObjectLockEnabledForBucket=True)`.

    `x-amz-bypass-governance-retention` is intentionally NOT a trigger — it is a no-op
    in the absence of any lock state and harmlessly accompanies normal DELETE traffic.
    Returns None when no Object Lock signal is present.
    """
    if any(name in request.query_params for name in _QUERY_SUBRESOURCES):
        return _not_implemented()

    for header in _OBJECT_LOCK_HEADERS:
        if request.headers.get(header):
            return _not_implemented()

    bucket_lock_enabled = request.headers.get(_BUCKET_LOCK_ENABLED_HEADER)
    if bucket_lock_enabled and bucket_lock_enabled.strip().lower() == "true":
        return _not_implemented()

    return None


def _not_implemented() -> Response:
    return errors.s3_error_response(
        "NotImplemented",
        _NOT_IMPLEMENTED_MESSAGE,
        status_code=501,
    )
