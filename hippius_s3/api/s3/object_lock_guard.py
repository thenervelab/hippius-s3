"""Object Lock guard for surface that is still Tier 2 (per-object retention / legal hold).

S3 Object Lock has three tiers in this repo (see specs/s3-object-lock.md):

- Tier 0 (shipped): clean `501 NotImplemented` for everything.
- Tier 1 (shipped): bucket-level `?object-lock` and `x-amz-bucket-object-lock-enabled`
  are real endpoints — handled by `bucket_object_lock_endpoint`. These NO LONGER trip
  this guard.
- Tier 2 (still open): per-object retention / legal hold and the per-object
  `x-amz-object-lock-*` headers on PutObject / CreateMultipartUpload. Those continue to
  return 501 here.

This helper centralises the Tier 2 detection so each entry point can make a single
one-line call.
"""

from __future__ import annotations

from typing import Final

from fastapi import Request
from fastapi import Response

from hippius_s3.api.s3 import errors


_NOT_IMPLEMENTED_MESSAGE: Final[str] = (
    "S3 Object Lock per-object retention / legal hold is not supported by this "
    "implementation. Bucket-level Object Lock configuration (Tier 1) is supported. "
    "See specs/s3-object-lock.md."
)

_QUERY_SUBRESOURCES: Final[frozenset[str]] = frozenset({"retention", "legal-hold"})
_OBJECT_LOCK_HEADERS: Final[frozenset[str]] = frozenset(
    {
        "x-amz-object-lock-mode",
        "x-amz-object-lock-retain-until-date",
        "x-amz-object-lock-legal-hold",
    }
)


def maybe_object_lock_not_implemented_response(request: Request) -> Response | None:
    """Return a 501 NotImplemented S3 XML error if the request touches Tier 2 surface.

    Detected signals:
    - Query subresources: `?retention`, `?legal-hold`.
    - Per-object headers: `x-amz-object-lock-mode`, `x-amz-object-lock-retain-until-date`,
      `x-amz-object-lock-legal-hold`.

    `x-amz-bypass-governance-retention` is intentionally NOT a trigger — it is a no-op
    in the absence of any lock state and harmlessly accompanies normal DELETE traffic.
    The bucket-level `?object-lock` subresource and the
    `x-amz-bucket-object-lock-enabled` header are intentionally NOT caught here either —
    Tier 1 implements them in `bucket_object_lock_endpoint` and `bucket_create_endpoint`.

    Returns None when no Tier 2 Object Lock signal is present.
    """
    if any(name in request.query_params for name in _QUERY_SUBRESOURCES):
        return _not_implemented()

    for header in _OBJECT_LOCK_HEADERS:
        if request.headers.get(header):
            return _not_implemented()

    return None


def _not_implemented() -> Response:
    return errors.s3_error_response(
        "NotImplemented",
        _NOT_IMPLEMENTED_MESSAGE,
        status_code=501,
    )
