"""Centralised error classification for backend-talking workers.

We have three operational axes (upload / download / unpin) and they each treat
some error shapes differently. The most important divergence is 404:

  - on UPLOAD:   404 = permanent (we hit the wrong URL / endpoint changed)
  - on DOWNLOAD: 404 = permanent (the object doesn't exist; treat as miss)
  - on UNPIN:    404 = transient (the pin commit hasn't landed yet upstream)

Everything else - 5xx, throttling, transport errors - classifies the same
way across the three. The shared helpers (`extract_http_status_code`,
`extract_boto_error_code`, the keyword sets, the exception-class tuples)
encode every signal we currently know how to recognise.

Return values:
  - "transient"  worth retrying with backoff (network / throttle / 5xx)
  - "permanent"  do not retry; route to DLQ for inspection
  - "billing"    Arion-only 402; cannot recover without account top-up
                 (upload classifier only - never returned for download/unpin)
  - "unknown"    didn't match any rule; route to DLQ and alert so we can
                 add the missing pattern to the right bucket
"""

from __future__ import annotations

import asyncio
import errno
import random
from typing import Any


# --------------------------------------------------------------------------- #
# Status-code + boto-code extractors
# --------------------------------------------------------------------------- #


def extract_http_status_code(error: Exception) -> str:
    """Best-effort HTTP status code from a heterogeneous exception zoo.

    Recognises:
      - httpx-style: error.response.status_code (int)
      - botocore ClientError: error.response['ResponseMetadata']['HTTPStatusCode']
      - chained: error.__cause__ might carry the underlying HTTP exception
    Returns the code as a string, or "" if none can be extracted.
    """
    resp: Any = getattr(error, "response", None)
    if isinstance(resp, dict):
        code = resp.get("ResponseMetadata", {}).get("HTTPStatusCode")
        if code:
            return str(code)
    if resp is not None and hasattr(resp, "status_code"):
        try:
            return str(resp.status_code)
        except Exception:
            pass
    cause = getattr(error, "__cause__", None)
    if cause is not None and cause is not error:
        return extract_http_status_code(cause)
    return ""


def extract_boto_error_code(error: Exception) -> str:
    """The botocore ClientError 'Error.Code' field (a short string like 'SlowDown').

    Many real S3 errors carry the actionable signal here rather than in the HTTP
    status (e.g. SlowDown is HTTP 503 but the code is what's meaningful).
    """
    resp: Any = getattr(error, "response", None)
    if isinstance(resp, dict):
        return str(resp.get("Error", {}).get("Code") or "")
    cause = getattr(error, "__cause__", None)
    if cause is not None and cause is not error:
        return extract_boto_error_code(cause)
    return ""


def is_billing_error(error: Exception) -> bool:
    """True iff the failure is Arion rejecting the upload for insufficient credit (HTTP 402)."""
    if extract_http_status_code(error) == "402":
        return True
    msg = str(error).lower()
    return "payment required" in msg or "402" in msg and "credit" in msg


# --------------------------------------------------------------------------- #
# Exception-class tuples
# --------------------------------------------------------------------------- #
# Imported lazily so that we don't make these modules a hard dependency of the
# workers module (the tests don't need httpx/botocore loaded to assert
# str-based classification).


def _transient_exception_classes() -> tuple[type, ...]:
    classes: list[type] = [asyncio.TimeoutError, ConnectionError, TimeoutError]
    try:
        import httpx

        classes.extend(
            [
                httpx.TimeoutException,
                httpx.ConnectError,
                httpx.ReadError,
                httpx.WriteError,
                httpx.RemoteProtocolError,
                httpx.PoolTimeout,
                httpx.NetworkError,
            ]
        )
    except Exception:
        pass
    try:
        from botocore.exceptions import BotoCoreError
        from botocore.exceptions import ConnectTimeoutError
        from botocore.exceptions import EndpointConnectionError
        from botocore.exceptions import ReadTimeoutError

        classes.extend([EndpointConnectionError, ConnectTimeoutError, ReadTimeoutError, BotoCoreError])
    except Exception:
        pass
    return tuple(classes)


_TRANSIENT_OSERROR_ERRNOS = frozenset(
    {
        errno.ECONNRESET,
        errno.ECONNREFUSED,
        errno.ETIMEDOUT,
        errno.EHOSTUNREACH,
        errno.ENETUNREACH,
        errno.ENETDOWN,
        errno.EPIPE,
        errno.EAGAIN,
    }
)


# --------------------------------------------------------------------------- #
# Botocore error-code (string) sets
# --------------------------------------------------------------------------- #

_TRANSIENT_BOTO_CODES = frozenset(
    {
        "SlowDown",
        "Throttling",
        "ThrottlingException",
        "ProvisionedThroughputExceededException",
        "RequestTimeout",
        "RequestTimeoutException",
        "ServiceUnavailable",
        "InternalError",
        "InternalServerError",
        "RequestTimeTooSkewed",  # clock skew - usually self-heals after NTP catches up
    }
)

_PERMANENT_BOTO_CODES = frozenset(
    {
        "NoSuchBucket",
        "NoSuchKey",  # only permanent on download/upload-of-bad-URL
        "AccessDenied",
        "AccessControlListNotSupported",
        "SignatureDoesNotMatch",
        "InvalidAccessKeyId",
        "InvalidRequest",
        "InvalidArgument",
        "InvalidPart",
        "InvalidPartOrder",
        "MalformedJSON",
        "MalformedXML",
        "MalformedACLError",
        "MissingSecurityHeader",
        "MissingRequiredParameter",
        "MethodNotAllowed",
        "NotImplemented",
        "EntityTooLarge",
        "EntityTooSmall",
        "BadDigest",
        "TokenRefreshRequired",
        "NoSuchUpload",
    }
)


# --------------------------------------------------------------------------- #
# Keyword sets
# --------------------------------------------------------------------------- #

_TRANSIENT_KEYWORDS = (
    "timeout",
    "timed out",
    "connection",
    "network",
    "unreachable",
    "reset by peer",
    "broken pipe",
    "remote disconnected",
    "server disconnected",
    "remote protocol",
    "could not connect",
    "connection refused",
    "pool exhausted",
    "pool timeout",
    "max retries exceeded",
    "all connection attempts failed",
    "deadline exceeded",
    # DNS-resolution failures — these are typically intermittent (resolver
    # blip / name-server timeout) and worth retrying.
    "no address associated",
    "name or service not known",
    "name resolution",
    "temporary failure in name resolution",
    "getaddrinfo",
    "temporarily unavailable",
    "service unavailable",
    "upstream request timeout",
    "throttl",  # throttle / throttled / throttling
    "slow down",
    "slowdown",  # camelCase S3 code as substring
    "rate limit",
    "rate-limit",
    "ratelimit",
    "5xx",
    "502",
    "503",
    "504",
    "429",
    "part_meta_not_ready",
    "part_row_missing",
)

_PERMANENT_KEYWORDS = (
    "malformed",
    "invalid",
    "validation error",
    "validation errors",
    "negative size",
    "missing part",
    "insufficient storage",
    "integrity",
    "signature does not match",
    "signaturedoesnotmatch",
    "no such bucket",
    "no such key",
    "not implemented",
    "method not allowed",
    "access denied",
    "accessdenied",  # camelCase S3 code as substring
    "forbidden",
    "unauthorized",
    "permission denied",
    "authentication failed",
    "nosuchbucket",
    "nosuchkey",
    "invalid token",
    "expired token",
    "filesystem cache",  # FS evicted source bytes between enqueue and processing
    "key too long",
    "hippius api",  # explicit Hippius API failures don't self-heal (the
    # client's @retry_on_error decorator already burned its budget)
)

# Custom-exception class names that should always classify permanent. Matched
# substring-style against `type(error).__name__.lower()` so subclasses count too.
_PERMANENT_EXCEPTION_NAME_FRAGMENTS = (
    "hippiusapierror",
    "hippiusauthenticationerror",
)


# --------------------------------------------------------------------------- #
# Core classifier
# --------------------------------------------------------------------------- #


def _classify(
    error: Exception,
    *,
    enable_billing: bool,
    treat_404_as: str,
) -> str:
    """Internal rule engine shared by the three public classifiers.

    `enable_billing`  - if True, 402 returns 'billing'. Only the upload-path
                        classifier sets this True (download / unpin can't 402).
    `treat_404_as`    - either 'permanent' or 'transient'. Upload + download
                        treat it as permanent; unpin treats it as transient.
    """
    err_str = str(error).lower()
    err_type = type(error).__name__.lower()

    # ---- 1) Custom class (Hippius API hierarchy = always permanent) ------ #
    if any(frag in err_type for frag in _PERMANENT_EXCEPTION_NAME_FRAGMENTS):
        return "permanent"

    # ---- 2) Botocore Error.Code (e.g. "SlowDown") ------------------------ #
    # Beats the HTTP-status layer below because S3 sometimes returns Throttling
    # with HTTP 400 - the code is the authoritative actionable signal.
    boto_code = extract_boto_error_code(error)
    if boto_code:
        if boto_code in _TRANSIENT_BOTO_CODES:
            return "transient"
        if boto_code in _PERMANENT_BOTO_CODES:
            # NoSuchKey on unpin is transient (pin commit pending); upload/download permanent.
            if boto_code == "NoSuchKey" and treat_404_as == "transient":
                return "transient"
            return "permanent"

    # ---- 3) HTTP status: 507 first (preempts the billing keyword check) -- #
    status = extract_http_status_code(error)
    if status == "507":
        return "permanent"

    # ---- 4) Billing (upload only) ---------------------------------------- #
    if enable_billing and is_billing_error(error):
        return "billing"

    # ---- 5) Other HTTP status codes -------------------------------------- #
    if status.isdigit():
        code = int(status)
        if code == 404:
            return treat_404_as
        if code in (408, 429):  # request timeout / too many requests
            return "transient"
        if 500 <= code <= 599:
            return "transient"
        if 400 <= code <= 499:
            return "permanent"

    # ---- 6) Exception class (httpx / botocore / builtins) ---------------- #
    if isinstance(error, _transient_exception_classes()):
        return "transient"
    if isinstance(error, OSError):
        e_errno = getattr(error, "errno", None)
        if e_errno in _TRANSIENT_OSERROR_ERRNOS:
            return "transient"

    # ---- 7) Keyword fallback --------------------------------------------- #
    if any(k in err_str for k in _PERMANENT_KEYWORDS):
        return "permanent"
    if any(k in err_str for k in _TRANSIENT_KEYWORDS):
        return "transient"

    # ---- 8) Path-specific tail rule: unpin treats bare "404"/"not found" - #
    # strings as transient. Upload/download fall through to "unknown" because
    # without a real exception we cannot confirm a 404 vs a stale log line.
    if treat_404_as == "transient" and ("404" in err_str or "not found" in err_str):
        return "transient"

    # ---- 9) Walk into __cause__ once before giving up -------------------- #
    cause = getattr(error, "__cause__", None)
    if cause is not None and cause is not error:
        chained = _classify(cause, enable_billing=enable_billing, treat_404_as=treat_404_as)
        if chained != "unknown":
            return chained

    return "unknown"


# --------------------------------------------------------------------------- #
# Public classifiers
# --------------------------------------------------------------------------- #


def classify_upload_error(error: Exception) -> str:
    """Classify a backend-upload failure. Returns transient / permanent / billing / unknown."""
    return _classify(error, enable_billing=True, treat_404_as="permanent")


def classify_download_error(error: Exception) -> str:
    """Classify a backend-download failure. Returns transient / permanent / unknown."""
    return _classify(error, enable_billing=False, treat_404_as="permanent")


def classify_unpin_error(error: Exception) -> str:
    """Classify an unpin failure. 404 is transient here (pin commit pending upstream)."""
    return _classify(error, enable_billing=False, treat_404_as="transient")


# --------------------------------------------------------------------------- #
# Backward-compatible re-exports
# --------------------------------------------------------------------------- #

# Old call sites import `classify_error` from hippius_s3.workers.uploader.
# That symbol stays as an alias to the upload classifier (its original meaning).
classify_error = classify_upload_error


def is_retryable(error: Exception, attempts: int, max_attempts: int) -> bool:
    """Whether an upload payload with `attempts` prior attempts should be requeued.

    Mirrors the s3-backup contract so the two repos share the same calling
    convention even though they live in separate codebases.
    """
    return classify_upload_error(error) == "transient" and (attempts + 1) < max_attempts


def compute_backoff_ms(attempt: int, base_ms: int = 1000, max_ms: int = 30000) -> float:
    """Exponential backoff with 10% jitter, capped at max_ms."""
    exp_backoff = base_ms * (2 ** (attempt - 1))
    jitter = random.uniform(0, exp_backoff * 0.1)
    return float(min(exp_backoff + jitter, max_ms))


__all__ = [
    "classify_upload_error",
    "classify_download_error",
    "classify_unpin_error",
    "classify_error",
    "is_billing_error",
    "extract_http_status_code",
    "extract_boto_error_code",
    "is_retryable",
    "compute_backoff_ms",
]
