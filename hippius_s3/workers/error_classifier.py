"""Backward-compatible re-export shim.

The classifier lives in `hippius_s3.workers.errors`. This module is kept so
existing imports (`from hippius_s3.workers.error_classifier import classify_unpin_error`)
continue to work without churn.
"""

from hippius_s3.workers.errors import classify_download_error
from hippius_s3.workers.errors import classify_error
from hippius_s3.workers.errors import classify_unpin_error
from hippius_s3.workers.errors import classify_upload_error


__all__ = [
    "classify_download_error",
    "classify_error",
    "classify_unpin_error",
    "classify_upload_error",
]
