"""Utility modules and functions for hippius_s3.

This package combines utility functions from utils_core.py with new timing utilities.
"""

# Explicit imports only - no star imports to avoid namespace pollution
from hippius_s3.utils.timing import async_timing_context  # noqa: F401
from hippius_s3.utils.timing import log_timing  # noqa: F401
from hippius_s3.utils.timing import timing_context  # noqa: F401
from hippius_s3.utils_core import env  # noqa: F401
from hippius_s3.utils_core import get_object_download_info  # noqa: F401
from hippius_s3.utils_core import get_query  # noqa: F401
from hippius_s3.utils_core import get_request_body  # noqa: F401
from hippius_s3.utils_core import upsert_cid_and_get_id  # noqa: F401


__all__ = [
    # From utils_core.py
    "env",
    "get_request_body",
    "get_query",
    "get_object_download_info",
    "upsert_cid_and_get_id",
    # From timing.py
    "async_timing_context",
    "log_timing",
    "timing_context",
]
