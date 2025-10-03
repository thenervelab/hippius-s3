"""Timing instrumentation utilities for performance monitoring."""

from __future__ import annotations

import logging
import time
from contextlib import asynccontextmanager
from contextlib import contextmanager
from typing import Any
from typing import AsyncGenerator
from typing import Generator


logger = logging.getLogger(__name__)


@contextmanager
def timing_context(
    operation: str, *, log_threshold_ms: float = 0.0, extra: dict[str, Any] | None = None
) -> Generator[dict[str, Any], None, None]:
    """Context manager for timing synchronous operations.

    Args:
        operation: Name of the operation being timed
        log_threshold_ms: Only log if duration exceeds this threshold (ms). 0 = always log.
        extra: Additional context to include in log

    Yields:
        Timing dict with 'start' field, will have 'duration_ms' on exit

    Example:
        with timing_context("download_chunk", log_threshold_ms=100, extra={"cid": cid}) as t:
            data = download(cid)
        # Logs: "TIMING download_chunk duration_ms=150.5 cid=abc123"
    """
    ctx: dict[str, Any] = {"start": time.perf_counter()}
    if extra:
        ctx.update(extra)

    try:
        yield ctx
    finally:
        duration_ms = (time.perf_counter() - ctx["start"]) * 1000.0
        ctx["duration_ms"] = duration_ms

        if duration_ms >= log_threshold_ms:
            extra_str = " ".join(f"{k}={v}" for k, v in (extra or {}).items())
            logger.info(f"TIMING {operation} duration_ms={duration_ms:.2f} {extra_str}".strip())


@asynccontextmanager
async def async_timing_context(
    operation: str, *, log_threshold_ms: float = 0.0, extra: dict[str, Any] | None = None
) -> AsyncGenerator[dict[str, Any], None]:
    """Async context manager for timing async operations.

    Args:
        operation: Name of the operation being timed
        log_threshold_ms: Only log if duration exceeds this threshold (ms). 0 = always log.
        extra: Additional context to include in log

    Yields:
        Timing dict with 'start' field, will have 'duration_ms' on exit

    Example:
        async with async_timing_context("fetch_manifest", extra={"object_id": oid}) as t:
            manifest = await build_manifest(db, oid)
        # Logs: "TIMING fetch_manifest duration_ms=42.3 object_id=abc-123"
    """
    ctx: dict[str, Any] = {"start": time.perf_counter()}
    if extra:
        ctx.update(extra)

    try:
        yield ctx
    finally:
        duration_ms = (time.perf_counter() - ctx["start"]) * 1000.0
        ctx["duration_ms"] = duration_ms

        if duration_ms >= log_threshold_ms:
            extra_str = " ".join(f"{k}={v}" for k, v in (extra or {}).items())
            logger.info(f"TIMING {operation} duration_ms={duration_ms:.2f} {extra_str}".strip())


def log_timing(operation: str, duration_ms: float, *, extra: dict[str, Any] | None = None) -> None:
    """Direct timing log helper for manual timing.

    Args:
        operation: Name of the operation
        duration_ms: Duration in milliseconds
        extra: Additional context

    Example:
        start = time.perf_counter()
        do_work()
        log_timing("work", (time.perf_counter() - start) * 1000, extra={"items": 5})
    """
    extra_str = " ".join(f"{k}={v}" for k, v in (extra or {}).items())
    logger.info(f"TIMING {operation} duration_ms={duration_ms:.2f} {extra_str}".strip())
