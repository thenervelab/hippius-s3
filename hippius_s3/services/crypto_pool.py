"""Dedicated thread pool for AES-GCM encrypt/decrypt (RD-2 / WU-1).

AES-GCM in `cryptography` releases the GIL, so running it on a worker thread lets the event loop
service other requests instead of head-of-line-blocking on each ~2-5 ms per-chunk crypto call. The
pool is kept separate from asyncio's default executor (which already carries FS md5 offload and
blocking FS work) so crypto never starves those, and vice versa.

Callers await the offloaded call sequentially per chunk, so ciphertext order — and therefore
`chunk_cipher_sizes` recording and the running md5 — are unchanged from the inline path; only the CPU
moves off the loop thread.
"""

from __future__ import annotations

import asyncio
import atexit
from concurrent.futures import ThreadPoolExecutor
from typing import Any
from typing import Callable
from typing import TypeVar

from hippius_s3.config import get_config


T = TypeVar("T")

_pool: ThreadPoolExecutor | None = None


def _get_pool() -> ThreadPoolExecutor:
    global _pool
    if _pool is None:
        workers = int(getattr(get_config(), "crypto_pool_workers", 4) or 4)
        _pool = ThreadPoolExecutor(max_workers=workers, thread_name_prefix="crypto")
    return _pool


async def run_crypto(func: Callable[..., T], /, *args: Any, **kwargs: Any) -> T:
    """Run a CPU-bound crypto callable on the dedicated pool and await the result."""
    loop = asyncio.get_running_loop()
    if kwargs:
        from functools import partial

        return await loop.run_in_executor(_get_pool(), partial(func, *args, **kwargs))
    return await loop.run_in_executor(_get_pool(), func, *args)


def shutdown_crypto_pool() -> None:
    global _pool
    if _pool is not None:
        _pool.shutdown(wait=False)
        _pool = None


atexit.register(shutdown_crypto_pool)
