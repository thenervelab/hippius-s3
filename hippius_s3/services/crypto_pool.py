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


def submit_crypto(func: Callable[..., T], /, *args: Any, **kwargs: Any) -> "asyncio.Future[T]":
    """Schedule a crypto callable on the pool WITHOUT awaiting it.

    The writer's streaming pipeline uses this to keep a bounded look-ahead of encrypts
    in flight while the event loop returns to reading the socket. Safe out-of-order:
    AES-GCM's nonce/AAD identity comes from the explicit chunk_index argument, never
    from call order.
    """
    loop = asyncio.get_running_loop()
    if kwargs:
        from functools import partial

        return loop.run_in_executor(_get_pool(), partial(func, *args, **kwargs))
    return loop.run_in_executor(_get_pool(), func, *args)


# Dedicated SINGLE-thread pool for rolling-hash updates (MD5 for ETags). One worker is
# the correctness mechanism, not a sizing choice: ThreadPoolExecutor feeds its worker
# from a FIFO queue, so updates submitted in chunk order EXECUTE in chunk order — a
# rolling hashlib digest stays correct without any locking, while the update itself
# (which releases the GIL for buffers >2 KiB) runs off the event loop. Shared by every
# request on this worker process: the same serialization the on-loop hashing imposed,
# minus the loop blocking. Kept separate from the crypto pool so a burst of encrypts
# can never reorder or starve the ordered hash stream.
_hash_pool: ThreadPoolExecutor | None = None


def _get_hash_pool() -> ThreadPoolExecutor:
    global _hash_pool
    if _hash_pool is None:
        _hash_pool = ThreadPoolExecutor(max_workers=1, thread_name_prefix="etag-hash")
    return _hash_pool


def submit_hash(func: Callable[..., T], /, *args: Any) -> "asyncio.Future[T]":
    """Schedule an ordered hash update on the single-thread hash pool (see above)."""
    loop = asyncio.get_running_loop()
    return loop.run_in_executor(_get_hash_pool(), func, *args)


def shutdown_crypto_pool() -> None:
    global _pool, _hash_pool
    if _pool is not None:
        _pool.shutdown(wait=False)
        _pool = None
    if _hash_pool is not None:
        _hash_pool.shutdown(wait=False)
        _hash_pool = None


atexit.register(shutdown_crypto_pool)
