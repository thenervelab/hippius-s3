from __future__ import annotations

import asyncio
import contextlib
import logging
import os
import signal
import time
from collections.abc import Callable
from collections.abc import Coroutine
from typing import Any


logger = logging.getLogger(__name__)

# Must fit inside terminationGracePeriodSeconds on the worker Deployments, or the kubelet
# SIGKILLs us mid-drain and the cleanup this module exists to run never finishes.
DEFAULT_DRAIN_TIMEOUT_SECONDS = 20.0
DEFAULT_RESTART_DELAY_SECONDS = 5.0

CoroFactory = Callable[[], Coroutine[Any, Any, Any]]


def _drain_timeout() -> float:
    return float(os.environ.get("HIPPIUS_WORKER_DRAIN_TIMEOUT_SECONDS", DEFAULT_DRAIN_TIMEOUT_SECONDS))


async def _supervise(factory: CoroFactory, name: str, drain_timeout: float) -> bool:
    """Run the worker until it finishes or a shutdown signal arrives.

    Returns True if we stopped because of a signal, False if the worker returned by itself.
    Re-raises whatever the worker raised, so the caller can decide to restart.
    """
    loop = asyncio.get_running_loop()
    task = asyncio.ensure_future(factory())
    stop = asyncio.Event()

    def _request_stop(signame: str) -> None:
        # Log before cancelling: if the drain then wedges, this line is the only evidence
        # that we ever received the signal at all.
        logger.info("%s: %s received, draining (bounded at %.0fs)", name, signame, drain_timeout)
        stop.set()

    for signame in ("SIGTERM", "SIGINT"):
        loop.add_signal_handler(getattr(signal, signame), _request_stop, signame)

    waiter = asyncio.ensure_future(stop.wait())
    done, _ = await asyncio.wait({task, waiter}, return_when=asyncio.FIRST_COMPLETED)
    waiter.cancel()
    with contextlib.suppress(asyncio.CancelledError):
        await waiter

    if task in done:
        await task  # surface a crash to the caller's restart logic
        return False

    task.cancel()
    # Cancelling is the whole point of this module. Every worker loop closes its asyncpg
    # pool and redis clients in a `finally`, and that cleanup is what tells Postgres the
    # client is gone. A backend that is never told keeps running its statement — and keeps
    # pinning the xmin horizon — long after the pod has been deleted, which is how one
    # mpu-reaper query survived its own SIGKILLed pod for 7,377s on prod (2026-07-23) and
    # blocked VACUUM database-wide. wait_for bounds the drain so a wedged cleanup cannot
    # outlive the grace period and get SIGKILLed anyway.
    try:
        await asyncio.wait_for(task, timeout=drain_timeout)
    except asyncio.CancelledError:
        pass  # our own cancellation completing; the `finally` blocks have run
    except TimeoutError:
        logger.error("%s: drain exceeded %.0fs, exiting with cleanup incomplete", name, drain_timeout)
    return True


def run_worker(
    factory: CoroFactory,
    name: str,
    *,
    restart_on_crash: bool = False,
    restart_delay: float = DEFAULT_RESTART_DELAY_SECONDS,
    drain_timeout: float | None = None,
) -> None:
    """Entry point for a long-running worker: run `factory()` until SIGTERM/SIGINT.

    `restart_on_crash` mirrors whatever each entrypoint did before this module existed, and
    defaults off deliberately. Restarting in-process keeps the pod Ready with restart_count
    at 0, so a persistently crashing worker becomes invisible to the pod-restart alerting;
    letting the crash exit hands that job to the kubelet, which makes it visible. Only the
    entrypoints that already had a `while True / except Exception / sleep(5)` wrapper pass
    True, so this PR changes shutdown behaviour without also changing crash behaviour.

    A shutdown signal never restarts, either way — a pod being terminated must not start a
    fresh cycle it has no time to finish.
    """
    timeout = _drain_timeout() if drain_timeout is None else drain_timeout
    while True:
        try:
            signalled = asyncio.run(_supervise(factory, name, timeout))
        except Exception as exc:
            if not restart_on_crash:
                logger.error("%s: crashed, exiting for the kubelet to restart: %s", name, exc, exc_info=True)
                raise
            logger.error("%s: crashed, restarting in %.0fs: %s", name, restart_delay, exc, exc_info=True)
            time.sleep(restart_delay)
            continue
        if signalled:
            logger.info("%s: shutdown complete", name)
        return
