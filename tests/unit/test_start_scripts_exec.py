"""The container entrypoints must hand PID 1 to uvicorn.

If a start script runs uvicorn as a child instead of `exec`ing it, the script stays PID 1
and a non-interactive bash defers SIGTERM until its foreground child exits — which uvicorn
never does on its own. The kubelet then SIGKILLs the pod when terminationGracePeriodSeconds
expires and every in-flight request dies mid-flight, surfacing at the gateway as
`httpx.RemoteProtocolError: Server disconnected without sending a response`.

Measured on 2026-07-22 with a minimal container (python:3.11-slim, bash wrapper, child that
handles SIGTERM):

    without exec:  child never saw the signal, `docker stop -t 5` took the full 5s,
                   exit code 137 (SIGKILL), zero "Application shutdown complete" lines
    with exec:     child received signal 15, container exited in 3s having finished an
                   in-flight request, exit code 0

tests/e2e/test_GracefulShutdown.py proves the same end-to-end against the real image.
"""

from __future__ import annotations

import re
from pathlib import Path

import pytest


REPO_ROOT = Path(__file__).resolve().parents[2]

START_SCRIPTS = [
    REPO_ROOT / "start-api.sh",
    REPO_ROOT / "gateway" / "start-gateway.sh",
]


@pytest.mark.parametrize("script", START_SCRIPTS, ids=lambda p: p.name)
def test_uvicorn_is_execed(script: Path) -> None:
    assert script.is_file(), f"{script} is missing"
    body = script.read_text()

    invocations = re.findall(r"^\s*(exec\s+)?uvicorn\b", body, flags=re.MULTILINE)
    assert invocations, f"{script.name} does not invoke uvicorn at all"

    bare = [m for m in invocations if not m]
    assert not bare, (
        f"{script.name} invokes uvicorn without `exec`. The script would stay PID 1, so "
        f"SIGTERM never reaches uvicorn, no in-flight request is drained, and the pod is "
        f"SIGKILLed at the end of its grace period."
    )


@pytest.mark.parametrize("script", START_SCRIPTS, ids=lambda p: p.name)
def test_graceful_shutdown_timeout_is_set(script: Path) -> None:
    """Draining needs a bound, and that bound has to fit inside the pod's grace period."""
    body = script.read_text()
    assert "--timeout-graceful-shutdown" in body, (
        f"{script.name} does not pass --timeout-graceful-shutdown, so uvicorn waits "
        f"indefinitely for in-flight requests and can be SIGKILLed mid-drain instead."
    )
