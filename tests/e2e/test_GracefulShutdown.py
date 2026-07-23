"""SIGTERM must drain the real container, not leave it to be killed.

This is the end-to-end counterpart to tests/unit/test_start_scripts_exec.py. That test reads
the start script; this one runs the shipped image and signals it the way a kubelet does:
SIGTERM, then SIGKILL once the grace period expires (`docker stop -t`).

The failure it guards against is not hypothetical. Until 2026-07-22 the start scripts ran
uvicorn as a child of bash rather than `exec`ing it, so the script held PID 1, bash deferred
SIGTERM while a foreground child was running, and uvicorn never learned it should stop. Every
rolling deploy therefore ended each pod with a SIGKILL, cutting in-flight requests. It showed
up as a burst of `httpx.RemoteProtocolError: Server disconnected without sending a response`
across the gateway fleet, and as 500s and read timeouts for whoever was mid-request.

Broken, this test sees exit code 137 after the full grace period. Fixed, exit code 0 in ~1s.
"""

from __future__ import annotations

import subprocess
import time

import pytest
import requests


# Bounded so a regression fails in seconds rather than hanging CI. Anything above the ~1s a
# healthy drain takes is slack; a broken build burns the whole window and then reports 137.
GRACE_SECONDS = 15
RESTART_TIMEOUT_SECONDS = 120

pytestmark = pytest.mark.e2e


def _container_id(service: str) -> str:
    result = subprocess.run(
        ["docker", "ps", "-q", "--filter", f"label=com.docker.compose.service={service}"],
        capture_output=True,
        text=True,
        check=True,
    )
    ids = (result.stdout or "").strip().splitlines()
    if not ids:
        pytest.skip(f"no running container for compose service {service!r}")
    return ids[0]


def _inspect(container_id: str, template: str) -> str:
    result = subprocess.run(
        ["docker", "inspect", "-f", template, container_id],
        capture_output=True,
        text=True,
        check=True,
    )
    return (result.stdout or "").strip()


def _wait_for_gateway(timeout: float) -> None:
    deadline = time.monotonic() + timeout
    last_error: Exception | None = None
    while time.monotonic() < deadline:
        try:
            if requests.get("http://localhost:8080/", timeout=5).status_code < 500:
                return
        except Exception as exc:  # noqa: BLE001 - polling until the stack is back
            last_error = exc
        time.sleep(2)
    raise AssertionError(f"stack did not come back within {timeout}s: {last_error}")


@pytest.mark.usefixtures("docker_services")
def test_api_container_drains_on_sigterm() -> None:
    container_id = _container_id("api")
    logs_before = len(subprocess.run(["docker", "logs", container_id], capture_output=True).stdout)

    try:
        started = time.monotonic()
        # Exactly the kubelet's contract: SIGTERM, then SIGKILL when the grace period runs out.
        subprocess.run(["docker", "stop", "-t", str(GRACE_SECONDS), container_id], check=True, capture_output=True)
        elapsed = time.monotonic() - started

        exit_code = int(_inspect(container_id, "{{.State.ExitCode}}"))
        logs = subprocess.run(["docker", "logs", container_id], capture_output=True).stdout[logs_before:]
        shutdown_log = logs.decode("utf-8", "replace")

        assert exit_code != 137, (
            f"the api container was SIGKILLed after {elapsed:.1f}s instead of shutting down. "
            f"uvicorn never received SIGTERM — almost certainly start-api.sh stopped `exec`ing "
            f"it, so the shell holds PID 1. Every deploy will cut in-flight requests.\n"
            f"--- container log tail ---\n{shutdown_log[-2000:]}"
        )
        assert exit_code == 0, f"unclean exit {exit_code} after {elapsed:.1f}s\n{shutdown_log[-2000:]}"
        assert elapsed < GRACE_SECONDS, (
            f"shutdown took {elapsed:.1f}s, i.e. it ran to the grace period rather than draining and exiting on its own"
        )
        # The ASGI lifespan shutdown only runs once uvicorn has stopped accepting and drained
        # its in-flight requests, so reaching it is the proof that draining happened rather
        # than the process simply dying. hippius_s3.logging_config reformats uvicorn's own
        # "Application shutdown complete" line, so key off the app's teardown instead.
        assert "Postgres connection pool closed" in shutdown_log, (
            "the ASGI lifespan shutdown never ran, so uvicorn did not drain — the process "
            "went away without completing in-flight requests.\n"
            f"--- container log tail ---\n{shutdown_log[-2000:]}"
        )
    finally:
        # `restart: unless-stopped` deliberately does not restart an explicitly stopped
        # container, so bring it back for the rest of the suite.
        subprocess.run(["docker", "start", container_id], check=False, capture_output=True)
        _wait_for_gateway(RESTART_TIMEOUT_SECONDS)
