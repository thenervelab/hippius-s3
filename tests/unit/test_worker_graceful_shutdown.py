"""Workers must run their cleanup on SIGTERM, not be killed before it.

PR #319 gave api/gateway/api-local `exec` + preStop + a grace period. The worker
deployments were out of that scope, and they had a different hole: `start-worker.sh`
already `exec`s, so uvicorn-style PID-1 was never the problem — the problem was that no
worker installed a SIGTERM handler at all. Python's default disposition terminates the
process outright, so the `finally:` blocks that close the asyncpg pool never ran.

That is not cosmetic. Closing the pool is what tells Postgres the client is gone. A
backend that is never told keeps executing its statement, and a long statement keeps
pinning the xmin horizon, so VACUUM reclaims nothing database-wide. Measured on prod
2026-07-23: the mpu-reaper pod exited 137 (SIGKILL) during a rollout and its
`list_abandoned_versions` query was still running 7,377s later, with
`cephor_replication_status` sitting at 746,972 dead tuples.
"""

from __future__ import annotations

import asyncio
import re
import signal
from pathlib import Path

import pytest
import yaml

from hippius_s3.workers.shutdown import DEFAULT_DRAIN_TIMEOUT_SECONDS
from hippius_s3.workers.shutdown import _drain_timeout
from hippius_s3.workers.shutdown import _supervise
from hippius_s3.workers.shutdown import run_worker


REPO_ROOT = Path(__file__).resolve().parents[2]


async def _raise_signal_soon(signame: int) -> None:
    await asyncio.sleep(0.05)
    signal.raise_signal(signame)


@pytest.mark.parametrize("signame", [signal.SIGTERM, signal.SIGINT])
def test_cleanup_runs_on_signal(signame: int) -> None:
    """The whole point: a signalled worker still gets through its `finally`."""
    cleaned: list[str] = []

    async def worker() -> None:
        try:
            await asyncio.Event().wait()  # never completes on its own
        finally:
            cleaned.append("pool closed")

    async def scenario() -> bool:
        asyncio.ensure_future(_raise_signal_soon(signame))
        return await _supervise(worker, "test-worker", 5.0)

    signalled = asyncio.run(scenario())

    assert signalled is True
    assert cleaned == ["pool closed"], "worker was killed before its cleanup ran"


def test_worker_returning_normally_is_not_reported_as_signalled() -> None:
    async def worker() -> None:
        return None

    assert asyncio.run(_supervise(worker, "test-worker", 5.0)) is False


def test_crash_propagates_to_the_caller() -> None:
    async def worker() -> None:
        raise RuntimeError("boom")

    with pytest.raises(RuntimeError, match="boom"):
        asyncio.run(_supervise(worker, "test-worker", 5.0))


def test_wedged_cleanup_is_bounded_not_hung() -> None:
    """A cleanup that never finishes must not outlive terminationGracePeriodSeconds."""

    async def worker() -> None:
        try:
            await asyncio.Event().wait()
        finally:
            await asyncio.sleep(30)  # wedged cleanup

    async def scenario() -> bool:
        asyncio.ensure_future(_raise_signal_soon(signal.SIGTERM))
        return await _supervise(worker, "test-worker", 0.2)

    loop = asyncio.new_event_loop()
    started = loop.time()
    try:
        signalled = loop.run_until_complete(scenario())
    finally:
        loop.close()

    assert signalled is True
    assert (loop.time() - started) < 5, "drain was not bounded by drain_timeout"


def test_signal_does_not_restart_the_worker() -> None:
    """A pod being terminated must not begin a cycle it has no time to finish."""
    starts = 0

    # run_worker owns its own asyncio.run, so arm the signal from inside the worker.
    async def worker_that_signals_itself() -> None:
        nonlocal starts
        starts += 1
        asyncio.ensure_future(_raise_signal_soon(signal.SIGTERM))
        await asyncio.Event().wait()

    run_worker(worker_that_signals_itself, "test-worker", drain_timeout=1.0)
    assert starts == 1


def test_crash_restarts_then_stops_when_opted_in() -> None:
    attempts = 0

    async def flaky() -> None:
        nonlocal attempts
        attempts += 1
        if attempts < 3:
            raise RuntimeError("transient")
        return None

    run_worker(flaky, "test-worker", restart_on_crash=True, restart_delay=0.0, drain_timeout=1.0)
    assert attempts == 3


def test_crash_exits_by_default() -> None:
    """Restarting in-process hides a persistent crash: the pod stays Ready with
    restart_count 0. Only entrypoints that already restarted themselves opt in."""
    attempts = 0

    async def always_broken() -> None:
        nonlocal attempts
        attempts += 1
        raise RuntimeError("boom")

    with pytest.raises(RuntimeError, match="boom"):
        run_worker(always_broken, "test-worker", restart_delay=0.0, drain_timeout=1.0)
    assert attempts == 1, "default must not retry in-process"


def test_only_previously_self_restarting_entrypoints_opt_in() -> None:
    """Guards the behaviour-preservation claim: before this module, exactly these three
    entrypoints wrapped themselves in `while True / except Exception / sleep(5)`."""
    expected = {
        "run_mpu_reaper_in_loop.py",
        "run_orphan_checker_in_loop.py",
        "run_arion_unpinner_in_loop.py",
    }
    opted_in = {
        script.name
        for script in (REPO_ROOT / "workers").glob("run_*_in_loop.py")
        if "restart_on_crash=True" in script.read_text()
    }
    assert opted_in == expected


# ---- the manifests have to give that cleanup room to run ----

WORKER_MANIFESTS = [
    REPO_ROOT / "k8s" / "base" / "workers-deployments.yaml",
    REPO_ROOT / "k8s" / "staging" / "mpu-reaper-deployment.yaml",
    REPO_ROOT / "k8s" / "production" / "mpu-reaper-deployment.yaml",
]

DRAIN_BOUND_SECONDS = 20  # DEFAULT_DRAIN_TIMEOUT_SECONDS


def _worker_deployments() -> list[tuple[str, dict]]:
    found = []
    for path in WORKER_MANIFESTS:
        assert path.is_file(), f"{path} is missing"
        for doc in yaml.safe_load_all(path.read_text()):
            if not doc or doc.get("kind") != "Deployment":
                continue
            found.append((doc["metadata"]["name"], doc["spec"]["template"]["spec"]))
    return found


def test_every_worker_deployment_sets_a_grace_period() -> None:
    deployments = _worker_deployments()
    assert deployments, "no worker Deployments parsed — did the manifests move?"

    missing = [name for name, spec in deployments if "terminationGracePeriodSeconds" not in spec]
    assert not missing, (
        f"{missing} have no terminationGracePeriodSeconds, so they fall back to the 30s "
        f"default with no headroom stated. Cleanup that overruns is SIGKILLed and the "
        f"Postgres backend is orphaned."
    )


def test_grace_period_exceeds_the_drain_bound() -> None:
    for name, spec in _worker_deployments():
        grace = spec["terminationGracePeriodSeconds"]
        assert grace > DRAIN_BOUND_SECONDS, (
            f"{name} allows {grace}s but run_worker drains for up to "
            f"{DRAIN_BOUND_SECONDS}s; the kubelet would SIGKILL it mid-cleanup."
        )


def test_worker_entrypoints_use_the_supervisor() -> None:
    """A bare `asyncio.run(...)` in an entrypoint is the bug this PR fixes."""
    offenders = []
    for script in sorted((REPO_ROOT / "workers").glob("run_*_in_loop.py")):
        body = script.read_text()
        main_block = body.split('if __name__ == "__main__":', 1)
        if len(main_block) < 2:
            continue
        if re.search(r"^\s*asyncio\.run\(", main_block[1], flags=re.MULTILINE):
            offenders.append(script.name)
    assert not offenders, (
        f"{offenders} call asyncio.run() directly instead of run_worker(), so they install "
        f"no SIGTERM handler and their cleanup never runs."
    )


def test_drain_timeout_honors_env_override(monkeypatch) -> None:
    """The drain bound is operator-tunable via HIPPIUS_WORKER_DRAIN_TIMEOUT_SECONDS; the supervisor
    reads that env, not just the compiled-in default. Exercises the override path (unset in every
    manifest today, so otherwise never covered)."""
    monkeypatch.delenv("HIPPIUS_WORKER_DRAIN_TIMEOUT_SECONDS", raising=False)
    assert _drain_timeout() == DEFAULT_DRAIN_TIMEOUT_SECONDS

    monkeypatch.setenv("HIPPIUS_WORKER_DRAIN_TIMEOUT_SECONDS", "37")
    assert _drain_timeout() == 37.0


def test_effective_drain_timeout_fits_every_worker_grace_period(monkeypatch) -> None:
    """Guard the EFFECTIVE drain bound (what _drain_timeout() actually returns at the default),
    not just the hardcoded 20, against every worker's terminationGracePeriodSeconds. If the default
    ever drifts above a grace, the kubelet SIGKILLs mid-cleanup — the exact bug this PR fixes.
    (Raising the env above a grace is an operator's explicit choice and out of scope here.)"""
    monkeypatch.delenv("HIPPIUS_WORKER_DRAIN_TIMEOUT_SECONDS", raising=False)
    effective = _drain_timeout()
    for name, spec in _worker_deployments():
        grace = spec["terminationGracePeriodSeconds"]
        assert grace > effective, f"{name} grace={grace}s <= effective drain {effective}s → kubelet SIGKILL mid-cleanup"
