"""Every HTTP-serving workload must drain rather than be killed during a roll.

`exec`ing uvicorn (tests/unit/test_start_scripts_exec.py) is only half of it. The kubelet
dispatches endpoint removal and SIGTERM concurrently, so a pod can still be receiving
traffic from a kube-proxy that has not yet observed the removal at the moment uvicorn stops
accepting. The preStop sleep holds the pod open across that window; SIGTERM lands after it.

The grace period has to cover preStop + the drain, otherwise the kubelet SIGKILLs mid-drain
and we are back to severed connections — which is what produced the 2026-07-22 20:27:22
burst of `RemoteProtocolError: Server disconnected` across the gateway fleet during a
routine production deploy.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

import pytest
import yaml


REPO_ROOT = Path(__file__).resolve().parents[2]

# (manifest, workload name) for every workload that serves client HTTP traffic.
SERVING_WORKLOADS = [
    ("k8s/base/api-deployment.yaml", "api"),
    ("k8s/base/gateway-deployment.yaml", "gateway"),
    ("k8s/production/api-local-deployments-production.yaml", "api-local"),
    ("k8s/staging/api-local-deployments-staging.yaml", "api-local"),
]

# uvicorn's --timeout-graceful-shutdown default in start-api.sh / start-gateway.sh.
UVICORN_DRAIN_SECONDS = 25


def _load_workload(rel_path: str, name: str) -> dict[str, Any]:
    docs = yaml.safe_load_all((REPO_ROOT / rel_path).read_text())
    for doc in docs:
        if not doc:
            continue
        if doc.get("kind") in {"Deployment", "DaemonSet"} and doc["metadata"]["name"] == name:
            return doc
    raise AssertionError(f"no Deployment/DaemonSet named {name!r} in {rel_path}")


def _pod_spec(rel_path: str, name: str) -> dict[str, Any]:
    return _load_workload(rel_path, name)["spec"]["template"]["spec"]


def _serving_container(pod_spec: dict[str, Any]) -> dict[str, Any]:
    containers = pod_spec["containers"]
    assert len(containers) == 1, "expected a single serving container; update this test"
    return containers[0]


@pytest.mark.parametrize(("rel_path", "name"), SERVING_WORKLOADS, ids=lambda v: str(v))
def test_has_prestop_hook(rel_path: str, name: str) -> None:
    container = _serving_container(_pod_spec(rel_path, name))
    lifecycle = container.get("lifecycle") or {}
    pre_stop = lifecycle.get("preStop")
    assert pre_stop, (
        f"{rel_path}:{name} has no preStop hook. Endpoint removal races SIGTERM, so a "
        f"rolling update will cut requests that were routed here microseconds earlier."
    )
    command = pre_stop["exec"]["command"]
    assert command[0] == "sleep" and int(command[1]) > 0, f"unexpected preStop command: {command}"


@pytest.mark.parametrize(("rel_path", "name"), SERVING_WORKLOADS, ids=lambda v: str(v))
def test_grace_period_covers_prestop_plus_drain(rel_path: str, name: str) -> None:
    """The preStop sleep is deducted from the grace period — the drain gets what's left."""
    pod_spec = _pod_spec(rel_path, name)
    grace = pod_spec.get("terminationGracePeriodSeconds")
    assert grace is not None, (
        f"{rel_path}:{name} does not set terminationGracePeriodSeconds explicitly; it would "
        f"inherit the 30s default, which is less than the preStop sleep plus the drain."
    )

    container = _serving_container(pod_spec)
    prestop_seconds = int(container["lifecycle"]["preStop"]["exec"]["command"][1])

    assert grace >= prestop_seconds + UVICORN_DRAIN_SECONDS, (
        f"{rel_path}:{name} allows {grace}s but needs at least "
        f"{prestop_seconds + UVICORN_DRAIN_SECONDS}s ({prestop_seconds}s preStop + "
        f"{UVICORN_DRAIN_SECONDS}s uvicorn drain). The kubelet would SIGKILL mid-drain."
    )
