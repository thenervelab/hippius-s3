"""Every workload gated on `postgres-rw` must be patched for production.

`postgres-rw` is the Ceph cluster's Service. It still EXISTS in production but has no
endpoints — the cluster was hibernated — so `nc -z postgres-rw 5432` never succeeds there.
The shared base keeps that hostname because staging has no postgres-nvme cluster, and
k8s/production/postgres-nvme-initwait-patch.yaml repoints each gate at postgres-nvme-rw.

A workload that gets the base gate and no production patch therefore comes up in
production and sits in Init forever, waiting on a Service that will never answer. That is
not hypothetical: it is exactly what happened to plans-cacher on its first production
deploy, and it is invisible until the pod is scheduled, because every check short of a real
rollout passes.

The patch file's own header predicted this ("otherwise these gates would hang once the
postgres-rw Service is removed"). What it could not do is notice the next workload nobody
remembered to add. That is what this test is for.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

import yaml


REPO_ROOT = Path(__file__).resolve().parents[2]

BASE_MANIFESTS = [
    "k8s/base/workers-deployments.yaml",
    "k8s/base/api-deployment.yaml",
]
PROD_PATCH = "k8s/production/postgres-nvme-initwait-patch.yaml"

DEAD_HOST = "postgres-rw"
LIVE_HOST = "postgres-nvme-rw"


def _docs(rel: str) -> list[dict[str, Any]]:
    text = (REPO_ROOT / rel).read_text()
    return [d for d in yaml.safe_load_all(text) if isinstance(d, dict)]


def _init_commands(doc: dict[str, Any]) -> list[str]:
    spec = (doc.get("spec") or {}).get("template", {}).get("spec") or {}
    return [" ".join(c.get("command") or []) for c in spec.get("initContainers") or []]


def _workloads_gated_on(host: str, manifests: list[str]) -> set[str]:
    """Names of workloads whose init containers probe `host`.

    Matched with surrounding spaces so `postgres-rw` does not also match
    `postgres-nvme-rw`, which is the whole distinction under test.
    """
    needle = f" {host} "
    found: set[str] = set()
    for rel in manifests:
        for doc in _docs(rel):
            name = (doc.get("metadata") or {}).get("name")
            if name and any(needle in cmd for cmd in _init_commands(doc)):
                found.add(name)
    return found


def test_every_base_postgres_gate_is_repointed_for_production() -> None:
    needs_patch = _workloads_gated_on(DEAD_HOST, BASE_MANIFESTS)
    patched = _workloads_gated_on(LIVE_HOST, [PROD_PATCH])

    missing = sorted(needs_patch - patched)
    assert not missing, (
        f"These workloads wait on {DEAD_HOST} in the base manifests but have no "
        f"{LIVE_HOST} override in {PROD_PATCH}, so they will hang in Init in production: {missing}"
    )


def test_the_patch_does_not_drift_onto_workloads_that_no_longer_gate() -> None:
    """A patch entry for a workload that no longer has the gate is dead weight, and kustomize
    is happy to keep applying it — so it would sit there indefinitely, implying coverage that
    no longer means anything."""
    needs_patch = _workloads_gated_on(DEAD_HOST, BASE_MANIFESTS)
    patched = _workloads_gated_on(LIVE_HOST, [PROD_PATCH])

    # db-migrations is a Job, not a Deployment, and lives in its own base manifest; it is
    # legitimately patched without appearing in the files scanned above.
    stale = sorted(patched - needs_patch - {"db-migrations"})
    assert not stale, f"{PROD_PATCH} patches workloads that no longer gate on {DEAD_HOST}: {stale}"


def test_plans_cacher_specifically_is_covered() -> None:
    """Named explicitly because it is the one that actually broke, and because a regression
    here is silent until a production pod is scheduled."""
    assert "plans-cacher" in _workloads_gated_on(LIVE_HOST, [PROD_PATCH])
