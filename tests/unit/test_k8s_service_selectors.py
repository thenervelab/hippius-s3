"""Every Service selector in an overlay must match a workload that overlay actually renders.

A Service whose selector matches no pod template renders without a kustomize error and applies
without a kubectl error; it simply has no endpoints, and the first sign is the public entrypoint
timing out. The base `api` and `gateway` Services carried `app: api` for months after the last
workload with that label was scaled to zero and then deleted — they only worked because each
overlay patched the selector identically. This test is the check kustomize does not do.

Files are read directly rather than through `kubectl kustomize`, so the test needs no binary:
an overlay's workloads are every Deployment/DaemonSet/StatefulSet with a pod template in
`k8s/base` plus that overlay's own files, and a strategic-merge patch that carries no pod labels
is skipped by construction.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

import pytest
import yaml


REPO_ROOT = Path(__file__).resolve().parents[2]
WORKLOAD_KINDS = {"Deployment", "DaemonSet", "StatefulSet"}


def _docs(directory: Path) -> list[dict[str, Any]]:
    docs: list[dict[str, Any]] = []
    for path in sorted(directory.glob("*.yaml")):
        docs.extend(d for d in yaml.safe_load_all(path.read_text()) if d)
    return docs


def _pod_labels(doc: dict[str, Any]) -> dict[str, str] | None:
    template = ((doc.get("spec") or {}).get("template") or {}).get("metadata") or {}
    labels = template.get("labels")
    return dict(labels) if labels else None


@pytest.mark.parametrize("overlay", ["staging", "production"])
def test_every_service_selector_matches_a_rendered_workload(overlay: str) -> None:
    docs = _docs(REPO_ROOT / "k8s" / "base") + _docs(REPO_ROOT / "k8s" / overlay)
    pod_label_sets = [labels for d in docs if d.get("kind") in WORKLOAD_KINDS for labels in [_pod_labels(d)] if labels]
    assert pod_label_sets, "no workloads found — the manifest layout moved and this test needs updating"

    dangling = []
    for doc in docs:
        if doc.get("kind") != "Service":
            continue
        selector = (doc.get("spec") or {}).get("selector")
        if not selector:
            continue
        if not any(selector.items() <= labels.items() for labels in pod_label_sets):
            dangling.append(f"{doc['metadata']['name']} selects {selector}")
    assert not dangling, f"{overlay}: Services with no matching pod template: {dangling}"
