"""Every Service selector must match a workload the same overlay actually runs.

A Service whose selector matches nothing renders without a kustomize error and applies without a
kubectl error; it simply has no endpoints, and the first sign is the public entrypoint timing out.
The base `api` and `gateway` Services carried `app: api` for months after the only workload with
that label was scaled to zero, and kept working solely because each overlay patched the selector.

"Actually runs" is the whole point, so a workload the overlay scales to zero does not count as a
match — that was the real state for months, and a Service aimed at it has no endpoints either.
The replica count therefore comes from the overlay's patches, not just the base manifest.
Documents come from the overlay's `resources:` graph rather than a directory glob, so a manifest
staged on disk but absent from every kustomization cannot satisfy a selector.
"""

from __future__ import annotations

from typing import Any

import pytest

from tests.unit.k8s_manifests import WORKLOAD_KINDS
from tests.unit.k8s_manifests import overlay_docs
from tests.unit.k8s_manifests import overlay_replicas
from tests.unit.k8s_manifests import pod_labels


def _running_pod_label_sets(docs: list[dict[str, Any]], replicas: dict[tuple[str, str], int]) -> list[dict[str, str]]:
    running = []
    for doc in docs:
        kind = doc.get("kind")
        if kind not in WORKLOAD_KINDS:
            continue
        name = (doc.get("metadata") or {}).get("name")
        declared = (doc.get("spec") or {}).get("replicas")
        if replicas.get((kind, name), declared) == 0:
            continue
        labels = pod_labels(doc)
        if labels:
            running.append(labels)
    return running


@pytest.mark.parametrize("overlay", ["staging", "production"])
def test_every_service_selector_matches_a_running_workload(overlay: str) -> None:
    docs = overlay_docs(overlay)
    label_sets = _running_pod_label_sets(docs, overlay_replicas(overlay))
    assert label_sets, "no workloads found — the manifest layout moved and this test needs updating"

    dangling = [
        f"{doc['metadata']['name']} selects {selector}"
        for doc in docs
        if doc.get("kind") == "Service" and (selector := (doc.get("spec") or {}).get("selector"))
        if not any(selector.items() <= labels.items() for labels in label_sets)
    ]
    assert not dangling, f"{overlay}: Services with no matching running workload: {dangling}"
