"""Shared manifest loading for the k8s invariant tests.

Four tests assert properties of `k8s/` by reading the YAML directly rather than shelling out
to kustomize, so CI needs no binary. They were each parsing it their own way, with three
different answers to "what counts as a document"; this is that parsing, once.

`overlay_docs` resolves an overlay's `resources:` graph rather than globbing a directory, so a
manifest that is on disk but deliberately not in any kustomization — `k8s/base/redis-queues-ha.yaml`
is one, a cutover staged but not applied — cannot satisfy an assertion about what is deployed.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

import yaml


REPO_ROOT = Path(__file__).resolve().parents[2]
WORKLOAD_KINDS = frozenset({"Deployment", "DaemonSet", "StatefulSet"})


def load_docs(*rel_paths: str) -> list[dict[str, Any]]:
    """Every YAML document in `rel_paths`, which may name files or directories."""
    docs: list[dict[str, Any]] = []
    for rel in rel_paths:
        path = REPO_ROOT / rel
        files = sorted(path.glob("*.y*ml")) if path.is_dir() else [path]
        for file in files:
            docs.extend(d for d in yaml.safe_load_all(file.read_text()) if isinstance(d, dict))
    return docs


def overlay_docs(overlay: str) -> list[dict[str, Any]]:
    """Every document an overlay renders, resolved through the `resources:` graph.

    Patches are excluded: they carry no identity of their own, and a strategic-merge patch of a
    workload is not evidence that the workload exists.
    """
    return _kustomize_docs(REPO_ROOT / "k8s" / overlay)


def _kustomize_docs(directory: Path) -> list[dict[str, Any]]:
    kustomization = yaml.safe_load((directory / "kustomization.yaml").read_text()) or {}
    docs: list[dict[str, Any]] = []
    for entry in kustomization.get("resources") or []:
        target = (directory / entry).resolve()
        if (target / "kustomization.yaml").is_file():
            docs.extend(_kustomize_docs(target))
        else:
            docs.extend(load_docs(str(target.relative_to(REPO_ROOT))))
    return docs


def overlay_replicas(overlay: str) -> dict[tuple[str, str], int]:
    """Replica counts an overlay's strategic-merge patches set, keyed by (kind, name).

    Scaling a base workload to zero is how this repo retires one, and it happens in a patch —
    so a test that reads only `resources:` sees a workload the cluster is not running.
    """
    directory = REPO_ROOT / "k8s" / overlay
    kustomization = yaml.safe_load((directory / "kustomization.yaml").read_text()) or {}
    overrides: dict[tuple[str, str], int] = {}
    for patch in kustomization.get("patches") or []:
        path = patch.get("path") if isinstance(patch, dict) else None
        if not path:
            continue
        for doc in load_docs(str((directory / path).resolve().relative_to(REPO_ROOT))):
            replicas = (doc.get("spec") or {}).get("replicas")
            name = (doc.get("metadata") or {}).get("name")
            if isinstance(replicas, int) and doc.get("kind") and name:
                overrides[(doc["kind"], name)] = replicas
    return overrides


def pod_spec(doc: dict[str, Any]) -> dict[str, Any]:
    return ((doc.get("spec") or {}).get("template") or {}).get("spec") or {}


def pod_labels(doc: dict[str, Any]) -> dict[str, str]:
    return dict((((doc.get("spec") or {}).get("template") or {}).get("metadata") or {}).get("labels") or {})


def workload(docs: list[dict[str, Any]], name: str, kinds: frozenset[str] = WORKLOAD_KINDS) -> dict[str, Any]:
    for doc in docs:
        if doc.get("kind") in kinds and (doc.get("metadata") or {}).get("name") == name:
            return doc
    raise AssertionError(f"no {'/'.join(sorted(kinds))} named {name!r}")
