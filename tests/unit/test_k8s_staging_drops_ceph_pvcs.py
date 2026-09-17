"""Staging overlay unmounts every object-pool CephFS claim.

The shared cache PVC, plus the unused persist/dlq claims on api-local, must go
in one apply. Postgres/Redis storageClass is out of scope — those are data
services, not the object pool. CI's kustomize panics on `$patch: delete`, so
the claims are omitted from the staging overlay (they live in production) and
the leftover mounts are JSON-patched off.
"""

from __future__ import annotations

import yaml

from tests.unit.k8s_manifests import REPO_ROOT
from tests.unit.k8s_manifests import load_docs
from tests.unit.k8s_manifests import overlay_docs
from tests.unit.k8s_manifests import pod_spec


def test_janitor_patch_disables_fs_gc() -> None:
    docs = load_docs("k8s/staging/janitor-db-only.yaml")
    assert len(docs) == 1
    container = pod_spec(docs[0])["containers"][0]
    env = {item["name"]: item.get("value") for item in container.get("env") or []}
    assert env["HIPPIUS_JANITOR_FS_GC_ENABLED"] == "false"
    assert env["HIPPIUS_JANITOR_CEPH_MGR_METRICS_URL"] == ""
    assert env["HIPPIUS_JANITOR_CEPH_POOLS"] == ""


def test_global_uploader_stays_at_zero() -> None:
    docs = load_docs("k8s/staging/arion-uploader-scale.yaml")
    assert docs[0]["spec"]["replicas"] == 0


def test_staging_overlay_omits_object_pool_claims() -> None:
    names = {
        (d.get("metadata") or {}).get("name")
        for d in overlay_docs("staging")
        if d.get("kind") == "PersistentVolumeClaim"
    }
    assert "object-cache-pvc" not in names
    assert "persist-pvc" not in names
    assert "dlq-pvc" not in names


def test_production_overlay_still_declares_object_pool_claims() -> None:
    names = {
        (d.get("metadata") or {}).get("name")
        for d in overlay_docs("production")
        if d.get("kind") == "PersistentVolumeClaim"
    }
    assert {"object-cache-pvc", "persist-pvc", "dlq-pvc"} <= names


def test_staging_json_patches_drop_cache_volumes() -> None:
    kust = yaml.safe_load((REPO_ROOT / "k8s/staging/kustomization.yaml").read_text())
    by_name = {
        (p.get("target") or {}).get("name"): yaml.safe_load(p["patch"])
        for p in kust.get("patches") or []
        if isinstance(p, dict) and p.get("patch")
    }
    assert by_name["janitor"] == [
        {"op": "remove", "path": "/spec/template/spec/containers/0/volumeMounts"},
        {"op": "remove", "path": "/spec/template/spec/volumes"},
    ]
    assert by_name["arion-uploader"] == [
        {"op": "remove", "path": "/spec/template/spec/containers/0/volumeMounts/0"},
        {"op": "remove", "path": "/spec/template/spec/volumes/0"},
    ]


def test_api_local_does_not_mount_cephfs_claims() -> None:
    docs = load_docs("k8s/staging/api-local-deployments-staging.yaml")
    spec = pod_spec(docs[0])
    claims = {
        (vol.get("persistentVolumeClaim") or {}).get("claimName")
        for vol in spec.get("volumes") or []
        if vol.get("persistentVolumeClaim")
    }
    assert claims == set()
    mounts = {m.get("mountPath") for c in spec.get("containers") or [] for m in c.get("volumeMounts") or []}
    assert "/var/lib/hippius/persist" not in mounts
    assert "/tmp/hippius_dlq" not in mounts
    assert "/var/lib/hippius/object_cache" not in mounts
