"""Staging and production overlays unmount every object-pool CephFS claim.

The shared cache PVC, plus the unused persist/dlq claims on api-local, must go
in one apply. Postgres/Redis storageClass is out of scope — those are data
services, not the object pool. CI's kustomize panics on `$patch: delete`, so
the claims are omitted from both overlays and leftover mounts are JSON-patched
off. Deploy workflows delete the live claims after apply (apply does not prune).
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


def test_production_overlay_omits_object_pool_claims() -> None:
    names = {
        (d.get("metadata") or {}).get("name")
        for d in overlay_docs("production")
        if d.get("kind") == "PersistentVolumeClaim"
    }
    assert "object-cache-pvc" not in names
    assert "persist-pvc" not in names
    assert "dlq-pvc" not in names


def _json_patches_by_name(overlay: str) -> dict:
    kust = yaml.safe_load((REPO_ROOT / f"k8s/{overlay}/kustomization.yaml").read_text())
    return {
        (p.get("target") or {}).get("name"): yaml.safe_load(p["patch"])
        for p in kust.get("patches") or []
        if isinstance(p, dict) and p.get("patch")
    }


def test_staging_json_patches_drop_cache_volumes() -> None:
    by_name = _json_patches_by_name("staging")
    assert by_name["janitor"] == [
        {"op": "remove", "path": "/spec/template/spec/containers/0/volumeMounts"},
        {"op": "remove", "path": "/spec/template/spec/volumes"},
    ]
    assert by_name["arion-uploader"] == [
        {"op": "remove", "path": "/spec/template/spec/containers/0/volumeMounts/0"},
        {"op": "remove", "path": "/spec/template/spec/volumes/0"},
    ]


def test_production_json_patches_drop_cache_volumes() -> None:
    by_name = _json_patches_by_name("production")
    assert by_name["janitor"] == [
        {"op": "remove", "path": "/spec/template/spec/containers/0/volumeMounts"},
        {"op": "remove", "path": "/spec/template/spec/volumes"},
    ]
    assert by_name["arion-uploader"] == [
        {"op": "remove", "path": "/spec/template/spec/containers/0/volumeMounts/0"},
        {"op": "remove", "path": "/spec/template/spec/volumes/0"},
    ]


def test_production_janitor_disables_fs_gc() -> None:
    docs = load_docs("k8s/production/janitor-db-only.yaml")
    container = pod_spec(docs[0])["containers"][0]
    env = {item["name"]: item.get("value") for item in container.get("env") or []}
    assert env["HIPPIUS_JANITOR_FS_GC_ENABLED"] == "false"


def test_production_global_uploader_replicas_zero() -> None:
    docs = load_docs("k8s/production/resource-limits.yaml")
    uploader = next(d for d in docs if d.get("kind") == "Deployment" and (d.get("metadata") or {}).get("name") == "arion-uploader")
    assert uploader["spec"]["replicas"] == 0


def _assert_api_local_has_no_cephfs_claims(path: str) -> None:
    docs = load_docs(path)
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


def test_api_local_does_not_mount_cephfs_claims() -> None:
    _assert_api_local_has_no_cephfs_claims("k8s/staging/api-local-deployments-staging.yaml")


def test_deploy_workflows_delete_the_three_claims() -> None:
    staging = (REPO_ROOT / ".github/workflows/staging-deploy.yaml").read_text()
    production = (REPO_ROOT / ".github/workflows/production-deploy.yaml").read_text()
    for text in (staging, production):
        assert "kubectl delete pvc object-cache-pvc persist-pvc dlq-pvc" in text


def test_production_deploy_deletes_leftover_ceph_postgres_cluster() -> None:
    production = (REPO_ROOT / ".github/workflows/production-deploy.yaml").read_text()
    staging = (REPO_ROOT / ".github/workflows/staging-deploy.yaml").read_text()
    assert "kubectl delete cluster.postgresql.cnpg.io postgres" in production
    assert "hippius-s3-prod" in production
    assert "kubectl delete cluster.postgresql.cnpg.io postgres" not in staging


def test_production_api_local_does_not_mount_cephfs_claims() -> None:
    _assert_api_local_has_no_cephfs_claims("k8s/production/api-local-deployments-production.yaml")
    docs = load_docs("k8s/production/api-local-deployments-production.yaml")
    env = {e["name"]: e.get("value") for e in pod_spec(docs[0])["containers"][0].get("env") or []}
    assert "HIPPIUS_OBJECT_CACHE_FALLBACK_DIR" not in env


def _assert_api_local_peer_fetch_is_on_without_a_pool(path: str) -> None:
    docs = load_docs(path)
    env = {e["name"]: e.get("value") for e in pod_spec(docs[0])["containers"][0].get("env") or []}
    assert env.get("HIPPIUS_PEER_FETCH_ENABLED") == "true"
    assert env.get("HIPPIUS_PEER_SERVE_ENABLED") == "true"
    assert "HIPPIUS_OBJECT_CACHE_FALLBACK_DIR" not in env, (
        "a pool fallback on api-local hides a missing peer_fetch; production has no pool"
    )


def test_staging_api_local_peer_fetches_without_a_pool() -> None:
    _assert_api_local_peer_fetch_is_on_without_a_pool("k8s/staging/api-local-deployments-staging.yaml")


def test_production_api_local_peer_fetches_without_a_pool() -> None:
    _assert_api_local_peer_fetch_is_on_without_a_pool("k8s/production/api-local-deployments-production.yaml")
