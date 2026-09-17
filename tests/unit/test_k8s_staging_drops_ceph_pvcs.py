"""Staging overlay unmounts every object-pool CephFS claim.

The shared cache PVC, plus the unused persist/dlq claims on api-local, must go
in one apply. Postgres/Redis storageClass is out of scope — those are data
services, not the object pool.
"""

from __future__ import annotations

from tests.unit.k8s_manifests import load_docs
from tests.unit.k8s_manifests import pod_spec


def test_janitor_patch_disables_fs_gc_and_drops_the_cache_mount() -> None:
    docs = load_docs("k8s/staging/janitor-db-only.yaml")
    assert len(docs) == 1
    container = pod_spec(docs[0])["containers"][0]
    env = {item["name"]: item.get("value") for item in container.get("env") or []}
    assert env["HIPPIUS_JANITOR_FS_GC_ENABLED"] == "false"
    assert env["HIPPIUS_JANITOR_CEPH_MGR_METRICS_URL"] == ""
    assert env["HIPPIUS_JANITOR_CEPH_POOLS"] == ""
    mounts = container.get("volumeMounts") or []
    assert any(m.get("$patch") == "delete" and m.get("mountPath") == "/var/lib/hippius/object_cache" for m in mounts)
    volumes = (docs[0].get("spec") or {}).get("template", {}).get("spec", {}).get("volumes") or []
    assert any(v.get("$patch") == "delete" and v.get("name") == "object-cache" for v in volumes)


def test_global_uploader_drops_the_cache_mount() -> None:
    docs = load_docs("k8s/staging/arion-uploader-scale.yaml")
    assert docs[0]["spec"]["replicas"] == 0
    container = pod_spec(docs[0])["containers"][0]
    mounts = container.get("volumeMounts") or []
    assert any(m.get("$patch") == "delete" and m.get("mountPath") == "/var/lib/hippius/object_cache" for m in mounts)


def test_staging_deletes_the_three_cephfs_claims() -> None:
    docs = load_docs("k8s/staging/drop-ceph-pvcs.yaml")
    names = {(d.get("metadata") or {}).get("name") for d in docs}
    assert names == {"object-cache-pvc", "persist-pvc", "dlq-pvc"}
    assert all(d.get("$patch") == "delete" for d in docs)


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
