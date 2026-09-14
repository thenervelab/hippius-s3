"""A worker that reads the schema should wait for migrations before starting.

Workers `exec python <script>` with no migrate step of their own — unlike the API, whose
start-api.sh runs migrate before uvicorn. So a freshly-rolled worker can start against a schema
older than its code. The base manifest's own comment says this; the `wait-for-migrations` init
container is the fix, and most workers have it.

`usage-rollup` shipped without it and hit exactly that on the staging rollout:
`UndefinedFunctionError: function storage_usage_rollup_lock_key() does not exist`. It self-healed
on the next tick, because its loop catches and retries — but a worker whose entire job is calling
functions a migration creates is the last one that should be racing that migration.

This test does not try to settle whether the three workers below ought to gate too; it pins the
CURRENT set so that adding a worker forces a deliberate choice rather than inheriting whichever
block was copy-pasted.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

import yaml


REPO_ROOT = Path(__file__).resolve().parents[2]
MANIFEST = "k8s/base/workers-deployments.yaml"

GATE = "wait-for-migrations"

# Workers that declare DATABASE_URL and deliberately do NOT gate on migrations. Each is here because
# a schema race is survivable for it, not because the gate would be wrong:
#
#   plans-cacher    -- every cycle is wrapped by run_cycle, which records the failure, keeps the
#                      previous roll serving and retries in 60s. Blocking its startup would delay
#                      cache warming for no gain, and a stale roll is its designed degradation.
#   account-cacher  -- mirrors Substrate state into Redis; its DB DSN comes from the shared env
#                      block rather than from reading application tables on the hot path.
#   cachet-health-checker -- pushes status to an external status page.
#
# Adding to this set is a decision to make explicitly, with the reason written down.
UNGATED_BY_DESIGN = {"plans-cacher", "account-cacher", "cachet-health-checker"}


def _worker_docs() -> list[dict[str, Any]]:
    text = (REPO_ROOT / MANIFEST).read_text()
    return [d for d in yaml.safe_load_all(text) if isinstance(d, dict) and (d.get("metadata") or {}).get("name")]


def _pod_spec(doc: dict[str, Any]) -> dict[str, Any]:
    return (doc.get("spec") or {}).get("template", {}).get("spec") or {}


def _declares_database_url(doc: dict[str, Any]) -> bool:
    for container in _pod_spec(doc).get("containers") or []:
        for env in container.get("env") or []:
            if env.get("name") == "DATABASE_URL":
                return True
    return False


def _gates_on_migrations(doc: dict[str, Any]) -> bool:
    return any(c.get("name") == GATE for c in _pod_spec(doc).get("initContainers") or [])


def test_every_db_reading_worker_gates_on_migrations_or_is_a_named_exception() -> None:
    missing = sorted(
        (doc.get("metadata") or {})["name"]
        for doc in _worker_docs()
        if _declares_database_url(doc)
        and not _gates_on_migrations(doc)
        and (doc.get("metadata") or {})["name"] not in UNGATED_BY_DESIGN
    )

    assert not missing, (
        f"These workers read the schema but do not wait for migrations, so a freshly-rolled pod can "
        f"start against an older schema: {missing}. Add the '{GATE}' init container, or add the "
        f"worker to UNGATED_BY_DESIGN with the reason it is safe."
    )


def test_usage_rollup_specifically_gates() -> None:
    """Named because it is the one that actually raced its own migration, and because its whole job
    is calling functions that migration creates."""
    doc = next(d for d in _worker_docs() if (d.get("metadata") or {})["name"] == "usage-rollup")

    assert _gates_on_migrations(doc)


def test_the_exception_list_does_not_rot() -> None:
    """An entry for a worker that has since gained the gate, or been deleted, is stale — it would
    keep silently excusing something that no longer needs excusing."""
    names = {(d.get("metadata") or {})["name"] for d in _worker_docs()}
    gated = {(d.get("metadata") or {})["name"] for d in _worker_docs() if _gates_on_migrations(d)}

    assert not (UNGATED_BY_DESIGN - names), f"UNGATED_BY_DESIGN names workers that no longer exist: {UNGATED_BY_DESIGN - names}"
    assert not (UNGATED_BY_DESIGN & gated), (
        f"These are listed as ungated by design but now have the gate; drop them from the list: "
        f"{sorted(UNGATED_BY_DESIGN & gated)}"
    )
