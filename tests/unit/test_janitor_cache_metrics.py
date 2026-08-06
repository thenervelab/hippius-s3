"""The janitor must report the work it actually does.

Two independent gaps made every cache panel on the "Hippius S3 — System Load" dashboard read
zero on prod while the janitor was busy:

1. `cleanup_stale_parts` deletes the overwhelming majority of parts in prod and incremented
   nothing. Only its `abandoned` sub-case touched a counter, and only a dedicated one. So
   `fs_janitor_deleted_total` — the metric behind the GC-rate panel — had never been
   incremented at all and did not exist as a series in Prometheus.

2. The census gauges (`fs_store_parts_on_disk`, `fs_cache_age_bucket_parts`,
   `fs_cache_hot_parts`) are only assigned at the END of the GC phase, which runs after
   `cleanup_stale_parts`. When that earlier phase runs long they stay at their initial 0,
   which is indistinguishable from "the cache is empty".

Measured on prod 2026-07-23: 18,737 parts deleted in <2h with zero cycle completions, disk at
5.18 TB, and every one of those gauges reporting 0.
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest


REPO_ROOT = Path(__file__).resolve().parents[2]
JANITOR = REPO_ROOT / "workers" / "run_janitor_in_loop.py"
DASHBOARD = REPO_ROOT / "monitoring" / "grafana" / "dashboards" / "system-load.json"


@pytest.fixture(scope="module")
def janitor_source() -> str:
    return JANITOR.read_text()


@pytest.fixture(scope="module")
def dashboard() -> dict:
    return json.loads(DASHBOARD.read_text())


def _panel(dashboard: dict, panel_id: int) -> dict:
    for panel in dashboard["panels"]:
        if panel.get("id") == panel_id:
            return panel
    raise AssertionError(f"panel {panel_id} is missing from the dashboard")


def _exprs(panel: dict) -> list[str]:
    return [t["expr"] for t in panel.get("targets", []) if t.get("expr")]


# ---- emission ----


@pytest.mark.parametrize("reason", ["stale_mtime", "gc_age", "abandoned"])
def test_every_delete_path_increments_the_counter(janitor_source: str, reason: str) -> None:
    assert f'attributes={{"reason": "{reason}"}}' in janitor_source, (
        f"the {reason} deletion path does not increment fs_janitor_deleted_total, so the "
        f"delete-rate panel under-reports (it read flat zero on prod because stale_mtime — "
        f"the busiest path by far — counted nothing)."
    )


def test_stale_path_counts_outside_the_abandoned_branch(janitor_source: str) -> None:
    """The bug was that only the `abandoned` sub-case counted; the plain path fell through."""
    assert janitor_source.count("_janitor_deleted_counter.add(") >= 3, (
        "expected the counter to be incremented from the gc_age, stale_mtime and abandoned paths"
    )


def test_cycle_progress_gauges_are_registered(janitor_source: str) -> None:
    """Without these, a stuck phase is indistinguishable from an empty cache."""
    for name in ("fs_janitor_phase", "fs_janitor_last_cycle_age_seconds", "fs_janitor_cycle_seconds"):
        assert f'name="{name}"' in janitor_source, f"{name} gauge is not registered"


def test_phase_index_stays_within_the_phase_names(janitor_source: str) -> None:
    """_obs_janitor_phase indexes JANITOR_PHASES directly — an out-of-range assignment would
    raise inside the OTel callback and silently drop the whole metric export."""
    import re

    assigned = {int(m) for m in re.findall(r"^\s*_janitor_phase = (\d+)$", janitor_source, re.MULTILINE)}
    phases = re.search(r"JANITOR_PHASES = \((.*?)\)", janitor_source, re.DOTALL)
    assert phases, "JANITOR_PHASES tuple not found"
    count = len([p for p in phases.group(1).split(",") if p.strip()])
    assert assigned, "no phase assignments found"
    assert max(assigned) < count, f"phase index {max(assigned)} is out of range for {count} names"


def test_last_cycle_age_reports_negative_before_any_cycle_completes() -> None:
    """-1 distinguishes 'never completed a cycle' from 'completed one just now' (0)."""
    import importlib.util

    spec = importlib.util.spec_from_file_location("janitor_probe", JANITOR)
    assert spec and spec.loader
    source = JANITOR.read_text()
    assert "_janitor_last_cycle_completed_at <= 0" in source
    assert "Observation(-1.0, {})" in source


# ---- dashboard ----


def test_delete_rate_panel_breaks_out_by_reason(dashboard: dict) -> None:
    exprs = _exprs(_panel(dashboard, 64))
    assert any("by (reason)" in e and "fs_janitor_deleted_total" in e for e in exprs), (
        "the delete-rate panel must group by reason, otherwise the stale_mtime path (the bulk "
        "of prod deletions) is invisible next to gc_age"
    )


def test_cycle_health_panel_exists(dashboard: dict) -> None:
    exprs = _exprs(_panel(dashboard, 65))
    assert any("fs_janitor_last_cycle_age_seconds" in e for e in exprs)


def test_census_panels_say_they_can_be_stale(dashboard: dict) -> None:
    """A zero on these panels means 'not measured yet' as often as it means 'empty'."""
    for panel_id in (54, 62):
        description = _panel(dashboard, panel_id).get("description", "")
        assert "COMPLETED" in description, (
            f"panel {panel_id} does not say its census comes from the last completed GC phase, "
            f"so a stale zero reads as a real zero"
        )


def test_ssd_backlog_is_per_node(dashboard: dict) -> None:
    """The drain is per-node; a bare max() hides which node is falling behind."""
    exprs = _exprs(_panel(dashboard, 87))
    assert any("by (service_instance_id)" in e for e in exprs), "SSD backlog collapses every ingest node into one line"


def test_dashboard_is_valid_json_and_panel_ids_are_unique(dashboard: dict) -> None:
    ids = [p.get("id") for p in dashboard["panels"]]
    assert len(ids) == len(set(ids)), f"duplicate panel ids: {[i for i in ids if ids.count(i) > 1]}"
