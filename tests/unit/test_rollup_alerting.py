"""The alerting for the storage rollup must exist, and must reference metrics that exist.

Two failure modes, both of which this feature actually had:

  1. A DESIGN THAT PROMISES AN ALERT AND SHIPS NONE. `k8s/base/workers-deployments.yaml` says
     "Alert on storage_rollup_ledger_lag_seconds, not on pod restarts", and until these rules were
     written Prometheus had zero rule groups loaded cluster-wide -- `/api/v1/rules` returned
     `{"groups":[]}`. A maintained billing counter whose staleness alarm does not exist is the one
     thing this design cannot afford, because a frozen counter is a WRONG BILL, not an outage, and
     nothing else notices it.

  2. A RULE THAT REFERENCES A METRIC NOBODY EMITS. That fails silently and forever -- an expression
     over a non-existent series is simply never true, so the rule looks healthy in the UI and
     protects nothing. Every metric named in the rules is checked against monitoring.py here.

A third thing worth pinning: the health signals must be GAUGES. As histograms they export only
`_bucket`/`_count`/`_sum`, so "what is the lag right now" has to be faked as `_sum/_count` over a
window -- which is an AVERAGE, not a current value, and averages away exactly the spike you are
alerting on. That is how they were originally defined.
"""

from __future__ import annotations

import pathlib
import re

import yaml


_ROOT = pathlib.Path(__file__).resolve().parents[2]
_VALUES = _ROOT / "k8s" / "otel" / "values" / "prometheus.yaml"
_MONITORING = _ROOT / "hippius_s3" / "monitoring.py"

# Health signals that an alert must be able to read as a CURRENT value.
_MUST_BE_GAUGES = (
    "storage_rollup_ledger_depth",
    "storage_rollup_ledger_lag_seconds",
    "storage_rollup_negative_buckets",
    "plans_cache_age_seconds",
)


def _rule_groups() -> list[dict]:
    """The alerting rules as Prometheus itself will parse them.

    They live in the chart's `serverFiles."alerting_rules.yml"`, which the helm release renders into
    the prometheus-server ConfigMap key of the same name -- the path already wired into
    `rule_files: /etc/config/alerting_rules.yml`. Parsing the embedded YAML rather than trusting the
    string means a malformed rule fails here instead of being silently dropped at load time.
    """
    values = yaml.safe_load(_VALUES.read_text())
    embedded = values["serverFiles"]["alerting_rules.yml"]
    parsed = embedded if isinstance(embedded, dict) else yaml.safe_load(embedded)
    return parsed["groups"]


def _all_rules() -> list[dict]:
    return [rule for group in _rule_groups() for rule in group.get("rules", [])]


def test_alerting_rules_exist_and_parse() -> None:
    groups = _rule_groups()

    assert groups, "no alerting rule groups at all -- the design's stated safety net does not exist"
    rules = _all_rules()
    assert len(rules) >= 5, f"only {len(rules)} alert rules; the rollout depends on more than that"


def test_every_metric_referenced_by_a_rule_is_actually_emitted() -> None:
    """A rule over a metric nobody emits is never true, so it looks healthy and protects nothing."""
    monitoring = _MONITORING.read_text()
    unknown: list[str] = []

    for rule in _all_rules():
        expr = rule["expr"]
        # Metric-shaped identifiers, minus PromQL functions/keywords and the histogram suffixes.
        for token in set(re.findall(r"\b([a-z_][a-z0-9_]{6,})\b", expr)):
            base = re.sub(r"_(bucket|count|sum)$", "", token)
            if base in {"absent_over_time", "increase", "changes", "success", "outcome", "namespace", "exported_job"}:
                continue
            if base.startswith(("storage_rollup", "plans_cach", "plan_gate")) and base not in monitoring:
                unknown.append(f"{rule['alert']}: {token}")

    assert not unknown, (
        "these rules reference metrics that hippius_s3/monitoring.py does not define, so they can "
        "never fire:\n  " + "\n  ".join(unknown)
    )


def test_the_frozen_counter_alert_is_absence_based() -> None:
    """The one alert that can catch the stated failure mode.

    A value threshold on lag CANNOT fire when the worker is down, which is the failure the design
    names: these are push-based recordings from inside the worker loop, so no worker means no
    samples and `lag > N` has no series to evaluate. Depth is also sampled immediately after the
    drain, so it reads ~0 by construction. Absence of the cycle counter is what actually detects it.
    """
    absence = [r for r in _all_rules() if "absent" in r["expr"]]

    assert absence, (
        "no absence-based alert. Without one, a usage-rollup worker that is down or crash-looping "
        "silently freezes every plan account's usage figure and nothing fires."
    )
    assert any("storage_rollup_cycles_total" in r["expr"] for r in absence), (
        "the absence alert does not watch storage_rollup_cycles_total, which is the only series "
        "that stops being produced when the worker stops running"
    )


def test_drift_is_alerted_on() -> None:
    """Drift is the signal that a write path is moving bytes without emitting a delta.

    It is expected to be exactly zero once backfilled. It is also the only thing that would catch a
    sibling of the over-count bug this feature already shipped once.
    """
    assert any("drift" in r["expr"] for r in _all_rules()), "nothing alerts on rollup drift"


def test_the_health_signals_are_gauges_not_histograms() -> None:
    """As histograms these export only _bucket/_count/_sum, so a rule has to average them.

    `_sum/_count` over a window is the MEAN, which averages away the spike being alerted on -- and
    it is also wrong for a value that is sampled once per cycle. All four were histograms.
    """
    monitoring = _MONITORING.read_text()
    wrong: list[str] = []

    for metric in _MUST_BE_GAUGES:
        # Find the create_* call that defines this metric name.
        match = re.search(r"create_(\w+)\(\s*\n?\s*name=\"" + re.escape(metric) + r"\"", monitoring)
        if match is None:
            match = re.search(r"create_(\w+)\(\s*\n?\s*name=\"" + re.escape(metric) + r"\",", monitoring)
        assert match, f"{metric} is not defined in monitoring.py at all"
        if match.group(1) != "gauge":
            wrong.append(f"{metric} is a {match.group(1)}, must be a gauge")

    assert not wrong, "\n  ".join(wrong)
