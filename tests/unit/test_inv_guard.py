"""Unit truth-tables for the inv-guard predicates (WI-19 §4.2).

Pure logic: a FakeProbe returns canned Postgres/Prometheus results, so every guard's
ok/breach/skip verdict — and the stateful epoch/terminal-transition tracking — is exercised
without a live cluster."""

from __future__ import annotations

import pathlib
import sys


sys.path.insert(0, str(pathlib.Path(__file__).resolve().parents[2] / "stress-test"))

from inv import guards  # noqa: E402


class FakeProbe:
    """Duck-typed ClusterProbe: canned `prom_scalar` values and `pg` rows, or None (unreachable)."""

    def __init__(self, prom: dict[str, float | None] | None = None, rows: list[list[str]] | None = None, scalar: str | None = None):
        self._prom = prom or {}
        self._rows = rows
        self._scalar = scalar

    def prom_scalar(self, promql: str) -> float | None:
        return self._prom.get(promql)

    def pg(self, sql: str) -> list[list[str]] | None:
        return self._rows

    def pg_scalar(self, sql: str) -> str | None:
        return self._scalar


# ----------------------------------------------------------------- G1 single-leader + epoch
def test_g1_ok_single_leader():
    g = guards.SingleLeaderEpoch()
    r = g.check(FakeProbe(prom={"sum(drain_leader)": 1.0, "max(drain_leader_epoch)": 7.0}))
    assert r.status == "ok"


def test_g1_breach_two_leaders():
    g = guards.SingleLeaderEpoch()
    r = g.check(FakeProbe(prom={"sum(drain_leader)": 2.0, "max(drain_leader_epoch)": 7.0}))
    assert r.status == "breach" and "split-brain" in r.detail


def test_g1_breach_on_epoch_decrease():
    g = guards.SingleLeaderEpoch()
    assert g.check(FakeProbe(prom={"sum(drain_leader)": 1.0, "max(drain_leader_epoch)": 9.0})).status == "ok"
    # counter reset: epoch drops below the high-water mark.
    r = g.check(FakeProbe(prom={"sum(drain_leader)": 1.0, "max(drain_leader_epoch)": 4.0}))
    assert r.status == "breach" and "decreased" in r.detail


def test_g1_ok_on_epoch_increase():
    g = guards.SingleLeaderEpoch()
    g.check(FakeProbe(prom={"sum(drain_leader)": 1.0, "max(drain_leader_epoch)": 3.0}))
    assert g.check(FakeProbe(prom={"sum(drain_leader)": 1.0, "max(drain_leader_epoch)": 5.0})).status == "ok"


def test_g1_skip_when_prometheus_unreachable():
    assert guards.SingleLeaderEpoch().check(FakeProbe(prom={})).status == "skip"


# ----------------------------------------------------------------- G2 replication-gate coverage
def test_g2_ok_no_underreplicated():
    assert guards.ReplicationGateCoverage().check(FakeProbe(prom={"max(janitor_underreplicated_live_chunks)": 0.0})).status == "ok"


def test_g2_breach_underreplicated():
    r = guards.ReplicationGateCoverage().check(FakeProbe(prom={"max(janitor_underreplicated_live_chunks)": 3.0}))
    assert r.status == "breach" and r.value == 3.0


def test_g2_skip_when_metric_absent():
    assert guards.ReplicationGateCoverage().check(FakeProbe(prom={})).status == "skip"


# ----------------------------------------------------------------- G3 stalled-drain (SQL half)
def test_g3_ok_nothing_stalled():
    assert guards.StalledDrain().check(FakeProbe(scalar="0")).status == "ok"


def test_g3_breach_stalled_parts():
    r = guards.StalledDrain(stall_secs=60).check(FakeProbe(scalar="4"))
    assert r.status == "breach" and "stalled" in r.detail


def test_g3_skip_when_pg_unreachable():
    assert guards.StalledDrain().check(FakeProbe(scalar=None)).status == "skip"


# ----------------------------------------------------------------- G4 sole-producer (runtime)
def test_g4_ok_no_duplicates():
    assert guards.SoleProducer().check(FakeProbe(rows=[])).status == "ok"


def test_g4_breach_duplicate_backend_row():
    r = guards.SoleProducer().check(FakeProbe(rows=[["chunk-1", "arion", "2"]]))
    assert r.status == "breach"


def test_g4_skip_when_pg_unreachable():
    assert guards.SoleProducer().check(FakeProbe(rows=None)).status == "skip"


# ----------------------------------------------------------------- G6 terminal monotonicity
def _crs_rows(status_by_part: dict[int, str]) -> list[list[str]]:
    return [["obj-a", "1", str(pn), st] for pn, st in status_by_part.items()]


def test_g6_seeds_then_ok_on_forward_progress():
    g = guards.TerminalMonotonicity()
    # first poll seeds the baseline (pending), no verdict yet beyond ok
    assert g.check(FakeProbe(rows=_crs_rows({0: "pending"}))).status == "ok"
    # pending -> replicated is legal forward progress
    assert g.check(FakeProbe(rows=_crs_rows({0: "replicated"}))).status == "ok"


def test_g6_breach_on_terminal_regression():
    g = guards.TerminalMonotonicity()
    g.check(FakeProbe(rows=_crs_rows({0: "replicated"})))  # seed a terminal row
    # replicated -> pending is a terminal-state regression
    r = g.check(FakeProbe(rows=_crs_rows({0: "pending"})))
    assert r.status == "breach" and "regression" in r.detail


def test_g6_breach_on_failed_leaving_terminal():
    g = guards.TerminalMonotonicity()
    g.check(FakeProbe(rows=_crs_rows({0: "failed"})))
    r = g.check(FakeProbe(rows=_crs_rows({0: "draining"})))
    assert r.status == "breach"


def test_g6_skip_when_pg_unreachable():
    assert guards.TerminalMonotonicity().check(FakeProbe(rows=None)).status == "skip"


# ----------------------------------------------------------------- G5/G7/G8 are inv-det/scenario
def test_invdet_guards_skip():
    for name in ("G5", "G7", "G8"):
        assert guards.make_guard(name).check(FakeProbe()).status == "skip"


# ----------------------------------------------------------------- run_once aggregation
def test_run_once_counts_breaches_and_emits_each():
    g1 = guards.SingleLeaderEpoch()
    g4 = guards.SoleProducer()
    probe = FakeProbe(prom={"sum(drain_leader)": 2.0}, rows=[])  # G1 breaches, G4 ok
    events: list[guards.GuardResult] = []
    breaches = guards.run_once([g1, g4], probe, events.append)
    assert breaches == 1
    assert [e.guard for e in events] == ["G1", "G4"]
    assert events[0].status == "breach" and events[1].status == "ok"


def test_make_guard_rejects_unknown():
    import pytest

    with pytest.raises(KeyError):
        guards.make_guard("G99")
