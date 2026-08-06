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

    def __init__(
        self,
        prom: dict[str, float | None] | None = None,
        rows: list[list[str]] | None = None,
        scalar: str | None = None,
    ):
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
    assert (
        guards.ReplicationGateCoverage().check(FakeProbe(prom={"max(janitor_underreplicated_live_chunks)": 0.0})).status
        == "ok"
    )


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


# ----------------------------------------------------------------- G9 aged-pending-orphan backlog
def _orphan_probe(value: float | None) -> FakeProbe:
    return FakeProbe(prom={"max(janitor_aged_pending_orphans)": value})


def test_g9_skip_when_gauge_absent():
    assert guards.AgedPendingOrphanBacklog().check(FakeProbe(prom={})).status == "skip"


def test_g9_ok_when_bounded_and_flat():
    g = guards.AgedPendingOrphanBacklog(bound=100, rise_window=5, rise_delta=5.0)
    for _ in range(6):
        r = g.check(_orphan_probe(3.0))  # steady low backlog
    assert r.status == "ok" and r.value == 3.0


def test_g9_breach_over_bound():
    g = guards.AgedPendingOrphanBacklog(bound=50)
    r = g.check(_orphan_probe(51.0))
    assert r.status == "breach" and "bound" in r.detail


def test_g9_breach_on_rising_trough_within_bound():
    # A leak below the absolute bound must breach via the trough-rising check. The signal is a
    # SAWTOOTH (accumulate then sweep-drop); the peaks (50, 55) are noise — the leak shows as the
    # post-sweep FLOOR climbing 5 -> 12. span = 2*rise_window = 4.
    g = guards.AgedPendingOrphanBacklog(bound=1000, rise_window=2, rise_delta=5.0)
    statuses = [g.check(_orphan_probe(float(v))).status for v in (5, 50, 12, 55)]
    assert statuses[-1] == "breach", "rising trough (5->12) over the window must breach"
    assert statuses[:3] == ["ok", "ok", "ok"], "no verdict until both window halves are full"


def test_g9_ok_on_sawtooth_with_flat_trough():
    # The false-positive a two-point endpoint delta would hit: big sawtooth swings but a FLAT
    # trough (5 -> 6) is healthy — the trough comparison must NOT breach.
    g = guards.AgedPendingOrphanBacklog(bound=1000, rise_window=2, rise_delta=5.0)
    r = None
    for v in (5, 50, 6, 55):
        r = g.check(_orphan_probe(float(v)))
    assert r.status == "ok", "a flat trough under a swinging sawtooth is healthy, not a leak"


# ----------------------------------------------------------------- G5/G7/G8 are inv-det/scenario
def test_invdet_guards_skip():
    for name in ("G5", "G7", "G8"):
        assert guards.make_guard(name).check(FakeProbe()).status == "skip"


def test_g9_registered_in_factory():
    assert isinstance(guards.make_guard("G9"), guards.AgedPendingOrphanBacklog)


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


# ----------------------------------------------------------------- inv_guard all-skip verdict (A4)
# A run where a REQUIRED guard (G1/G2) skipped EVERY poll never actually asserted the invariant, so
# it must NOT read green. `required_never_asserted` is the pure verdict helper main() gates on.
from inv import inv_guard  # noqa: E402


def test_required_guard_all_skip_is_flagged():
    # G1 requested, polled 5x, never once asserted (prometheus down all run) -> flagged as not-green.
    flagged = inv_guard.required_never_asserted(
        names=["G1", "G3"], guard_asserts={"G3": 5}, guard_polls={"G1": 5, "G3": 5}
    )
    assert flagged == ["G1"]


def test_required_guard_that_asserted_once_is_not_flagged():
    # A single real evaluation (ok or breach) means the guard ran — not the silent-green case.
    flagged = inv_guard.required_never_asserted(names=["G1"], guard_asserts={"G1": 1}, guard_polls={"G1": 5})
    assert flagged == []


def test_non_required_guard_all_skip_is_not_flagged():
    # G3 is not load-bearing; an all-skip G3 (pg unreachable) does not by itself fail the run.
    flagged = inv_guard.required_never_asserted(names=["G3"], guard_asserts={}, guard_polls={"G3": 5})
    assert flagged == []


def test_unrequested_required_guard_is_not_flagged():
    # G2 not in --guards -> never polled -> not enforced (the operator opted out of it).
    flagged = inv_guard.required_never_asserted(names=["G1"], guard_asserts={"G1": 3}, guard_polls={"G1": 3})
    assert flagged == []


def test_both_required_guards_all_skip_are_both_flagged():
    flagged = inv_guard.required_never_asserted(
        names=["G1", "G2", "G4"], guard_asserts={"G4": 4}, guard_polls={"G1": 4, "G2": 4, "G4": 4}
    )
    assert flagged == ["G1", "G2"]
