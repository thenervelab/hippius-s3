"""Continuous invariant guards (WI-19 §4.2) — the predicates `inv_guard` polls every run.

Each guard is a small stateful object with `.check(probe) -> GuardResult`, where `probe` is
duck-typed on `harness.probes.ClusterProbe` (`pg`, `pg_scalar`, `prom_scalar`). Kept free of any
cluster/harness import so the predicate logic is unit-testable with a fake probe. `inv_guard.py`
is the thin runtime wrapper that wires a real `ClusterProbe` and streams the results to JSONL.

Guard coverage here (the drain-internal invariants queryable from Postgres + Prometheus):
  G1 single-leader + epoch-monotonicity · G2 replication-gate coverage · G3 stalled-drain (SQL
  half; the byte-identical re-GET half is ledger/scenario-driven) · G4 sole-producer (runtime
  uniqueness; the static half is inv-det) · G6 terminal-state monotonicity.
G5/G7/G8 are inv-det / scenario-driven (at-least-once-no-dup, AEAD determinism, reaper safety) and
report `skip` here so a run's guard table is explicit about what this asserter does NOT cover.
"""

from __future__ import annotations

import dataclasses
from typing import Protocol


class Probe(Protocol):
    """The subset of `harness.probes.ClusterProbe` the guards use (duck-typed for testing)."""

    def pg(self, sql: str) -> list[list[str]] | None: ...
    def pg_scalar(self, sql: str) -> str | None: ...
    def prom_scalar(self, promql: str) -> float | None: ...


@dataclasses.dataclass
class GuardResult:
    """One guard's verdict for one poll. `status` is `ok` | `breach` | `skip` (skip = could not
    evaluate, e.g. the probe was unreachable — NOT a pass)."""

    guard: str
    status: str
    detail: str
    value: float | None = None


class _Guard(Protocol):
    name: str

    def check(self, probe: Probe) -> GuardResult: ...


def _pg_bool(cell: str) -> bool:
    """Read a boolean column out of a `probe.pg` row. `psql -tA` renders booleans as `t`/`f`, so
    anything else — including the empty cell a NULL would give — is False."""
    return cell.strip().lower() in ("t", "true")


class SingleLeaderEpoch:
    """G1: at most one allocator leads, and the leadership epoch never decreases.

    `sum(drain_leader) >= 2` is split-brain. A decrease in `max(drain_leader_epoch)` means the
    redis `cephor:epoch` counter was reset (data loss / flush), which silently breaks the write
    fence — so the guard carries the high-water epoch across polls and breaches on any drop.
    """

    name = "G1"

    def __init__(self) -> None:
        self._max_epoch: float | None = None

    def check(self, probe: Probe) -> GuardResult:
        leaders = probe.prom_scalar("sum(drain_leader)")
        if leaders is None:
            return GuardResult(self.name, "skip", "prometheus unreachable")
        if leaders >= 2:
            return GuardResult(self.name, "breach", f"{int(leaders)} concurrent leaders (split-brain)", leaders)
        epoch = probe.prom_scalar("max(drain_leader_epoch)")
        if epoch is not None:
            if self._max_epoch is not None and epoch < self._max_epoch:
                return GuardResult(
                    self.name, "breach", f"epoch decreased {int(self._max_epoch)}->{int(epoch)} (counter reset)", epoch
                )
            self._max_epoch = epoch if self._max_epoch is None else max(self._max_epoch, epoch)
        return GuardResult(
            self.name, "ok", f"leaders={int(leaders)} epoch={int(epoch) if epoch is not None else 'n/a'}", leaders
        )


class ReplicationGateCoverage:
    """G2: no live, serveable chunk lacks full-union backend coverage.

    Reads George's janitor replication-gate sentinel metric (WI-20 / #233): a nonzero count is a
    live chunk at data-loss risk the gate must never reclaim. Absent metric → skip (sentinel not
    deployed yet), so the guard never silently passes on a missing signal.
    """

    name = "G2"

    def check(self, probe: Probe) -> GuardResult:
        under = probe.prom_scalar("max(janitor_underreplicated_live_chunks)")
        if under is None:
            return GuardResult(self.name, "skip", "janitor_underreplicated_live_chunks absent (sentinel not deployed)")
        if under > 0:
            return GuardResult(self.name, "breach", f"{int(under)} under-replicated live chunk(s)", under)
        return GuardResult(self.name, "ok", "no under-replicated live chunks", under)


class StalledDrain:
    """G3 (SQL half): no `pending`/`draining` part stuck past the lag SLO.

    The byte-identical re-GET half of G3 is driven by the ledger (§4.3) in the scenario runner, not
    here. This half catches a drain that has quietly stopped making progress on the backlog.
    """

    name = "G3"

    def __init__(self, stall_secs: int = 120) -> None:
        self._stall_secs = stall_secs

    def check(self, probe: Probe) -> GuardResult:
        stalled = probe.pg_scalar(
            "SELECT count(*) FROM cephor_replication_status "
            f"WHERE status IN ('pending','draining') AND landed_at < now() - interval '{self._stall_secs} seconds'"
        )
        if stalled is None:
            return GuardResult(self.name, "skip", "postgres unreachable")
        count = int(stalled)
        if count > 0:
            return GuardResult(
                self.name, "breach", f"{count} part(s) stalled > {self._stall_secs}s in pending/draining", float(count)
            )
        return GuardResult(self.name, "ok", f"no part stalled past {self._stall_secs}s", 0.0)


class SoleProducer:
    """G4 (runtime half): every `(chunk_id, backend)` is written at most once.

    A duplicate means a second upload producer re-appeared (the drain must be the only one; the
    static callerless-audit half is inv-det). Soft-deleted rows are excluded — a re-pin after an
    unpin is legitimate.
    """

    name = "G4"

    def check(self, probe: Probe) -> GuardResult:
        rows = probe.pg(
            "SELECT chunk_id, backend, count(*) FROM chunk_backend "
            "WHERE deleted = false GROUP BY chunk_id, backend HAVING count(*) > 1 LIMIT 5"
        )
        if rows is None:
            return GuardResult(self.name, "skip", "postgres unreachable")
        if rows:
            return GuardResult(self.name, "breach", f"duplicate (chunk_id, backend): {rows[:3]}")
        return GuardResult(self.name, "ok", "no duplicate (chunk_id, backend)")


@dataclasses.dataclass(frozen=True)
class _StatusSample:
    """One `cephor_replication_status` row as G6 samples it: its status plus whether the row
    carries a fresh B-2 re-landing stamp (see `TerminalMonotonicity`)."""

    status: str
    recently_relanded: bool


class TerminalMonotonicity:
    """G6: a terminal replication state (`replicated`/`failed`) is never left.

    Samples every row's status each poll and compares to the previous sample; any
    `replicated->*` or `failed->*` transition is a breach (a terminal row must be inert). First
    poll only seeds the baseline.

    CAVEAT (poll-sampled — parity with the note in scenarios.py `invariant_assert`): this only sees
    state at each poll boundary (inv_guard --interval, default 2s), so a fast
    `replicated->pending->replicated` round-trip that both leaves and re-enters the terminal state
    inside ONE interval is invisible. The 2s cadence narrows that blind window but cannot close it;
    the real fix is the WAL-tailed event-driven guard (plan §4.2) that sees every transition rather
    than a sampled snapshot.

    NOTE: `corrupt` (R4) is deliberately NOT terminal — the bounded re-drive worker legitimately
    resets `corrupt->pending` to re-copy a live object's intact SSD source over its corrupt pool
    copy. Only `replicated`/`failed` are inert sinks, so `corrupt->pending` must not be flagged.

    CARVE-OUT (B-2 content-change re-drive): `Store::redrive_diverged_part` legitimately returns a
    `replicated` part to `pending` when a re-landed part's `content_sha256` no longer matches what
    was committed — the pool holds superseded bytes and must be re-copied. That is a real exit from
    a terminal state, so it is permitted, but only under the predicate that makes it recognisable:
    the re-drive is causally DOWNSTREAM of a re-landing, and `record_landed_part` stamps
    `relanded_at` in the same statement that reads the prior `replicated` status — strictly before
    the divergence check can flip the row. So a legitimate `replicated->pending` ALWAYS carries a
    fresh `relanded_at`, and the guard permits the transition only when the current sample says so
    (`_RELAND_RECENCY`, sized as the drain's 10-minute reland eviction grace plus slack, since the
    stamp is never cleared and only its recency is meaningful). A `replicated->pending` with a NULL
    or stale stamp is the silent-data-loss shape G6 exists to catch and still breaches, as does
    every other terminal exit — `replicated->failed`, `replicated->draining`, `failed->*`. On a
    cluster predating the B-2 migration the carve-out cannot apply at all; see `_sample`.
    """

    name = "G6"
    _TERMINAL = frozenset({"replicated", "failed"})
    _KEY_COLUMNS = "object_id, version, part_number, status"
    # Evaluated in the sample SQL rather than against a fetched timestamp: `now()` is then
    # Postgres' clock, so the predicate never depends on the asserter host's clock skew.
    _RECENCY = "(relanded_at IS NOT NULL AND relanded_at > now() - interval '15 minutes')"

    def __init__(self) -> None:
        self._prev: dict[tuple[str, str, str], _StatusSample] = {}
        self._samples_recency = True

    def check(self, probe: Probe) -> GuardResult:
        rows, columns = self._sample(probe)
        if rows is None:
            return GuardResult(self.name, "skip", "postgres unreachable")
        current = {
            (r[0], r[1], r[2]): _StatusSample(r[3], columns == 5 and _pg_bool(r[4])) for r in rows if len(r) >= columns
        }
        if rows and not current:
            # Every sampled row was too short to read, i.e. the sample query and this parser have
            # diverged. Dropping them silently would leave the guard tracking nothing and reporting
            # `ok` forever — a silent pass on the invariant, which is worse than an explicit skip.
            return GuardResult(
                self.name, "skip", f"unreadable sample rows (got {len(rows[0])} columns, want {columns})"
            )
        breaches = [
            f"{key}: {prev.status}->{current[key].status}"
            for key, prev in self._prev.items()
            if key in current and self._is_regression(prev.status, current[key])
        ]
        self._prev = current
        if breaches:
            return GuardResult(self.name, "breach", f"terminal-state regression: {breaches[:3]}")
        return GuardResult(self.name, "ok", f"{len(current)} rows tracked, no terminal regression")

    def _sample(self, probe: Probe) -> tuple[list[list[str]] | None, int]:
        """Sample every row's status, with the reland-recency column when the cluster has it.

        A cluster older than the B-2 migration (0019) has no `relanded_at`, and psql fails that
        query indistinguishably from an unreachable pg. So the absent-column case falls back to the
        plain sample ONCE and latches: with no column no re-drive can have happened, making the
        pre-B-2 rule (every terminal exit breaches) the correct one — far better than mislabelling
        the whole run's G6 as `postgres unreachable` and asserting nothing. Both queries failing is
        the genuinely-unreachable case and does NOT latch, so the recency sample resumes on recovery.
        """
        if self._samples_recency:
            rows = probe.pg(f"SELECT {self._KEY_COLUMNS}, {self._RECENCY} FROM cephor_replication_status")
            if rows is not None:
                return (rows, 5)
            rows = probe.pg(f"SELECT {self._KEY_COLUMNS} FROM cephor_replication_status")
            if rows is None:
                return (None, 5)
            self._samples_recency = False
            return (rows, 4)
        return (probe.pg(f"SELECT {self._KEY_COLUMNS} FROM cephor_replication_status"), 4)

    def _is_regression(self, prev: str, now: _StatusSample) -> bool:
        if prev not in self._TERMINAL or now.status == prev:
            return False
        return not (prev == "replicated" and now.status == "pending" and now.recently_relanded)


class AgedPendingOrphanBacklog:
    """G9 (A21 soak-gate feed): the aged-pending-orphan backlog stays bounded and flat.

    Reads the janitor's `janitor_aged_pending_orphans` gauge — the standing count of aged,
    unservable `pending`/`draining` versions, i.e. the exact population the reaper's orphan
    sweep clears (gauge grace == sweep grace by construction). On a healthy stack this hovers
    near zero; a re-introduced A21 leak makes it climb. Two breach conditions:

      * BOUNDED — the backlog exceeds `bound` (a standing leak the sweep is not clearing).
      * RISING  — the post-sweep TROUGH is climbing. The gauge is inherently a sawtooth (it
        accumulates as versions age past the grace, then drops toward 0 each time the reaper's
        sweep clears them), so a two-point endpoint delta gives false positives/negatives. The
        real leak signal is the FLOOR rising: over a `2*rise_window` history, breach if the
        minimum of the recent half exceeds the minimum of the older half by >= `rise_delta`.
        Comparing troughs is immune to where in the sawtooth each sample landed.

    Absent metric → skip (janitor sentinel not deployed), never a silent pass.
    """

    name = "G9"

    def __init__(self, bound: int = 100, rise_window: int = 5, rise_delta: float = 5.0) -> None:
        self._bound = bound
        self._rise_window = rise_window
        self._rise_delta = rise_delta
        self._history: list[float] = []

    def check(self, probe: Probe) -> GuardResult:
        backlog = probe.prom_scalar("max(janitor_aged_pending_orphans)")
        if backlog is None:
            return GuardResult(self.name, "skip", "janitor_aged_pending_orphans absent (gauge not deployed)")
        span = 2 * self._rise_window
        self._history.append(backlog)
        if len(self._history) > span:
            self._history = self._history[-span:]
        if backlog > self._bound:
            return GuardResult(
                self.name, "breach", f"aged-pending-orphan backlog {int(backlog)} > bound {self._bound}", backlog
            )
        # Trough-rising check — only assertable once both halves of the window are full.
        if len(self._history) == span:
            old_trough = min(self._history[: self._rise_window])
            new_trough = min(self._history[self._rise_window :])
            if new_trough - old_trough >= self._rise_delta:
                return GuardResult(
                    self.name,
                    "breach",
                    f"aged-pending-orphan post-sweep trough rising {old_trough:g}->{new_trough:g} over {span} polls",
                    backlog,
                )
        return GuardResult(self.name, "ok", f"aged-pending-orphan backlog {int(backlog)} bounded & flat", backlog)


class _InvDetGuard:
    """G5/G7/G8: covered by inv-det or the scenario runner, not this periodic asserter — reported
    as `skip` so the guard table is explicit rather than silently omitting them."""

    def __init__(self, name: str, where: str) -> None:
        self.name = name
        self._where = where

    def check(self, probe: Probe) -> GuardResult:
        return GuardResult(self.name, "skip", f"covered by {self._where}, not the continuous asserter")


def make_guard(name: str) -> _Guard:
    """Instantiate a guard by its G-number. Raises KeyError on an unknown name."""
    factories = {
        "G1": SingleLeaderEpoch,
        "G2": ReplicationGateCoverage,
        "G3": StalledDrain,
        "G4": SoleProducer,
        "G5": lambda: _InvDetGuard("G5", "inv-det + scenario (at-least-once-no-dup)"),
        "G6": TerminalMonotonicity,
        "G7": lambda: _InvDetGuard("G7", "inv-det (AEAD determinism)"),
        "G8": lambda: _InvDetGuard("G8", "inv-det + reaper scenario (reaper safety)"),
        "G9": AgedPendingOrphanBacklog,
    }
    return factories[name]()


def run_once(guards: list[_Guard], probe: Probe, emit) -> int:
    """Poll every guard once, pass each result to `emit`, and return the number of BREACHES.

    Pure orchestration: `probe` is duck-typed and `emit` is a callback, so a test drives this with a
    fake probe and captures the emitted events without any cluster or file I/O.
    """
    breaches = 0
    for guard in guards:
        result = guard.check(probe)
        emit(result)
        if result.status == "breach":
            breaches += 1
    return breaches
