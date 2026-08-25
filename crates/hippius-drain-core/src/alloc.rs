//! The pure allocator core: turns fleet state + a Ceph ceiling into a per-node
//! write-budget. mClock vocabulary — reservation (guaranteed floor) / limit
//! (ceiling) / weight (pressure share) — over an AIMD-estimated capacity.
//!
//! Everything here is a pure function of its inputs: no clock, no I/O, integer
//! arithmetic only (so the result is deterministic and the invariants are
//! proptest-able). `u128` intermediates guard the proportional multiplications
//! against overflow; no `f64` appears, so there is no NaN/ordering hazard.

use crate::ids::NodeId;
use crate::state::CephCeiling;
use crate::units::{ByteRate, Bytes, DiskPressure};
use std::collections::BTreeMap;
use std::collections::btree_map::Iter as BTreeIter;
use std::time::Duration;

/// One node's self-reported state — the input to allocation for that node.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct NodeObservation {
    /// The node's drain urgency — the water-fill weight and the reservation-floor gate.
    ///
    /// Asymmetric by design, and only until the fleet is fully rolled: an *agent* fills this
    /// with the raw SSD fullness it measured (a fact it can always report), while the
    /// allocator's wire boundary ([`NodeStateJson::into_observation`]) replaces it with
    /// [`DiskPressure::from_drain_demand`] whenever the reporting agent is new enough to send
    /// [`free`](Self::free)/[`cache`](Self::cache). Raw fullness is the conservative fallback
    /// for a not-yet-rolled agent, and stops being reachable once every agent reports
    /// residency.
    pub pressure: DiskPressure,
    /// Bytes currently waiting to drain. Zero backlog means zero demand.
    pub backlog: Bytes,
    /// Free bytes on the node's SSD.
    pub free: Bytes,
    /// Retained, already-`replicated` bytes held on the SSD to serve reads. Evictable on
    /// demand, so this counts toward ingest headroom rather than against it — the distinction
    /// that keeps a full read cache from reading as a drain emergency.
    pub cache: Bytes,
    /// The fastest the node can push to Ceph locally (its demand cap).
    pub max_drain_rate: ByteRate,
    /// The node's observed Ceph write p99 latency (feeds the saturation signal).
    pub observed_p99: Duration,
    /// The node's observed Ceph write error rate, in basis points (`0..=10000`).
    pub error_bps: u16,
}

/// Snapshot of the fleet keyed by node id.
///
/// `BTreeMap` (not `HashMap`) so allocation order — and therefore the
/// deterministic tie-breaking in water-filling — is stable across runs.
#[derive(Debug, Clone, Default)]
pub struct FleetView {
    nodes: BTreeMap<NodeId, NodeObservation>,
}

impl FleetView {
    /// An empty fleet.
    #[must_use]
    pub fn new() -> Self {
        Self { nodes: BTreeMap::new() }
    }

    /// Records (or replaces) a node's observation.
    pub fn insert(&mut self, node: NodeId, observation: NodeObservation) {
        self.nodes.insert(node, observation);
    }

    /// Iterates nodes in id order.
    pub fn iter(&self) -> BTreeIter<'_, NodeId, NodeObservation> {
        self.nodes.iter()
    }

    /// Number of nodes.
    #[must_use]
    pub fn len(&self) -> usize {
        self.nodes.len()
    }

    /// Whether the fleet is empty.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.nodes.is_empty()
    }
}

impl<'a> IntoIterator for &'a FleetView {
    type Item = (&'a NodeId, &'a NodeObservation);
    type IntoIter = BTreeIter<'a, NodeId, NodeObservation>;

    fn into_iter(self) -> Self::IntoIter {
        self.nodes.iter()
    }
}

/// Tuning knobs for the allocation control loop.
///
/// Precondition (caller's responsibility): `min_total <= max_total` and
/// `decrease_permille <= 1000`. These are operational tuning values (the design
/// doc defers them to a staging load test), so they are plain public fields.
#[derive(Debug, Clone, Copy)]
pub struct AllocConfig {
    /// Floor the AIMD estimate never drops below.
    pub min_total: ByteRate,
    /// Ceiling the AIMD estimate never climbs above.
    pub max_total: ByteRate,
    /// Additive increase applied each healthy tick.
    pub additive_increase: ByteRate,
    /// Multiplicative decrease as parts-per-thousand of the current estimate
    /// (e.g. `800` => keep 80% on back-off).
    pub decrease_permille: u16,
    /// p99 latency above which the fleet is considered saturated.
    pub target_p99: Duration,
    /// Error rate (basis points) above which the fleet is considered saturated.
    pub max_error_bps: u16,
    /// Pressure at or above which a node earns a reservation floor.
    pub critical_pressure: DiskPressure,
    /// The guaranteed per-node floor for critical-pressure nodes.
    pub reservation_floor: ByteRate,
    /// Free-space floor the evictor holds when the drain is keeping up, in permille of disk.
    pub base_reserve_permille: u16,
    /// Free-space floor when the drain is fully stalled. Raising the reserve is what buys
    /// ingest runway: a throttled drain means backlog grows, and freeing cache EARLY is the
    /// only lever that keeps `fs_cache_pressure` from refusing PUTs later.
    pub max_reserve_permille: u16,
}

/// The carried AIMD state between ticks.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BudgetController {
    total: ByteRate,
}

impl BudgetController {
    /// Starts the controller at `initial` bytes/s.
    #[must_use]
    pub fn new(initial: ByteRate) -> Self {
        Self { total: initial }
    }

    /// The current fleet-wide estimate (before the instantaneous ceiling clamp).
    #[must_use]
    pub fn total(self) -> ByteRate {
        self.total
    }
}

/// One node's allocated write-budget for the next tick.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Allocation {
    /// The node this budget is for.
    pub node: NodeId,
    /// The write rate the node may use this tick.
    pub budget: ByteRate,
    /// The free-space floor the node's evictor should hold, in permille of its disk.
    ///
    /// The allocator sets this because it is the only component that knows *why* a node is
    /// not draining: it sees the fleet-wide Ceph ceiling and this node's budget against its
    /// demand. The evictor alone can only react once free space has already fallen, which on
    /// a stalled drain is too late to avoid 503s.
    ///
    /// **This value steers a threshold in another language.** The agent derives the api's
    /// read-through promote floor from whatever reserve is in force
    /// (`hippius-drain-agent`'s `published_promote_floor`) and publishes it on the queues Redis
    /// for `hippius_s3/promote_floor.py` to consume, so raising this reserve also raises the
    /// free-space level at which the api stops warming that node's cache — by design, since
    /// this reserve rises precisely when the drain is in trouble. Widening the range this can
    /// take is therefore a change to the api's read path too, not only to eviction.
    pub reserve_permille: u16,
}

/// The result of an allocation tick.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AllocationPlan {
    /// The evolved AIMD state to carry into the next tick.
    pub controller: BudgetController,
    /// Per-node budgets, in node-id order.
    pub allocations: Vec<Allocation>,
    /// `false` when reservations could not all be honored within capacity
    /// (the "node-full + Ceph-critical" infeasible state — alert-worthy).
    pub feasible: bool,
}

/// Allocates the fleet-wide write budget for one tick.
///
/// Pure and total: degenerate inputs (empty fleet, zero capacity) yield a
/// well-formed plan with zero budgets, never a panic.
#[must_use]
pub fn allocate(fleet: &FleetView, ceiling: CephCeiling, prev: BudgetController, config: &AllocConfig) -> AllocationPlan {
    let (controller, capacity) = next_capacity(fleet, ceiling, prev, config);
    let (allocations, feasible) = distribute(fleet, capacity, config, ceph_severity_permille(ceiling));
    AllocationPlan {
        controller,
        allocations,
        feasible,
    }
}

/// How far the fleet's Ceph ceiling is from healthy, in permille.
///
/// `Critical` is total: the pool accepts no writes at all, so every node buffers on SSD and
/// backlog grows at the full ingest rate. `NearFull` is treated as half-severity — the drain
/// still moves, just slower.
fn ceph_severity_permille(ceiling: CephCeiling) -> u32 {
    match ceiling {
        CephCeiling::Open(_) => 0,
        CephCeiling::NearFull(_) => 500,
        CephCeiling::Critical => 1_000,
    }
}

/// The free-space floor for one node, in permille of its disk.
///
/// Interpolates between `base_reserve_permille` and `max_reserve_permille` on the WORSE of two
/// severities, because either alone is enough to starve the drain:
///
/// - the fleet's Ceph ceiling — a `Critical` pool stalls every node at once;
/// - this node's own shortfall, `(demand - budget) / demand` — the water-fill may leave one
///   node short while its peers are satisfied, and that node is the one accumulating backlog.
///
/// A node with no demand has no shortfall (0/0 is 0, not "totally starved"), so a caught-up
/// node never evicts beyond the base floor.
fn reserve_permille(demand: u64, budget: u64, ceph_permille: u32, config: &AllocConfig) -> u16 {
    let shortfall_permille = if demand == 0 {
        0
    } else {
        let short = demand.saturating_sub(budget);
        u32::try_from(u128::from(short) * 1_000 / u128::from(demand)).unwrap_or(1_000)
    };
    let severity = ceph_permille.max(shortfall_permille).min(1_000);
    let base = u32::from(config.base_reserve_permille);
    let ceiling = u32::from(config.max_reserve_permille).max(base);
    let span = ceiling - base;
    u16::try_from(base + span * severity / 1_000).unwrap_or(config.max_reserve_permille)
}

/// A node's working state during distribution.
struct Entry {
    node: NodeId,
    /// Most this node can use (its drain rate, or 0 with an empty backlog).
    demand: u64,
    /// Guaranteed floor for a critical-pressure node, capped at its demand.
    reserved: u64,
    /// Allocation weight (pressure basis points, at least 1 so any hungry node drains).
    weight: u128,
    /// Bytes/s allocated so far.
    filled: u64,
}

/// Splits `capacity` across the fleet: reservations first, then a weighted
/// water-fill of the remainder, capped at each node's demand.
///
/// Returns `(allocations_in_id_order, feasible)`. `feasible` is `false` when the
/// reservations alone exceed capacity (they are then scaled down proportionally).
fn distribute(fleet: &FleetView, capacity: ByteRate, config: &AllocConfig, ceph_permille: u32) -> (Vec<Allocation>, bool) {
    let cap = capacity.get();
    let critical = config.critical_pressure;
    let floor = config.reservation_floor.get();

    let mut entries: Vec<Entry> = fleet
        .iter()
        .map(|(id, obs)| {
            let demand = if obs.backlog.get() == 0 { 0 } else { obs.max_drain_rate.get() };
            let reserved = if obs.pressure >= critical && demand > 0 { floor.min(demand) } else { 0 };
            Entry {
                node: id.clone(),
                demand,
                reserved,
                weight: u128::from(obs.pressure.bps()).max(1),
                filled: 0,
            }
        })
        .collect();

    let total_reserved = entries.iter().fold(0u64, |acc, e| acc.saturating_add(e.reserved));

    if total_reserved > cap {
        // Infeasible: cannot honor every reservation. Scale them to fit and flag
        // the state (this is the node-full + Ceph-critical alert condition).
        for entry in &mut entries {
            entry.filled = if total_reserved == 0 {
                0
            } else {
                u64::try_from(u128::from(entry.reserved) * u128::from(cap) / u128::from(total_reserved)).unwrap_or(0)
            };
        }
        // Exact reconciliation, as on the feasible path: integer flooring above
        // sums to at most `cap`, leaving a residual below the node count. Hand it
        // to nodes still under their reservation (id order, capped at `reserved`
        // so rationing never exceeds a node's floor), so the critical fleet uses
        // every byte of scarce capacity rather than silently dropping it. The
        // residual always fits: `total_reserved > cap` means the unfilled
        // reservation headroom strictly exceeds `cap - sum(filled)`.
        let mut remaining = cap - entries.iter().fold(0u64, |acc, e| acc.saturating_add(e.filled));
        for entry in &mut entries {
            if remaining == 0 {
                break;
            }
            let give = remaining.min(entry.reserved.saturating_sub(entry.filled));
            entry.filled = entry.filled.saturating_add(give);
            remaining -= give;
        }
        return (to_allocations(entries, config, ceph_permille), false);
    }

    for entry in &mut entries {
        entry.filled = entry.reserved;
    }
    let mut remaining = cap - total_reserved;

    // Weighted water-fill: each round hands out `remaining` in proportion to
    // weight, clamped at demand. A node that hits its demand drops out and its
    // surplus flows to the rest on the next round. The pass ends once nothing is
    // capped; integer flooring leaves a tiny residual handled by the mop-up below.
    loop {
        let active_weight: u128 = entries.iter().filter(|e| e.filled < e.demand).map(|e| e.weight).sum();
        if remaining == 0 || active_weight == 0 {
            break;
        }
        let round_start = u128::from(remaining);
        let mut round_given = 0u64;
        let mut capped_any = false;
        for entry in &mut entries {
            let headroom = entry.demand.saturating_sub(entry.filled);
            if headroom == 0 {
                continue;
            }
            let want = u64::try_from(round_start * entry.weight / active_weight).unwrap_or(0);
            let give = want.min(headroom);
            if give < want {
                capped_any = true;
            }
            entry.filled = entry.filled.saturating_add(give);
            round_given = round_given.saturating_add(give);
        }
        remaining -= round_given;
        if !capped_any {
            break;
        }
    }

    // Exact reconciliation: hand any flooring residual to nodes that still have
    // headroom (id order), so the total is exactly min(capacity, total demand).
    for entry in &mut entries {
        if remaining == 0 {
            break;
        }
        let give = remaining.min(entry.demand.saturating_sub(entry.filled));
        entry.filled = entry.filled.saturating_add(give);
        remaining -= give;
    }

    (to_allocations(entries, config, ceph_permille), true)
}

fn to_allocations(entries: Vec<Entry>, config: &AllocConfig, ceph_permille: u32) -> Vec<Allocation> {
    entries
        .into_iter()
        .map(|e| Allocation {
            reserve_permille: reserve_permille(e.demand, e.filled, ceph_permille, config),
            node: e.node,
            budget: ByteRate::new(e.filled),
        })
        .collect()
}

/// Evolves the AIMD estimate and clamps it by the ceiling.
///
/// Returns `(carried_state, distributable_capacity)`. The carried state tracks
/// the fleet estimate independent of the instantaneous ceiling so the fleet
/// resumes from a sane rate when the ceiling lifts; the distributable capacity
/// is the estimate clamped by `ceiling.budget()`.
fn next_capacity(fleet: &FleetView, ceiling: CephCeiling, prev: BudgetController, config: &AllocConfig) -> (BudgetController, ByteRate) {
    let latency_saturated = fleet
        .iter()
        .any(|(_, obs)| obs.observed_p99 > config.target_p99 || obs.error_bps > config.max_error_bps);
    // Back off on latency/error saturation OR whenever Ceph is not fully open —
    // never ramp the estimate up while we are only buffering on SSD.
    let back_off = latency_saturated || !matches!(ceiling, CephCeiling::Open(_));

    let prev_total = prev.total.get();
    let evolved = if back_off {
        u64::try_from(u128::from(prev_total) * u128::from(config.decrease_permille) / 1000).unwrap_or(prev_total)
    } else {
        prev_total.saturating_add(config.additive_increase.get())
    };
    // Both arms land in the same band. Clamping only the arm that moves toward each
    // bound leaves a carried estimate that started outside the band stuck there: a
    // `prev` above `max_total` (an operator lowering the ceiling between ticks) would
    // back off only geometrically instead of being capped on the first tick.
    let new_total = evolved.max(config.min_total.get()).min(config.max_total.get());

    let capacity = new_total.min(ceiling.budget().get());
    (BudgetController::new(ByteRate::new(new_total)), ByteRate::new(capacity))
}

#[cfg(test)]
#[expect(clippy::unwrap_used, reason = "tests")]
mod tests {
    use super::{AllocConfig, Allocation, BudgetController, FleetView, NodeObservation, allocate};
    use crate::ids::NodeId;
    use crate::state::CephCeiling;
    use crate::units::{ByteRate, Bytes, DiskPressure};
    use core::str::FromStr;
    use proptest::prelude::*;
    use std::time::Duration;

    fn config() -> AllocConfig {
        AllocConfig {
            min_total: ByteRate::new(1_000),
            max_total: ByteRate::new(1_000_000_000),
            additive_increase: ByteRate::new(10_000),
            decrease_permille: 800,
            target_p99: Duration::from_millis(50),
            max_error_bps: 100,
            critical_pressure: DiskPressure::try_from(9_000).unwrap(),
            reservation_floor: ByteRate::new(50_000),
            base_reserve_permille: 150,
            max_reserve_permille: 400,
        }
    }

    // ------------------------------------------------ eviction reserve (Phase 4)

    #[test]
    fn a_healthy_fleet_that_gets_its_demand_keeps_the_base_reserve() {
        // Nothing is starved and Ceph is open, so backlog is being cleared as fast as it
        // arrives. There is no reason to give up cached reads for headroom nobody needs.
        let fleet = fleet_of(&[node("a", 1_000, 1_000, 10_000), node("b", 1_000, 1_000, 10_000)]);
        let plan = allocate(
            &fleet,
            CephCeiling::Open(ByteRate::new(1_000_000)),
            BudgetController::new(ByteRate::new(1_000_000)),
            &config(),
        );

        for allocation in &plan.allocations {
            assert_eq!(allocation.reserve_permille, 150, "base reserve for {}", allocation.node);
        }
    }

    #[test]
    fn a_critical_ceph_ceiling_raises_the_reserve_to_buy_ingest_runway() {
        // Ceph cannot accept writes, so the fleet buffers on SSD: backlog now grows at the
        // full ingest rate and free space is the scarce resource. Freeing cache EARLY is what
        // buys runway before fs_cache_pressure starts refusing PUTs. Holding more cache here
        // — the intuition that Ceph pressure means "keep more locally" — would consume the
        // very space the incoming backlog needs.
        //
        // Note both terms saturate here (a Critical ceiling zeroes capacity, so the node is
        // also fully starved); the Ceph term on its own is pinned by the NearFull case above.
        let fleet = fleet_of(&[node("a", 1_000, 1_000, 10_000)]);
        let plan = allocate(&fleet, CephCeiling::Critical, BudgetController::new(ByteRate::new(1_000_000)), &config());

        assert_eq!(plan.allocations[0].reserve_permille, 400, "a stalled drain evicts hardest");
    }

    #[test]
    fn a_near_full_pool_raises_the_reserve_even_on_a_fully_satisfied_node() {
        // Isolates the CEPH term from the shortfall term. Capacity here comfortably exceeds
        // demand, so this node gets everything it asks for and its shortfall is zero — any
        // reserve above the base can therefore only have come from the pool being NearFull.
        //
        // Without this case the Critical test below proves nothing about the Ceph term: a
        // Critical ceiling zeroes capacity, so the shortfall term alone would drive the
        // reserve to max and an inverted Ceph sign would still pass.
        let fleet = fleet_of(&[node("a", 1_000, 1_000, 10_000)]);
        let plan = allocate(
            &fleet,
            CephCeiling::NearFull(ByteRate::new(1_000_000)),
            BudgetController::new(ByteRate::new(1_000_000)),
            &config(),
        );

        assert_eq!(plan.allocations[0].budget.get(), 10_000, "the node got its full demand");
        assert_eq!(
            plan.allocations[0].reserve_permille, 275,
            "half severity: base 150 + half of the 250 span",
        );
    }

    #[test]
    fn a_starved_node_reserves_more_than_a_satisfied_peer_in_the_same_tick() {
        // Same tick, same healthy Ceph, equal weights: "small" wants only 1 KB/s and gets all
        // of it, while "big" wants 1 MB/s and is capped by fleet capacity. Ceph is fine and the
        // fleet-wide severity is zero, so the ONLY thing separating them is each node's own
        // shortfall — which is exactly why the reserve is per-node rather than a fleet
        // constant. Both pressures are below critical_pressure so the reservation floor stays
        // out of it; tripping that lands in the infeasible path and allocates by a different
        // rule entirely.
        let fleet = fleet_of(&[node("big", 8_000, 1_000, 1_000_000), node("small", 8_000, 1_000, 1_000)]);
        let plan = allocate(
            &fleet,
            CephCeiling::Open(ByteRate::new(20_000)),
            BudgetController::new(ByteRate::new(20_000)),
            &config(),
        );

        let big = plan.allocations.iter().find(|a| a.node.as_str() == "big").unwrap();
        let small = plan.allocations.iter().find(|a| a.node.as_str() == "small").unwrap();
        assert_eq!(small.budget.get(), 1_000, "the small node got everything it asked for");
        assert_eq!(small.reserve_permille, 150, "so it keeps the base reserve");
        assert!(
            big.reserve_permille > small.reserve_permille,
            "the node left short must reserve more: big {} vs small {}",
            big.reserve_permille,
            small.reserve_permille,
        );
    }

    #[test]
    fn a_caught_up_node_never_reserves_above_the_base() {
        // Zero backlog means zero demand, so this node is not short of anything — a shortfall
        // computed as (demand - budget) / demand must not read 0/0 as "totally starved" and
        // evict a healthy node's cache for nothing.
        let fleet = fleet_of(&[node("idle", 100, 0, 10_000)]);
        let plan = allocate(
            &fleet,
            CephCeiling::Open(ByteRate::new(1_000_000)),
            BudgetController::new(ByteRate::new(1_000_000)),
            &config(),
        );

        assert_eq!(plan.allocations[0].reserve_permille, 150);
    }

    fn node(id: &str, pressure_bps: u16, backlog: u64, max_rate: u64) -> (NodeId, NodeObservation) {
        (
            NodeId::from_str(id).unwrap(),
            NodeObservation {
                pressure: DiskPressure::try_from(pressure_bps).unwrap(),
                backlog: Bytes::new(backlog),
                free: Bytes::ZERO,
                cache: Bytes::ZERO,
                max_drain_rate: ByteRate::new(max_rate),
                observed_p99: Duration::from_millis(10),
                error_bps: 0,
            },
        )
    }

    fn fleet_of(nodes: &[(NodeId, NodeObservation)]) -> FleetView {
        let mut fleet = FleetView::new();
        for (id, obs) in nodes {
            fleet.insert(id.clone(), *obs);
        }
        fleet
    }

    fn budget_of(plan: &[Allocation], id: &str) -> u64 {
        let wanted = NodeId::from_str(id).unwrap();
        plan.iter().find(|a| a.node == wanted).map_or(0, |a| a.budget.get())
    }

    fn node_p99(id: &str, p99: Duration, backlog: u64) -> (NodeId, NodeObservation) {
        (
            NodeId::from_str(id).unwrap(),
            NodeObservation {
                pressure: DiskPressure::try_from(3_000).unwrap(),
                backlog: Bytes::new(backlog),
                free: Bytes::ZERO,
                cache: Bytes::ZERO,
                max_drain_rate: ByteRate::new(1_000_000_000),
                observed_p99: p99,
                error_bps: 0,
            },
        )
    }

    fn config_target_p99(ms: u64) -> AllocConfig {
        AllocConfig {
            target_p99: Duration::from_millis(ms),
            ..config()
        }
    }

    #[test]
    fn a_nearfull_budget_caps_capacity_below_the_aimd_floor() {
        // Regression for the 2026-07-24 incident: ops had raised min_total to 50 MB/s
        // (for latency reasons), and NearFull carried the full open rate, so the only
        // brake was the AIMD floor — five nodes kept flushing 10 MB/s each into a
        // ~98%-full pool. The NearFull budget must bound the distributed total even
        // when the AIMD estimate is pinned at a floor far above it.
        let min_total = ByteRate::new(50_000_000);
        let cfg = AllocConfig { min_total, ..config() };
        let nearfull_rate = ByteRate::new(10_000_000);
        let fleet = fleet_of(&[
            node("a", 5_000, 10_000_000_000, 1_000_000_000),
            node("b", 5_000, 10_000_000_000, 1_000_000_000),
            node("c", 5_000, 10_000_000_000, 1_000_000_000),
            node("d", 5_000, 10_000_000_000, 1_000_000_000),
            node("e", 5_000, 10_000_000_000, 1_000_000_000),
        ]);
        let plan = allocate(
            &fleet,
            CephCeiling::NearFull(nearfull_rate),
            BudgetController::new(ByteRate::new(1_000_000_000)),
            &cfg,
        );
        let distributed: u64 = plan.allocations.iter().map(|a| a.budget.get()).sum();
        assert!(
            distributed <= nearfull_rate.get(),
            "NearFull({nearfull}) must bound the fleet total; got {distributed} (AIMD floor {floor})",
            nearfull = nearfull_rate.get(),
            floor = min_total.get(),
        );
        assert!(
            plan.controller.total().get() >= min_total.get(),
            "the carried AIMD estimate keeps its floor so the fleet resumes promptly when the ceiling reopens",
        );
    }

    #[test]
    fn the_first_tick_starts_from_initial_total_not_the_floor() {
        // A fresh leader carries `initial_total` into its first tick, so a deployment that
        // starts high (500 MB/s over a 250 MB/s floor) must ramp from there. Reading the
        // start as "the floor" would cost the whole ramp — at 50 MB/s per tick, ~5 ticks —
        // every time leadership moves.
        let cfg = AllocConfig {
            min_total: ByteRate::new(250_000_000),
            additive_increase: ByteRate::new(50_000_000),
            ..config()
        };
        let fleet = fleet_of(&[node("a", 5_000, 10_000_000_000, 1_000_000_000)]);
        let plan = allocate(
            &fleet,
            CephCeiling::Open(ByteRate::new(1_000_000_000)),
            BudgetController::new(ByteRate::new(500_000_000)),
            &cfg,
        );
        assert_eq!(
            plan.controller.total().get(),
            550_000_000,
            "a healthy first tick increases the initial estimate, it does not snap to the floor",
        );
    }

    #[test]
    fn a_critical_ceiling_zeroes_capacity_even_with_a_floor_far_above_it() {
        // The floor bounds the ESTIMATE, never the distributed capacity: a pool that
        // accepts no writes must hand out nothing however high the AIMD floor is tuned.
        // The estimate still keeps its floor so the fleet resumes at rate — not from
        // scratch — the moment the ceiling reopens.
        let min_total = ByteRate::new(250_000_000);
        let cfg = AllocConfig { min_total, ..config() };
        let fleet = fleet_of(&[
            node("a", 9_500, 10_000_000_000, 1_000_000_000),
            node("b", 9_500, 10_000_000_000, 1_000_000_000),
        ]);
        let plan = allocate(&fleet, CephCeiling::Critical, BudgetController::new(ByteRate::new(1_000_000_000)), &cfg);
        for allocation in &plan.allocations {
            assert_eq!(allocation.budget.get(), 0, "node {} must get nothing", allocation.node);
        }
        assert!(plan.controller.total().get() >= min_total.get(), "the carried estimate keeps its floor");
    }

    #[test]
    fn a_healthy_slow_fleet_ramps_the_estimate_up() {
        // Regression for the drain throttle deadlock: a whole-part SSD->CephFS drain has a p99 of
        // hundreds of ms even when perfectly healthy (measured ~330-410 ms on staging). With a
        // realistic target (2 s) a 400 ms p99 is NOT saturation, so the AIMD must ramp the estimate
        // UP by additive_increase — not back off to min_total. Under the old 50 ms target this
        // fleet backed off every tick, pinning the whole fleet budget at the 1 MB/s floor.
        let fleet = fleet_of(&[node_p99("a", Duration::from_millis(400), 10_000_000_000)]);
        let cfg = config_target_p99(2_000);
        let plan = allocate(
            &fleet,
            CephCeiling::Open(ByteRate::new(1_000_000_000)),
            BudgetController::new(ByteRate::new(190_000)),
            &cfg,
        );
        assert_eq!(
            plan.controller.total().get(),
            190_000 + cfg.additive_increase.get(),
            "a healthy-but-slow fleet (p99 below the realistic target) ramps up by additive_increase",
        );
    }

    #[test]
    fn a_genuinely_saturated_fleet_still_backs_off() {
        // The other side of the fix: a p99 ABOVE the (realistic) target IS saturation and must back
        // off multiplicatively toward min_total — proving raising the target did not disable back-off.
        let fleet = fleet_of(&[node_p99("a", Duration::from_secs(3), 10_000_000_000)]);
        let cfg = config_target_p99(2_000);
        let plan = allocate(
            &fleet,
            CephCeiling::Open(ByteRate::new(1_000_000_000)),
            BudgetController::new(ByteRate::new(190_000)),
            &cfg,
        );
        assert!(plan.controller.total().get() < 190_000, "a p99 above the target backs the estimate off");
    }

    #[test]
    fn empty_fleet_yields_no_allocations() {
        let plan = allocate(
            &FleetView::new(),
            CephCeiling::Open(ByteRate::new(1_000_000)),
            BudgetController::new(ByteRate::new(100_000)),
            &config(),
        );
        assert!(plan.allocations.is_empty());
        assert!(plan.feasible);
    }

    #[test]
    fn a_single_node_takes_capacity_up_to_its_demand() {
        // Degenerate fleet (#31): one node, capacity below its demand -> it takes
        // all of capacity; no other node to share with, no flooring residual lost.
        let fleet = fleet_of(&[node("solo", 5_000, 10_000_000, 10_000_000)]);
        let plan = allocate(
            &fleet,
            CephCeiling::Open(ByteRate::new(1_000_000_000)),
            BudgetController::new(ByteRate::new(190_000)),
            &config(),
        );
        let capacity = plan.controller.total().get();
        assert!(plan.feasible);
        assert_eq!(
            budget_of(&plan.allocations, "solo"),
            capacity,
            "a lone node receives all the capacity it can use"
        );
    }

    #[test]
    fn an_all_idle_fleet_allocates_nothing() {
        // Degenerate fleet (#31): every node has zero backlog -> zero demand, so
        // no budget is handed out even though capacity is plentiful, and the plan
        // is still feasible (nothing to be infeasible about).
        let fleet = fleet_of(&[node("a", 9_500, 0, 10_000_000), node("b", 100, 0, 10_000_000)]);
        let plan = allocate(
            &fleet,
            CephCeiling::Open(ByteRate::new(1_000_000_000)),
            BudgetController::new(ByteRate::new(500_000)),
            &config(),
        );
        assert!(plan.feasible);
        let used: u64 = plan.allocations.iter().map(|a| a.budget.get()).sum();
        assert_eq!(used, 0, "an all-idle fleet drains nothing");
    }

    #[test]
    fn an_all_equal_fleet_splits_capacity_evenly() {
        // Degenerate fleet (#31): identical pressure and demand, capacity below the
        // combined demand and below the reservation threshold -> equal weights give
        // an even split (within one byte from integer flooring + the id-order mop-up).
        let fleet = fleet_of(&[node("a", 5_000, 10_000_000, 10_000_000), node("b", 5_000, 10_000_000, 10_000_000)]);
        let plan = allocate(
            &fleet,
            CephCeiling::Open(ByteRate::new(300_001)),
            BudgetController::new(ByteRate::new(290_000)),
            &config(),
        );
        let (a, b) = (budget_of(&plan.allocations, "a"), budget_of(&plan.allocations, "b"));
        let capacity = plan.controller.total().get().min(300_001);
        assert_eq!(a + b, capacity, "an even split still conserves capacity exactly");
        assert!(a.abs_diff(b) <= 1, "equal nodes split within one byte, got {a} and {b}");
    }

    #[test]
    fn infeasible_path_with_uneven_reservations_conserves_capacity() {
        // Complements the proptest (which uses uniform 50_000 reservations): here
        // the three critical nodes reserve 30_000 / 40_000 / 50_000 (two below the
        // floor via small demands), so the residual mop-up must cap each at its own
        // reservation while still placing every byte of the odd capacity.
        let fleet = fleet_of(&[
            node("n0", 10_000, 10_000_000, 30_000),
            node("n1", 10_000, 10_000_000, 40_000),
            node("n2", 10_000, 10_000_000, 10_000_000),
        ]);
        let plan = allocate(
            &fleet,
            CephCeiling::Open(ByteRate::new(50_001)),
            BudgetController::new(ByteRate::new(45_000)),
            &config(),
        );
        let capacity = plan.controller.total().get().min(50_001);
        let used: u64 = plan.allocations.iter().map(|a| a.budget.get()).sum();
        assert!(!plan.feasible, "reservations (120_000) exceed capacity");
        assert_eq!(used, capacity, "uneven rationing still conserves capacity exactly");
        assert!(budget_of(&plan.allocations, "n0") <= 30_000, "n0 never exceeds its reservation");
        assert!(budget_of(&plan.allocations, "n1") <= 40_000, "n1 never exceeds its reservation");
        assert!(budget_of(&plan.allocations, "n2") <= 50_000, "n2 never exceeds its reservation");
    }

    #[test]
    fn critical_ceiling_allocates_nothing() {
        let fleet = fleet_of(&[node("a", 5_000, 1_000_000, 1_000_000)]);
        let plan = allocate(&fleet, CephCeiling::Critical, BudgetController::new(ByteRate::new(500_000)), &config());
        assert_eq!(budget_of(&plan.allocations, "a"), 0);
    }

    #[test]
    fn idle_node_gets_zero_busy_node_gets_capacity() {
        // Capacity (~200k after one increase from 190k) is less than the busy
        // node's demand, so it should receive (nearly) all of it; idle gets 0.
        let fleet = fleet_of(&[node("busy", 5_000, 10_000_000, 10_000_000), node("idle", 5_000, 0, 10_000_000)]);
        let plan = allocate(
            &fleet,
            CephCeiling::Open(ByteRate::new(1_000_000_000)),
            BudgetController::new(ByteRate::new(190_000)),
            &config(),
        );
        assert_eq!(budget_of(&plan.allocations, "idle"), 0);
        assert!(budget_of(&plan.allocations, "busy") > 0);
    }

    #[test]
    fn higher_pressure_node_gets_more_under_contention() {
        // Two equally-hungry nodes, capacity well below their combined demand,
        // both below the reservation threshold: the weight (pressure) decides.
        let fleet = fleet_of(&[node("low", 2_000, 10_000_000, 10_000_000), node("high", 8_000, 10_000_000, 10_000_000)]);
        let plan = allocate(
            &fleet,
            CephCeiling::Open(ByteRate::new(300_000)),
            BudgetController::new(ByteRate::new(290_000)),
            &config(),
        );
        assert!(budget_of(&plan.allocations, "high") > budget_of(&plan.allocations, "low"));
    }

    #[test]
    fn critical_pressure_node_keeps_its_reservation_floor() {
        // A tiny-pressure hungry node would otherwise dominate, but the
        // critical-pressure node must still get at least its reservation floor.
        let fleet = fleet_of(&[node("hot", 9_500, 10_000_000, 10_000_000), node("cool", 100, 10_000_000, 10_000_000)]);
        let plan = allocate(
            &fleet,
            CephCeiling::Open(ByteRate::new(80_000)),
            BudgetController::new(ByteRate::new(70_000)),
            &config(),
        );
        assert!(budget_of(&plan.allocations, "hot") >= 50_000);
    }

    proptest! {
        /// Never allocate more in total than the distributable capacity.
        #[test]
        fn total_within_capacity(
            prev in 1_000u64..2_000_000_000,
            backlogs in prop::collection::vec(0u64..20_000_000, 1..6),
            pressures in prop::collection::vec(0u16..=10_000, 1..6),
        ) {
            let nodes: Vec<_> = backlogs.iter().zip(pressures.iter()).enumerate()
                .map(|(i, (&b, &p))| node(&format!("n{i}"), p, b, 5_000_000)).collect();
            let fleet = fleet_of(&nodes);
            let plan = allocate(&fleet, CephCeiling::Open(ByteRate::new(u64::MAX)), BudgetController::new(ByteRate::new(prev)), &config());
            let capacity = plan.controller.total().get(); // Open(u64::MAX) so capacity == controller total
            let used: u64 = plan.allocations.iter().map(|a| a.budget.get()).sum();
            prop_assert!(used <= capacity, "used {used} > capacity {capacity}");
        }

        /// No node is allocated more than it can consume (demand cap).
        #[test]
        fn never_exceeds_demand(
            prev in 1_000u64..2_000_000_000,
            specs in prop::collection::vec((0u64..20_000_000, 0u16..=10_000, 1u64..5_000_000), 1..6),
        ) {
            let nodes: Vec<_> = specs.iter().enumerate()
                .map(|(i, &(b, p, r))| node(&format!("n{i}"), p, b, r)).collect();
            let fleet = fleet_of(&nodes);
            let plan = allocate(&fleet, CephCeiling::Open(ByteRate::new(u64::MAX)), BudgetController::new(ByteRate::new(prev)), &config());
            for (id, obs) in &fleet {
                let demand = if obs.backlog.get() == 0 { 0 } else { obs.max_drain_rate.get() };
                let got = budget_of(&plan.allocations, id.as_str());
                prop_assert!(got <= demand, "node {} got {got} > demand {demand}", id.as_str());
            }
        }

        /// A node with no backlog is never allocated anything.
        #[test]
        fn zero_backlog_zero_budget(
            prev in 1_000u64..2_000_000_000,
            pressure in 0u16..=10_000,
        ) {
            let fleet = fleet_of(&[node("idle", pressure, 0, 5_000_000), node("busy", 5_000, 9_000_000, 5_000_000)]);
            let plan = allocate(&fleet, CephCeiling::Open(ByteRate::new(u64::MAX)), BudgetController::new(ByteRate::new(prev)), &config());
            prop_assert_eq!(budget_of(&plan.allocations, "idle"), 0);
        }

        /// Conservation on the INFEASIBLE path: when reservations alone exceed
        /// capacity, the scaled-down rationing must still hand out exactly `cap`.
        /// Without the residual mop-up, integer flooring drops up to (n-1) bytes in
        /// the very state (node-full + Ceph-critical) where capacity is scarcest.
        #[test]
        fn infeasible_path_conserves_capacity(
            ceiling in 1u64..80_000,
            prev in 1_000u64..200_000,
            n in 2usize..6,
        ) {
            // Every node critical-pressure (>= 9_000) and far hungrier than the
            // 50_000 floor, so each reserves the full floor; n*50_000 >= 100_000
            // always overshoots the sub-80_000 ceiling -> the infeasible branch.
            let nodes: Vec<_> = (0..n).map(|i| node(&format!("n{i}"), 10_000, 10_000_000, 10_000_000)).collect();
            let fleet = fleet_of(&nodes);
            let plan = allocate(&fleet, CephCeiling::Open(ByteRate::new(ceiling)), BudgetController::new(ByteRate::new(prev)), &config());
            prop_assert!(!plan.feasible, "construction should force the infeasible branch");
            let capacity = plan.controller.total().get().min(ceiling);
            let used: u64 = plan.allocations.iter().map(|a| a.budget.get()).sum();
            prop_assert_eq!(used, capacity, "infeasible path must conserve cap exactly");
            // No node is rationed above its reservation floor.
            for alloc in &plan.allocations {
                prop_assert!(alloc.budget.get() <= 50_000, "rationing must not exceed the reservation floor");
            }
        }

        /// The carried estimate never leaves `[min_total, max_total]`, on either arm.
        ///
        /// The back-off arm clamped only the floor, so an estimate that entered the tick
        /// above `max_total` stayed above it and decayed geometrically instead of being
        /// capped at once.
        #[test]
        fn the_carried_estimate_always_stays_within_its_bounds(
            prev in 1u64..u64::MAX,
            specs in prop::collection::vec((0u64..20_000_000, 0u16..=10_000), 1..6),
            p99_ms in 0u64..5_000,
            band in 0u8..3,
        ) {
            let cfg = config();
            let nodes: Vec<_> = specs.iter().enumerate()
                .map(|(i, &(backlog, pressure))| {
                    let (id, mut obs) = node(&format!("n{i}"), pressure, backlog, 5_000_000);
                    obs.observed_p99 = Duration::from_millis(p99_ms);
                    (id, obs)
                }).collect();
            let ceiling = match band {
                0 => CephCeiling::Open(ByteRate::new(1_000_000_000)),
                1 => CephCeiling::NearFull(ByteRate::new(10_000_000)),
                _ => CephCeiling::Critical,
            };
            let plan = allocate(&fleet_of(&nodes), ceiling, BudgetController::new(ByteRate::new(prev)), &cfg);
            let total = plan.controller.total().get();
            prop_assert!(total >= cfg.min_total.get(), "estimate {total} below the floor {}", cfg.min_total.get());
            prop_assert!(total <= cfg.max_total.get(), "estimate {total} above the ceiling {}", cfg.max_total.get());
        }

        /// The ceiling bounds the distributed total whatever the AIMD floor is tuned to.
        ///
        /// Generalizes the 2026-07-24 regression above (which pins one floor/rate pair) over
        /// arbitrary floors and bands: the floor is a property of the ESTIMATE, and must never
        /// leak into the capacity a near-full or critical pool allows.
        #[test]
        fn a_ceiling_always_bounds_the_distributed_total_whatever_the_floor(
            floor in 1u64..=1_000_000_000,
            prev in 1u64..2_000_000_000,
            rate in 1u64..1_000_000_000,
            backlogs in prop::collection::vec(0u64..20_000_000_000, 1..6),
            band in 0u8..3,
        ) {
            let cfg = AllocConfig { min_total: ByteRate::new(floor), max_total: ByteRate::new(1_000_000_000), ..config() };
            let nodes: Vec<_> = backlogs.iter().enumerate()
                .map(|(i, &backlog)| node(&format!("n{i}"), 5_000, backlog, 1_000_000_000)).collect();
            let ceiling = match band {
                0 => CephCeiling::Open(ByteRate::new(rate)),
                1 => CephCeiling::NearFull(ByteRate::new(rate)),
                _ => CephCeiling::Critical,
            };
            let plan = allocate(&fleet_of(&nodes), ceiling, BudgetController::new(ByteRate::new(prev)), &cfg);
            let distributed: u64 = plan.allocations.iter().map(|a| a.budget.get()).sum();
            prop_assert!(
                distributed <= ceiling.budget().get(),
                "distributed {distributed} exceeds the ceiling {} (floor {floor})", ceiling.budget().get(),
            );
        }

        /// A fleet saturated on every tick converges to exactly `min_total` and stays there.
        ///
        /// Documents the absorbing state: multiplicative decrease has no lower stop but the
        /// floor, so a permanently-saturated fleet ends up writing `min_total` forever. That
        /// makes the floor the operating rate under sustained saturation — the reason it is
        /// tuned above the drain's collapse threshold rather than left at a safe-looking
        /// small value.
        #[test]
        fn a_fleet_saturated_on_every_tick_converges_to_the_floor_and_stays(
            prev in 1u64..1_000_000_000,
        ) {
            let cfg = AllocConfig { min_total: ByteRate::new(50_000_000), ..config() };
            let fleet = fleet_of(&[node_p99("a", Duration::from_secs(30), 10_000_000_000)]);
            let mut controller = BudgetController::new(ByteRate::new(prev));
            let mut totals = Vec::new();
            for _ in 0..50 {
                controller = allocate(&fleet, CephCeiling::Open(ByteRate::new(1_000_000_000)), controller, &cfg).controller;
                totals.push(controller.total().get());
            }
            // 20% off per tick closes a <=20x gap to the floor in ~14 ticks, so the tail of a
            // 50-tick run must be pinned — not merely trending down.
            for total in &totals[40..] {
                prop_assert_eq!(*total, cfg.min_total.get(), "saturated ticks must settle at the floor exactly");
            }
        }

        /// Conservation: when feasible, distribute exactly min(capacity, total demand).
        #[test]
        fn conservation_of_capacity(
            prev in 100_000u64..500_000_000,
            specs in prop::collection::vec((1u64..20_000_000, 0u16..8_000, 1u64..5_000_000), 1..6),
        ) {
            let nodes: Vec<_> = specs.iter().enumerate()
                .map(|(i, &(b, p, r))| node(&format!("n{i}"), p, b, r)).collect();
            let fleet = fleet_of(&nodes);
            let plan = allocate(&fleet, CephCeiling::Open(ByteRate::new(u64::MAX)), BudgetController::new(ByteRate::new(prev)), &config());
            prop_assume!(plan.feasible);
            let capacity = plan.controller.total().get();
            let total_demand: u64 = fleet.iter().map(|(_, o)| o.max_drain_rate.get()).sum();
            let used: u64 = plan.allocations.iter().map(|a| a.budget.get()).sum();
            prop_assert_eq!(used, capacity.min(total_demand), "used {} != min(cap {}, demand {})", used, capacity, total_demand);
        }
    }
}
