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
    /// How full the node's SSD is (drives weight and the reservation floor).
    pub pressure: DiskPressure,
    /// Bytes currently waiting to drain. Zero backlog means zero demand.
    pub backlog: Bytes,
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
    let (allocations, feasible) = distribute(fleet, capacity, config);
    AllocationPlan {
        controller,
        allocations,
        feasible,
    }
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
fn distribute(fleet: &FleetView, capacity: ByteRate, config: &AllocConfig) -> (Vec<Allocation>, bool) {
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
        return (to_allocations(entries), false);
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

    (to_allocations(entries), true)
}

fn to_allocations(entries: Vec<Entry>) -> Vec<Allocation> {
    entries
        .into_iter()
        .map(|e| Allocation {
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
    let new_total = if back_off {
        let decreased = u64::try_from(u128::from(prev_total) * u128::from(config.decrease_permille) / 1000).unwrap_or(prev_total);
        decreased.max(config.min_total.get())
    } else {
        prev_total.saturating_add(config.additive_increase.get()).min(config.max_total.get())
    };

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
        }
    }

    fn node(id: &str, pressure_bps: u16, backlog: u64, max_rate: u64) -> (NodeId, NodeObservation) {
        (
            NodeId::from_str(id).unwrap(),
            NodeObservation {
                pressure: DiskPressure::try_from(pressure_bps).unwrap(),
                backlog: Bytes::new(backlog),
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
