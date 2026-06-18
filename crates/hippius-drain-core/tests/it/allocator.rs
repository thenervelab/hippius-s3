//! Decision table for the allocator's fleet-wide water-filling split.
//!
//! `allocate` is pure: a fleet snapshot + a Ceph ceiling + the carried AIMD
//! state go in, a per-node budget plan comes out. These scenarios mirror the
//! crate's own unit fixtures (`alloc.rs`) and tabulate *why* each node got the
//! budget it did — demand (backlog), weight (disk pressure), and the
//! reservation floor that protects a near-full node.

use core::str::FromStr;
use hippius_drain_core::{
    AllocConfig, Allocation, AllocationPlan, BudgetController, ByteRate, Bytes, CephCeiling, DiskPressure, FleetView, NodeId, NodeObservation,
    allocate,
};
use std::time::Duration;

use crate::table::Table;

/// The control-loop tuning the crate's own allocator tests use; reused verbatim
/// so the demo stands on already-validated numbers, not fresh guesses.
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

fn budget_of(allocations: &[Allocation], id: &str) -> u64 {
    let wanted = NodeId::from_str(id).unwrap();
    allocations.iter().find(|a| a.node == wanted).map_or(0, |a| a.budget.get())
}

/// Prints the per-node allocation as a table, one row per node, so a reader can
/// see the split alongside the inputs (pressure, backlog) that drove it.
fn print_plan(title: &str, nodes: &[(NodeId, NodeObservation)], plan: &AllocationPlan) {
    let total: u64 = plan.allocations.iter().map(|a| a.budget.get()).sum();
    let mut table = Table::new(title, &["node", "pressure", "backlog(B)", "max(B/s)", "budget(B/s)", "share"]);
    for (id, obs) in nodes {
        let budget = budget_of(&plan.allocations, id.as_str());
        // checked_div yields None on an empty plan (total == 0), shown as 0%.
        let share = budget.saturating_mul(100).checked_div(total).unwrap_or(0);
        table.row(&[
            id.to_string(),
            format!("{:.1}%", obs.pressure.as_fraction() * 100.0),
            obs.backlog.get().to_string(),
            obs.max_drain_rate.get().to_string(),
            budget.to_string(),
            format!("{share}%"),
        ]);
    }
    println!("\n{}", table.render());
    println!("feasible: {}   capacity used: {total} B/s", plan.feasible);
}

#[test]
fn zero_backlog_node_gets_nothing_busy_node_takes_capacity() {
    let nodes = [node("busy", 5_000, 10_000_000, 10_000_000), node("idle", 5_000, 0, 10_000_000)];
    let plan = allocate(
        &fleet_of(&nodes),
        CephCeiling::Open(ByteRate::new(1_000_000_000)),
        BudgetController::new(ByteRate::new(190_000)),
        &config(),
    );
    print_plan("demand gates the split: zero backlog -> zero budget", &nodes, &plan);

    assert_eq!(
        budget_of(&plan.allocations, "idle"),
        0,
        "a node with no backlog has no demand, so it earns nothing"
    );
    assert!(budget_of(&plan.allocations, "busy") > 0, "the hungry node takes the freed capacity");
}

#[test]
fn higher_disk_pressure_wins_under_contention() {
    let nodes = [node("low", 2_000, 10_000_000, 10_000_000), node("high", 8_000, 10_000_000, 10_000_000)];
    let plan = allocate(
        &fleet_of(&nodes),
        CephCeiling::Open(ByteRate::new(300_000)),
        BudgetController::new(ByteRate::new(290_000)),
        &config(),
    );
    print_plan("equal demand, capacity < demand -> disk pressure (weight) decides", &nodes, &plan);

    let (high, low) = (budget_of(&plan.allocations, "high"), budget_of(&plan.allocations, "low"));
    assert!(high > low, "the fuller disk (higher weight) gets the larger share: high={high} low={low}");
    let total: u64 = plan.allocations.iter().map(|a| a.budget.get()).sum();
    assert!(total <= 300_000, "the split never exceeds the Ceph ceiling: {total} <= 300000");
}

#[test]
fn critical_pressure_node_keeps_its_reservation_floor() {
    let nodes = [node("hot", 9_500, 10_000_000, 10_000_000), node("cool", 100, 10_000_000, 10_000_000)];
    let plan = allocate(
        &fleet_of(&nodes),
        CephCeiling::Open(ByteRate::new(80_000)),
        BudgetController::new(ByteRate::new(70_000)),
        &config(),
    );
    print_plan("near-full node keeps its reservation floor (>= 50000) vs a hungrier peer", &nodes, &plan);

    assert!(
        budget_of(&plan.allocations, "hot") >= 50_000,
        "the critical-pressure node keeps its floor even against a hungrier low-pressure peer",
    );
}

#[test]
fn ceph_critical_freezes_every_budget() {
    let nodes = [node("a", 5_000, 1_000_000, 1_000_000)];
    let plan = allocate(
        &fleet_of(&nodes),
        CephCeiling::Critical,
        BudgetController::new(ByteRate::new(500_000)),
        &config(),
    );
    print_plan("Ceph critical -> fleet must buffer on SSD (all budgets zero)", &nodes, &plan);

    assert_eq!(budget_of(&plan.allocations, "a"), 0, "no node may write while Ceph cannot accept writes");
}
