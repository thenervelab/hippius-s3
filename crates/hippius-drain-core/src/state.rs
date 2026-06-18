//! Domain state enums for replication, disk-pressure bands, and the Ceph ceiling.

use crate::units::ByteRate;

/// Replication status of a chunk; mirrors the existing `Postgres` column (wired in M3).
///
/// Exhaustive on purpose (not `#[non_exhaustive]`): a new state should be a compile
/// error at every match site across the workspace, not a silently-defaulted branch.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ReplicationState {
    /// On SSD, not yet draining.
    Pending,
    /// Claimed by an agent and copying to `CephFS`.
    Draining,
    /// Durable on `CephFS` and verified; the SSD copy may be unlinked.
    Replicated,
    /// Drain abandoned after exhausting retries (reconciled to a client error).
    Failed,
}

/// Disk-pressure severity band.
///
/// `Ord` follows declaration order, so `Healthy < Elevated < High < Critical`.
/// That ordering is a contract — comparisons like `zone >= High` depend on it, so
/// reordering the variants is a behavior change, not a cosmetic edit.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum PressureZone {
    /// Plenty of headroom.
    Healthy,
    /// Draining should be favored.
    Elevated,
    /// Near the watermark; escalate this node's allocation weight.
    High,
    /// At/over the critical watermark; gets a guaranteed reservation floor.
    Critical,
}

/// Ceph's gate on the fleet-wide write budget.
///
/// `Critical` is fieldless, so "still permitting bytes while Ceph is critical" is
/// unrepresentable: the only way to express the critical state carries no rate.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CephCeiling {
    /// Ceph is healthy; the contained rate is the ceiling.
    Open(ByteRate),
    /// Ceph is near-full; the contained (clamped) rate is the ceiling.
    NearFull(ByteRate),
    /// Ceph cannot accept writes; the fleet must buffer on SSD.
    Critical,
}

impl CephCeiling {
    /// The maximum write rate this ceiling permits.
    ///
    /// `Critical` yields [`ByteRate::ZERO`]; the other variants yield their rate.
    #[must_use]
    pub fn budget(self) -> ByteRate {
        match self {
            Self::Open(rate) | Self::NearFull(rate) => rate,
            Self::Critical => ByteRate::ZERO,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{CephCeiling, PressureZone, ReplicationState};
    use crate::units::ByteRate;

    #[test]
    fn replication_states_are_distinct() {
        assert_ne!(ReplicationState::Pending, ReplicationState::Replicated);
        assert_eq!(ReplicationState::Failed, ReplicationState::Failed);
    }

    #[test]
    fn pressure_zone_orders_by_severity() {
        assert!(PressureZone::Healthy < PressureZone::Elevated);
        assert!(PressureZone::Elevated < PressureZone::High);
        assert!(PressureZone::High < PressureZone::Critical);
    }

    #[test]
    fn ceiling_budget_is_the_rate_except_when_critical() {
        let rate = ByteRate::new(5_000);
        assert_eq!(CephCeiling::Open(rate).budget(), rate);
        assert_eq!(CephCeiling::NearFull(rate).budget(), rate);
        assert_eq!(CephCeiling::Critical.budget(), ByteRate::ZERO);
    }
}
