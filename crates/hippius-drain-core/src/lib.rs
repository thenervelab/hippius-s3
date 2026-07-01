//! Core domain vocabulary and pure logic for the cephor drain service.
//!
//! This crate is the single source of truth for cephor's types. It performs no
//! I/O: every item here is a pure value type or pure function, so the binaries
//! (`hippius-drain-agent`, `hippius-drain-allocator`) and tests share one definition of each
//! domain concept rather than re-inventing incompatible copies.

mod alloc;
mod apipart;
mod clock;
#[cfg(feature = "coord")]
mod coordination;
mod enforce;
mod error;
mod gc;
mod ids;
mod mgr;
mod partdrain;
mod reconcile;
mod snapshot;
mod ssd_reclaim;
mod state;
#[cfg(feature = "pg")]
mod store;
#[cfg(feature = "coord")]
mod tick;
mod units;

pub use alloc::{AllocConfig, Allocation, AllocationPlan, BudgetController, FleetView, NodeObservation, allocate};
pub use apipart::{ChunkIndex, META_FILE_NAME, ObjectId, PartKey, PartMeta, PartNumber, PartPathError, Version, chunk_file_name, parse_part_dir};
pub use clock::{Clock, SystemClock};
pub use enforce::{
    BreakerConfig, BreakerSignal, BreakerState, CircuitBreaker, ConcurrencyLimiter, DenyReason, DrainDecision, Enforcer, TokenBucket, decay_rate,
};
pub use error::{Error, Result};
pub use gc::{CephFs, GcClaim, GcError, GcOutcome, GcTarget, SsdCache, gc_object};
pub use ids::{FileId, NodeId};
pub use mgr::{CephReport, CephThresholds, ProbeParseError, classify, decay, parse_prometheus_metrics};
pub use partdrain::{
    ClaimedPart, DrainOutcome, DrainStep, PartDrainError, PartPool, PartReplicationStore, PartSource, PartVerified, UploadEnqueuer, drain_part,
};
pub use reconcile::{DiscoveredPart, PartLandingLog, PartScan, ReconcileError, ReconcileReport, reconcile_parts};
pub use snapshot::{AgentSnapshot, SnapshotCell};
pub use ssd_reclaim::{PartRemover, PartStatusAge, ReclaimError, ReclaimLog, ReclaimReport, reclaim_ssd};
pub use state::{CephCeiling, PressureZone, ReplicationState};
pub use units::{ByteRate, Bytes, DiskPressure};

#[cfg(any(test, feature = "test-util"))]
pub use clock::TestClock;

#[cfg(feature = "coord")]
pub use coordination::{CoordError, Coordinator, Lease, StoredAllocation};
#[cfg(feature = "pg")]
pub use store::{PendingPart, Store, StoreError, UploadContext};
#[cfg(feature = "coord")]
pub use tick::{CephCeilingSource, StaticCeiling, TickConfig, TickOutcome, run_tick};
