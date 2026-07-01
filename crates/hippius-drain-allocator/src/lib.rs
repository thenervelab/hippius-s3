//! hippius-drain-allocator: the singleton, leader-elected budget allocator.
//!
//! The allocation *logic* lives in `hippius-drain-core` ([`hippius_drain_core::allocate`] and
//! [`hippius_drain_core::run_tick`]); this crate is the deploy wrapper that runs it: it
//! parses [`config::AllocatorConfig`] from the environment and drives
//! [`run::run_allocator`], the tick loop that contends for leadership and writes
//! the fleet's write-budget each tick. Split into a library so the config parsing
//! and the loop are unit-/integration-testable without the process entry point.

pub mod config;
#[cfg(feature = "otel")]
pub mod metrics;
#[cfg(feature = "http")]
pub mod probe;
pub mod run;
