//! hippius-drain-agent library: the node-local drain implementations.
//!
//! The crash-safe drain *logic* and its trait contracts live in `hippius-drain-core`
//! (which performs no I/O); this crate supplies the real `tokio`/`sha2`-backed
//! implementations of those contracts that the daemon binary wires together.

pub mod config;
pub mod disk;
pub mod enqueue;
pub mod localfs;
pub mod runtime;
pub mod supervisor;
pub mod worker;
