//! The crate's error type for validated-type construction failures.

use thiserror::Error;

/// Errors produced when constructing cephor's validated domain types.
///
/// `#[non_exhaustive]` because error variants are the canonical case where
/// adding a case later must not be a breaking change for downstream matches.
#[derive(Debug, Error)]
#[non_exhaustive]
pub enum Error {
    /// An identifier (node id, file id, chunk key) was empty or all whitespace.
    #[error("{kind} must be a non-empty identifier")]
    EmptyIdentifier {
        /// Which identifier kind failed validation, e.g. `"NodeId"`.
        kind: &'static str,
    },
    /// An object id was not a canonical hyphenated UUID. The api names each
    /// object's cache folder by its UUID, so a non-UUID component is either junk
    /// in the cache root or a path-traversal attempt — rejected at construction.
    #[error("object id {value:?} is not a hyphenated UUID")]
    InvalidObjectId {
        /// The rejected value.
        value: String,
    },
    /// A disk-pressure value in basis points exceeded the `0..=10000` range.
    #[error("disk pressure {actual_bps} bps is outside 0..=10000")]
    PressureOutOfRange {
        /// The offending basis-points value.
        actual_bps: u16,
    },
    /// A disk-pressure fraction was not a finite number in `0.0..=1.0`.
    #[error("disk pressure fraction {value} is not a finite value in 0.0..=1.0")]
    PressureFractionInvalid {
        /// The offending fraction.
        value: f64,
    },
    /// Ceph near-full thresholds were ordered so the near-full watermark was above
    /// the full watermark — a configuration that would classify a full cluster as
    /// merely near-full.
    #[error("near-full threshold {nearfull_bps} bps must not exceed the full threshold {full_bps} bps")]
    CephThresholdOrder {
        /// The configured near-full watermark, in basis points.
        nearfull_bps: u16,
        /// The configured full watermark, in basis points.
        full_bps: u16,
    },
}

/// Shorthand for results carrying this crate's [`Error`].
pub type Result<T> = core::result::Result<T, Error>;
