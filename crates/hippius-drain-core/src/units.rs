//! Quantity newtypes: byte counts, byte rates, and disk pressure.

use crate::error::{Error, Result};
use crate::state::PressureZone;

/// A non-negative count of bytes.
///
/// Modeled over `u64`: byte counts are never negative, so the domain type is
/// unsigned. Conversion to a signed Postgres `BIGINT` is the persistence
/// layer's responsibility (a checked `u64 -> i64` at that boundary, M3).
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Bytes(u64);

impl Bytes {
    /// Zero bytes.
    pub const ZERO: Self = Self(0);

    /// Wraps a raw byte count.
    #[must_use]
    pub const fn new(count: u64) -> Self {
        Self(count)
    }

    /// The raw byte count.
    #[must_use]
    pub const fn get(self) -> u64 {
        self.0
    }
}

impl From<u64> for Bytes {
    fn from(count: u64) -> Self {
        Self(count)
    }
}

/// A throughput in bytes per second.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct ByteRate(u64);

impl ByteRate {
    /// A rate of zero (fully throttled).
    pub const ZERO: Self = Self(0);

    /// Wraps a raw bytes-per-second value.
    #[must_use]
    pub const fn new(bytes_per_sec: u64) -> Self {
        Self(bytes_per_sec)
    }

    /// The raw bytes-per-second value.
    #[must_use]
    pub const fn get(self) -> u64 {
        self.0
    }
}

impl From<u64> for ByteRate {
    fn from(bytes_per_sec: u64) -> Self {
        Self(bytes_per_sec)
    }
}

/// Disk pressure as basis points (hundredths of a percent), invariant `0..=10000`.
///
/// Integer basis points rather than an `f64` fraction so the type is `Eq`, `Ord`,
/// and `Hash` and carries no float non-determinism — it is used as an allocation
/// weight and compared across the fleet. [`DiskPressure::as_fraction`] exposes the
/// `f64` view for the allocator's weighting math.
///
/// Every construction path funnels through [`TryFrom<u16>`] so the `0..=10000`
/// invariant cannot be bypassed (no `Default`, no public field, no mutable accessor).
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(serde::Deserialize))]
#[cfg_attr(feature = "serde", serde(try_from = "u16"))]
pub struct DiskPressure(u16);

impl DiskPressure {
    /// The maximum basis-points value (100.00% full).
    pub const MAX_BPS: u16 = 10_000;

    /// No pressure (empty disk).
    pub const ZERO: Self = Self(0);

    /// Full pressure (disk at capacity).
    pub const FULL: Self = Self(Self::MAX_BPS);

    /// The pressure in basis points (`0..=10000`).
    #[must_use]
    pub const fn bps(self) -> u16 {
        self.0
    }

    /// Classifies the pressure into a named severity band for observability.
    ///
    /// These are fixed human-facing watermarks (`<25%` healthy, `<50%` elevated,
    /// `<90%` high, `>=90%` critical) used for logging and operator dashboards —
    /// NOT the allocator's decision input. The allocator deliberately keeps a
    /// *configurable* `critical_pressure` threshold and weights by raw [`bps`](Self::bps),
    /// so this classifier is read-only severity, not a control signal. The `90%`
    /// critical watermark matches the default `critical_pressure`, so the band an
    /// operator sees lines up with when reservations kick in.
    #[must_use]
    pub const fn zone(self) -> PressureZone {
        match self.0 {
            0..2_500 => PressureZone::Healthy,
            2_500..5_000 => PressureZone::Elevated,
            5_000..9_000 => PressureZone::High,
            _ => PressureZone::Critical,
        }
    }

    /// The pressure as a fraction in `0.0..=1.0`.
    #[must_use]
    pub fn as_fraction(self) -> f64 {
        f64::from(self.0) / f64::from(Self::MAX_BPS)
    }

    /// Builds a pressure from a fraction in `0.0..=1.0`, rounding to the nearest
    /// basis point.
    ///
    /// # Errors
    ///
    /// Returns [`Error::PressureFractionInvalid`] if `fraction` is not finite or
    /// lies outside `0.0..=1.0`.
    pub fn from_fraction(fraction: f64) -> Result<Self> {
        if !fraction.is_finite() || !(0.0..=1.0).contains(&fraction) {
            return Err(Error::PressureFractionInvalid { value: fraction });
        }
        // Range-checked above, so the product is in 0..=10000 and the rounded
        // cast cannot truncate meaningfully or lose the sign.
        #[expect(
            clippy::cast_possible_truncation,
            clippy::cast_sign_loss,
            reason = "fraction is validated to 0.0..=1.0, so the rounded product is in 0..=10000"
        )]
        let bps = (fraction * f64::from(Self::MAX_BPS)).round() as u16;
        Ok(Self(bps))
    }
}

impl TryFrom<u16> for DiskPressure {
    type Error = Error;

    fn try_from(bps: u16) -> Result<Self> {
        if bps > Self::MAX_BPS {
            return Err(Error::PressureOutOfRange { actual_bps: bps });
        }
        Ok(Self(bps))
    }
}

#[cfg(test)]
#[expect(clippy::unwrap_used, reason = "tests")]
mod tests {
    use super::{ByteRate, Bytes, DiskPressure};
    use crate::error::Error;
    use crate::state::PressureZone;
    use proptest::prelude::*;

    #[test]
    fn bytes_and_rate_round_trip() {
        assert_eq!(Bytes::new(42).get(), 42);
        assert_eq!(Bytes::ZERO.get(), 0);
        assert_eq!(ByteRate::from(1_000).get(), 1_000);
    }

    #[test]
    fn disk_pressure_boundaries() {
        assert_eq!(DiskPressure::try_from(0).unwrap(), DiskPressure::ZERO);
        assert_eq!(DiskPressure::try_from(DiskPressure::MAX_BPS).unwrap(), DiskPressure::FULL);
        let err = DiskPressure::try_from(DiskPressure::MAX_BPS + 1).unwrap_err();
        assert!(matches!(err, Error::PressureOutOfRange { actual_bps } if actual_bps == 10_001));
    }

    #[test]
    fn from_fraction_maps_to_basis_points() {
        assert_eq!(DiskPressure::from_fraction(0.0).unwrap(), DiskPressure::ZERO);
        assert_eq!(DiskPressure::from_fraction(1.0).unwrap(), DiskPressure::FULL);
        assert_eq!(DiskPressure::from_fraction(0.5).unwrap().bps(), 5_000);
        assert!(matches!(
            DiskPressure::from_fraction(f64::NAN),
            Err(Error::PressureFractionInvalid { .. })
        ));
        assert!(matches!(DiskPressure::from_fraction(-0.01), Err(Error::PressureFractionInvalid { .. })));
        assert!(matches!(DiskPressure::from_fraction(1.01), Err(Error::PressureFractionInvalid { .. })));
    }

    #[test]
    fn zone_classifies_each_band_at_its_watermark() {
        let zone = |bps: u16| DiskPressure::try_from(bps).unwrap().zone();
        // Lower edge, just-below, and at each watermark — the boundary cases.
        assert_eq!(zone(0), PressureZone::Healthy);
        assert_eq!(zone(2_499), PressureZone::Healthy);
        assert_eq!(zone(2_500), PressureZone::Elevated, "the 25% watermark crosses into elevated");
        assert_eq!(zone(4_999), PressureZone::Elevated);
        assert_eq!(zone(5_000), PressureZone::High, "the 50% watermark crosses into high");
        assert_eq!(zone(8_999), PressureZone::High);
        assert_eq!(zone(9_000), PressureZone::Critical, "the 90% watermark crosses into critical");
        assert_eq!(zone(DiskPressure::MAX_BPS), PressureZone::Critical);
    }

    #[cfg(feature = "serde")]
    #[test]
    fn serde_deserialization_routes_through_the_range_check() {
        // `#[serde(try_from = "u16")]` runs the range check on wire data, so an
        // out-of-range value deserializes to an error rather than an invalid pressure.
        assert_eq!(serde_json::from_str::<DiskPressure>("5000").unwrap().bps(), 5_000);
        assert_eq!(serde_json::from_str::<DiskPressure>("10000").unwrap(), DiskPressure::FULL);
        assert!(serde_json::from_str::<DiskPressure>("10001").is_err());
    }

    proptest! {
        /// Every in-range basis-points value constructs and round-trips.
        #[test]
        fn in_range_bps_round_trips(bps in 0u16..=DiskPressure::MAX_BPS) {
            let pressure = DiskPressure::try_from(bps).unwrap();
            prop_assert_eq!(pressure.bps(), bps);
        }

        /// Every out-of-range basis-points value is rejected with the offending value.
        #[test]
        fn out_of_range_bps_rejected(bps in (DiskPressure::MAX_BPS + 1)..=u16::MAX) {
            let rejected = matches!(
                DiskPressure::try_from(bps),
                Err(Error::PressureOutOfRange { actual_bps }) if actual_bps == bps
            );
            prop_assert!(rejected);
        }

        /// `as_fraction` inverts `from_fraction` to within one basis point.
        #[test]
        fn fraction_round_trips_within_one_bps(fraction in 0.0f64..=1.0) {
            let pressure = DiskPressure::from_fraction(fraction).unwrap();
            prop_assert!((pressure.as_fraction() - fraction).abs() <= 1.0 / f64::from(DiskPressure::MAX_BPS));
        }
    }
}
