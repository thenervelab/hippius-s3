//! Disk-pressure measurement via `statvfs`.
//!
//! The heartbeat the agent sends the allocator carries how full its SSD cache is
//! — the weight that decides its share of the Ceph write budget. We read it with
//! a safe `statvfs` wrapper (`nix`); the workspace forbids `unsafe`, so a direct
//! `libc` call is out.

use hippius_drain_core::DiskPressure;
use nix::sys::statvfs::statvfs;
use std::io;
use std::path::Path;

/// The used fraction (`0.0..=1.0`) of a filesystem with `total` blocks, of which
/// `available` are free to an unprivileged process.
///
/// Pure and total so its edges are property-tested without touching a real
/// filesystem: a zero-block mount reads as full (`1.0`), and an `available` that
/// exceeds `total` (which some filesystems report) is clamped rather than
/// producing a negative fraction.
fn used_fraction(total: u64, available: u64) -> f64 {
    if total == 0 {
        return 1.0;
    }
    let used = total.saturating_sub(available);
    #[expect(
        clippy::cast_precision_loss,
        reason = "block counts feed a 0..=1 fraction; f64 precision loss above 2^53 blocks is irrelevant to a pressure value"
    )]
    let fraction = used as f64 / total as f64;
    fraction.clamp(0.0, 1.0)
}

/// A point-in-time view of a filesystem for the heartbeat: how full it is, and how much
/// space is free.
///
/// This deliberately does NOT report occupancy as a drain backlog. It used to: the SSD held
/// only undrained chunks (the drain unlinked each part the instant it replicated), so
/// occupancy and backlog were the same number. Retaining replicated parts to serve reads
/// breaks that identity — occupancy then counts warm read cache, which is not work waiting to
/// drain. The true backlog is `Store::node_backlog_bytes`, summed from the replication rows;
/// `free_bytes` is reported instead, as one half of the node's ingest headroom.
#[derive(Debug, Clone, Copy)]
pub struct DiskUsage {
    /// How full the filesystem is (`0.0..=1.0`). Reported for the `drain_ssd_pressure` gauge
    /// and as the pre-residency fallback urgency for a not-yet-rolled allocator.
    pub pressure: DiskPressure,
    /// Bytes free to an unprivileged writer — what a new PUT can claim without evicting.
    pub free_bytes: u64,
}

/// The [`DiskUsage`] of the filesystem containing `path`, from one `statvfs`.
///
/// # Errors
///
/// [`io::Error`] if `statvfs` fails (e.g. `path` does not exist).
pub fn disk_usage(path: &Path) -> io::Result<DiskUsage> {
    let stats = statvfs(path).map_err(|errno| io::Error::from_raw_os_error(errno as i32))?;
    // `statvfs` counts are `fsblkcnt_t` / `c_ulong`, whose width varies by
    // platform (u32 on some, u64 on others) — `widen` makes the reads portable.
    // BOTH reads use `f_bavail` (free to an unprivileged process) rather than
    // `f_bfree`: the reserved blocks a privileged writer could reach are not space
    // the drain or an ingest PUT can actually claim, so counting them would
    // overstate headroom.
    let pressure = DiskPressure::from_fraction(used_fraction(widen(stats.blocks()), widen(stats.blocks_available()))).map_err(io::Error::other)?;
    let free = widen(stats.blocks_available()).saturating_mul(widen(stats.fragment_size()));
    Ok(DiskUsage { pressure, free_bytes: free })
}

/// Widens a platform-dependent-width `statvfs` count to `u64`.
fn widen(value: impl Into<u64>) -> u64 {
    value.into()
}

#[cfg(test)]
#[expect(clippy::unwrap_used, reason = "tests")]
mod tests {
    use super::{disk_usage, used_fraction};
    use proptest::prelude::*;
    use std::path::Path;

    #[test]
    #[expect(clippy::float_cmp, reason = "these fractions (n/n, 0/n, 75/100) are exactly representable in f64")]
    fn used_fraction_maps_the_documented_edges() {
        assert_eq!(used_fraction(100, 0), 1.0, "no free blocks -> fully used");
        assert_eq!(used_fraction(100, 100), 0.0, "all blocks free -> empty");
        assert_eq!(used_fraction(0, 0), 1.0, "degenerate zero-block mount -> full");
        assert_eq!(used_fraction(100, 25), 0.75, "75 of 100 blocks used");
        assert_eq!(used_fraction(100, 200), 0.0, "available > total is clamped, not negative");
    }

    proptest! {
        /// For any block counts, the used fraction is a finite value in 0.0..=1.0
        /// — so `DiskPressure::from_fraction` can never reject it.
        #[test]
        fn used_fraction_is_always_a_finite_0_to_1(total in any::<u64>(), available in any::<u64>()) {
            let fraction = used_fraction(total, available);
            prop_assert!(fraction.is_finite());
            prop_assert!((0.0..=1.0).contains(&fraction), "out of range: {fraction}");
        }
    }

    #[test]
    fn usage_of_a_live_filesystem_reports_pressure_and_nonzero_free() {
        // Free space (not occupancy) is what the heartbeat now reports: it is half of the
        // node's ingest headroom, and unlike occupancy it keeps its meaning once the drain
        // retains replicated parts on the SSD to serve reads.
        let tmp = tempfile::tempdir().unwrap();
        let usage = disk_usage(tmp.path()).unwrap();
        let fraction = usage.pressure.as_fraction();
        assert!((0.0..=1.0).contains(&fraction), "pressure is a fraction, got {fraction}");
        assert!(usage.free_bytes > 0, "a live filesystem has free space");
    }

    #[test]
    fn a_missing_path_is_a_not_found_error() {
        let err = disk_usage(Path::new("/no/such/path/cephor-statvfs-xyz")).unwrap_err();
        assert_eq!(err.kind(), std::io::ErrorKind::NotFound);
    }
}
