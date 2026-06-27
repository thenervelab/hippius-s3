//! Real filesystem implementations of the drain/GC contracts over POSIX mounts.
//!
//! [`LocalFs`] is the durable `CephFS` pool (a POSIX mount); [`LocalSsd`] is the
//! node-local ingest cache. The drain replicates the api's part layout
//! `<root>/<object_id>/v<version>/part_<n>/` path-preservingly: [`LocalSsd`]
//! implements [`PartSource`]/[`PartScan`] (list, locate, hash, scan, unlink a
//! part) and [`LocalFs`] implements [`PartPool`] (persist chunk/meta, hash,
//! remove). The crash-safety guarantee the drain depends on lives in
//! [`persist_into`]: it copies, fsyncs, atomically renames within the part folder,
//! and fsyncs the folder so a power loss never leaves a torn file. Both also
//! implement the GC-only [`CephFs`]/[`SsdCache`] `remove_object` reclaim (by
//! `<object_id>` folder).

use hippius_drain_core::{
    CephFs, ChunkIndex, DiscoveredPart, FileId, META_FILE_NAME, PartKey, PartPool, PartRemover, PartScan, PartSource, SsdCache, chunk_file_name,
    parse_part_dir,
};
use sha2::{Digest, Sha256};
use std::ffi::OsStr;
use std::io;
use std::path::{Path, PathBuf};
use std::time::{Duration, SystemTime};
use tokio::fs;
use tokio::io::AsyncReadExt;

/// Streaming hash read buffer. Bounds memory so multi-gigabyte chunks are never
/// read whole into memory just to hash them.
const HASH_BUF_BYTES: usize = 1 << 16;

/// Confine an identifier to a single path component beneath a root.
///
/// Both the file id (a folder) and the chunk key (a filename) are externally
/// derived, and treating either as a path is the classic traversal hole. Reject
/// the relative specials, every path separator, and embedded NUL so a crafted id
/// like `../etc/passwd` can never escape the pool/cache root. This is the
/// path-traversal guard the drain and GC paths share, applied to each component.
fn safe_component(raw: &str) -> io::Result<&str> {
    let rejected = raw.is_empty() || raw == "." || raw == ".." || raw.contains('/') || raw.contains('\\') || raw.contains('\0');
    if rejected {
        return Err(io::Error::new(io::ErrorKind::InvalidInput, format!("unsafe path component {raw:?}")));
    }
    Ok(raw)
}

/// The confined `<root>/<file_id>` folder that holds one file's chunks.
fn object_dir(root: &Path, file: &FileId) -> io::Result<PathBuf> {
    Ok(root.join(safe_component(file.as_str())?))
}

/// Lowercase-hex encode bytes (the on-wire form of a content hash).
fn hex_lower(bytes: &[u8]) -> String {
    let mut out = String::with_capacity(bytes.len() * 2);
    for &byte in bytes {
        // Each nibble is in `0..16`, so `from_digit` always yields `Some`; the
        // `'0'` fallback is unreachable and only avoids a denied panic on `None`.
        out.push(char::from_digit(u32::from(byte >> 4), 16).unwrap_or('0'));
        out.push(char::from_digit(u32::from(byte & 0x0f), 16).unwrap_or('0'));
    }
    out
}

/// Fsync a directory so a freshly-renamed entry inside it survives a crash.
///
/// Opening the directory and `sync_all`-ing its descriptor flushes the directory
/// entry, not file data. The design flags this as the weakest crash-safety joint
/// (some network mounts handle directory fsync differently); errors propagate
/// rather than being swallowed, and real `CephFS` behavior is measured on staging.
async fn sync_parent_dir(dir: &Path) -> io::Result<()> {
    let handle = fs::File::open(dir).await?;
    handle.sync_all().await
}

/// Removes a half-written `.tmp-*` file unless the persist that created it
/// reached its atomic rename. The drop runs on an early `?` return *and* on
/// future cancellation (a dropped persist future) — the latter an explicit
/// error-path cleanup cannot reach.
///
/// The unlink is synchronous because [`Drop`] cannot `.await`; a single unlink
/// is cheap, and per the leak-safety rule a leaked temp is only wasted space (the
/// scan skips `.tmp-*`, and the next persist overwrites it), never a correctness
/// failure — so best-effort `Drop` cleanup is the right tool and must not panic.
struct TmpGuard {
    path: PathBuf,
    armed: bool,
}

impl TmpGuard {
    /// Arms cleanup for `path` (the temp file a persist is about to write).
    fn arm(path: PathBuf) -> Self {
        Self { path, armed: true }
    }

    /// Disarms the guard once the rename has consumed the temp, so the success
    /// path performs no unlink. Mirrors the drop-guard `dismiss` idiom.
    fn dismiss(mut self) {
        self.armed = false;
    }
}

impl Drop for TmpGuard {
    fn drop(&mut self) {
        if self.armed {
            // Best-effort: a failure leaves the temp as harmless wasted space,
            // reclaimed by the next persist's overwrite. Never panic in Drop.
            let _ = std::fs::remove_file(&self.path);
        }
    }
}

/// The durable shared `CephFS` pool, rooted at a POSIX mount directory.
#[derive(Debug, Clone)]
pub struct LocalFs {
    root: PathBuf,
}

impl LocalFs {
    /// Roots the pool at `root` (the `CephFS` mount point).
    #[must_use]
    pub fn new(root: impl Into<PathBuf>) -> Self {
        Self { root: root.into() }
    }
}

impl CephFs for LocalFs {
    async fn remove_object(&self, file: &FileId) -> io::Result<()> {
        // GC reclaim: remove the file's whole folder. `remove_dir_all` is NOT
        // idempotent (NotFound when already absent) and may report
        // DirectoryNotEmpty under a concurrent writer; the raw result is returned
        // so [`hippius_drain_core::gc`] can classify both into a GC outcome.
        fs::remove_dir_all(object_dir(&self.root, file)?).await
    }
}

/// The node-local SSD ingest cache, rooted at a directory.
#[derive(Debug, Clone)]
pub struct LocalSsd {
    root: PathBuf,
}

impl LocalSsd {
    /// Roots the cache at `root` (the local SSD ingest directory).
    #[must_use]
    pub fn new(root: impl Into<PathBuf>) -> Self {
        Self { root: root.into() }
    }

    /// The cache's root directory — the filesystem the heartbeat probes for
    /// disk pressure.
    #[must_use]
    pub fn root(&self) -> &Path {
        self.root.as_path()
    }

    /// Removes orphaned write-temp files left on the SSD by a crashed mid-write PUT
    /// (the api's `<name>.tmp.<uuid>`) or a cancelled persist (the agent's
    /// `.tmp-<name>`), once older than `max_age`.
    ///
    /// Walks the `<object>/v<version>/part_<n>/` layout and only ever unlinks a temp
    /// FILE — never a real `chunk_*.bin`/`meta.json`, never a directory — so it cannot
    /// touch a complete or in-flight part; the age gate keeps a temp an active writer
    /// is still using. Returns how many temps it removed; a missing root is an empty
    /// cache, not an error. The companion to whole-part reclaim (`reclaim_ssd`), which
    /// already clears temps inside a reclaimed part dir.
    ///
    /// # Errors
    ///
    /// [`io::Error`] if walking the cache or unlinking a temp fails for a reason other
    /// than a concurrently-removed entry (which is tolerated).
    pub async fn sweep_orphan_tmp(&self, max_age: Duration) -> io::Result<u64> {
        let mut removed = 0;
        let Some(mut objects) = open_dir(&self.root).await? else {
            return Ok(0);
        };
        while let Some(object) = objects.next_entry().await? {
            if !object.file_type().await?.is_dir() {
                continue;
            }
            let Some(mut versions) = open_dir(&object.path()).await? else {
                continue;
            };
            while let Some(version) = versions.next_entry().await? {
                if !version.file_type().await?.is_dir() {
                    continue;
                }
                let Some(mut parts) = open_dir(&version.path()).await? else {
                    continue;
                };
                while let Some(part) = parts.next_entry().await? {
                    if !part.file_type().await?.is_dir() {
                        continue;
                    }
                    removed += sweep_part_tmp(&part.path(), max_age).await?;
                }
            }
        }
        Ok(removed)
    }
}

impl SsdCache for LocalSsd {
    async fn remove_object(&self, file: &FileId) -> io::Result<()> {
        // GC reclaim of the file's SSD folder; raw result for the GC classifier
        // (see [`CephFs::remove_object`]).
        fs::remove_dir_all(object_dir(&self.root, file)?).await
    }
}

// ---- api part layout: drain a whole part path-preservingly SSD <-> CephFS ----

/// The confined `<root>/<object>/v<version>/part_<n>` directory of one part.
///
/// [`PartKey::relative_dir`] is built from a validated UUID object id and formatted
/// numeric version/part segments, so it carries no separator, `..`, or NUL — joining
/// it onto `root` cannot escape the part folder. This is the part-layout analogue of
/// the per-component `safe_component` guard the chunk paths apply.
fn part_dir(root: &Path, part: &PartKey) -> PathBuf {
    root.join(part.relative_dir())
}

/// Streaming SHA-256 of a file as lowercase hex, bounded by [`HASH_BUF_BYTES`] so a
/// multi-gigabyte chunk is never read whole into memory just to hash it.
async fn hash_file(path: &Path) -> io::Result<String> {
    let mut handle = fs::File::open(path).await?;
    let mut hasher = Sha256::new();
    let mut buf = vec![0_u8; HASH_BUF_BYTES];
    loop {
        let read = handle.read(&mut buf).await?;
        if read == 0 {
            break;
        }
        hasher.update(&buf[..read]);
    }
    Ok(hex_lower(hasher.finalize().as_slice()))
}

/// Atomically place `source`'s bytes into `dir` as `name`: copy into a temp inside
/// `dir`, fsync it, rename into place, then fsync `dir`. A crash leaves either no file
/// or the complete one. The single-writer-per-part claim makes the `.tmp-<name>`
/// temp collision-free (mirrors the chunk persist's within-folder atomic rename).
async fn persist_into(dir: &Path, name: &str, source: &Path) -> io::Result<()> {
    let dest = dir.join(name);
    let tmp = dir.join(format!(".tmp-{name}"));
    fs::create_dir_all(dir).await?;
    // Arm temp cleanup before the copy: any failure or cancellation before the rename
    // must not leave a `.tmp-*` orphan in the pool (the drop-guard idiom, RfR ch.1).
    let guard = TmpGuard::arm(tmp.clone());
    fs::copy(source, &tmp).await?;
    let written = fs::File::open(&tmp).await?;
    written.sync_all().await?;
    drop(written);
    fs::rename(&tmp, &dest).await?;
    guard.dismiss();
    sync_parent_dir(dir).await
}

/// Remove a part's whole directory; an already-absent dir is `Ok` (idempotent, so a
/// re-drive after a crash still converges). Shared by the SSD-source unlink and the
/// pool corrupt-copy cleanup.
async fn remove_part_dir(root: &Path, part: &PartKey) -> io::Result<()> {
    match fs::remove_dir_all(part_dir(root, part)).await {
        Ok(()) => Ok(()),
        Err(err) if err.kind() == io::ErrorKind::NotFound => Ok(()),
        Err(err) => Err(err),
    }
}

/// Opens `dir` for reading, mapping a vanished dir (a concurrent reclaim removed it)
/// to `None` rather than an error, so a sweep walking a live tree never aborts.
async fn open_dir(dir: &Path) -> io::Result<Option<fs::ReadDir>> {
    match fs::read_dir(dir).await {
        Ok(read) => Ok(Some(read)),
        Err(err) if err.kind() == io::ErrorKind::NotFound => Ok(None),
        Err(err) => Err(err),
    }
}

/// Whether a directory-entry name is a write temp — the agent's `.tmp-<name>` or the
/// api's `<name>.tmp.<uuid>`. A real `chunk_<i>.bin`/`meta.json` matches neither.
fn is_temp_name(name: &str) -> bool {
    name.starts_with(".tmp-") || name.contains(".tmp.")
}

/// How long ago `meta` was last modified, or [`Duration::ZERO`] if its mtime is
/// unavailable or in the future (clock skew) — the fail-safe direction (reads young,
/// so the temp is kept rather than removed).
fn file_age(meta: &std::fs::Metadata) -> Duration {
    meta.modified()
        .ok()
        .and_then(|mtime| SystemTime::now().duration_since(mtime).ok())
        .unwrap_or(Duration::ZERO)
}

/// Unlinks aged orphan write-temps directly inside one part dir. Only temp FILES older
/// than `max_age` are removed; real chunk/meta files, fresh temps, and any subdirectory
/// are left untouched. A temp another writer renamed away mid-sweep is already gone.
async fn sweep_part_tmp(part_path: &Path, max_age: Duration) -> io::Result<u64> {
    let mut removed = 0;
    let Some(mut entries) = open_dir(part_path).await? else {
        return Ok(0);
    };
    while let Some(entry) = entries.next_entry().await? {
        let raw = entry.file_name();
        let Some(name) = raw.to_str() else {
            continue;
        };
        if !is_temp_name(name) {
            continue;
        }
        let meta = entry.metadata().await?;
        if !meta.is_file() || file_age(&meta) < max_age {
            continue;
        }
        match fs::remove_file(entry.path()).await {
            Ok(()) => removed += 1,
            Err(err) if err.kind() == io::ErrorKind::NotFound => {}
            Err(err) => return Err(err),
        }
    }
    Ok(removed)
}

/// Parse a `chunk_<index>.bin` file name into its [`ChunkIndex`], or `None` for any
/// other entry (`meta.json`, a hidden temp, junk).
fn parse_chunk_index(name: &OsStr) -> Option<ChunkIndex> {
    let digits = name.to_str()?.strip_prefix("chunk_")?.strip_suffix(".bin")?;
    digits.parse::<u32>().ok().map(ChunkIndex::new)
}

/// The chunk indices present in a part dir, sorted ascending. A missing dir is an
/// empty part (tolerated like the cache-root scan), so `drain_part` then fails at the
/// meta copy rather than here.
async fn list_chunk_indices(dir: &Path) -> io::Result<Vec<ChunkIndex>> {
    let mut out = Vec::new();
    let mut entries = match fs::read_dir(dir).await {
        Ok(dir) => dir,
        Err(err) if err.kind() == io::ErrorKind::NotFound => return Ok(out),
        Err(err) => return Err(err),
    };
    while let Some(entry) = entries.next_entry().await? {
        if !entry.file_type().await?.is_file() {
            continue;
        }
        if let Some(index) = parse_chunk_index(&entry.file_name()) {
            out.push(index);
        }
    }
    out.sort_unstable();
    Ok(out)
}

/// A directory-entry name as a plain UTF-8 component, or `None` if hidden (leading
/// `.`) or non-UTF-8. Real object/version/part dirs never start with `.`.
fn plain_name(name: &OsStr) -> Option<String> {
    let raw = name.to_str()?;
    if raw.starts_with('.') {
        return None;
    }
    Some(raw.to_owned())
}

/// Whether a part dir holds its `meta.json` marker. The api writes meta last, so its
/// presence means the part is complete and safe to drain (an incomplete part is
/// skipped by the scan).
async fn part_has_meta(part_path: &Path) -> io::Result<bool> {
    match fs::metadata(part_path.join(META_FILE_NAME)).await {
        Ok(meta) => Ok(meta.is_file()),
        Err(err) if err.kind() == io::ErrorKind::NotFound => Ok(false),
        Err(err) => Err(err),
    }
}

impl PartSource for LocalSsd {
    async fn list_chunks(&self, part: &PartKey) -> io::Result<Vec<ChunkIndex>> {
        list_chunk_indices(&part_dir(&self.root, part)).await
    }

    fn chunk_source(&self, part: &PartKey, index: ChunkIndex) -> io::Result<PathBuf> {
        Ok(part_dir(&self.root, part).join(chunk_file_name(index)))
    }

    fn meta_source(&self, part: &PartKey) -> io::Result<PathBuf> {
        Ok(part_dir(&self.root, part).join(META_FILE_NAME))
    }

    async fn chunk_hash(&self, part: &PartKey, index: ChunkIndex) -> io::Result<String> {
        hash_file(&part_dir(&self.root, part).join(chunk_file_name(index))).await
    }

    async fn remove_part(&self, part: &PartKey) -> io::Result<()> {
        remove_part_dir(&self.root, part).await
    }
}

impl PartPool for LocalFs {
    async fn persist_chunk(&self, source: &Path, part: &PartKey, index: ChunkIndex) -> io::Result<()> {
        persist_into(&part_dir(&self.root, part), &chunk_file_name(index), source).await
    }

    async fn persist_meta(&self, source: &Path, part: &PartKey) -> io::Result<()> {
        persist_into(&part_dir(&self.root, part), META_FILE_NAME, source).await
    }

    async fn chunk_hash(&self, part: &PartKey, index: ChunkIndex) -> io::Result<String> {
        // Re-reads the pooled file from disk — independent of the SSD-source hash — so
        // drain_part's source==pool comparison catches a torn copy (audit flag, slice 1).
        hash_file(&part_dir(&self.root, part).join(chunk_file_name(index))).await
    }

    async fn remove_part(&self, part: &PartKey) -> io::Result<()> {
        remove_part_dir(&self.root, part).await
    }
}

/// Walks one `<root>/<object>` dir, descending into its version dirs.
async fn scan_object_dir(path: &Path, object: &str, out: &mut Vec<DiscoveredPart>) -> io::Result<()> {
    let mut versions = match fs::read_dir(path).await {
        Ok(dir) => dir,
        Err(err) if err.kind() == io::ErrorKind::NotFound => return Ok(()),
        Err(err) => return Err(err),
    };
    while let Some(entry) = versions.next_entry().await? {
        if !entry.file_type().await?.is_dir() {
            continue;
        }
        let Some(version) = plain_name(&entry.file_name()) else {
            continue;
        };
        scan_version_dir(&entry.path(), object, &version, out).await?;
    }
    Ok(())
}

/// Walks one `<root>/<object>/v<version>` dir, emitting a [`DiscoveredPart`] for each
/// complete part (one bearing a `meta.json` marker). A directory whose `(object,
/// version, part)` triple does not parse — a non-UUID object, a malformed segment —
/// is skipped, so junk in the cache cannot abort the scan.
async fn scan_version_dir(path: &Path, object: &str, version: &str, out: &mut Vec<DiscoveredPart>) -> io::Result<()> {
    let mut parts = match fs::read_dir(path).await {
        Ok(dir) => dir,
        Err(err) if err.kind() == io::ErrorKind::NotFound => return Ok(()),
        Err(err) => return Err(err),
    };
    while let Some(entry) = parts.next_entry().await? {
        if !entry.file_type().await?.is_dir() {
            continue;
        }
        let Some(part) = plain_name(&entry.file_name()) else {
            continue;
        };
        if !part_has_meta(&entry.path()).await? {
            continue;
        }
        let rel = Path::new(object).join(version).join(&part);
        if let Ok(key) = parse_part_dir(&rel) {
            out.push(DiscoveredPart { part: key });
        }
    }
    Ok(())
}

impl PartScan for LocalSsd {
    async fn scan_parts(&self) -> io::Result<Vec<DiscoveredPart>> {
        let mut out = Vec::new();
        let mut objects = match fs::read_dir(&self.root).await {
            Ok(dir) => dir,
            // A missing cache root is an empty cache, not a failure.
            Err(err) if err.kind() == io::ErrorKind::NotFound => return Ok(out),
            Err(err) => return Err(err),
        };
        // Three levels: <object>/v<version>/part_<n>/, each part gated on its
        // meta.json marker — the layout the api writes and the drain replicates.
        while let Some(entry) = objects.next_entry().await? {
            if !entry.file_type().await?.is_dir() {
                continue;
            }
            let Some(object) = plain_name(&entry.file_name()) else {
                continue;
            };
            scan_object_dir(&entry.path(), &object, &mut out).await?;
        }
        Ok(out)
    }
}

impl PartRemover for LocalSsd {
    async fn unlink_part(&self, part: &PartKey) -> io::Result<()> {
        // The reclaim worker's removal seam — the same idempotent whole-part unlink the
        // drain's success path (`PartSource::remove_part`) uses, so a reclaim racing
        // that unlink is harmless.
        remove_part_dir(&self.root, part).await
    }
}

#[cfg(test)]
#[expect(clippy::unwrap_used, reason = "tests")]
mod tests {
    use super::{LocalSsd, TmpGuard, hex_lower, safe_component};
    use core::str::FromStr;
    use hippius_drain_core::{FileId, SsdCache};
    use proptest::prelude::*;
    use std::io;
    use std::path::Path;
    use tempfile::TempDir;

    fn fid(raw: &str) -> FileId {
        FileId::from_str(raw).unwrap()
    }

    #[tokio::test]
    async fn remove_object_deletes_the_whole_file_folder_and_surfaces_absence() {
        let pool_dir = TempDir::new().unwrap();
        let folder = pool_dir.path().join("file-7");
        std::fs::create_dir_all(&folder).unwrap();
        std::fs::write(folder.join("chunk-a"), b"a").unwrap();
        std::fs::write(folder.join("chunk-b"), b"b").unwrap();

        let ssd = LocalSsd::new(pool_dir.path());

        // First reclaim removes the whole folder.
        ssd.remove_object(&fid("file-7")).await.unwrap();
        assert!(!folder.exists(), "the file's folder and all its chunks are gone");

        // `remove_dir_all` is non-idempotent: a second reclaim surfaces NotFound,
        // which the GC layer (hippius_drain_core::gc) classifies as AlreadyGone.
        let err = ssd.remove_object(&fid("file-7")).await.unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::NotFound);
    }

    #[test]
    fn tmp_guard_unlinks_when_armed_and_preserves_when_dismissed() {
        let dir = TempDir::new().unwrap();

        // Armed: dropping the guard removes the file.
        let armed_path = dir.path().join(".tmp-armed");
        std::fs::write(&armed_path, b"x").unwrap();
        drop(TmpGuard::arm(armed_path.clone()));
        assert!(!armed_path.exists(), "an armed guard unlinks on drop");

        // Dismissed: the success path keeps the file (the rename owns it).
        let kept_path = dir.path().join(".tmp-kept");
        std::fs::write(&kept_path, b"x").unwrap();
        TmpGuard::arm(kept_path.clone()).dismiss();
        assert!(kept_path.exists(), "a dismissed guard leaves the file in place");
    }

    #[test]
    fn safe_component_rejects_traversal_and_separators() {
        for bad in ["", "..", ".", "a/b", "../x", "/abs", "a\\b", "a/../b", "a\0b"] {
            assert!(safe_component(bad).is_err(), "{bad:?} must be rejected");
        }
        for ok in ["abc", "deadbeef", "a-b_c", "a..b"] {
            assert_eq!(safe_component(ok).unwrap(), ok);
        }
    }

    proptest! {
        /// Any safe component joins to a path whose parent is exactly the root —
        /// it never escapes.
        #[test]
        fn safe_components_stay_directly_under_root(s in "[0-9a-f]{1,64}") {
            let root = Path::new("/pool");
            let joined = root.join(safe_component(&s).unwrap());
            prop_assert_eq!(joined.parent(), Some(root));
        }

        /// Any component bearing a separator is rejected, never normalized.
        #[test]
        fn components_with_a_separator_are_rejected(prefix in "[a-z]{1,6}", suffix in "[a-z]{1,6}") {
            let raw = format!("{prefix}/{suffix}");
            prop_assert!(safe_component(&raw).is_err());
        }

        /// `hex_lower` round-trips: decoding the hex recovers the original bytes,
        /// and the output is always twice the input length.
        #[test]
        fn hex_lower_round_trips(bytes in prop::collection::vec(any::<u8>(), 0..96)) {
            let hex = hex_lower(&bytes);
            prop_assert_eq!(hex.len(), bytes.len() * 2);
            let decoded: Vec<u8> = (0..bytes.len())
                .map(|i| u8::from_str_radix(&hex[i * 2..i * 2 + 2], 16).unwrap())
                .collect();
            prop_assert_eq!(decoded, bytes);
        }
    }
}

#[cfg(test)]
#[expect(clippy::unwrap_used, reason = "tests")]
mod part_tests {
    use super::{LocalFs, LocalSsd, hex_lower};
    use core::future::Future;
    use core::str::FromStr;
    use hippius_drain_core::{
        ChunkIndex, ClaimedPart, DrainOutcome, ObjectId, PartKey, PartNumber, PartPool, PartRemover, PartReplicationStore, PartScan, PartSource,
        PartVerified, ReplicationState, UploadEnqueuer, Version, drain_part,
    };
    use sha2::{Digest, Sha256};
    use std::collections::HashMap;
    use std::io;
    use std::sync::Mutex;
    use std::time::Duration;
    use tempfile::TempDir;

    /// A no-op upload enqueuer for the localfs drain test.
    struct NoopEnqueuer;
    impl UploadEnqueuer for NoopEnqueuer {
        type Error = io::Error;
        async fn enqueue(&self, _part: &PartKey) -> Result<(), io::Error> {
            Ok(())
        }
    }

    const UUID: &str = "466916c0-d61b-4518-b81b-9576b574270a";

    fn part_key(version: u32, number: u32) -> PartKey {
        PartKey::new(ObjectId::from_str(UUID).unwrap(), Version::new(version), PartNumber::new(number))
    }

    fn sha256_hex(bytes: &[u8]) -> String {
        let mut hasher = Sha256::new();
        hasher.update(bytes);
        hex_lower(hasher.finalize().as_slice())
    }

    /// Lays a complete SSD part (`chunk_<i>.bin` files + meta.json) under `root`.
    fn seed_ssd_part(root: &std::path::Path, part: &PartKey, chunks: &[(u32, &[u8])]) {
        let dir = root.join(part.relative_dir());
        std::fs::create_dir_all(&dir).unwrap();
        for &(index, bytes) in chunks {
            std::fs::write(dir.join(format!("chunk_{index}.bin")), bytes).unwrap();
        }
        std::fs::write(dir.join("meta.json"), br#"{"chunk_size":4,"num_chunks":1,"size_bytes":4}"#).unwrap();
    }

    /// Minimal in-memory part replication store for the end-to-end drain tests; it
    /// receives `&PartVerified` from the orchestrator but, like any external store,
    /// cannot construct one — the seal in practice.
    #[derive(Default)]
    struct MemPartStore {
        status: Mutex<HashMap<String, ReplicationState>>,
    }

    impl MemPartStore {
        fn key(part: &PartKey) -> String {
            part.relative_dir().to_string_lossy().into_owned()
        }

        fn set(&self, part: &PartKey, state: ReplicationState) {
            self.status.lock().unwrap().insert(Self::key(part), state);
        }

        fn status_of(&self, part: &PartKey) -> Option<ReplicationState> {
            self.status.lock().unwrap().get(&Self::key(part)).copied()
        }
    }

    impl PartReplicationStore for MemPartStore {
        type Error = io::Error;

        fn status(&self, part: &PartKey) -> impl Future<Output = Result<Option<ReplicationState>, io::Error>> + Send {
            let key = Self::key(part);
            async move { Ok(self.status.lock().unwrap().get(&key).copied()) }
        }

        fn mark_replicated(&self, part: &ClaimedPart, _proof: &PartVerified) -> impl Future<Output = Result<(), io::Error>> + Send {
            let key = Self::key(part.part());
            async move {
                self.status.lock().unwrap().insert(key, ReplicationState::Replicated);
                Ok(())
            }
        }

        fn mark_failed(&self, part: &ClaimedPart, _reason: &str) -> impl Future<Output = Result<(), io::Error>> + Send {
            let key = Self::key(part.part());
            async move {
                self.status.lock().unwrap().insert(key, ReplicationState::Failed);
                Ok(())
            }
        }
    }

    #[tokio::test]
    async fn list_chunks_returns_sorted_indices_and_skips_meta() {
        let dir = TempDir::new().unwrap();
        let part = part_key(5, 1);
        // Out-of-order on disk; list_chunks must return them sorted and skip meta.json.
        seed_ssd_part(dir.path(), &part, &[(10, b"j"), (0, b"a"), (2, b"c")]);

        let ssd = LocalSsd::new(dir.path());
        let indices = ssd.list_chunks(&part).await.unwrap();
        assert_eq!(
            indices,
            vec![ChunkIndex::new(0), ChunkIndex::new(2), ChunkIndex::new(10)],
            "indices are parsed, sorted, and meta.json is excluded",
        );
    }

    #[tokio::test]
    async fn chunk_and_meta_source_render_the_part_layout() {
        let ssd = LocalSsd::new("/cache");
        let part = part_key(5, 1);
        assert_eq!(
            ssd.chunk_source(&part, ChunkIndex::new(3)).unwrap(),
            std::path::Path::new("/cache").join(UUID).join("v5").join("part_1").join("chunk_3.bin"),
        );
        assert_eq!(
            ssd.meta_source(&part).unwrap(),
            std::path::Path::new("/cache").join(UUID).join("v5").join("part_1").join("meta.json"),
        );
    }

    #[tokio::test]
    async fn part_source_chunk_hash_matches_sha256() {
        let dir = TempDir::new().unwrap();
        let part = part_key(5, 1);
        let content = b"the quick brown fox";
        seed_ssd_part(dir.path(), &part, &[(0, content)]);

        let ssd = LocalSsd::new(dir.path());
        assert_eq!(ssd.chunk_hash(&part, ChunkIndex::new(0)).await.unwrap(), sha256_hex(content));
    }

    #[tokio::test]
    async fn source_remove_part_is_idempotent() {
        let dir = TempDir::new().unwrap();
        let part = part_key(5, 1);
        seed_ssd_part(dir.path(), &part, &[(0, b"x")]);
        let ssd = LocalSsd::new(dir.path());

        ssd.remove_part(&part).await.unwrap();
        assert!(!dir.path().join(part.relative_dir()).exists(), "the part dir is gone");
        // A second remove of an already-absent part is Ok (idempotent re-drive).
        ssd.remove_part(&part).await.unwrap();
    }

    #[tokio::test]
    async fn persist_chunk_lands_a_verifiable_copy_and_persist_meta_lands_the_marker() {
        let ssd_dir = TempDir::new().unwrap();
        let pool_dir = TempDir::new().unwrap();
        let part = part_key(5, 1);
        let content = b"durable chunk bytes";
        seed_ssd_part(ssd_dir.path(), &part, &[(0, content)]);

        let ssd = LocalSsd::new(ssd_dir.path());
        let ceph = LocalFs::new(pool_dir.path());
        let source = ssd.chunk_source(&part, ChunkIndex::new(0)).unwrap();
        ceph.persist_chunk(&source, &part, ChunkIndex::new(0)).await.unwrap();

        let pooled = pool_dir.path().join(part.relative_dir()).join("chunk_0.bin");
        assert_eq!(std::fs::read(&pooled).unwrap(), content, "the pooled bytes match the source");
        assert_eq!(ceph.chunk_hash(&part, ChunkIndex::new(0)).await.unwrap(), sha256_hex(content));

        let meta = ssd.meta_source(&part).unwrap();
        ceph.persist_meta(&meta, &part).await.unwrap();
        assert!(pool_dir.path().join(part.relative_dir()).join("meta.json").exists(), "meta marker landed");
    }

    #[tokio::test]
    async fn pool_remove_part_drops_a_corrupt_pool_copy_idempotently() {
        let pool_dir = TempDir::new().unwrap();
        let part = part_key(5, 1);
        let dir = pool_dir.path().join(part.relative_dir());
        std::fs::create_dir_all(&dir).unwrap();
        std::fs::write(dir.join("chunk_0.bin"), b"corrupt").unwrap();

        let ceph = LocalFs::new(pool_dir.path());
        ceph.remove_part(&part).await.unwrap();
        assert!(!dir.exists(), "the corrupt pool part dir is removed");
        ceph.remove_part(&part).await.unwrap(); // idempotent
    }

    #[tokio::test]
    async fn scan_discovers_complete_parts_and_skips_incomplete_and_junk() {
        let dir = TempDir::new().unwrap();
        let root = dir.path();
        let complete = part_key(5, 1);
        let other = part_key(7, 2);
        seed_ssd_part(root, &complete, &[(0, b"a"), (1, b"b")]);
        seed_ssd_part(root, &other, &[(0, b"c")]);

        // An incomplete part: chunk present but NO meta.json -> must be skipped.
        let incomplete = root.join(UUID).join("v5").join("part_9");
        std::fs::create_dir_all(&incomplete).unwrap();
        std::fs::write(incomplete.join("chunk_0.bin"), b"x").unwrap();

        // Junk: a non-UUID object dir and a stray top-level file -> skipped.
        std::fs::create_dir_all(root.join("not-a-uuid").join("v1").join("part_1")).unwrap();
        std::fs::write(root.join("not-a-uuid").join("v1").join("part_1").join("meta.json"), b"{}").unwrap();
        std::fs::write(root.join("stray.txt"), b"junk").unwrap();

        let ssd = LocalSsd::new(root);
        let mut found: Vec<String> = ssd
            .scan_parts()
            .await
            .unwrap()
            .into_iter()
            .map(|d| d.part.relative_dir().to_string_lossy().into_owned())
            .collect();
        found.sort();
        let mut expected = vec![
            complete.relative_dir().to_string_lossy().into_owned(),
            other.relative_dir().to_string_lossy().into_owned(),
        ];
        expected.sort();
        assert_eq!(found, expected, "only complete UUID-object parts are discovered");
    }

    #[tokio::test]
    async fn scan_of_a_missing_cache_root_is_empty() {
        let ssd = LocalSsd::new("/no/such/cephor/cache/dir");
        assert!(
            ssd.scan_parts().await.unwrap().is_empty(),
            "a missing cache root is an empty cache, not an error"
        );
    }

    #[tokio::test]
    async fn end_to_end_part_drain_copies_verifies_commits_and_unlinks() {
        let ssd_dir = TempDir::new().unwrap();
        let pool_dir = TempDir::new().unwrap();
        let part = part_key(5, 1);
        seed_ssd_part(ssd_dir.path(), &part, &[(0, b"chunk zero"), (1, b"chunk one!")]);

        let ssd = LocalSsd::new(ssd_dir.path());
        let ceph = LocalFs::new(pool_dir.path());
        let store = MemPartStore::default();
        store.set(&part, ReplicationState::Pending);
        let claim = ClaimedPart::new(part.clone(), 0);

        let outcome = drain_part(&ceph, &ssd, &store, &NoopEnqueuer, &claim).await.unwrap();

        let pool_part = pool_dir.path().join(part.relative_dir());
        let ssd_part = ssd_dir.path().join(part.relative_dir());
        assert_eq!(outcome, DrainOutcome::Replicated);
        assert_eq!(
            std::fs::read(pool_part.join("chunk_0.bin")).unwrap(),
            b"chunk zero",
            "pool holds the durable copy"
        );
        assert!(pool_part.join("chunk_1.bin").exists());
        assert!(pool_part.join("meta.json").exists(), "meta marker copied last");
        assert!(!ssd_part.exists(), "SSD part unlinked only after a verified, committed copy exists");
        assert_eq!(store.status_of(&part), Some(ReplicationState::Replicated));
    }

    #[tokio::test]
    async fn part_remover_unlinks_the_part_dir_idempotently() {
        let dir = TempDir::new().unwrap();
        let part = part_key(5, 1);
        seed_ssd_part(dir.path(), &part, &[(0, b"x")]);
        let ssd = LocalSsd::new(dir.path());

        ssd.unlink_part(&part).await.unwrap();
        assert!(!dir.path().join(part.relative_dir()).exists(), "the reclaimed part dir is gone");
        // A second reclaim of an already-absent part is Ok (mirrors the drain's unlink).
        ssd.unlink_part(&part).await.unwrap();
    }

    #[tokio::test]
    async fn sweep_orphan_tmp_removes_aged_temps_and_keeps_real_and_fresh_files() {
        let dir = TempDir::new().unwrap();
        let part = part_key(5, 1);
        let part_dir = dir.path().join(part.relative_dir());
        std::fs::create_dir_all(&part_dir).unwrap();
        // Real files alongside both temp flavors: the api's `<name>.tmp.<uuid>` and the
        // agent's `.tmp-<name>`.
        std::fs::write(part_dir.join("chunk_0.bin"), b"real").unwrap();
        std::fs::write(part_dir.join("meta.json"), b"{}").unwrap();
        let api_tmp = part_dir.join("chunk_0.bin.tmp.deadbeefdeadbeef");
        let agent_tmp = part_dir.join(".tmp-chunk_0.bin");
        std::fs::write(&api_tmp, b"partial").unwrap();
        std::fs::write(&agent_tmp, b"partial").unwrap();

        let ssd = LocalSsd::new(dir.path());

        // A long window keeps the just-written temps (younger than max_age).
        assert_eq!(ssd.sweep_orphan_tmp(Duration::from_hours(1)).await.unwrap(), 0);
        assert!(api_tmp.exists() && agent_tmp.exists(), "fresh temps within the window are kept");

        // A zero window ages every temp, so both flavors are removed; real files stay.
        assert_eq!(ssd.sweep_orphan_tmp(Duration::ZERO).await.unwrap(), 2, "both temp flavors removed");
        assert!(!api_tmp.exists() && !agent_tmp.exists(), "aged temps unlinked");
        assert!(part_dir.join("chunk_0.bin").exists(), "the real chunk is untouched");
        assert!(part_dir.join("meta.json").exists(), "the meta marker is untouched");
    }

    #[tokio::test]
    async fn sweep_orphan_tmp_of_a_missing_root_is_zero() {
        let ssd = LocalSsd::new("/no/such/cephor/cache/dir");
        assert_eq!(ssd.sweep_orphan_tmp(Duration::ZERO).await.unwrap(), 0);
    }
}
