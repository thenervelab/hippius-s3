//! Real filesystem implementations of the drain/GC contracts over POSIX mounts.
//!
//! [`LocalFs`] is the durable `CephFS` pool (a POSIX mount); [`LocalSsd`] is the
//! node-local ingest cache. The drain replicates the api's part layout
//! `<root>/<object_id>/v<version>/part_<n>/` path-preservingly: [`LocalSsd`]
//! implements [`PartSource`]/[`PartScan`] (list, locate, hash, scan, unlink a
//! part) and [`LocalFs`] implements [`PartPool`] (persist chunk/meta, hash,
//! remove). The crash-safety guarantee the drain depends on lives in
//! [`copy_into`]: it streams+hashes the bytes, `fdatasync`s the temp, atomically
//! renames within the part folder, and (once per part, via `finalize_part`) fsyncs
//! the folder so a power loss never leaves a torn file. Both also
//! implement the GC-only [`CephFs`]/[`SsdCache`] `remove_object` reclaim (by
//! `<object_id>` folder).

use core::future::Future;
use hippius_drain_core::{
    CephFs, ChunkIndex, DiscoveredPart, FileId, FreeSpaceProbe, META_FILE_NAME, PartKey, PartMeta, PartPool, PartRemover, PartScan, PartSource,
    SsdCache, chunk_file_name, parse_part_dir,
};
use nix::fcntl::{Flock, FlockArg};
use sha2::{Digest, Sha256};
use std::ffi::OsStr;
use std::hash::BuildHasher;
use std::io;
use std::path::{Path, PathBuf};
use std::sync::LazyLock;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, SystemTime};
use tokio::fs;
use tokio::io::AsyncReadExt;
use tokio::io::AsyncWriteExt;

/// Streaming hash read buffer. Bounds memory so multi-gigabyte chunks are never
/// read whole into memory just to hash them.
// DR-3: 1 MiB (was 64 KiB). tokio::fs dispatches each read/write to the blocking pool, so a small
// buffer means ~128 dispatches per 4 MiB chunk copy + ~64 for the readback hash; 1 MiB cuts that
// ~16x. Peak memory is bounded by drain concurrency × this buffer (a few MiB at the default).
const HASH_BUF_BYTES: usize = 1 << 20;

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

    /// Whether an absent part under this root means "that part is gone" rather than
    /// "the whole volume is gone".
    ///
    /// A per-part `ENOENT` is ambiguous on its own: an ext4 ingest volume that fails
    /// to mount leaves an EMPTY directory on the PARENT filesystem, so every part
    /// stats `NotFound` cleanly, with no error to distinguish it from a genuinely
    /// vanished source. Escalating that to the terminal missing-source write-off would
    /// retire a node's entire backlog as unrecoverable within hours of a mount failure.
    ///
    /// Two positive signals, either of which proves the volume is really there:
    /// - the root has at least one entry (data is present, so absence is real), or
    /// - the root is its own mount point (`st_dev` differs from its parent), which
    ///   covers the legitimately-empty case of a fully-drained node — the orphan rows
    ///   this write-off exists to clear.
    ///
    /// Unreadable, or empty AND not a mount point, returns `false`: the caller must
    /// treat that as a node-global condition instead of blaming the part. Erring this
    /// way only delays a write-off; the opposite error is silent replication loss.
    pub async fn root_is_available(&self) -> bool {
        use std::os::unix::fs::MetadataExt;

        let Ok(meta) = fs::metadata(&self.root).await else {
            return false;
        };
        if !meta.is_dir() {
            return false;
        }
        match fs::read_dir(&self.root).await {
            Ok(mut entries) => {
                if matches!(entries.next_entry().await, Ok(Some(_))) {
                    return true;
                }
            }
            Err(_) => return false,
        }
        // Empty. Only a distinct st_dev separates "drained volume" from "never mounted".
        match self.root.parent() {
            Some(parent) => match fs::metadata(parent).await {
                Ok(parent_meta) => parent_meta.dev() != meta.dev(),
                Err(_) => false,
            },
            // No parent means the root IS the filesystem root; nothing to compare.
            None => true,
        }
    }

    /// Removes orphaned write-temp files left on the SSD by a crashed mid-write PUT
    /// (the api's `<name>.tmp.<uuid>`) or a cancelled persist (the agent's
    /// `.tmp-<name>`), once older than `max_age`; and the api's staged chunks
    /// (`chunk_<i>.bin.staged.<attempt>`) once older than `staged_max_age`.
    ///
    /// The two graces differ because the files differ. A write-temp exists for
    /// milliseconds, so anything older is a crash orphan. A staged chunk is deliberately
    /// held for the WHOLE of one `UploadPart` — that is what stops a duplicate attempt
    /// overwriting an already-acknowledged part — so it is legitimately as old as the
    /// upload, and a multi-GB part on a slow link outlives the write-temp grace many times
    /// over. Reaping it on the write-temp grace would delete a live upload's data.
    ///
    /// Walks the `<object>/v<version>/part_<n>/` layout and only ever unlinks a temp or
    /// staged FILE — never a real `chunk_*.bin`/`meta.json`, never a directory — so it
    /// cannot touch a complete or in-flight part. Returns how many files it removed; a
    /// missing root is an empty cache, not an error. The companion to whole-part reclaim
    /// (`reclaim_ssd`), which already clears temps inside a reclaimed part dir.
    ///
    /// # Errors
    ///
    /// [`io::Error`] if walking the cache or unlinking a temp fails for a reason other
    /// than a concurrently-removed entry (which is tolerated).
    pub async fn sweep_orphan_tmp(&self, max_age: Duration, staged_max_age: Duration) -> io::Result<u64> {
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
                    removed += sweep_part_tmp(&part.path(), max_age, staged_max_age).await?;
                }
            }
        }
        Ok(removed)
    }

    /// Removes the empty `part_<n>/`, `<object_id>/v<version>/` and `<object_id>/` shells left
    /// behind by a drained or swept part, once untouched for `max_age`.
    ///
    /// `remove_part_dir` deletes only `part_<n>/`, so over millions of drained parts the two
    /// ancestor dirs accumulate as empty shells — an unbounded directory/inode leak on the
    /// node-local SSD (observed at 100k-338k dirs per prod node, all `<oid>/v1/` with nothing
    /// inside). The crash-orphan reclaim never sees them: it keys on `meta.json`, and a shell
    /// has none.
    ///
    /// The part level is swept for the same reason one level up: an `UploadPart` killed after
    /// staging chunks but before publishing leaves a dir that NOTHING else can reach — with no
    /// `meta.json` it is invisible to the reclaim scan, and with neither a residency nor a
    /// replication row the evictor's `remove_dir_all` is never called on it. `sweep_orphan_tmp`
    /// empties it; this is what then collects the shell.
    ///
    /// THE AGE GATE IS THE SAFETY PROPERTY, not the emptiness check. Pruning inline at removal
    /// time looks safe because `rmdir` refuses a non-empty dir — but `mkdir -p` is not atomic.
    /// The api's writer (`fs_store.set_chunk`) calls `mkdir(parents=True)`, which on ENOENT
    /// creates the parent and then retries the leaf; a pruner that rmdirs that freshly-created,
    /// still-empty parent inside the gap makes the retry raise. FS writes are fatal in
    /// `object_writer`, so that is a 500 on PutObject/UploadPart — and it is reachable, because
    /// the drain unlinks in-flight MPU parts and so prunes the very directory a client is
    /// uploading siblings into. Creating an entry in a directory updates that directory's
    /// mtime, so requiring the shell to have been untouched for `max_age` means no writer has
    /// created anything in it recently — closing the window rather than narrowing it.
    ///
    /// Sweeps innermost-first — parts, then versions, then objects — so a tree emptied from the
    /// bottom collapses entirely in ONE pass rather than one level per poll. Returns how many
    /// shells it removed; a missing root is an empty cache, not an error.
    ///
    /// # Errors
    ///
    /// [`io::Error`] if walking the cache fails for a reason other than a concurrently-removed
    /// entry (which is tolerated).
    pub async fn sweep_empty_shells(&self, max_age: Duration) -> io::Result<u64> {
        let mut removed = 0;
        let Some(mut objects) = open_dir(&self.root).await? else {
            return Ok(0);
        };
        while let Some(object) = objects.next_entry().await? {
            if !object.file_type().await?.is_dir() {
                continue;
            }
            if let Some(mut versions) = open_dir(&object.path()).await? {
                while let Some(version) = versions.next_entry().await? {
                    if !version.file_type().await?.is_dir() {
                        continue;
                    }
                    if let Some(mut parts) = open_dir(&version.path()).await? {
                        while let Some(part) = parts.next_entry().await? {
                            if !part.file_type().await?.is_dir() {
                                continue;
                            }
                            removed += u64::from(rmdir_if_stale_and_empty(&part.path(), max_age).await?);
                        }
                    }
                    // Only after its parts, so a version emptied by its last part just now goes too.
                    removed += u64::from(rmdir_if_stale_and_empty(&version.path(), max_age).await?);
                }
            }
            // Only after its versions, so an object whose last version just went is collected now.
            removed += u64::from(rmdir_if_stale_and_empty(&object.path(), max_age).await?);
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

/// Streams `source` into `dest`, computing the SHA-256 of the bytes in the same pass so the
/// copy needs no second read to verify (the audit's hash-once win), and `fdatasync`s `dest`
/// — [`File::sync_data`] skips the atime/mtime metadata flush a `sync_all` would force.
/// Bounded by [`HASH_BUF_BYTES`] so a multi-gigabyte chunk is never read whole into memory.
async fn stream_copy_hash(source: &Path, dest: &Path) -> io::Result<String> {
    let mut reader = fs::File::open(source).await?;
    let mut writer = fs::File::create(dest).await?;
    let mut hasher = Sha256::new();
    let mut buf = vec![0_u8; HASH_BUF_BYTES];
    loop {
        let read = reader.read(&mut buf).await?;
        if read == 0 {
            break;
        }
        hasher.update(&buf[..read]);
        writer.write_all(&buf[..read]).await?;
    }
    writer.flush().await?;
    writer.sync_data().await?;
    Ok(hex_lower(hasher.finalize().as_slice()))
}

/// A monotonic per-process counter that makes each write temp unique WITHIN a process —
/// so two tasks persisting the SAME `name` concurrently never share a temp file (C11).
static TEMP_SEQ: AtomicU64 = AtomicU64::new(0);

/// A per-process random nonce that makes write temps unique ACROSS processes. `pid` alone
/// is not enough: two agent pods overlapping on one SSD mount during a rolling update
/// commonly both run as pid 1, so both would emit `.tmp-<name>.1.0` for their first write
/// and tear each other's copy (the post-copy SHA re-verify catches the tear, but this makes
/// it impossible). Sourced from `RandomState`, whose `new()` is "initialized with random
/// keys" and whose two instances are documented "unlikely to produce the same result for the
/// same values" — process-unique entropy with no added dependency. Computed once on first use.
static TEMP_NONCE: LazyLock<u64> = LazyLock::new(|| std::collections::hash_map::RandomState::new().hash_one(0u8));

/// The write-temp name for `name`: the agent's `.tmp-` prefix (recognized by
/// [`is_temp_name`] and the orphan sweep) plus a `pid.nonce.counter` suffix — the
/// per-process [`TEMP_NONCE`] separates overlapping pods and the [`TEMP_SEQ`] counter
/// separates concurrent writers in one process, so no two persists share a temp path.
fn temp_name(name: &str) -> String {
    format!(
        ".tmp-{name}.{}.{:x}.{}",
        std::process::id(),
        *TEMP_NONCE,
        TEMP_SEQ.fetch_add(1, Ordering::Relaxed)
    )
}

/// Atomically place `source`'s bytes into `dir` as `name`, returning the lowercase-hex
/// SHA-256 of the bytes streamed during the copy: stream into a temp inside `dir`
/// (`fdatasync`'d), rename into place, then — only when `sync_dir` is set — fsync `dir`.
/// A crash leaves either no file or the complete one. The temp is suffixed per-writer
/// ([`temp_name`]), so two tasks draining the SAME part concurrently — a re-claim after a
/// lease lapses under slow Ceph, which the single-writer claim does NOT prevent (`claim_seq`
/// fences the COMMIT, not the in-flight copy) — write to distinct temps; last rename wins,
/// harmless since the bytes are deterministic per (part, chunk).
///
/// The directory fsync is deferred (`sync_dir = false` for chunks + meta) so the whole
/// part costs ONE dir-fsync via [`sync_parent_dir`], not one per file — the caller drives
/// it through `PartPool::finalize_part` after every rename lands.
async fn copy_into(dir: &Path, name: &str, source: &Path, sync_dir: bool) -> io::Result<String> {
    let dest = dir.join(name);
    let tmp = dir.join(temp_name(name));
    fs::create_dir_all(dir).await?;
    // Arm temp cleanup before the copy: any failure or cancellation before the rename
    // must not leave a `.tmp-*` orphan in the pool (the drop-guard idiom, RfR ch.1).
    let guard = TmpGuard::arm(tmp.clone());
    let hash = stream_copy_hash(source, &tmp).await?;
    fs::rename(&tmp, &dest).await?;
    guard.dismiss();
    if sync_dir {
        sync_parent_dir(dir).await?;
    }
    Ok(hash)
}

/// Remove a part's whole directory; an already-absent dir is `Ok` (idempotent, so a
/// re-drive after a crash still converges). Used by the pool corrupt-copy cleanup;
/// the SSD-source unlink goes through [`remove_part_dir_exclusive`] instead.
///
/// Removes ONLY the part dir. The now-empty `v<version>/`/`<object_id>/` parents are left
/// for [`LocalSsd::sweep_empty_shells`], deliberately — see that method for why pruning them
/// inline is unsafe.
async fn remove_part_dir(root: &Path, part: &PartKey) -> io::Result<()> {
    match fs::remove_dir_all(part_dir(root, part)).await {
        Ok(()) => Ok(()),
        Err(err) if err.kind() == io::ErrorKind::NotFound => Ok(()),
        Err(err) => Err(err),
    }
}

/// [`remove_part_dir`], but contending on the api's part-publish advisory lock first.
///
/// The api's `fs_store.publish_part` renames an attempt's staged chunk set onto the canonical
/// `chunk_<i>.bin` names while holding `flock(part_dir_fd, LOCK_EX)` (`_part_dir_flock` in
/// `hippius_s3/cache/fs_store.py`). `remove_dir_all` deletes by directory listing, so racing
/// that swap unguarded can unlink freshly-acknowledged chunks — the part looked evictable
/// because the PREVIOUS attempt's content had replicated (the B-2 key reuse). Taking the same
/// lock — same `flock(2)` call, same dir inode — makes the two mutually exclusive.
///
/// Non-blocking, deliberately: a held lock means a publish is mid-swap and the part must NOT
/// be removed now. `EWOULDBLOCK` is surfaced as an ordinary removal failure (raw errno kept
/// for the caller's classification); the part stays and a later pass retries. The lock is
/// held across the whole `remove_dir_all`: a publisher arriving mid-removal blocks in its own
/// `flock` and then fails its renames with `ENOENT` — a visible 500 the client retries, never
/// a silently half-deleted publish.
async fn remove_part_dir_exclusive(root: &Path, part: &PartKey) -> io::Result<()> {
    let dir = part_dir(root, part);
    // `flock` + `remove_dir_all` both block; one hop to the blocking pool covers the pair.
    tokio::task::spawn_blocking(move || {
        let file = match std::fs::File::open(&dir) {
            Ok(file) => file,
            Err(err) if err.kind() == io::ErrorKind::NotFound => return Ok(()),
            Err(err) => return Err(err),
        };
        let lock = match Flock::lock(file, FlockArg::LockExclusiveNonblock) {
            Ok(lock) => lock,
            Err((_, errno)) => return Err(io::Error::from(errno)),
        };
        let removed = match std::fs::remove_dir_all(&dir) {
            Ok(()) => Ok(()),
            Err(err) if err.kind() == io::ErrorKind::NotFound => Ok(()),
            Err(err) => Err(err),
        };
        // The kernel drops the lock with the fd; the dir inode it locked is already unlinked.
        drop(lock);
        removed
    })
    .await
    .map_err(io::Error::other)?
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

/// Whether a directory-entry name is one of the api's staged chunks — `chunk_<i>.bin.staged.
/// <attempt>`, an upload attempt's private copy that becomes `chunk_<i>.bin` only when the
/// attempt publishes the whole set. Deliberately NOT spelled with `.tmp.`, so it survives the
/// write-temp grace for as long as an upload legitimately runs; [`sweep_part_tmp`] gives it its
/// own, longer one. Never counted by [`parse_chunk_index`], so it is invisible to the
/// completeness gate either way.
fn is_staged_name(name: &str) -> bool {
    name.starts_with("chunk_") && name.contains(".bin.staged.")
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
/// `rmdir` `dir` if it is empty AND has been untouched for `max_age`; `Ok(false)` otherwise.
///
/// Both conditions are load-bearing. Emptiness is enforced by the kernel (`rmdir` returns
/// `ENOTEMPTY` otherwise), so a sibling part can never be collateral. The age gate is what
/// makes it safe against a concurrent `mkdir -p`, whose non-atomicity leaves a window where a
/// parent is created but still empty — see [`LocalSsd::sweep_empty_shells`].
///
/// A concurrently-removed dir (`NotFound`) and a dir that filled up between the stat and the
/// `rmdir` (`DirectoryNotEmpty`) are both normal races, not failures. Anything else is
/// surfaced: a silently-swallowed `EACCES`/`EIO` would let the leak grow unbounded while the
/// sweep reports success.
async fn rmdir_if_stale_and_empty(dir: &Path, max_age: Duration) -> io::Result<bool> {
    let meta = match fs::metadata(dir).await {
        Ok(meta) => meta,
        Err(err) if err.kind() == io::ErrorKind::NotFound => return Ok(false),
        Err(err) => return Err(err),
    };
    if file_age(&meta) < max_age {
        return Ok(false);
    }
    match fs::remove_dir(dir).await {
        Ok(()) => Ok(true),
        Err(err) if matches!(err.kind(), io::ErrorKind::NotFound | io::ErrorKind::DirectoryNotEmpty) => Ok(false),
        Err(err) => Err(err),
    }
}

async fn sweep_part_tmp(part_path: &Path, max_age: Duration, staged_max_age: Duration) -> io::Result<u64> {
    let mut removed = 0;
    let Some(mut entries) = open_dir(part_path).await? else {
        return Ok(0);
    };
    while let Some(entry) = entries.next_entry().await? {
        let raw = entry.file_name();
        let Some(name) = raw.to_str() else {
            continue;
        };
        let grace = if is_temp_name(name) {
            max_age
        } else if is_staged_name(name) {
            staged_max_age
        } else {
            continue;
        };
        let meta = entry.metadata().await?;
        if !meta.is_file() || file_age(&meta) < grace {
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

/// The age of a part's `meta.json` marker (`now() - mtime`), or `None` when the marker is
/// absent. The api writes meta last, so its presence means the part is complete and safe
/// to drain (an incomplete part is skipped by the scan); its mtime is the part's landing
/// age, carried on [`DiscoveredPart`] for the reclaim's orphan grace (a deleted-object
/// part has no DB row to date). `ZERO` on clock skew — the fail-safe direction (reads
/// young, so a borderline orphan is kept rather than reclaimed early).
async fn part_meta_age(part_path: &Path) -> io::Result<Option<Duration>> {
    match fs::metadata(part_path.join(META_FILE_NAME)).await {
        Ok(meta) if meta.is_file() => Ok(Some(file_age(&meta))),
        Ok(_) => Ok(None),
        Err(err) if err.kind() == io::ErrorKind::NotFound => Ok(None),
        Err(err) => Err(err),
    }
}

/// The subset of the api's `meta.json` the drain reads. Only `num_chunks` gates the
/// completeness check today; `chunk_size`/`size_bytes` are carried for a future size
/// assertion and to fail-closed (via `InvalidData`) if the schema drifts.
#[derive(serde::Deserialize)]
struct MetaJson {
    chunk_size: u64,
    num_chunks: u32,
    size_bytes: u64,
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

    async fn part_meta(&self, part: &PartKey) -> io::Result<PartMeta> {
        let bytes = fs::read(part_dir(&self.root, part).join(META_FILE_NAME)).await?;
        // A malformed manifest is corruption, not a not-ready shortfall, so it maps to
        // InvalidData (non-benign) — the completeness `IncompleteSource` is only for a
        // well-formed meta whose declared chunk count exceeds what is on disk.
        let parsed: MetaJson =
            serde_json::from_slice(&bytes).map_err(|err| io::Error::new(io::ErrorKind::InvalidData, format!("malformed meta.json: {err}")))?;
        Ok(PartMeta {
            chunk_size: parsed.chunk_size,
            num_chunks: parsed.num_chunks,
            size_bytes: parsed.size_bytes,
        })
    }

    async fn chunk_hash(&self, part: &PartKey, index: ChunkIndex) -> io::Result<String> {
        hash_file(&part_dir(&self.root, part).join(chunk_file_name(index))).await
    }
}

impl PartPool for LocalFs {
    async fn persist_chunk(&self, source: &Path, part: &PartKey, index: ChunkIndex) -> io::Result<String> {
        // sync_dir=false: the per-part dir fsync is batched into finalize_part below.
        copy_into(&part_dir(&self.root, part), &chunk_file_name(index), source, false).await
    }

    async fn persist_meta(&self, source: &Path, part: &PartKey) -> io::Result<()> {
        // sync_dir=false: meta's dir entry flushes with the chunks' in the single finalize.
        copy_into(&part_dir(&self.root, part), META_FILE_NAME, source, false).await.map(|_| ())
    }

    async fn finalize_part(&self, part: &PartKey) -> io::Result<()> {
        // Fsync the part dir first (all chunk + meta renames become durable together —
        // Task 1's batched fsync), THEN each ancestor up to the pool root. A part-dir fsync
        // alone leaves the part reachable only via parent dir entries a crash could still
        // lose (the new version/object dirs this drain just created), so the ancestor walk
        // is what makes "committed => durably reachable on CephFS" hold — not merely
        // "chunks fsynced". Fsyncing an already-durable dir is a near-noop, so the extra
        // two/three syncs are negligible next to the per-chunk data fsyncs.
        let part_path = part_dir(&self.root, part);
        let version_path = part_path.parent().unwrap_or(self.root.as_path());
        let object_path = version_path.parent().unwrap_or(self.root.as_path());
        sync_parent_dir(&part_path).await?;
        sync_parent_dir(version_path).await?;
        sync_parent_dir(object_path).await?;
        sync_parent_dir(self.root.as_path()).await
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
        let Some(age) = part_meta_age(&entry.path()).await? else {
            continue;
        };
        let rel = Path::new(object).join(version).join(&part);
        if let Ok(key) = parse_part_dir(&rel) {
            out.push(DiscoveredPart { part: key, age });
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
        // The sole whole-part unlink seam, shared by the reclaim worker (debris) and the
        // read-tier evictor (resident parts). Idempotent, so two of them racing — or a
        // re-drive after a crash — is harmless. The drain itself no longer unlinks at all:
        // it retains its copy to serve reads. Exclusive against the api's part-publish
        // flock — see [`remove_part_dir_exclusive`] — so an eviction can never interleave
        // with a publish's renames into the same dir.
        remove_part_dir_exclusive(&self.root, part).await
    }
}

impl FreeSpaceProbe for LocalSsd {
    type Error = io::Error;

    /// Re-probes free space between eviction pages, so a long pass converges on the disk's
    /// real state rather than on the single reading it started with.
    ///
    /// This matters more than a periodic refresh usually would: the evictor's own accounting
    /// sums `cephor_ssd_residency.bytes`, which is denormalized at residency time and records
    /// zero for a part whose size was unknown. A pass trusting that sum alone could believe it
    /// had freed nothing while reclaiming real space, and page until its time budget.
    ///
    /// `statvfs` blocks, so it goes to the blocking pool (axiom `r4r_ch10_01`) exactly as the
    /// heartbeat and the pass's initial probe do.
    fn free_bytes(&self) -> impl Future<Output = io::Result<u64>> + Send {
        let root = self.root.clone();
        async move {
            tokio::task::spawn_blocking(move || crate::disk::disk_usage(&root))
                .await
                .map_err(io::Error::other)?
                .map(|usage| usage.free_bytes)
        }
    }
}

#[cfg(test)]
#[expect(clippy::unwrap_used, reason = "tests")]
mod tests {
    use super::{LocalSsd, TEMP_NONCE, TmpGuard, hex_lower, is_temp_name, part_dir, remove_part_dir, safe_component, temp_name};
    use core::str::FromStr;
    use core::time::Duration;
    use hippius_drain_core::{FileId, ObjectId, PartKey, PartNumber, SsdCache, Version};
    use proptest::prelude::*;
    use std::io;
    use std::path::Path;
    use tempfile::TempDir;

    fn fid(raw: &str) -> FileId {
        FileId::from_str(raw).unwrap()
    }

    #[test]
    fn temp_name_is_unique_per_writer_and_still_recognized_as_a_temp() {
        // C11: two persists of the SAME name must get DISTINCT temp files. Sharing one temp
        // is a real tear risk when a lapsed lease under slow Ceph lets two tasks drain one
        // part at once (claim_seq fences the commit, not the in-flight copy).
        let a = temp_name("chunk_0.bin");
        let b = temp_name("chunk_0.bin");
        assert_ne!(a, b, "two temps for the same name must differ");
        for t in [&a, &b] {
            assert!(t.starts_with(".tmp-"), "keeps the agent temp prefix: {t}");
            assert!(is_temp_name(t), "the orphan sweep still recognizes it as a temp: {t}");
        }
        // The process nonce is constant within a run (only the trailing counter advances), so
        // the two names differ only in the final field — the cross-process separator is stable.
        let nonce = format!(".{:x}.", *TEMP_NONCE);
        assert!(
            a.contains(&nonce) && b.contains(&nonce),
            "both carry the stable per-process nonce: {a} {b}"
        );
    }

    // `root_is_available` gates the terminal missing-source write-off. Its whole job is
    // to tell "this part vanished" apart from "the ingest volume vanished", because both
    // surface as a bare ENOENT on the part path and only the first may retire a row.

    #[tokio::test]
    async fn a_populated_root_proves_the_volume_is_present() {
        let dir = TempDir::new().unwrap();
        std::fs::create_dir_all(dir.path().join("some-object")).unwrap();
        assert!(
            LocalSsd::new(dir.path()).root_is_available().await,
            "any entry under the root is positive proof the volume is mounted"
        );
    }

    #[tokio::test]
    async fn an_empty_root_that_is_not_a_mount_point_is_not_available() {
        // The mount-failure shape: an EMPTY directory sitting on its parent's
        // filesystem. Indistinguishable from a drained volume by content alone, so the
        // st_dev comparison is what has to catch it. Nested inside a TempDir, both
        // share one st_dev — exactly an ingest SSD that never mounted.
        let dir = TempDir::new().unwrap();
        let never_mounted = dir.path().join("local_object_cache");
        std::fs::create_dir_all(&never_mounted).unwrap();
        assert!(
            !LocalSsd::new(&never_mounted).root_is_available().await,
            "an empty non-mount root must NOT authorize write-offs — this is the mass \
             write-off hazard the guard exists for"
        );
    }

    #[tokio::test]
    async fn a_missing_or_non_directory_root_is_not_available() {
        let dir = TempDir::new().unwrap();
        assert!(
            !LocalSsd::new(dir.path().join("absent")).root_is_available().await,
            "a root that isn't there proves nothing about the parts under it"
        );
        let file = dir.path().join("not-a-dir");
        std::fs::write(&file, b"x").unwrap();
        assert!(
            !LocalSsd::new(&file).root_is_available().await,
            "a non-directory root is a misconfiguration, not a drained volume"
        );
    }

    // NB: the remaining arm — empty AND a real mount point, i.e. a fully-drained node
    // whose orphan rows SHOULD still be written off — needs an actual mount and so is
    // not unit-testable here. It is the `parent_meta.dev() != meta.dev()` branch.

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

    fn part(uuid: &str, version: u32, number: u32) -> PartKey {
        PartKey::new(ObjectId::from_str(uuid).unwrap(), Version::new(version), PartNumber::new(number))
    }

    fn seed_part(root: &Path, p: &PartKey) {
        let dir = part_dir(root, p);
        std::fs::create_dir_all(&dir).unwrap();
        std::fs::write(dir.join("meta.json"), b"{}").unwrap();
        std::fs::write(dir.join("chunk_0.bin"), b"data").unwrap();
    }

    #[tokio::test]
    async fn the_sweep_removes_stale_empty_version_and_object_shells() {
        // The leak fix: a drained part leaves `v<version>/` and `<object_id>/` behind, and the
        // sweep collects both once they have been untouched for the grace.
        let root = TempDir::new().unwrap();
        let p = part("466916c0-d61b-4518-b81b-9576b574270a", 1, 1);
        seed_part(root.path(), &p);
        let part_path = part_dir(root.path(), &p);
        let version_dir = part_path.parent().unwrap().to_path_buf();
        let object_dir = version_dir.parent().unwrap().to_path_buf();
        let ssd = LocalSsd::new(root.path().to_path_buf());

        remove_part_dir(root.path(), &p).await.unwrap();
        assert!(version_dir.exists(), "removal itself leaves the shells — that is the point");

        assert_eq!(ssd.sweep_empty_shells(Duration::ZERO).await.unwrap(), 2, "version + object");

        assert!(!version_dir.exists(), "the empty v<version> shell is swept");
        assert!(!object_dir.exists(), "the empty <object_id> shell is swept");
        assert!(root.path().exists(), "the SSD root is never removed");
    }

    #[tokio::test]
    async fn a_killed_attempts_staged_part_is_fully_reclaimed_by_the_two_sweeps() {
        // THE reaping story for staging. An UploadPart killed after staging chunks but before
        // publishing leaves a dir with NO meta.json, so the reclaim scan never discovers it, and
        // with neither a residency nor a replication row the evictor's remove_dir_all is never
        // called on it either. These two sweeps are the ONLY thing that can reach it: the tmp
        // sweep empties the dir, the shell sweep then collapses the whole branch.
        let root = TempDir::new().unwrap();
        let p = part("466916c0-d61b-4518-b81b-9576b574270a", 1, 1);
        let part_path = part_dir(root.path(), &p);
        std::fs::create_dir_all(&part_path).unwrap();
        std::fs::write(part_path.join("chunk_0.bin.staged.0123456789abcdef"), b"staged").unwrap();
        std::fs::write(part_path.join("chunk_1.bin.staged.0123456789abcdef"), b"staged").unwrap();
        let object_dir = part_path.parent().unwrap().parent().unwrap().to_path_buf();
        let ssd = LocalSsd::new(root.path().to_path_buf());

        assert_eq!(ssd.sweep_orphan_tmp(Duration::ZERO, Duration::ZERO).await.unwrap(), 2);
        assert_eq!(
            ssd.sweep_empty_shells(Duration::ZERO).await.unwrap(),
            3,
            "part + version + object, all in one pass"
        );

        assert!(!object_dir.exists(), "nothing of the killed attempt survives");
        assert!(root.path().exists(), "the SSD root is never removed");
    }

    #[tokio::test]
    async fn the_sweep_spares_a_part_dir_that_still_holds_staged_chunks() {
        // The live-upload guard at the part level: a staged set inside its own grace keeps the
        // dir non-empty, and `rmdir` is kernel-gated on emptiness, so an in-flight UploadPart
        // cannot have its directory pulled out from under it.
        let root = TempDir::new().unwrap();
        let p = part("466916c0-d61b-4518-b81b-9576b574270a", 1, 1);
        let part_path = part_dir(root.path(), &p);
        std::fs::create_dir_all(&part_path).unwrap();
        std::fs::write(part_path.join("chunk_0.bin.staged.0123456789abcdef"), b"in flight").unwrap();
        let ssd = LocalSsd::new(root.path().to_path_buf());

        assert_eq!(ssd.sweep_orphan_tmp(Duration::ZERO, Duration::from_hours(24)).await.unwrap(), 0);
        assert_eq!(ssd.sweep_empty_shells(Duration::ZERO).await.unwrap(), 0);

        assert!(part_path.join("chunk_0.bin.staged.0123456789abcdef").exists());
    }

    #[tokio::test]
    async fn the_sweep_spares_a_shell_younger_than_the_grace() {
        // THE race guard. A shell younger than the grace may be a parent that a writer's
        // non-atomic `mkdir -p` has just created and is about to create its part dir inside.
        // Removing it there makes the writer's retry raise — a fatal FS write, i.e. a 500 on
        // PutObject. Only an untouched-for-the-grace shell can be collected.
        let root = TempDir::new().unwrap();
        let p = part("466916c0-d61b-4518-b81b-9576b574270a", 1, 1);
        let version_dir = part_dir(root.path(), &p).parent().unwrap().to_path_buf();
        std::fs::create_dir_all(&version_dir).unwrap();
        let ssd = LocalSsd::new(root.path().to_path_buf());

        assert_eq!(ssd.sweep_empty_shells(Duration::from_hours(1)).await.unwrap(), 0);

        assert!(version_dir.exists(), "a freshly-created shell is left for a possible writer");
    }

    #[tokio::test]
    async fn a_sibling_part_protects_the_shared_parents() {
        // `rmdir` is kernel-gated on emptiness, so a sibling `part_N` under the same version
        // keeps both shared parents alive — no cross-part collateral removal.
        let root = TempDir::new().unwrap();
        let p1 = part("466916c0-d61b-4518-b81b-9576b574270a", 1, 1);
        let p2 = part("466916c0-d61b-4518-b81b-9576b574270a", 1, 2);
        seed_part(root.path(), &p1);
        seed_part(root.path(), &p2);
        let shared_version = part_dir(root.path(), &p1).parent().unwrap().to_path_buf();
        let ssd = LocalSsd::new(root.path().to_path_buf());

        remove_part_dir(root.path(), &p1).await.unwrap();
        assert_eq!(ssd.sweep_empty_shells(Duration::ZERO).await.unwrap(), 0, "nothing is empty");

        assert!(part_dir(root.path(), &p2).exists(), "the sibling part is untouched");
        assert!(shared_version.exists(), "the shared version dir is kept while a sibling remains");
    }

    #[tokio::test]
    async fn sweeping_one_version_keeps_a_populated_sibling_version() {
        // The sweep collects only EMPTY dirs: v1 goes, but the object dir stays because v2 is
        // still populated.
        let root = TempDir::new().unwrap();
        let v1 = part("466916c0-d61b-4518-b81b-9576b574270a", 1, 1);
        let v2 = part("466916c0-d61b-4518-b81b-9576b574270a", 2, 1);
        seed_part(root.path(), &v1);
        seed_part(root.path(), &v2);
        let v1_dir = part_dir(root.path(), &v1).parent().unwrap().to_path_buf();
        let object_dir = v1_dir.parent().unwrap().to_path_buf();
        let ssd = LocalSsd::new(root.path().to_path_buf());

        remove_part_dir(root.path(), &v1).await.unwrap();
        assert_eq!(ssd.sweep_empty_shells(Duration::ZERO).await.unwrap(), 1, "v1 only");

        assert!(!v1_dir.exists(), "the emptied v1 dir is swept");
        assert!(object_dir.exists(), "the object dir survives while v2 is populated");
        assert!(part_dir(root.path(), &v2).exists(), "v2's part is untouched");
    }

    #[tokio::test]
    async fn the_sweep_collects_an_object_emptied_by_its_last_version_in_one_pass() {
        // Version dirs are swept before their object, so the object shell does not need a
        // second pass — otherwise the leak would drain at one level per reclaim tick.
        let root = TempDir::new().unwrap();
        let p = part("00000000-0000-4000-8000-000000000000", 7, 1);
        let version_dir = part_dir(root.path(), &p).parent().unwrap().to_path_buf();
        std::fs::create_dir_all(&version_dir).unwrap();
        let object_dir = version_dir.parent().unwrap().to_path_buf();
        let ssd = LocalSsd::new(root.path().to_path_buf());

        assert_eq!(ssd.sweep_empty_shells(Duration::ZERO).await.unwrap(), 2);

        assert!(!version_dir.exists());
        assert!(!object_dir.exists(), "collected in the SAME pass, not the next one");
    }

    #[tokio::test]
    async fn removing_an_already_absent_part_is_ok() {
        // Idempotent: a re-drive after the part is already gone returns Ok.
        let root = TempDir::new().unwrap();
        let p = part("00000000-0000-4000-8000-000000000000", 1, 1);

        remove_part_dir(root.path(), &p).await.unwrap();
    }

    #[tokio::test]
    async fn the_sweep_never_breaks_a_concurrent_writers_mkdir_p() {
        // The regression this whole change exists for. Pruning inline at removal time raced the
        // api's `mkdir(parents=True)`: that call is not atomic — on ENOENT it creates the parent
        // then retries the leaf — so removing the freshly-created, still-empty parent in the gap
        // made the retry fail. FS writes are fatal in object_writer, so that is a 500.
        //
        // The sweep is genuinely BUSY here, not idling: 200 pre-aged shells are collected while
        // the writer runs, so this exercises concurrent removal rather than a no-op. What keeps
        // the writer safe is the grace — its own dirs are touched on every iteration and so are
        // never old enough to collect.
        let root = TempDir::new().unwrap();
        let ssd = LocalSsd::new(root.path().to_path_buf());
        let grace = Duration::from_millis(100);

        for i in 0..200 {
            let shell = root.path().join(format!("0000{i:04}-0000-4000-8000-000000000000")).join("v1");
            std::fs::create_dir_all(&shell).unwrap();
        }
        tokio::time::sleep(grace * 2).await; // age the shells past the grace

        let p = part("466916c0-d61b-4518-b81b-9576b574270a", 1, 2);
        let dir = part_dir(root.path(), &p);

        let stop = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
        let sweeper_stop = std::sync::Arc::clone(&stop);
        let swept = tokio::spawn(async move {
            let mut total = 0;
            while !sweeper_stop.load(std::sync::atomic::Ordering::Relaxed) {
                total += ssd.sweep_empty_shells(grace).await.unwrap_or(0);
                tokio::task::yield_now().await;
            }
            total
        });

        let mut failures = 0_u32;
        for _ in 0..3000 {
            if tokio::fs::create_dir_all(&dir).await.is_err() {
                failures += 1;
            }
            let _ = tokio::fs::remove_dir(&dir).await;
        }
        stop.store(true, std::sync::atomic::Ordering::Relaxed);
        let total_swept = swept.await.unwrap();

        assert_eq!(failures, 0, "the sweep broke a concurrent writer's mkdir -p");
        assert!(total_swept > 0, "the sweep must have been actively removing, or this proves nothing");
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
    use super::{LocalFs, LocalSsd, hex_lower, list_chunk_indices};
    use core::future::Future;
    use core::str::FromStr;
    use hippius_drain_core::{
        ChunkIndex, ClaimedPart, DrainOutcome, ObjectId, PartDrainError, PartKey, PartNumber, PartPool, PartRemover, PartReplicationStore, PartScan,
        PartSource, PartVerified, ReplicationState, UploadEnqueuer, Version, drain_part,
    };
    use sha2::{Digest, Sha256};
    use std::collections::HashMap;
    use std::io;
    use std::sync::Mutex;
    use std::time::{Duration, SystemTime};
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
        // num_chunks must match the seeded set, or the drain's completeness gate rejects it.
        let meta = format!(r#"{{"chunk_size":4,"num_chunks":{},"size_bytes":4}}"#, chunks.len());
        std::fs::write(dir.join("meta.json"), meta).unwrap();
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

        // Residency accounting is the Postgres store's job; this in-memory double only
        // exercises the drain's copy/verify/commit ordering.
        async fn mark_resident(&self, _part: &PartKey, _bytes: u64) -> Result<(), io::Error> {
            Ok(())
        }

        fn mark_replicated(&self, part: &ClaimedPart, _proof: &PartVerified) -> impl Future<Output = Result<(), io::Error>> + Send {
            let key = Self::key(part.part());
            async move {
                self.status.lock().unwrap().insert(key, ReplicationState::Replicated);
                Ok(())
            }
        }

        async fn mark_upload_enqueued(&self, _part: &PartKey) -> Result<(), io::Error> {
            // The localfs drain tests assert the SSD/pool copy + commit; the upload_enqueued_at
            // stamp is exercised by the core partdrain tests + the store integration tests.
            Ok(())
        }

        fn mark_failed(&self, part: &ClaimedPart, _reason: &str) -> impl Future<Output = Result<(), io::Error>> + Send {
            let key = Self::key(part.part());
            async move {
                self.status.lock().unwrap().insert(key, ReplicationState::Failed);
                Ok(())
            }
        }

        fn mark_corrupt(&self, part: &ClaimedPart, _reason: &str) -> impl Future<Output = Result<(), io::Error>> + Send {
            let key = Self::key(part.part());
            async move {
                self.status.lock().unwrap().insert(key, ReplicationState::Corrupt);
                Ok(())
            }
        }

        fn is_version_servable(&self, _part: &PartKey) -> impl Future<Output = Result<bool, io::Error>> + Send {
            // The e2e drain tests exercise the happy/abandoned paths; an unservable default keeps
            // a mismatch on the `Failed` path (the R4 servable→Corrupt branch is unit-tested in
            // partdrain against a servable fake).
            let servable = false;
            async move { Ok(servable) }
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
    async fn unlink_part_is_idempotent() {
        let dir = TempDir::new().unwrap();
        let part = part_key(5, 1);
        seed_ssd_part(dir.path(), &part, &[(0, b"x")]);
        let ssd = LocalSsd::new(dir.path());

        ssd.unlink_part(&part).await.unwrap();
        assert!(!dir.path().join(part.relative_dir()).exists(), "the part dir is gone");
        // A second remove of an already-absent part is Ok (idempotent re-drive).
        ssd.unlink_part(&part).await.unwrap();
    }

    #[tokio::test]
    async fn unlink_part_skips_while_a_publisher_holds_the_part_dir_flock() {
        let dir = TempDir::new().unwrap();
        let part = part_key(5, 1);
        seed_ssd_part(dir.path(), &part, &[(0, b"x")]);
        let ssd = LocalSsd::new(dir.path());

        // Simulate the api's `_part_dir_flock`: an exclusive flock on the part dir's own fd.
        // `flock` contends per open file description, so one process is enough to exercise it.
        let part_dir = dir.path().join(part.relative_dir());
        let publisher = nix::fcntl::Flock::lock(std::fs::File::open(&part_dir).unwrap(), nix::fcntl::FlockArg::LockExclusive)
            .map_err(|(_, errno)| errno)
            .unwrap();

        let err = ssd.unlink_part(&part).await.unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::WouldBlock, "a busy lock is a skip, not a removal");
        assert!(part_dir.join("chunk_0.bin").exists(), "the publisher's chunks survive");

        drop(publisher);
        ssd.unlink_part(&part).await.unwrap();
        assert!(!part_dir.exists(), "with the lock released the part is removable");
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
        let copy_hash = ceph.persist_chunk(&source, &part, ChunkIndex::new(0)).await.unwrap();
        // The hash-once win: persist_chunk returns the SHA computed during the copy stream,
        // so the drain needs no separate readback of the source to verify.
        assert_eq!(copy_hash, sha256_hex(content), "persist_chunk returns the copy-time hash");

        let pooled = pool_dir.path().join(part.relative_dir()).join("chunk_0.bin");
        assert_eq!(std::fs::read(&pooled).unwrap(), content, "the pooled bytes match the source");

        let meta = ssd.meta_source(&part).unwrap();
        ceph.persist_meta(&meta, &part).await.unwrap();
        // One dir-fsync for the whole part: after it, every chunk + meta is durably present.
        ceph.finalize_part(&part).await.unwrap();
        assert!(pool_dir.path().join(part.relative_dir()).join("meta.json").exists(), "meta marker landed");
        assert_eq!(
            ceph.chunk_hash(&part, ChunkIndex::new(0)).await.unwrap(),
            sha256_hex(content),
            "the pooled chunk is durable + verifiable after finalize",
        );
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
    async fn scan_carries_the_meta_mtime_age_for_the_orphan_grace() {
        // The reclaim's orphan grace keys on this FS age (a deleted-object part has no DB
        // row to date). Backdate the meta.json mtime and assert the scan reports it, so a
        // future regression that hardcodes ZERO (which would reclaim every orphan
        // instantly) is caught.
        let dir = TempDir::new().unwrap();
        let part = part_key(5, 1);
        seed_ssd_part(dir.path(), &part, &[(0, b"a")]);
        let meta = dir.path().join(part.relative_dir()).join("meta.json");
        let handle = std::fs::OpenOptions::new().write(true).open(&meta).unwrap();
        handle.set_modified(SystemTime::now() - Duration::from_hours(2)).unwrap();

        let discovered = LocalSsd::new(dir.path()).scan_parts().await.unwrap();
        assert_eq!(discovered.len(), 1);
        assert!(
            discovered[0].age >= Duration::from_hours(1),
            "the scanned age reflects the backdated meta mtime, not a hardcoded zero (got {:?})",
            discovered[0].age,
        );
    }

    #[tokio::test]
    async fn end_to_end_part_drain_copies_verifies_commits_and_retains() {
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
        assert!(
            ssd_part.exists(),
            "the SSD copy is retained as this node's read tier once a verified pool copy exists",
        );
        assert_eq!(store.status_of(&part), Some(ReplicationState::Replicated));
    }

    #[tokio::test]
    async fn end_to_end_drain_of_a_part_whose_meta_overdeclares_defers_and_keeps_the_ssd_copy() {
        // WI-1 against the real FS: meta claims 2 chunks but only chunk 0 is on disk (a
        // chunk removed after meta landed). The drain must defer with the SSD copy intact
        // and nothing committed to the pool.
        let ssd_dir = TempDir::new().unwrap();
        let pool_dir = TempDir::new().unwrap();
        let part = part_key(5, 1);
        seed_ssd_part(ssd_dir.path(), &part, &[(0, b"only chunk")]);
        // Overwrite the (matching) meta with one that over-declares num_chunks.
        std::fs::write(
            ssd_dir.path().join(part.relative_dir()).join("meta.json"),
            br#"{"chunk_size":4,"num_chunks":2,"size_bytes":8}"#,
        )
        .unwrap();

        let ssd = LocalSsd::new(ssd_dir.path());
        let ceph = LocalFs::new(pool_dir.path());
        let store = MemPartStore::default();
        store.set(&part, ReplicationState::Pending);
        let claim = ClaimedPart::new(part.clone(), 0);

        let err = drain_part(&ceph, &ssd, &store, &NoopEnqueuer, &claim).await.unwrap_err();

        assert!(
            matches!(err, PartDrainError::IncompleteSource { declared: 2, present: 1 }),
            "got: {err:?}"
        );
        assert!(ssd_dir.path().join(part.relative_dir()).exists(), "SSD copy kept");
        assert!(!pool_dir.path().join(part.relative_dir()).exists(), "nothing committed to the pool");
        assert_eq!(store.status_of(&part), Some(ReplicationState::Pending));
    }

    #[tokio::test]
    async fn finalize_part_fsyncs_the_new_part_version_and_object_dirs() {
        // WI-15: finalize walks the fresh object/version/part dir chain (created by the
        // first drain of a new object). It must succeed against a freshly-created deep tree
        // and be a cheap no-op on a second call over the now-durable tree.
        let ssd_dir = TempDir::new().unwrap();
        let pool_dir = TempDir::new().unwrap();
        let part = part_key(5, 1);
        seed_ssd_part(ssd_dir.path(), &part, &[(0, b"bytes")]);
        let ssd = LocalSsd::new(ssd_dir.path());
        let ceph = LocalFs::new(pool_dir.path());
        let source = ssd.chunk_source(&part, ChunkIndex::new(0)).unwrap();
        ceph.persist_chunk(&source, &part, ChunkIndex::new(0)).await.unwrap();
        ceph.persist_meta(&ssd.meta_source(&part).unwrap(), &part).await.unwrap();

        ceph.finalize_part(&part).await.unwrap();
        let part_path = pool_dir.path().join(part.relative_dir());
        assert!(part_path.join("chunk_0.bin").exists() && part_path.join("meta.json").exists());
        // A second finalize over an already-durable tree is a cheap no-op, not an error.
        ceph.finalize_part(&part).await.unwrap();
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
        assert_eq!(ssd.sweep_orphan_tmp(Duration::from_hours(1), Duration::from_hours(24)).await.unwrap(), 0);
        assert!(api_tmp.exists() && agent_tmp.exists(), "fresh temps within the window are kept");

        // A zero window ages every temp, so both flavors are removed; real files stay.
        assert_eq!(
            ssd.sweep_orphan_tmp(Duration::ZERO, Duration::from_hours(24)).await.unwrap(),
            2,
            "both temp flavors removed"
        );
        assert!(!api_tmp.exists() && !agent_tmp.exists(), "aged temps unlinked");
        assert!(part_dir.join("chunk_0.bin").exists(), "the real chunk is untouched");
        assert!(part_dir.join("meta.json").exists(), "the meta marker is untouched");
    }

    #[tokio::test]
    async fn sweep_orphan_tmp_of_a_missing_root_is_zero() {
        let ssd = LocalSsd::new("/no/such/cephor/cache/dir");
        assert_eq!(ssd.sweep_orphan_tmp(Duration::ZERO, Duration::from_hours(24)).await.unwrap(), 0);
    }

    #[tokio::test]
    async fn sweep_orphan_tmp_reaches_a_temp_in_an_incomplete_no_meta_part_dir() {
        // The real orphan case: a PUT crashed mid-write, leaving a temp in a part dir with
        // NO meta.json (and no completed chunk). scan_parts skips such dirs, so only the
        // sweep can reclaim it — it must walk no-meta dirs, not just complete ones.
        let dir = TempDir::new().unwrap();
        let part = part_key(7, 3);
        let part_dir = dir.path().join(part.relative_dir());
        std::fs::create_dir_all(&part_dir).unwrap();
        let orphan = part_dir.join("chunk_0.bin.tmp.cafebabecafebabe");
        std::fs::write(&orphan, b"half-written").unwrap();

        let ssd = LocalSsd::new(dir.path());
        assert_eq!(
            ssd.sweep_orphan_tmp(Duration::ZERO, Duration::from_hours(24)).await.unwrap(),
            1,
            "the temp in a no-meta dir is swept"
        );
        assert!(!orphan.exists());
    }

    #[tokio::test]
    async fn sweep_orphan_tmp_tolerates_non_dir_entries_at_every_level() {
        // The walk must skip non-directory junk at the object/version/part levels rather
        // than abort, and still find the real temp.
        let dir = TempDir::new().unwrap();
        let root = dir.path();
        let part = part_key(1, 1); // root/<uuid>/v1/part_1
        let part_dir = root.join(part.relative_dir());
        std::fs::create_dir_all(&part_dir).unwrap();
        // Stray files where dirs would be, at each level.
        std::fs::write(root.join("stray-at-root"), b"x").unwrap();
        std::fs::write(root.join(part.object().as_str()).join("stray-at-object"), b"x").unwrap();
        std::fs::write(part_dir.parent().unwrap().join("stray-at-version"), b"x").unwrap();
        // A real aged temp in the proper part dir -> still swept.
        std::fs::write(part_dir.join("meta.json.tmp.feedfacefeedface"), b"partial").unwrap();

        let ssd = LocalSsd::new(root);
        assert_eq!(
            ssd.sweep_orphan_tmp(Duration::ZERO, Duration::from_hours(24)).await.unwrap(),
            1,
            "non-dir junk is skipped and the real temp is swept",
        );
    }

    #[tokio::test]
    async fn sweep_orphan_tmp_keeps_a_staged_chunk_past_the_write_temp_grace() {
        // A staged chunk is held for the WHOLE of one UploadPart by design, so it is
        // legitimately older than a write-temp ever is. Reaping it on the write-temp grace
        // would delete a live multi-GB upload's own data; it gets its own, longer window.
        let dir = TempDir::new().unwrap();
        let part = part_key(9, 2);
        let part_dir = dir.path().join(part.relative_dir());
        std::fs::create_dir_all(&part_dir).unwrap();
        let staged = part_dir.join("chunk_0.bin.staged.0123456789abcdef");
        let write_tmp = part_dir.join("chunk_0.bin.tmp.deadbeefdeadbeef");
        std::fs::write(&staged, b"in flight").unwrap();
        std::fs::write(&write_tmp, b"partial").unwrap();

        let ssd = LocalSsd::new(dir.path());

        // Write temps aged out; the staged chunk is inside its own window and survives.
        assert_eq!(ssd.sweep_orphan_tmp(Duration::ZERO, Duration::from_hours(24)).await.unwrap(), 1);
        assert!(staged.exists(), "a staged chunk outlives the write-temp grace");
        assert!(!write_tmp.exists());

        // Past its own grace it is a crash orphan (the api never published) and goes.
        assert_eq!(ssd.sweep_orphan_tmp(Duration::ZERO, Duration::ZERO).await.unwrap(), 1);
        assert!(!staged.exists(), "an aged staged chunk is reclaimed");
    }

    #[tokio::test]
    async fn a_staged_chunk_is_not_counted_as_a_chunk() {
        // The completeness gate must not see staging: a part with one published chunk and one
        // staged file is a ONE-chunk part, not a two-chunk one.
        let dir = TempDir::new().unwrap();
        let part = part_key(4, 6);
        let part_dir = dir.path().join(part.relative_dir());
        std::fs::create_dir_all(&part_dir).unwrap();
        std::fs::write(part_dir.join("chunk_0.bin"), b"published").unwrap();
        std::fs::write(part_dir.join("chunk_1.bin.staged.0123456789abcdef"), b"in flight").unwrap();

        assert_eq!(list_chunk_indices(&part_dir).await.unwrap(), vec![ChunkIndex::new(0)]);
    }
}
