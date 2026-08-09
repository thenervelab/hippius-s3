//! Detecting that a committed part's SSD content has CHANGED since the drain copied it,
//! so the stale pool copy can be re-driven.
//!
//! # The hole this closes
//!
//! An S3 client may `UploadPart` the same part number twice with DIFFERENT bytes before
//! `CompleteMultipartUpload`. That is legal S3 and is also the shape of a late retry after a
//! perceived timeout. The retry reuses the same `object_version`, so the part key
//! `(object_id, version, part_number)` is IDENTICAL — attempt two overwrites attempt one's
//! SSD bytes under a row that already reads `replicated`.
//!
//! Nothing re-drove that row: [`crate::Store::record_landed_part`]'s conflict action only ever
//! filled a NULL `node_id`, the reconciler tallies a `replicated` part as an orphan and leaves
//! it alone, and the api's `wake_version_replication` only touches rows still `pending`. So the
//! pool kept attempt one's ciphertext while the SSD held attempt two's.
//!
//! It is SILENT rather than a 500. The DEK is per `object_version` and the AEAD AAD binds
//! `(bucket_id, object_id, part_number, chunk_index)` — every one of which the retry preserves
//! — so the stale ciphertext decrypts and authenticates cleanly. A reader served from the pool
//! gets attempt one's plaintext under attempt two's `ETag`, with no error anywhere.
//!
//! # Why the content hash, and why it is free
//!
//! The drain already streams every chunk through SHA-256 to byte-verify its pool copy
//! ([`crate::PartPool::persist_chunk`] returns that hash). Folding those per-chunk hashes into
//! one part digest at commit therefore costs no extra I/O — the hash the drain already computed
//! becomes the record of WHAT it committed. A later re-landing re-derives the digest from disk
//! and compares.
//!
//! The alternative (re-drive on every landed announcement, without a hash) guesses: it cannot
//! tell a genuine rewrite from a duplicate announcement, and a wrong guess re-copies a whole
//! part to Ceph for nothing.
//!
//! # Why the announcement and not the reconciler
//!
//! The reconciler walks every part on the disk, so it looks like the natural home. It is not:
//! comparing content there costs a full SSD read per part, and the obvious cheap pre-filter —
//! "has `meta.json`'s mtime moved since we committed?" — does not work, because the arion
//! uploader calls `touch_part` on every part after a successful backend upload
//! (`hippius_s3/workers/uploader.py` → `object_parts.expire` → `fs_store.touch_part`), which
//! `os.utime`s every file in the part dir. The mtime therefore moves for essentially EVERY
//! part after its drain, so an mtime pre-filter would force a re-hash of the node's entire
//! ~930 GB shard.
//!
//! The landed announcement, by contrast, fires exactly when content can change and only then.
//! `WriteThroughPartsWriter` announces from BOTH of its meta choke points and nowhere else:
//! `write_meta` (simple PUT, streamed PUT) and `publish_part` (MPU parts and append deltas,
//! whose meta lands inside the staged publish). Between them every upload path is covered, and
//! the download/promotion path writes meta without announcing. So an announcement naming a part
//! that is ALREADY `replicated` is, by construction, a rewrite of a drained part — the rare
//! event, and the only one worth paying a hash for. Both paths publish to the same landed queue
//! through the same publisher, so the re-drive covers a republished MPU part exactly as it
//! covers a rewritten simple PUT.
//!
//! # The eviction race, and the gate that closes it
//!
//! Between the rewrite and [`redrive_diverged_part`](crate::Store::redrive_diverged_part)'s
//! commit, the rewritten part's SSD copy is the ONLY copy of the client's new bytes — yet its
//! row still reads `replicated` and nothing on the rewrite path touches the residency recency
//! the LRU evictor sorts on, so the part ranks as maximally COLD. An evictor unlink inside that
//! window destroys the new bytes; the digest check then reads `NotFound`, nothing is re-driven,
//! and the pool permanently serves the superseded plaintext. Three defenses, outermost first:
//!
//! - the api stamps the part's residency recency at the same choke point that announces
//!   (`WriteThroughPartsWriter.write_meta`), so the part is LRU-hottest for the whole
//!   announcement latency (best-effort, ordering only — a pass that must walk the entire
//!   cursor ignores rank);
//! - [`record_landed_part`](crate::Store::record_landed_part) stamps `relanded_at` when the
//!   announced part already reads `replicated`, and the eviction worklist EXCLUDES rows
//!   re-landed within a grace window — a hard gate from the record to well past the check;
//! - a `NotFound` at digest time is classified [`RelandOutcome::Vanished`] by
//!   [`reland_read_outcome`] rather than lumped into `Unreadable`, so the lost-race signature
//!   — possible destruction of acknowledged bytes — is loud and counted apart from a flaky
//!   disk.
//!
//! What remains open: an eviction pass that fetched its worklist page BEFORE the stamps and
//! unlinks after (bounded by one page's processing time), and a rewrite whose announcement is
//! delayed past the recency stamp's protection while the evictor is walking its whole cursor.
//! Both end in `Vanished`, which is the alarm for exactly that.
//!
//! **The residual gap, stated plainly:** the announcement queue is at-most-once by design (a
//! Redis restart or a trimmed queue drops messages). A dropped announcement for a re-uploaded
//! part leaves that part in exactly today's broken state. This is a strict improvement, not a
//! total closure; closing the rest needs a background content scrubber, which is not this.

use crate::apipart::ChunkIndex;
use crate::partdrain::PartSource;
use crate::snapshot::RelandOutcome;
use crate::state::ReplicationState;
use sha2::{Digest, Sha256};

/// A part's content digest: the lowercase-hex SHA-256 of its chunk hashes in index order.
///
/// Compared, never interpreted — the only operation that matters is equality against the
/// digest stored at commit. Opaque so a caller cannot confuse it with a per-chunk hash.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct PartDigest(String);

impl PartDigest {
    /// The wire/storage form (lowercase hex), for the `content_sha256` column.
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }

    /// Re-wraps a digest read back from the store.
    #[must_use]
    pub fn from_stored(raw: String) -> Self {
        Self(raw)
    }
}

/// Folds a part's per-chunk content hashes into one digest.
///
/// The chunk COUNT is hashed first and every hash is length-delimited, so a truncated part
/// cannot collide with the full one and two different chunk splits cannot alias — the digest
/// binds the chunk set, not just its concatenated bytes. Order is the caller's: chunk hashes
/// arrive in ascending index, matching the drain's copy loop and `list_chunks`.
#[must_use]
pub fn part_digest<S: AsRef<str>>(chunk_hashes: &[S]) -> PartDigest {
    let mut hasher = Sha256::new();
    hasher.update(b"hippius-drain/part-digest/v1\n");
    hasher.update(chunk_hashes.len().to_le_bytes());
    for hash in chunk_hashes {
        let raw = hash.as_ref().as_bytes();
        hasher.update(u32::try_from(raw.len()).unwrap_or(u32::MAX).to_le_bytes());
        hasher.update(raw);
    }
    PartDigest(hex_lower(&hasher.finalize()))
}

/// Lowercase hex, matching the agent's chunk-hash rendering so both sides of a comparison
/// speak one alphabet.
fn hex_lower(bytes: &[u8]) -> String {
    let mut out = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        out.push(char::from_digit(u32::from(byte >> 4), 16).unwrap_or('0'));
        out.push(char::from_digit(u32::from(byte & 0x0f), 16).unwrap_or('0'));
    }
    out
}

/// What a landed announcement means for a part the store already knows about.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RelandVerdict {
    /// The part has not reached `replicated`, so the drain has committed nothing that could be
    /// stale — the ordinary path (a first landing, or a re-upload of a part still queued). The
    /// pending drain will copy whatever is on disk when it runs.
    NotDrained,
    /// The part is `replicated` and its SSD content still matches what was committed. Nothing
    /// to do; this is the common re-announcement (a duplicate message, or a backstop racing the
    /// fast path).
    Unchanged,
    /// The part is `replicated` and its SSD content DIFFERS from what was committed — the pool
    /// holds superseded bytes. Re-drive.
    Diverged,
    /// The part is `replicated` but was committed before content digests were recorded, so
    /// there is nothing to compare against. Treated as [`Diverged`](Self::Diverged) by
    /// [`Self::redrives`]: an announcement for an already-drained part is itself evidence of a
    /// rewrite, and "unknown" must never resolve to "fine" on an integrity check. Distinguished
    /// from `Diverged` only so the two can be counted apart.
    Unverifiable,
}

impl RelandVerdict {
    /// Whether this verdict must re-drive the part.
    #[must_use]
    pub fn redrives(self) -> bool {
        matches!(self, Self::Diverged | Self::Unverifiable)
    }
}

/// Decides what a landed announcement means, given the row the store returned and the digest
/// just derived from disk.
///
/// Pure, so the whole policy is unit-testable and the agent's wiring is a thin caller — the
/// shape `eviction_target` and `check_shared_disk` already use, and the one that matters here
/// because the agent's own store tests need a live Postgres.
///
/// `Corrupt` deliberately reads as [`NotDrained`](RelandVerdict::NotDrained): a corrupt part is
/// already owned by the bounded re-drive worker, and re-driving it from here would bypass the
/// `corrupt_attempts` cap that stops an unrecoverable pool copy looping forever.
#[must_use]
pub fn verdict_for_reland(state: ReplicationState, stored: Option<&PartDigest>, observed: &PartDigest) -> RelandVerdict {
    if state != ReplicationState::Replicated {
        return RelandVerdict::NotDrained;
    }
    match stored {
        None => RelandVerdict::Unverifiable,
        Some(stored) if stored == observed => RelandVerdict::Unchanged,
        Some(_) => RelandVerdict::Diverged,
    }
}

/// Classifies a failed digest read into the reland outcome it should count as.
///
/// `NotFound` is not disk trouble: an announcement fires only when a part was just written, so
/// its directory being absent moments later means something unlinked it in between — the
/// evictor or the reclaimer racing the check. If the rewrite had diverged, its only copy is
/// gone and the pool permanently serves the superseded bytes, so this maps to
/// [`RelandOutcome::Vanished`] — the possible-data-loss signature — while every other error
/// keeps the flaky-disk reading, [`RelandOutcome::Unreadable`]. Neither re-drives: a re-drive
/// without readable source bytes could only flip a servable row toward `failed`.
///
/// Pure, like [`verdict_for_reland`], so the split is pinned by unit tests rather than living
/// in the agent's log statements.
#[must_use]
pub fn reland_read_outcome(kind: std::io::ErrorKind) -> RelandOutcome {
    // ErrorKind is #[non_exhaustive]; everything that is not the vanished signature is, by
    // definition here, an unreadable part.
    match kind {
        std::io::ErrorKind::NotFound => RelandOutcome::Vanished,
        _ => RelandOutcome::Unreadable,
    }
}

/// Derives a part's digest from its SSD copy, by hashing every chunk the source lists.
///
/// The expensive half of the check, deliberately kept behind the state test: it reads the whole
/// part, so it must only run for an announcement naming an already-`replicated` part. Chunk
/// order follows [`PartSource::list_chunks`], which yields ascending indices — the same order
/// the drain hashes in, which is what makes the two digests comparable.
///
/// # Errors
///
/// The underlying [`io::Error`](std::io::Error). `NotFound` is reserved for a part that is
/// GONE — the vanished (lost-race) signature — on [`PartSource::list_chunks`]'s strict
/// contract (an absent part dir is `NotFound`, never an empty listing). A chunk that
/// disappears mid-fold is discriminated by re-LISTING, not by reading meta: a concurrent
/// republish unlinks `meta.json` for an instant while the dir persists, so a meta probe
/// would read a routine rewrite as the data-loss alarm. Dir gone → `NotFound`; dir standing
/// → remapped off `NotFound`, and every other failure is "cannot tell": the caller leaves
/// the row alone, since re-driving on a read error would let a flaky disk re-copy the shard.
pub async fn observed_part_digest<S: PartSource>(ssd: &S, part: &crate::apipart::PartKey) -> std::io::Result<PartDigest> {
    let chunks: Vec<ChunkIndex> = ssd.list_chunks(part).await?;
    let mut hashes = Vec::with_capacity(chunks.len());
    for index in chunks {
        match ssd.chunk_hash(part, index).await {
            Ok(hash) => hashes.push(hash),
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => {
                return match ssd.list_chunks(part).await {
                    Err(relist) if relist.kind() == std::io::ErrorKind::NotFound => Err(relist),
                    // The dir still stands, so this is a mid-fold trim by a concurrent
                    // rewrite, not the lost race — keep it off NotFound so the rewrite's
                    // own announcement re-checks quietly instead of firing the alarm.
                    _ => Err(std::io::Error::other(format!(
                        "chunk {} vanished mid-digest while its part remains",
                        index.get()
                    ))),
                };
            }
            Err(err) => return Err(err),
        }
    }
    Ok(part_digest(&hashes))
}

#[cfg(test)]
#[expect(clippy::unwrap_used, reason = "tests")]
mod tests {
    use super::{PartDigest, RelandOutcome, RelandVerdict, observed_part_digest, part_digest, reland_read_outcome, verdict_for_reland};
    use crate::apipart::{ChunkIndex, ObjectId, PartKey, PartMeta, PartNumber, Version};
    use crate::partdrain::PartSource;
    use crate::state::ReplicationState;
    use proptest::prelude::*;
    use std::future::Future;
    use std::io;
    use std::io::ErrorKind;
    use std::path::PathBuf;
    use std::str::FromStr;

    proptest! {
        /// The two collision classes the divergence check depends on, over random hash sets:
        /// a permuted chunk order and a truncated chunk set must never alias the original —
        /// otherwise a real rewrite could read as Unchanged and the pool would keep stale bytes.
        #[test]
        fn a_digest_never_survives_permutation_or_truncation(
            hashes in prop::collection::vec("[0-9a-f]{64}", 2..8),
            a in 0usize..8,
            b in 0usize..8,
        ) {
            let full = part_digest(&hashes);
            prop_assert_ne!(&full, &part_digest(&hashes[..hashes.len() - 1]), "truncation must not alias");
            let (a, b) = (a % hashes.len(), b % hashes.len());
            if hashes[a] != hashes[b] {
                let mut permuted = hashes.clone();
                permuted.swap(a, b);
                prop_assert_ne!(&full, &part_digest(&permuted), "reordering must not alias");
            }
        }
    }

    /// A source honoring `list_chunks`'s strict contract: an absent part is `Err(NotFound)`
    /// at the listing, exactly like `LocalSsd` — the fidelity that lets these tests stand in
    /// for the real filesystem.
    ///
    /// Chunk index → hash; a `None` hash is a chunk that vanished between the listing and
    /// its read.
    type FakeChunks = Vec<(u32, Option<&'static str>)>;

    struct SsdLike {
        /// `None` = the whole part dir is gone.
        part: Option<FakeChunks>,
    }

    impl PartSource for SsdLike {
        fn list_chunks(&self, _part: &PartKey) -> impl Future<Output = io::Result<Vec<ChunkIndex>>> + Send {
            let indices: io::Result<Vec<ChunkIndex>> = self
                .part
                .as_ref()
                .map(|chunks| chunks.iter().map(|&(i, _)| ChunkIndex::new(i)).collect())
                .ok_or_else(|| io::Error::new(ErrorKind::NotFound, "no part dir"));
            async move { indices }
        }

        fn chunk_source(&self, part: &PartKey, index: ChunkIndex) -> io::Result<PathBuf> {
            Ok(part.relative_dir().join(format!("chunk_{}.bin", index.get())))
        }

        fn meta_source(&self, part: &PartKey) -> io::Result<PathBuf> {
            Ok(part.relative_dir().join("meta.json"))
        }

        fn part_meta(&self, _part: &PartKey) -> impl Future<Output = io::Result<PartMeta>> + Send {
            let present = self.part.as_ref().map(Vec::len);
            async move {
                let num_chunks = present.ok_or_else(|| io::Error::new(ErrorKind::NotFound, "no part"))?;
                Ok(PartMeta {
                    chunk_size: 4,
                    num_chunks: u32::try_from(num_chunks).unwrap_or(u32::MAX),
                    size_bytes: 4,
                })
            }
        }

        fn chunk_hash(&self, _part: &PartKey, index: ChunkIndex) -> impl Future<Output = io::Result<String>> + Send {
            let hash = self
                .part
                .as_ref()
                .and_then(|chunks| chunks.iter().find(|&&(i, _)| i == index.get()))
                .and_then(|&(_, hash)| hash);
            async move { hash.map(str::to_owned).ok_or_else(|| io::Error::new(ErrorKind::NotFound, "chunk gone")) }
        }
    }

    fn key() -> PartKey {
        PartKey::new(
            ObjectId::from_str("6f2e8f64-9c9b-4d7a-8b0a-0f3f4c1d2e3a").unwrap(),
            Version::new(5),
            PartNumber::new(1),
        )
    }

    #[tokio::test]
    async fn an_absent_part_reads_as_not_found_not_as_an_empty_digest() {
        // The lost-race regression: an evicted (absent) part must be the Vanished signature,
        // never a well-formed empty digest that reads as Diverged and spuriously re-drives a
        // servable row toward `failed` — silencing the alarm built for exactly this loss.
        let err = observed_part_digest(&SsdLike { part: None }, &key()).await.unwrap_err();
        assert_eq!(err.kind(), ErrorKind::NotFound);
        assert_eq!(reland_read_outcome(err.kind()), RelandOutcome::Vanished);
    }

    #[tokio::test]
    async fn a_chunkless_part_that_still_stands_folds_to_the_empty_digest() {
        let observed = observed_part_digest(&SsdLike { part: Some(vec![]) }, &key()).await.unwrap();
        assert_eq!(observed, part_digest::<&str>(&[]));
    }

    #[tokio::test]
    async fn a_chunk_vanishing_under_a_standing_part_is_not_the_vanished_signature() {
        // A concurrent republish trims tail chunks between the listing and the fold. The part
        // is being rewritten, not destroyed — reporting NotFound would fire the data-loss
        // alarm for a routine rewrite, so the error must land in the quiet Unreadable arm.
        let source = SsdLike {
            part: Some(vec![(0, Some("aa")), (1, None)]),
        };
        let err = observed_part_digest(&source, &key()).await.unwrap_err();
        assert_ne!(err.kind(), ErrorKind::NotFound);
        assert_eq!(reland_read_outcome(err.kind()), RelandOutcome::Unreadable);
    }

    #[tokio::test]
    async fn a_chunk_vanishing_with_its_whole_part_is_the_vanished_signature() {
        // The whole-part loss caught mid-fold rather than at the listing: the first listing
        // saw the chunk, the read misses it, and the RE-listing (not a meta probe — a
        // concurrent republish unlinks meta for an instant while the dir persists) confirms
        // the part itself is gone.
        struct GoneMidFold {
            listed: std::sync::atomic::AtomicBool,
        }
        impl PartSource for GoneMidFold {
            fn list_chunks(&self, _part: &PartKey) -> impl Future<Output = io::Result<Vec<ChunkIndex>>> + Send {
                let first = !self.listed.swap(true, std::sync::atomic::Ordering::SeqCst);
                async move {
                    if first {
                        Ok(vec![ChunkIndex::new(0)])
                    } else {
                        Err(io::Error::new(ErrorKind::NotFound, "part dir gone"))
                    }
                }
            }
            fn chunk_source(&self, part: &PartKey, index: ChunkIndex) -> io::Result<PathBuf> {
                Ok(part.relative_dir().join(format!("chunk_{}.bin", index.get())))
            }
            fn meta_source(&self, part: &PartKey) -> io::Result<PathBuf> {
                Ok(part.relative_dir().join("meta.json"))
            }
            async fn part_meta(&self, _part: &PartKey) -> io::Result<PartMeta> {
                Err(io::Error::new(ErrorKind::NotFound, "no part"))
            }
            async fn chunk_hash(&self, _part: &PartKey, _index: ChunkIndex) -> io::Result<String> {
                Err(io::Error::new(ErrorKind::NotFound, "chunk gone"))
            }
        }
        let source = GoneMidFold {
            listed: std::sync::atomic::AtomicBool::new(false),
        };
        let err = observed_part_digest(&source, &key()).await.unwrap_err();
        assert_eq!(err.kind(), ErrorKind::NotFound);
        assert_eq!(reland_read_outcome(err.kind()), RelandOutcome::Vanished);
    }

    #[test]
    fn a_digest_is_stable_and_separates_content_order_and_count() {
        // The four properties the comparison rests on. Stability is what makes an unchanged part
        // read as unchanged; the other three are what stop a real change reading as unchanged.
        assert_eq!(part_digest(&["a", "b"]), part_digest(&["a", "b"]), "same input, same digest");
        assert_ne!(part_digest(&["a", "b"]), part_digest(&["a", "c"]), "different content");
        assert_ne!(part_digest(&["a", "b"]), part_digest(&["b", "a"]), "different chunk order");
        assert_ne!(part_digest(&["a", "b"]), part_digest(&["a"]), "a truncated chunk set");
        // Length-delimiting is what buys the last one: without it "ab" + "" and "a" + "b" would
        // hash the same byte stream and a re-chunked part could alias its predecessor.
        assert_ne!(part_digest(&["ab", ""]), part_digest(&["a", "b"]));
    }

    #[test]
    fn a_digest_is_lowercase_hex_of_the_right_width() {
        // Both sides of a comparison must render hashes the same way; the agent's chunk hashes
        // are lowercase hex, so a digest that came back uppercase would never match.
        let digest = part_digest(&["a"]);
        assert_eq!(digest.as_str().len(), 64);
        assert!(digest.as_str().chars().all(|c| c.is_ascii_digit() || ('a'..='f').contains(&c)));
        assert_eq!(PartDigest::from_stored(digest.as_str().to_owned()), digest, "storage round-trips");
    }

    #[test]
    fn only_a_replicated_part_can_diverge() {
        // A part that has not committed has nothing stale on the pool: whatever the drain
        // eventually copies is whatever is on disk then. Corrupt is excluded for a different
        // reason — `redrive_corrupt_parts` owns it, and its attempt cap is what stops an
        // unrecoverable pool copy looping forever.
        let stored = part_digest(&["old"]);
        let observed = part_digest(&["new"]);
        for state in [
            ReplicationState::Pending,
            ReplicationState::Draining,
            ReplicationState::Failed,
            ReplicationState::Corrupt,
        ] {
            let verdict = verdict_for_reland(state, Some(&stored), &observed);
            assert_eq!(verdict, RelandVerdict::NotDrained, "{state:?} has nothing committed to re-drive");
            assert!(!verdict.redrives());
        }
    }

    #[test]
    fn an_unverifiable_commit_redrives_but_is_counted_apart_from_a_proven_divergence() {
        // The NULL policy. "Cannot compare" must not resolve to "fine" on an integrity check,
        // so it re-drives — but it is a distinct verdict so the metrics can say how much of the
        // re-drive volume is legacy rows rather than genuine rewrites.
        let observed = part_digest(&["whatever"]);
        let verdict = verdict_for_reland(ReplicationState::Replicated, None, &observed);
        assert_eq!(verdict, RelandVerdict::Unverifiable);
        assert!(verdict.redrives());
        assert_ne!(verdict, RelandVerdict::Diverged);
    }

    #[test]
    fn a_vanished_part_is_the_lost_race_signature_not_a_flaky_disk() {
        // The discriminator this exists for. An announced part was JUST written, so NotFound at
        // digest time means something unlinked it between the rewrite and the check — the
        // evict-vs-reland race, whose worst case is destruction of the client's acknowledged
        // bytes with the pool left serving the superseded ones. Lumping it into Unreadable hid
        // that signature behind "local-disk trouble", which has the opposite operator response.
        assert_eq!(reland_read_outcome(ErrorKind::NotFound), RelandOutcome::Vanished);
    }

    #[test]
    fn every_other_read_failure_stays_the_flaky_disk_reading() {
        // The pre-existing fail-safe: a disk that cannot be read decides nothing, and re-driving
        // on it would re-copy the shard. Only the absent-directory signature is pulled out.
        for kind in [
            ErrorKind::PermissionDenied,
            ErrorKind::TimedOut,
            ErrorKind::UnexpectedEof,
            ErrorKind::InvalidData,
            ErrorKind::Other,
        ] {
            assert_eq!(reland_read_outcome(kind), RelandOutcome::Unreadable, "{kind:?}");
        }
    }
}
