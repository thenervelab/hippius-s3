//! The api's landed-part announcements: a per-node Redis queue the api pushes to when it
//! finishes writing a part, so the drain learns about it without walking the disk.
//!
//! # Why this exists
//!
//! Until now the reconciler was the SOLE discovery mechanism: every 15 s it walked the whole
//! SSD cache, materialised every part it found, and asked the database which of them it had
//! never seen. That was cheap while the SSD held only the undrained backlog — a drained part
//! was unlinked, so the tree was small. Retention broke that assumption: the disk now holds the
//! node's entire replicated shard, measured on prod 2026-08-07 at **2.28 M parts / ~930 GB per
//! node**, so the walk became ~2.28 M `stat` calls and ~4,560 batched Postgres round-trips
//! *every 15 seconds, per node*, to discover a handful of new parts.
//!
//! The announcement makes discovery O(new parts). The reconciler stays, demoted to a backstop
//! on a long poll: it is what recovers a part whose announcement was lost.
//!
//! # Why a queue and not a direct write from the api
//!
//! `cephor_replication_status` is the drain's table, owned by the drain's migrations. Letting
//! the api `INSERT` into it directly would give one table two writers across two services with
//! two independent migration systems. The repo already settled this shape for the mirror-image
//! handoff — the drain is the sole producer of upload requests and publishes them over Redis
//! rather than writing the consumer's state — so this follows it: the api announces, the agent
//! records.
//!
//! # Delivery guarantees, and why weak ones are enough
//!
//! At-most-once, deliberately. A lost message (Redis restart, a trimmed queue, an agent down
//! longer than the queue bound) costs latency and nothing else: the reconciler's next sweep
//! finds the part and records it exactly as it does today. So this is a fast path over a
//! correct-but-slow one, never a replacement for it — which is what lets the api treat its
//! publish as best-effort and lets this consumer drop a malformed message without ceremony.

use core::str::FromStr;
use hippius_drain_core::{ObjectId, PartKey, PartNumber, Version};
use redis::AsyncCommands;
use redis::aio::ConnectionManager;
use serde::Deserialize;
use thiserror::Error;

/// Redis key holding one node's landed-part announcements. Per node, because a part is only
/// ever on the SSD of the node that ingested it — a peer could not act on the message.
#[must_use]
pub fn landed_queue_key(node: &str) -> String {
    format!("cephor:landed:{node}")
}

/// A failure draining the announcement queue.
#[derive(Debug, Error)]
#[non_exhaustive]
pub enum LandedError {
    /// The Redis pop failed. Transient; the next poll retries and the reconciler backstops.
    #[error("reading the landed-part queue failed")]
    Redis(#[from] redis::RedisError),
}

/// One announcement as the api publishes it.
///
/// Field names are the wire contract with `hippius_s3/writer/landed.py`; changing either side
/// alone silently drops every message, since a parse failure is discarded by design.
#[derive(Debug, Deserialize)]
struct LandedMessage {
    object_id: String,
    version: u32,
    part_number: u32,
}

impl LandedMessage {
    /// The typed key, or `None` when the payload does not describe a real part.
    ///
    /// A malformed announcement is dropped rather than retried: it can only come from a
    /// version skew or a corrupted value, neither of which a retry fixes, and the reconciler
    /// still discovers the part from the disk.
    fn into_part(self) -> Option<PartKey> {
        let object = ObjectId::from_str(&self.object_id).ok()?;
        Some(PartKey::new(object, Version::new(self.version), PartNumber::new(self.part_number)))
    }
}

/// Pops landed-part announcements for one node.
///
/// Cheap to clone — the `ConnectionManager` is a shared multiplexed handle.
#[derive(Clone)]
pub struct LandedQueue {
    redis: ConnectionManager,
    key: String,
}

// Manual Debug: redis's `ConnectionManager` is not `Debug`, so derive cannot apply.
impl core::fmt::Debug for LandedQueue {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.debug_struct("LandedQueue").field("key", &self.key).finish_non_exhaustive()
    }
}

impl LandedQueue {
    /// Binds a queue to `node`'s announcement key.
    #[must_use]
    pub fn new(redis: ConnectionManager, node: &str) -> Self {
        Self {
            redis,
            key: landed_queue_key(node),
        }
    }

    /// Pops up to `limit` announcements, oldest first.
    ///
    /// `RPOP key count` takes the tail, which is the oldest end of an `LPUSH`ed list — so the
    /// queue is FIFO and a burst is drained in arrival order rather than newest-first.
    ///
    /// Unparseable entries are counted, not returned: see [`LandedMessage::into_part`].
    ///
    /// # Errors
    ///
    /// [`LandedError::Redis`] if the pop fails. Nothing is lost — the entries stay queued.
    pub async fn pop(&self, limit: usize) -> Result<(Vec<PartKey>, u64), LandedError> {
        let mut conn = self.redis.clone();
        let raw: Vec<String> = conn.rpop(&self.key, core::num::NonZeroUsize::new(limit)).await?;
        let mut parts = Vec::with_capacity(raw.len());
        let mut dropped = 0u64;
        for entry in raw {
            match serde_json::from_str::<LandedMessage>(&entry).ok().and_then(LandedMessage::into_part) {
                Some(part) => parts.push(part),
                None => dropped = dropped.saturating_add(1),
            }
        }
        Ok((parts, dropped))
    }
}

#[cfg(test)]
#[expect(clippy::unwrap_used, reason = "tests")]
mod tests {
    use super::{LandedMessage, landed_queue_key};

    const UUID: &str = "466916c0-d61b-4518-b81b-9576b574270a";

    #[test]
    fn the_queue_is_scoped_to_one_node() {
        // A part lives only on the SSD of the node that ingested it, so a peer receiving the
        // announcement could record a row pointing at a disk that does not hold the data —
        // and `claim_part` is node-scoped, so that part would never drain at all.
        assert_eq!(landed_queue_key("k8s-v3-node1"), "cephor:landed:k8s-v3-node1");
        assert_ne!(landed_queue_key("k8s-v3-node1"), landed_queue_key("k8s-v3-node2"));
    }

    #[test]
    fn a_well_formed_announcement_becomes_a_part_key() {
        // The wire contract with hippius_s3/writer/landed.py. These three field names are the
        // whole protocol; renaming one on either side silently drops every message, because a
        // parse failure is discarded by design.
        let raw = format!(r#"{{"object_id":"{UUID}","version":7,"part_number":3}}"#);
        let part = serde_json::from_str::<LandedMessage>(&raw).unwrap().into_part().unwrap();

        assert_eq!(part.object().as_str(), UUID);
        assert_eq!(part.version().get(), 7);
        assert_eq!(part.part().get(), 3);
    }

    #[test]
    fn a_non_uuid_object_id_is_dropped_rather_than_recorded() {
        // `object_id` reaches a SQL bind and a filesystem path, so an unvalidated value is both
        // a correctness and a traversal concern. Dropping is safe: the reconciler still finds
        // the part on disk, so a bad message costs latency, never the part.
        let raw = r#"{"object_id":"../../etc/passwd","version":1,"part_number":1}"#;
        assert!(serde_json::from_str::<LandedMessage>(raw).unwrap().into_part().is_none());
    }

    #[test]
    fn a_message_missing_a_field_does_not_parse() {
        let raw = format!(r#"{{"object_id":"{UUID}","version":1}}"#);
        assert!(serde_json::from_str::<LandedMessage>(&raw).is_err());
    }

    #[test]
    fn garbage_does_not_parse() {
        assert!(serde_json::from_str::<LandedMessage>("not json").is_err());
    }
}
