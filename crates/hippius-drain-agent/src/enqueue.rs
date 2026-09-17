//! The drain-direct upload enqueuer: turn a part into per-backend `UploadChainRequest`
//! pushes onto the Redis upload queues. This is the agent-side concrete
//! [`UploadEnqueuer`] — `hippius-drain-core` stays Redis-free; only this module knows the
//! wire contract and talks to Redis.
//!
//! The drain is the SOLE producer of new upload requests (the api no longer enqueues at
//! PUT). [`drain_part`](hippius_drain_core::drain_part) calls this BEFORE committing
//! `mark_uploading`, so the enqueue is at-least-once: a crash before the commit leaves the
//! part `draining` → re-drained → re-enqueued (a harmless duplicate; the Python uploader is
//! idempotent via `chunk_backend ON CONFLICT`).
//!
//! # Two queues
//!
//! A part the drain hands over lives on THIS node's SSD, so its request goes to the
//! node-scoped queue `{backend}_upload_requests:<node_id>`, consumed by the uploader
//! `DaemonSet` pod on the same node (the only process that can read those bytes). The legacy
//! enqueue sweep re-publishes pool-era `replicated` rows whose bytes are in the shared pool,
//! not necessarily on any node's SSD; those go to the global `{backend}_upload_requests`
//! queue the pool-reading uploader Deployment drains. [`RedisEnqueuer::node_scoped`] vs
//! [`RedisEnqueuer::pool`] is the whole difference; the payload carries `node_id` so a
//! DLQ re-queue can route the request back to the right queue.

use std::sync::Arc;
use std::time::SystemTime;
use std::time::UNIX_EPOCH;

use hippius_drain_core::EnqueueOutcome;
use hippius_drain_core::PartKey;
use hippius_drain_core::Store;
use hippius_drain_core::StoreError;
use hippius_drain_core::UploadEnqueuer;
use redis::AsyncCommands;
use redis::aio::ConnectionManager;
use serde::Serialize;
use thiserror::Error;

/// A failure publishing a part's upload request.
#[derive(Debug, Error)]
pub enum EnqueueError {
    /// Reading the part's upload context (bucket / key / address / `upload_id`) failed.
    #[error("loading the upload context failed")]
    Store(#[source] StoreError),
    /// The Redis push failed.
    #[error("redis enqueue failed")]
    Redis(#[from] redis::RedisError),
    /// Serializing the request to JSON failed (should be unreachable).
    #[error("serializing the upload request failed")]
    Serialize(#[from] serde_json::Error),
}

/// One queued chunk — a part number. Mirrors `hippius_s3.queue.Chunk`.
#[derive(Serialize)]
struct Chunk {
    id: u32,
}

/// The backend upload request. Mirrors `hippius_s3.queue.UploadChainRequest` (+ the
/// `RetryableRequest` base). The Python uploader deserializes this with pydantic
/// (`extra="ignore"` + field defaults), so emitting the required fields plus the
/// stamped retry fields is sufficient.
///
/// KEEP IN SYNC with `hippius_s3/queue.py::UploadChainRequest` — the drain is the
/// producer and the Python workers are the consumers of this exact shape.
#[derive(Serialize)]
struct UploadChainRequest {
    address: String,
    bucket_name: String,
    object_key: String,
    object_id: String,
    object_version: u32,
    chunks: Vec<Chunk>,
    upload_id: Option<String>,
    upload_backends: Vec<String>,
    /// The node whose SSD holds the part — the queue the request was published to, and the
    /// queue a DLQ re-queue must route it back to. `None` for a pool-era request (the
    /// global queue; the bytes are in the shared pool).
    node_id: Option<String>,
    // RetryableRequest base. request_id is left null — the Python retry path stamps it
    // on first failure; first_enqueued_at + attempts mirror enqueue_upload_to_backends.
    request_id: Option<String>,
    attempts: u32,
    first_enqueued_at: f64,
    bypass_billing: bool,
}

/// The Redis list a backend's upload requests are pushed to: node-scoped when the bytes are
/// on one node's SSD, global when they are in the shared pool. Mirrors
/// `hippius_s3.queue.upload_queue_name`.
#[must_use]
pub fn upload_queue_name(backend: &str, node_id: Option<&str>) -> String {
    match node_id {
        Some(node) => format!("{backend}_upload_requests:{node}"),
        None => format!("{backend}_upload_requests"),
    }
}

/// Publishes a part's backend upload request to the configured Redis queues.
/// Cheap to clone (the `ConnectionManager` is a shared multiplexed handle).
#[derive(Clone)]
pub struct RedisEnqueuer {
    store: Arc<Store>,
    redis: ConnectionManager,
    backends: Arc<Vec<String>>,
    /// `Some(node)`: publish to that node's queue (the part is on its SSD); `None`: the global
    /// queue (the part is in the pool).
    node_id: Option<Arc<str>>,
}

// Manual Debug: redis's `ConnectionManager` is not `Debug`, so derive can't apply.
impl std::fmt::Debug for RedisEnqueuer {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RedisEnqueuer")
            .field("backends", &self.backends)
            .field("node_id", &self.node_id)
            .finish_non_exhaustive()
    }
}

impl RedisEnqueuer {
    /// An enqueuer for parts on THIS node's SSD: requests go to `{backend}_upload_requests:<node>`,
    /// which only the uploader pod on `node` consumes. This is what the drain hands parts to.
    #[must_use]
    pub fn node_scoped(store: Arc<Store>, redis: ConnectionManager, backends: Vec<String>, node_id: &str) -> Self {
        Self {
            store,
            redis,
            backends: Arc::new(backends),
            node_id: Some(Arc::from(node_id)),
        }
    }

    /// An enqueuer for pool-era parts: requests go to the global `{backend}_upload_requests`
    /// queue the pool-reading uploader drains. Used by the legacy enqueue sweep only.
    // TODO: delete with the pool (PR 2).
    #[must_use]
    pub fn pool(store: Arc<Store>, redis: ConnectionManager, backends: Vec<String>) -> Self {
        Self {
            store,
            redis,
            backends: Arc::new(backends),
            node_id: None,
        }
    }
}

impl UploadEnqueuer for RedisEnqueuer {
    type Error = EnqueueError;

    async fn ready(&self, part: &PartKey) -> Result<bool, EnqueueError> {
        self.store.upload_address_ready(part).await.map_err(EnqueueError::Store)
    }

    async fn enqueue(&self, part: &PartKey) -> Result<EnqueueOutcome, EnqueueError> {
        let Some(ctx) = self.store.load_upload_context(part).await.map_err(EnqueueError::Store)? else {
            // Not-ready is an EXPECTED, common outcome: an in-flight MPU part (address NULL
            // until CompleteMultipartUpload) hits this on every drain attempt until the address
            // lands, and the drain defers it. debug, not warn, so it does not spam; a genuinely
            // abandoned part is surfaced by the aged-orphan gauge + the reaper, not by this line.
            tracing::debug!(
                object_id = %part.object().as_str(),
                version = part.version().get(),
                part = part.part().get(),
                "upload context not ready (address NULL or version row absent); deferred",
            );
            return Ok(EnqueueOutcome::NotReady);
        };
        let first_enqueued_at = SystemTime::now().duration_since(UNIX_EPOCH).map_or(0.0, |d| d.as_secs_f64());
        let request = UploadChainRequest {
            address: ctx.address,
            bucket_name: ctx.bucket_name,
            object_key: ctx.object_key,
            object_id: ctx.object_id,
            object_version: ctx.object_version,
            chunks: vec![Chunk { id: ctx.part_number }],
            upload_id: ctx.upload_id,
            upload_backends: (*self.backends).clone(),
            node_id: self.node_id.as_deref().map(str::to_owned),
            request_id: None,
            attempts: 0,
            first_enqueued_at,
            bypass_billing: false,
        };
        let payload = serde_json::to_string(&request)?;

        // Fan out to each backend's queue, mirroring enqueue_upload_to_backends. A clone
        // of the multiplexed manager is cheap; lpush is one round-trip per backend.
        let mut conn = self.redis.clone();
        for backend in self.backends.iter() {
            let queue = upload_queue_name(backend, self.node_id.as_deref());
            let _: () = conn.lpush(&queue, &payload).await?;
        }
        tracing::debug!(
            object_id = %request.object_id,
            version = request.object_version,
            part = request.chunks.first().map_or(0, |c| c.id),
            backends = ?self.backends,
            node_id = ?self.node_id,
            "published backend upload for part",
        );
        Ok(EnqueueOutcome::Published)
    }
}

#[cfg(test)]
#[expect(clippy::unwrap_used, reason = "tests")]
mod tests {
    use super::Chunk;
    use super::UploadChainRequest;
    use super::upload_queue_name;

    // The cross-language wire contract is pinned by a single golden fixture that BOTH
    // sides assert against: this test checks the Rust producer serializes to it, and
    // tests/unit/test_upload_chain_request_wire.py checks the Python consumer
    // (`hippius_s3.queue.UploadChainRequest.model_validate`) accepts the same bytes. If
    // either struct drifts, one of the two tests fails — the coupling the `KEEP IN SYNC`
    // comment on `UploadChainRequest` could previously only assert by inspection.
    const GOLDEN: &str = include_str!(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/../../tests/fixtures/upload_chain_request.golden.json"
    ));

    #[test]
    fn upload_chain_request_serializes_to_the_golden_wire_shape() {
        // Fixed field values (not SystemTime::now()) so the wire shape is deterministic.
        let request = UploadChainRequest {
            address: "5GrwvaEF5zXb26Fz9rcQpDWS57CtERHpNehXCPcNoHGKutQY".to_owned(),
            bucket_name: "my-bucket".to_owned(),
            object_key: "path/to/key.bin".to_owned(),
            object_id: "466916c0-d61b-4518-b81b-9576b574270a".to_owned(),
            object_version: 5,
            chunks: vec![Chunk { id: 1 }],
            upload_id: Some("11111111-1111-4111-8111-111111111111".to_owned()),
            upload_backends: crate::config::STORAGE_BACKENDS.iter().map(|b| (*b).to_owned()).collect(),
            node_id: Some("ingest-node-1".to_owned()),
            request_id: None,
            attempts: 0,
            first_enqueued_at: 0.0,
            bypass_billing: false,
        };
        // Compare as parsed JSON values so key order / whitespace are irrelevant.
        let produced: serde_json::Value = serde_json::from_str(&serde_json::to_string(&request).unwrap()).unwrap();
        let golden: serde_json::Value = serde_json::from_str(GOLDEN).unwrap();
        assert_eq!(produced, golden, "the Rust UploadChainRequest wire shape drifted from the golden fixture");
    }

    #[test]
    fn the_queue_name_is_node_scoped_only_when_a_node_is_given() {
        // Mirrors hippius_s3.queue.upload_queue_name: the uploader DaemonSet pod on a node
        // BRPOPs exactly `arion_upload_requests:<its node>`, the pool-reading Deployment the
        // bare name. A drift here strands every request on a queue nobody reads.
        assert_eq!(upload_queue_name("arion", Some("ingest-node-1")), "arion_upload_requests:ingest-node-1");
        assert_eq!(upload_queue_name("arion", None), "arion_upload_requests");
    }
}
