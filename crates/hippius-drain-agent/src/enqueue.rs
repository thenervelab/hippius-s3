//! The drain-direct upload enqueuer: once a part is durably on the pool, turn it into
//! per-backend `UploadChainRequest` pushes onto the Redis upload queues. This is the
//! agent-side concrete [`UploadEnqueuer`] — `hippius-drain-core` stays Redis-free; only
//! this module knows the wire contract and talks to Redis.
//!
//! The drain is now the SOLE producer of new upload requests (the api no longer enqueues
//! at PUT). [`drain_part`](hippius_drain_core::drain_part) calls this BEFORE committing
//! `mark_replicated`, so the enqueue is at-least-once: a crash before the commit leaves
//! the part `draining` → re-drained → re-enqueued (a harmless duplicate; the Python
//! uploader is idempotent via `skip_if_exists` + `chunk_backend ON CONFLICT`).

use std::sync::Arc;
use std::time::SystemTime;
use std::time::UNIX_EPOCH;

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
    /// The version row has no `address` yet — the api writes it at PUT/MPU-complete, so
    /// this is a rare race where the part landed on SSD before the api finished. The
    /// part is left `draining` and a later re-drain retries (the address will be there).
    #[error("upload context not ready (object_versions.address is NULL); will retry")]
    NotReady,
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
/// KEEP IN SYNC with `hippius_s3/queue.py::UploadChainRequest` — the drain is now the
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
    // RetryableRequest base. request_id is left null — the Python retry path stamps it
    // on first failure; first_enqueued_at + attempts mirror enqueue_upload_to_backends.
    request_id: Option<String>,
    attempts: u32,
    first_enqueued_at: f64,
    bypass_billing: bool,
}

/// Publishes a replicated part's backend upload request to the configured Redis queues.
/// Cheap to clone (the `ConnectionManager` is a shared multiplexed handle).
#[derive(Clone)]
pub struct RedisEnqueuer {
    store: Arc<Store>,
    redis: ConnectionManager,
    backends: Arc<Vec<String>>,
}

// Manual Debug: redis's `ConnectionManager` is not `Debug`, so derive can't apply.
impl std::fmt::Debug for RedisEnqueuer {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RedisEnqueuer").field("backends", &self.backends).finish_non_exhaustive()
    }
}

impl RedisEnqueuer {
    /// Builds an enqueuer over the shared store, a Redis connection, and the backend
    /// list (`HIPPIUS_UPLOAD_BACKENDS`).
    #[must_use]
    pub fn new(store: Arc<Store>, redis: ConnectionManager, backends: Vec<String>) -> Self {
        Self {
            store,
            redis,
            backends: Arc::new(backends),
        }
    }
}

impl UploadEnqueuer for RedisEnqueuer {
    type Error = EnqueueError;

    async fn enqueue(&self, part: &PartKey) -> Result<(), EnqueueError> {
        let Some(ctx) = self.store.load_upload_context(part).await.map_err(EnqueueError::Store)? else {
            return Err(EnqueueError::NotReady);
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
            let queue = format!("{backend}_upload_requests");
            let _: () = conn.lpush(&queue, &payload).await?;
        }
        tracing::debug!(
            object_id = %request.object_id,
            version = request.object_version,
            part = request.chunks.first().map_or(0, |c| c.id),
            backends = ?self.backends,
            "enqueued backend upload for replicated part",
        );
        Ok(())
    }
}
