# Workers

Background workers that process async operations via Redis queues. Each worker runs as a separate container with the `WORKER_SCRIPT` environment variable selecting which entry point to execute.

## Worker Types

| Worker | Entry Point | Queue | Scaling | Purpose |
|--------|------------|-------|---------|---------|
| Arion Uploader | `run_arion_uploader_in_loop.py` | `arion_upload_requests` | Single instance | Upload chunks to Arion backend |
| Arion Downloader | `run_arion_downloader_in_loop.py` | `arion_download_requests` | Horizontal | Download chunks from Arion |
| Arion Unpinner | `run_arion_unpinner_in_loop.py` | `arion_unpin_requests` | Single instance | Delete chunks from Arion |
| Janitor | `run_janitor_in_loop.py` | N/A (polling) | Single instance | FS cache cleanup at `/var/lib/hippius/object_cache` |
| Account Cacher | `run_account_cacher_in_loop.py` | N/A (scheduled) | Single instance | Warm account credit cache in Redis |
| Orphan Checker | `run_orphan_checker_in_loop.py` | N/A (scheduled) | Single instance | Detect blockchain orphan files, enqueue cleanup |
| Migrator | `run_migrator_once.py` | N/A (one-shot) | One-shot | Run DB migrations on startup, then exit |

### Scaling Notes

The uploader and unpinner must run as single instances to avoid exceeding Hippius blockchain rate limits (they create substrate transactions). The downloader only reads from Arion and Redis, so it can be safely replicated for higher throughput.

## Data Flow

**Upload path**: Client write → API write pipeline → chunks to Redis queue + FS cache → Arion uploader dequeues → uploads to Arion → publishes to Hippius blockchain

**Download path**: Client read → API read pipeline → check FS cache → cache miss enqueues to Redis → Arion downloader fetches from Arion → caches locally → streams to client

**Delete path**: Client delete → API marks `deleted=true` in `chunk_backend` table → enqueues unpin request → Arion unpinner removes from Arion backend

## Error Handling

Workers use a retry strategy with exponential backoff:

- **Transient failures** (network timeouts, 5xx responses): Retry with backoff up to `HIPPIUS_UPLOADER_MAX_ATTEMPTS` (default: 2) or `HIPPIUS_UNPINNER_MAX_ATTEMPTS` (default: 5)
- **Permanent failures** (4xx responses, data corruption): Move to Dead Letter Queue (DLQ)
- **DLQ persistence**: Failed operations saved to filesystem at `HIPPIUS_DLQ_DIR` (default: `/tmp/hippius_dlq`)

Requeue DLQ entries with:
```bash
python -m hippius_s3.scripts.dlq_requeue
```

## Configuration

Key environment variables for workers:

| Variable | Default | Description |
|----------|---------|-------------|
| `HIPPIUS_UPLOADER_MAX_ATTEMPTS` | `2` | Max retry attempts for uploads |
| `HIPPIUS_UPLOADER_BACKOFF_BASE_MS` | `100` | Base backoff delay (ms) |
| `HIPPIUS_UPLOADER_BACKOFF_MAX_MS` | `500` | Max backoff delay (ms) |
| `HIPPIUS_UNPINNER_MAX_ATTEMPTS` | `5` | Max retry attempts for unpins |
| `HIPPIUS_UNPINNER_BACKOFF_BASE_MS` | `1000` | Base backoff delay (ms) |
| `HIPPIUS_UNPINNER_BACKOFF_MAX_MS` | `60000` | Max backoff delay (ms) |
| `ORPHAN_CHECKER_LOOP_SLEEP` | `7200` | Orphan checker interval (seconds) |
| `ORPHAN_CHECKER_BATCH_SIZE` | `100` | Orphan checker batch size |

## Docker

Workers are launched via `start-worker.sh`, which:

1. Reads `WORKER_SCRIPT` env var to determine which `run_*.py` to execute
2. Configures OpenTelemetry exporters if `ENABLE_MONITORING=true`
3. Optionally enables hot-reload via `watchfiles` if `ENABLE_WATCHFILES=true`

In `docker-compose.yml`, each worker is a separate service:

```yaml
arion-uploader:
  environment:
    - WORKER_SCRIPT=workers/run_arion_uploader_in_loop.py
```
