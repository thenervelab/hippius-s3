## Object Lifecycle State Machine

```mermaid
stateDiagram-v2
    [*] --> publishing
    publishing --> pinning: chunks enqueued to backend workers
    pinning --> uploaded: all chunks stored in backends
    pinning --> failed: permanent error or max attempts → DLQ
    failed --> pinning: CLI requeue (resubmit_failed_pins.py)
    uploaded --> [*]
```

Valid states (enforced by DB constraint): `publishing`, `pinning`, `uploaded`, `failed`.

- **publishing** — default state on object creation; object metadata written to DB, chunks queued in Redis
- **pinning** — backend uploader workers are processing chunks (Arion)
- **uploaded** — all chunks confirmed stored; object is fully available
- **failed** — permanent error after max retries; chunks persisted to DLQ for manual requeue
