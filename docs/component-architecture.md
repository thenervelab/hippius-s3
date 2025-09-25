## Component Architecture

```mermaid
flowchart LR
    API[FastAPI S3 API]

    subgraph Workers
        Pinner
        ChainRetry[Substrate Submit Wrapper]
    end

    Redis[(Redis)]
    DLQFS[(DLQ Filesystem)]
    DB[(Postgres)]
    IPFS[Hippius SDK / IPFS]
    Substrate[Substrate Node]

    API -->|upsert object/parts| DB
    API -->|write part bytes and enqueue | Redis
    Pinner -->|dequeue| Redis
    Pinner -->|upload/pin parts| IPFS
    Pinner -->|manifest publish via adapter| IPFS
    Pinner -->|update status and manifest CID| DB
    Pinner -->|DLQ persist| DLQFS
    ChainRetry -->|submit storage_request| Substrate
    DLQFS -.->|hydrate requeue| Redis
```
