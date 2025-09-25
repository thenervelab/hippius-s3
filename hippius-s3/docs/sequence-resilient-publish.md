## Resilient Publish Adapter

```mermaid
sequenceDiagram
    autonumber
    participant Pinner
    participant Adapter as ResilientPublishAdapter
    participant SDK as Hippius SDK
    participant IPFS as IPFS Client

    Pinner->>Adapter: publish_manifest(bytes, file_name, should_encrypt, ctx)
    loop up to PUBLISH_MAX_RETRIES
        Adapter->>SDK: s3_publish(tmp_path, encrypt, seed, subaccount, bucket)
        alt success
            SDK-->>Adapter: {cid, tx_hash}
            Adapter-->>Pinner: {cid, path=sdk}
            break
        else failure
            Adapter->>Adapter: backoff (base..max with jitter)
        end
    end
    alt fallback enabled
        Adapter->>IPFS: upload_file(tmp_path, encrypt)
        IPFS-->>Adapter: {cid}
        Adapter->>IPFS: pin(cid)
        Adapter-->>Pinner: {cid, path=fallback}
    else error
        Adapter-->>Pinner: raise last error
    end
```
