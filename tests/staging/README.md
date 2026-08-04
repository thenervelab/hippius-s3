# tests/staging/ — live drain e2e

Acceptance tests for the SSD→CephFS drain (`hippius-drain`) against the **real
staging gateway** (`https://s3-staging.hippius.com`) with real credentials — no
docker-compose, no mocks. They assert the drain *outcome* an S3 client can observe:
a PUT completes and the object becomes durably, cross-node readable.

## The drain is live on staging (opt-in suite)

The drain is now **live and the sole producer** of backend upload requests on staging:
the api self-enqueue at PUT/MPU-complete has been removed, the drain-agent replicates
each part SSD→CephFS and enqueues the `UploadChainRequest` itself. The earlier blocker
below is **resolved**.

The suite is still **opt-in** via `HIPPIUS_DRAIN_LIVE=1` — not because the drain is a
no-op, but because these tests hit the **real staging gateway with real credentials**
and are not part of the default CI run. Set it (plus AWS creds) to run them; they now
assert a genuine drain outcome, not a known-broken state.

### Resolved blocker (historical)

The drain originally spoke the **cephor** on-disk contract — `<root>/<file_id>/<chunk_key>`
(2 levels, `sha256(bytes) == chunk_key`, no `meta.json`) — while the api writes a
4-level, AES-GCM layout:

```
<object_id>/v<version>/part_<n>/{meta.json, chunk_<i>.bin}   # 4 levels, AES-GCM ciphertext
```

The drain's `LocalSsd::scan` then walked only two levels and discovered **zero** chunks
(a safe no-op; the 502 persisted). That contract mismatch has since been reconciled — the
api-local layout + part-key derivation is what the drain reads today, so PUTs complete the
drain end-to-end.

## Running the suite

```bash
export AWS_ACCESS_KEY_ID=hip_...
export AWS_SECRET_ACCESS_KEY=...
export AWS_DEFAULT_REGION=decentralized
export HIPPIUS_DRAIN_LIVE=1

# TLS: staging's gateway cert SAN is us-east-1.hippius.com, not the gateway host.
# Prefer pointing at the staging CA; the insecure flag is the runnable-today fallback.
export HIPPIUS_S3_CA_BUNDLE=/path/to/staging-ca.pem   # preferred
# export HIPPIUS_S3_INSECURE=1                          # fallback (disables verification)

pytest tests/staging -v
```

### Optional Postgres-level checks

`test_drain_state.py` is additionally gated on `CEPHOR_DATABASE_URL` (or
`DATABASE_URL`). With it set, it asserts `cephor_replication_status` rows reach
`replicated` after uploads.

```bash
export CEPHOR_DATABASE_URL=postgres://.../objectstore
pytest tests/staging/test_drain_state.py -v
```

## Env

| Variable | Default | Notes |
|---|---|---|
| `HIPPIUS_DRAIN_LIVE` | unset | `1` enables the suite. |
| `HIPPIUS_S3_ENDPOINT` | `https://s3-staging.hippius.com` | Gateway URL. |
| `AWS_ACCESS_KEY_ID` / `AWS_SECRET_ACCESS_KEY` | — | Required (`hip_` access key). |
| `AWS_DEFAULT_REGION` | `decentralized` | SigV4 signing region. |
| `HIPPIUS_S3_CA_BUNDLE` | — | CA bundle for TLS verification (preferred). |
| `HIPPIUS_S3_INSECURE` | unset | `1` disables TLS verification (staging cert mismatch fallback). |
| `CEPHOR_DATABASE_URL` | — | Enables the optional Postgres-level checks. |
