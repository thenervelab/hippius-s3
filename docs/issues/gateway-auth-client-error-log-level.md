# Gateway auth: client-driven rejections logged at ERROR

## Summary

The gateway logs client-driven authentication rejections (malformed credentials,
bad signatures, missing/invalid signing headers, invalid presigned-URL query
params) inconsistently — many at `logger.error`. These are expected 4xx client
rejections, not server faults, yet they surface on ERROR-level logs and
dashboards alongside genuine failures. This change normalizes those specific
sites to `logger.warning`, matching the convention already applied elsewhere in
the same files (e.g. invalid access-key format, signature mismatch, invalid/
inactive key were already at WARNING).

## Impact

- Pure observability noise: ERROR dashboards / alerts are polluted by ordinary
  bad-client traffic.
- No data-path impact. Response codes, control flow, and message text are all
  unchanged — only the log level of the rejection lines moves ERROR -> WARNING.
- `server_errors=0` for these cases: they are 4xx rejections, not 5xx.

## Root cause

Inconsistent ERROR-vs-WARNING policy for client-driven auth rejections across
`gateway/services/auth_orchestrator.py`, `gateway/middlewares/access_key_auth.py`,
and `gateway/middlewares/sigv4.py`. Some rejections were already WARNING; the
sites below were left at ERROR.

Historical note: the specific "Bad seed phrase format / non-ASCII bytes" ERROR
lines were already removed in commit `899867a` (seed-phrase auth deprecation),
but the *class* of problem — client-driven rejections logged at ERROR — persisted
at the sites listed under Fix.

## Fix

Downgraded `logger.error` -> `logger.warning` (message text unchanged) at the
following unambiguously client-driven sites:

`gateway/services/auth_orchestrator.py`
- "Failed to extract credential" — `AuthParsingError` from a malformed client
  `Authorization` header.

`gateway/middlewares/access_key_auth.py` (`verify_access_key_signature`)
- "Missing required auth headers for access key verification" — client omitted
  `Authorization` / `x-amz-date`.

`gateway/middlewares/access_key_auth.py` (`verify_access_key_presigned_url`)
- "Missing required X-Amz-* query parameters for presigned URL verification"
- "Unsupported X-Amz-Algorithm in presigned URL"
- "Invalid X-Amz-Credential format"
- "Presigned URL credential ID mismatch"
- "X-Amz-Date ... and credential scope date ... mismatch in presigned URL"
- "Invalid X-Amz-Expires value"
- "X-Amz-Expires out of allowed range (1-604800)"
- "Invalid X-Amz-Date format"
- "Presigned URL missing required 'host' header in X-Amz-SignedHeaders"

`gateway/middlewares/sigv4.py`
- "FAIL: Missing x-amz-content-sha256 header" — client omitted the payload-hash
  header on a non-presigned request.

## Deliberately left at ERROR (not client-driven)

- `sigv4.py` `canonical_path_from_scope`: "ASGI scope missing 'raw_path'" —
  internal contract violation (raises `RuntimeError`), not client input.
- `auth_orchestrator.py`: "Hippius API error during presigned URL auth" /
  "Hippius API error during Bearer auth" / "Hippius API error during auth" —
  upstream `HippiusAPIError`, returns 503.
- `auth_orchestrator.py` / `access_key_auth.py`: "API returned empty
  account_address" and "API returned valid token without crypto material" —
  server-side / upstream data integrity problems.
- `access_key_auth.py`: "Invalid account address format" and "Invalid token
  type" (both in the header and presigned paths). **These were on the suggested
  downgrade list but were intentionally left at ERROR:** the offending value
  (`token_response.account_address` / `token_response.token_type`) comes from the
  upstream auth API (`cached_auth`), not from client input. A malformed SS58
  address or an unexpected token type from the API is an upstream data-integrity
  anomaly in the same class as the "empty account_address" line that must stay
  ERROR — a client cannot drive it.

## Testing

`tests/unit/gateway/test_auth_log_levels.py` — adversarial, failure-path only,
asserts on `record.levelname` explicitly:

1. Malformed client credential (`AuthParsingError` path via
   `authenticate_request`) logs WARNING, not ERROR.
2. Invalid presigned-URL credential format logs WARNING, not ERROR.
3. Presigned-URL signature mismatch logs WARNING, not ERROR.
4. Genuine upstream failure (`HippiusAPIError` from
   `verify_access_key_signature`) STILL logs ERROR — proves no over-downgrade.
5. Missing `x-amz-content-sha256` payload-hash header logs WARNING, not ERROR.
