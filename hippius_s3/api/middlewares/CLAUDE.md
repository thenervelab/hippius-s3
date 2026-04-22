# hippius_s3/api/middlewares/

API-side middleware. All of these assume `X-Hippius-*` headers are trustworthy — the gateway is responsible for setting/stripping them.

Registration order at [hippius_s3/main.py:293-299](../../main.py): `metrics → tracing → parse_internal_headers → ip_whitelist → fs_cache_pressure`. Reverse order = outer-most → inner-most on the request path.

## [parse_internal_headers.py](parse_internal_headers.py) — `parse_internal_headers_middleware`

Translates `X-Hippius-*` request headers into `request.state` so downstream endpoints don't each re-parse them. Populated ([parse_internal_headers.py:19-68](parse_internal_headers.py)):

- `request.state.ray_id` ← `X-Hippius-Ray-ID`
- `request.state.request_user_id`, `.account_id` ← `X-Hippius-Request-User`
- `request.state.bucket_owner_id` ← `X-Hippius-Bucket-Owner`
- `request.state.seed_phrase` ← `X-Hippius-Seed` (only set on seed-phrase auth)
- `request.state.account` ← `HippiusAccount(main_account, has_credits, upload, delete, id)` built from the header bundle. `has_credits`/`upload`/`delete` are parsed from `"True"`/`"False"` strings.
- `request.state.gateway_time_ms` ← `X-Hippius-Gateway-Time-Ms` as a float.

If the gateway didn't send these (e.g. a unit test calls the API directly), the middleware populates safe defaults.

## [fs_cache_pressure.py](fs_cache_pressure.py) — `fs_cache_pressure_middleware`

Gates PUT writes when the FS cache disk is under pressure. Flow ([fs_cache_pressure.py:17-45](fs_cache_pressure.py)):

1. If method != PUT, pass through.
2. If path has <2 segments (not a bucket/key write), pass through.
3. If neither `Transfer-Encoding: chunked` nor `Content-Length>0`, pass through.
4. Otherwise, `should_reject_fs_cache_write(config)` checks free/total ratio via `shutil.disk_usage(config.object_cache_dir)`.
5. If over threshold, return **503 SlowDown + Retry-After** **BEFORE consuming the request body**. This is the point — we decide to 503 without ever reading the upload off the wire, saving RAM and disk.

Threshold is configurable (see [hippius_s3/config.py](../../config.py)); today it's ~90%.

## [ip_whitelist.py](ip_whitelist.py) — `ip_whitelist_middleware`

Defence-in-depth: if `API_IP_WHITELIST` is set (comma-separated CIDR or IPs), the API rejects anything else with 403. Normally k8s NetworkPolicy provides the primary barrier — this is belt-and-braces for when the policy is misconfigured or the API is port-forwarded during debug.

## [metrics.py](metrics.py) — `metrics_middleware`

OTel counters for `http_requests_total`, duration histogram, account-attributed error counts. Paired with `recorded_gateway_bandwidth` from the gateway's forward service.

## [tracing.py](tracing.py) — `tracing_middleware`

OTel span lifecycle per request. Attaches standard span attributes (`hippius.ray_id`, `hippius.account.main`, method, path). Error spans carry stack traces.

Helper: `set_span_attributes(span, {...})` from [hippius_s3/api/middlewares/tracing.py](tracing.py) is imported across endpoints for ad-hoc attribute sets.

## [input_validation.py](input_validation.py) — if present

Content validation (not to be confused with gateway's [input_validation.py](../../../gateway/middlewares/input_validation.py) which is about bucket/key naming). Bounds object size at [config.max_object_size](../../config.py).

## [profiler.py](profiler.py) — `SpeedscopeProfilerMiddleware`

Gated by `ENABLE_REQUEST_PROFILING`. When on, captures a flame-graph-style profile per request and emits it to the speedscope format. Off by default.

## Adding a new middleware

Register it in [hippius_s3/main.py:293-299](../../main.py) with `app.middleware("http")(my_middleware)`. Remember that registration order is **reversed** on the request path — put the outermost (runs first) last in registration order.
