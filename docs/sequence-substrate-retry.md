## Hippius API Requests with Retries

The system interacts with the Hippius blockchain via the Hippius HTTP API (`hippius_api_service.py`),
not via direct Substrate node connections. All chain-related operations (pinning, account checks,
file status) go through this API layer with automatic retries.

```mermaid
sequenceDiagram
    autonumber
    participant Worker as Uploader Worker
    participant API as HippiusApiClient
    participant Hippius as Hippius API (api.hippius.com)

    Worker->>API: pin_file(cid, file_name, account_ss58)
    loop up to 3 retries (5s backoff)
        API->>Hippius: POST /pin/ (authenticated)
        alt success (2xx)
            Hippius-->>API: PinResponse (id, cid, status)
            API-->>Worker: PinResponse
        else 4xx/5xx error
            API->>API: backoff and retry
        end
    end
    alt exhausted
        API-->>Worker: raise HippiusAPIError
    end
```
