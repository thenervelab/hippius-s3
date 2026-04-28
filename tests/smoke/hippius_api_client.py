"""Self-contained client for api.hippius.com — used by sub-token smoke tests.

Lives in tests/smoke/ (not hippius_s3/services/) so the smoke CI can install
the lightweight requirements.txt and run the tests without pulling fastapi,
asyncpg, dotenv, and the rest of the project's runtime stack.

User-scoped (DRF Token auth), distinct from the validator-scoped
HippiusApiClient that lives in hippius_s3/services/hippius_api_service.py.

Endpoints exercised here:
- POST /objectstore/sub-tokens/             — mint a sub-token
- POST /objectstore/sub-tokens/{id}/revoke/ — revoke
- GET  /objectstore/sub-tokens/             — list (used for orphan sweep)

Auth: `Authorization: Token <DRF_TOKEN>` (per the OpenAPI spec at
https://api.hippius.com/?format=openapi).
"""

from __future__ import annotations

from dataclasses import dataclass
from dataclasses import field

import httpx


HIPPIUS_ACCOUNT_API_URL = "https://api.hippius.com/api/"


@dataclass
class SubTokenCreateResponse:
    # Real API uses camelCase on CREATE responses (accessKeyId, createdAt) and
    # snake_case on LIST responses (access_key_id, created_at). We normalise
    # both into snake_case fields here so callers see one shape.
    token_id: str
    name: str
    access_key_id: str
    secret: str
    created_at: str


@dataclass
class SubTokenListItem:
    token_id: str
    name: str
    access_key_id: str
    created_at: str
    status: str = ""
    ip_allowlist: list[str] = field(default_factory=list)


class HippiusUserApiClient:
    def __init__(self, *, user_token: str, base_url: str = HIPPIUS_ACCOUNT_API_URL) -> None:
        self.base_url = base_url
        self._token = user_token
        self._client = httpx.AsyncClient(
            base_url=base_url,
            timeout=httpx.Timeout(60.0, connect=10.0),
            follow_redirects=True,
        )

    async def __aenter__(self) -> "HippiusUserApiClient":
        return self

    async def __aexit__(self, *exc: object) -> None:
        await self.close()

    async def close(self) -> None:
        await self._client.aclose()

    def _headers(self) -> dict[str, str]:
        return {
            "Authorization": f"Token {self._token}",
            "Accept": "application/json",
            "Content-Type": "application/json",
        }

    async def create_sub_token(self, *, name: str) -> SubTokenCreateResponse:
        # `bucket_grants` is required by the OpenAPI schema but its semantics
        # are superseded by the gateway-side sub_token_scopes table.
        payload: dict[str, object] = {"name": name, "bucket_grants": [], "ip_allowlist": []}
        r = await self._client.post("objectstore/sub-tokens/", json=payload, headers=self._headers())
        r.raise_for_status()
        body = r.json()
        # CREATE returns camelCase (accessKeyId, createdAt); LIST returns snake_case.
        return SubTokenCreateResponse(
            token_id=body["id"],
            name=body["name"],
            access_key_id=body.get("accessKeyId") or body["access_key_id"],
            secret=body["secret"],
            created_at=body.get("createdAt") or body["created_at"],
        )

    async def revoke_sub_token(self, token_id: str) -> None:
        r = await self._client.post(
            f"objectstore/sub-tokens/{token_id}/revoke/",
            headers=self._headers(),
        )
        r.raise_for_status()

    async def list_sub_tokens(self) -> list[SubTokenListItem]:
        r = await self._client.get("objectstore/sub-tokens/", headers=self._headers())
        r.raise_for_status()
        return [
            SubTokenListItem(
                token_id=item["id"],
                name=item["name"],
                access_key_id=item["access_key_id"],
                created_at=item["created_at"],
                status=item.get("status", ""),
                ip_allowlist=item.get("ip_allowlist", []) or [],
            )
            for item in r.json()
        ]
