"""
Hippius API Client for interacting with the Hippius API.

This module provides an HTTP-based client that replaces direct blockchain
interactions with API calls authenticated via HIPPIUS_KEY.

API Documentation: https://api.hippius.com/?format=openapi
"""

import asyncio
import functools
import io
import logging
from typing import Any
from typing import Callable
from typing import Coroutine
from typing import Dict
from typing import TypeVar
from urllib.parse import urlparse
from urllib.parse import urlunparse

import httpx
from pydantic import BaseModel
from pydantic import ConfigDict

from hippius_s3.config import get_config


logger = logging.getLogger(__name__)

T = TypeVar("T")


class PinResponse(BaseModel):
    id: str
    request_id: str
    user: int
    account_ss58: str
    cid: str
    file_id: str
    original_name: str | None = None
    request_type: str
    status: str
    posted_by_vali: bool
    last_error: str | None = None
    published_at: str | None = None
    completed_at: str | None = None
    created_at: str | None = None
    updated_at: str | None = None


class UnpinResponse(BaseModel):
    id: str
    request_id: str
    user: int
    account_ss58: str
    cid: str
    file_id: str
    original_name: str | None = None
    request_type: str
    status: str
    posted_by_vali: bool
    last_error: str | None = None
    published_at: str | None = None
    completed_at: str | None = None
    created_at: str | None = None
    updated_at: str | None = None


class TokenAuthResponse(BaseModel):
    # api.hippius.com returns one of two shapes from POST /objectstore/tokens/auth/:
    #   - valid key  → {valid: true, status, account_address, token_type, encrypted_secret, nonce}
    #   - unknown    → {valid: false, detail: "unknown accessKeyId"}
    # All except `valid` are optional so the error shape parses cleanly; callers
    # gate on `valid` before consuming the rest.
    valid: bool
    status: str | None = None
    account_address: str | None = None
    token_type: str | None = None
    encrypted_secret: str | None = None
    nonce: str | None = None
    detail: str | None = None


class UploadResponse(BaseModel):
    id: str
    original_name: str
    content_type: str
    size_bytes: int
    sha256_hex: str
    cid: str
    status: str
    file_url: str
    created_at: str
    updated_at: str


class FileStatusResponse(BaseModel):
    id: str
    original_name: str
    content_type: str
    size_bytes: int
    sha256_hex: str
    cid: str
    status: str
    file_url: str
    created_at: str
    updated_at: str


class FileItem(BaseModel):
    file_id: str
    cid: str
    original_name: str
    size_bytes: int
    status: str
    pinned_node_ids: list[str]
    active_replica_count: int
    miners: Any
    updated_at: str
    created_at: str


class ListFilesResponse(BaseModel):
    count: int
    next: str | None
    previous: str | None
    results: list[FileItem]


# ---------------------------------------------------------------------------
# S3 billing plans. GET /api/s3/plans/accounts/?page=1&page_size=500
#
# ONE endpoint carries both halves: `plans` is the catalog (plan name -> allowance) and `results` is
# the per-account roll, paginated. So there is one scrape loop, not two.
#
# `extra="ignore"` throughout and every field but the identifier optional-with-default: a payload
# that grows a field must not crash the plans-cacher and strand the fleet on last-known-good.
#
# The wire -> internal translation lives in _parse_page() in workers/run_plans_cacher_in_loop.py.
# ---------------------------------------------------------------------------


class S3PlanCatalogEntry(BaseModel):
    model_config = ConfigDict(extra="ignore")

    # Opaque on-chain plan hash. Carried through for observability; nothing keys off it.
    h256: str | None = None
    # None means "this plan's allowance is unknown". NEVER read as "zero bytes allowed" -- see
    # PlanQuota.enforceable in hippius_s3/services/plans_cache.py.
    storage_bytes: int | None = None


class S3PlanAccountRow(BaseModel):
    model_config = ConfigDict(extra="ignore")

    ss58: str
    # "plan" | "pay_as_you_go". Anything that is not exactly "plan" is treated as pay-as-you-go.
    billing: str | None = None
    plan: str | None = None
    # A lapsed/cancelled subscription still appears with billing="plan" and its plan name, but
    # active=false. Defaulting to False is the safe direction: an unparseable row does not hand out
    # an allowance. See _is_enforceable_plan_row.
    active: bool = False
    # This account's CURRENT TOTAL S3 USAGE, computed on chain -- NOT its allowance. The allowance
    # is plans.<name>.storage_bytes. The two fields share a name and mean opposite things; reading
    # this one as the limit would give every account a quota equal to what it already stores.
    storage_bytes: int | None = None
    next_charge: str | None = None
    subscription_id: int | None = None


class S3PlanAccountsResponse(BaseModel):
    model_config = ConfigDict(extra="ignore")

    generated_at: str | None = None
    count: int | None = None
    # An ABSOLUTE url (e.g. https://api.hippius.com/api/s3/plans/accounts/?page=2&page_size=500).
    # Only its path+query is followed -- see get_s3_plan_accounts.
    next: str | None = None
    previous: str | None = None
    plans: dict[str, S3PlanCatalogEntry] = {}
    results: list[S3PlanAccountRow] = []


class HippiusAPIError(Exception):
    """Raised when there's an authentication issue with the API."""

    pass


class HippiusAuthenticationError(HippiusAPIError):
    """Raised when there's an authentication issue with the API."""

    pass


def retry_on_error(
    retries: int = 3, backoff: float = 5.0
) -> Callable[[Callable[..., Coroutine[Any, Any, T]]], Callable[..., Coroutine[Any, Any, T]]]:
    """
    Decorator to retry HTTP requests on 4xx/5xx errors.

    Args:
        retries: Number of retry attempts (default: 3)
        backoff: Seconds to wait between retries (default: 5.0)
    """

    def decorator(func: Callable[..., Coroutine[Any, Any, T]]) -> Callable[..., Coroutine[Any, Any, T]]:
        @functools.wraps(func)
        async def wrapper(*args: Any, **kwargs: Any) -> T:
            last_exception: Exception | None = None

            for attempt in range(retries + 1):
                try:
                    return await func(*args, **kwargs)
                except (httpx.HTTPStatusError, HippiusAPIError) as e:
                    last_exception = e

                    # Don't retry on authentication errors (401, 403)
                    if hasattr(e, "response") and e.response.status_code in [401, 403]:  # ty: ignore[unresolved-attribute]
                        raise HippiusAuthenticationError(f"Authentication failed: {e}") from None

                    # Don't retry on 404 Not Found - resource doesn't exist
                    if hasattr(e, "response") and e.response.status_code == 404:  # ty: ignore[unresolved-attribute]
                        raise

                    # Don't retry if this was the last attempt
                    if attempt == retries:
                        break

                    # Log retry attempt with response body and function arguments
                    func_name = func.__name__  # ty: ignore[unresolved-attribute]
                    args_repr = f"args={args}" if args else ""

                    # Filter out large binary data from kwargs to avoid log spam
                    filtered_kwargs = {}
                    for k, v in kwargs.items():
                        if k == "file_data" and isinstance(v, bytes):
                            filtered_kwargs[k] = f"<{len(v)} bytes>"
                        else:
                            filtered_kwargs[k] = v

                    kwargs_repr = f"kwargs={filtered_kwargs}" if kwargs else ""
                    args_str = ", ".join(filter(None, [args_repr, kwargs_repr]))

                    error_msg = f"Request failed (attempt {attempt + 1}/{retries + 1}): {e}"
                    if args_str:
                        error_msg += f" | Function: {func_name}({args_str})"
                    if hasattr(e, "response"):
                        error_msg += f" | Response body: {e.response.text}"  # ty: ignore[unresolved-attribute]
                    logger.error(error_msg)
                    await asyncio.sleep(backoff)
                except Exception:
                    # Don't retry on unexpected errors
                    raise

            # If we get here, all retries failed
            if last_exception is not None:
                raise last_exception
            raise HippiusAPIError("All retries failed with no exception captured")

        return wrapper

    return decorator


class HippiusApiClient:
    """
    HTTP API client for Hippius API.
    """

    def __init__(
        self,
    ) -> None:
        """
        Initialize the Hippius API client.
        """
        self._config = get_config()
        self.api_url = self._config.hippius_api_base_url
        self._client = httpx.AsyncClient(
            base_url=self.api_url,
            timeout=httpx.Timeout(
                60.0,
                connect=10.0,
            ),
            follow_redirects=True,
        )

    async def __aenter__(self) -> "HippiusApiClient":
        """Async context manager entry."""
        return self

    async def __aexit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        """Async context manager exit."""
        await self.close()

    async def close(self) -> None:
        """Close the HTTP client."""
        await self._client.aclose()

    def _get_headers(self, content_type: str = "application/json") -> Dict[str, str]:
        """
        Get HTTP headers with authentication.

        Returns:
            Dict[str, str]: Headers with authentication token
        """
        return {
            "Authorization": f"ServiceToken {self._config.hippius_service_key}",
            "Accept": "application/json",
            "Content-Type": content_type,
        }

    @retry_on_error(retries=3, backoff=5.0)
    async def pin_file(
        self,
        cid: str,
        size_bytes: int,
        account_ss58: str,
        filename: str | None = None,
    ) -> PinResponse:
        """
        Pin a file to IPFS and submit to blockchain.

        Maps to: POST /storage-control/requests/ with request_type="Pin"

        Args:
            cid: Content Identifier (CID) of the file to pin
            account_ss58: Account SS58 hash
            size_bytes: Size of file in bytes
            filename: Optional original filename

        Returns:
            PinResponse: Response with request_id and status

        Raises:
            HippiusAPIError: If the API request fails
        """
        filename = filename or f"s3-{cid}"

        payload = {
            "cid": cid,
            "original_name": filename,
            "size_bytes": size_bytes,
            "account_ss58": account_ss58,
            "request_type": "Pin",
        }

        response = await self._client.post(
            "/storage-control/requests/",
            json=payload,
            headers=self._get_headers(),
        )

        response.raise_for_status()
        return PinResponse.model_validate(response.json())

    @retry_on_error(retries=3, backoff=5.0)
    async def unpin_file(
        self,
        cid: str,
        account_ss58: str,
    ) -> UnpinResponse:
        """
        Unpin a file from IPFS and cancel storage on blockchain.

        Maps to: POST /storage-control/requests/ with request_type="Unpin"

        Args:
            cid: Content Identifier (CID) of the file to unpin
            account_ss58: Account SS58 hash

        Returns:
            UnpinResponse: Response with request_id and status

        Raises:
            HippiusFailedSubstrateDelete: If the unpin request fails
        """

        payload = {
            "cid": cid,
            "request_type": "Unpin",
            "account_ss58": account_ss58,
        }

        logger.info(f"Unpinning with {payload=}")

        response = await self._client.post(
            "/storage-control/requests/",
            json=payload,
            headers=self._get_headers(),
        )

        response.raise_for_status()
        return UnpinResponse.model_validate(response.json())

    @retry_on_error(retries=3, backoff=5.0)
    async def auth(
        self,
        access_key: str,
    ) -> TokenAuthResponse:
        """
        Authenticate access key and retrieve encrypted secret.

        Maps to: POST /objectstore/tokens/auth

        Args:
            access_key: Access key ID to authenticate

        Returns:
            TokenAuthResponse: Response with token info and encrypted secret

        Raises:
            HippiusAPIError: If the API request fails
        """
        payload = {"accessKeyId": access_key}

        response = await self._client.post(
            "/objectstore/tokens/auth/",
            json=payload,
            headers=self._get_headers(),
        )

        response.raise_for_status()
        return TokenAuthResponse.model_validate(response.json())

    @retry_on_error(retries=3, backoff=5.0)
    async def upload_file_and_get_cid(
        self,
        file_data: bytes,
        file_name: str,
        content_type: str,
        account_ss58: str,
    ) -> UploadResponse:
        """
        Upload file directly to api.hippius.com storage endpoint.

        Maps to: POST /storage-control/upload/

        Args:
            file_data: Binary file data to upload
            file_name: Original filename
            content_type: MIME type of the file
            account_ss58: Account SS58 address

        Returns:
            UploadResponse: Response with file_id, CID, and metadata

        Raises:
            HippiusAPIError: If the API request fails
        """

        files = {
            "file": (
                file_name,
                io.BytesIO(file_data),
                content_type,
            ),
        }
        data = {"account_ss58": account_ss58}

        headers = self._get_headers()
        del headers["Content-Type"]

        response = await self._client.post(
            "/storage-control/upload/",
            files=files,
            data=data,
            headers=headers,
        )
        response_json = response.json()
        response.raise_for_status()

        try:
            return UploadResponse.model_validate(response_json)
        except Exception as e:
            error_summary = str(e).split("\n")[0] if "\n" in str(e) else str(e)
            logger.error(f"API validation failed: {error_summary} | Response: {response_json}")
            raise ValueError(f"Invalid API response: {error_summary}") from None

    @retry_on_error(retries=3, backoff=5.0)
    async def get_file_status(
        self,
        file_id: str,
    ) -> FileStatusResponse:
        """
        Get file status from api.hippius.com.

        Maps to: GET /storage-control/files/{file_id}/

        Args:
            file_id: File ID to query status for

        Returns:
            FileStatusResponse: File status and metadata

        Raises:
            HippiusAPIError: If the API request fails
        """
        response = await self._client.get(
            f"/storage-control/files/{file_id}/",
            headers=self._get_headers(),
        )

        response.raise_for_status()
        return FileStatusResponse.model_validate(response.json())

    @retry_on_error(retries=3, backoff=5.0)
    async def list_files(
        self,
        account_ss58: str,
        page: int = 1,
        page_size: int = 100,
    ) -> ListFilesResponse:
        """
        List files from Hippius API with pagination.

        Maps to: GET /storage-control/files/

        Args:
            account_ss58: Account SS58 address
            page: Page number (default: 1)
            page_size: Results per page (default: 100)

        Returns:
            ListFilesResponse: Paginated list of files with metadata

        Raises:
            HippiusAPIError: If the API request fails
        """
        response = await self._client.get(
            "/storage-control/files/",
            params={
                "page": page,
                "page_size": page_size,
                "only_s3": True,
                "include_pending": True,
                "account_ss58": account_ss58,
            },
            headers=self._get_headers(),
        )

        response.raise_for_status()
        return ListFilesResponse.model_validate(response.json())

    @retry_on_error(retries=3, backoff=5.0)
    async def get_s3_plan_accounts(
        self,
        next_url: str | None = None,
        page_size: int = 500,
    ) -> S3PlanAccountsResponse:
        """Fetch one page of the S3 billing-plan roll: the catalog plus per-account rows.

        Maps to: GET /api/s3/plans/accounts/?page=1&page_size=500

        NOTE the path passed here is "s3/plans/accounts/", not "/api/s3/plans/accounts/".
        HIPPIUS_API_BASE_URL already ends in /api (see .env.defaults), and every other call on this
        client is written relative to it — "objectstore/tokens/auth/", "storage-control/files/".
        Spelling the /api again would request /api/api/s3/plans/accounts/ and 404 forever.

        `next_url` continues a previous page. Upstream returns it as an ABSOLUTE url, and we
        deliberately keep only its path and query, re-homing them on our OWN configured host. Two
        reasons, both load-bearing:
          * in e2e HIPPIUS_API_BASE_URL points at mock-hippius-api, and following the absolute
            https://api.hippius.com/... would walk straight out of the mock and hit production;
          * an upstream response field can never redirect our service-token-authenticated client
            at a host we did not choose.

        The caller MUST treat a mid-pagination failure as "publish nothing": a partial roll demotes
        every missing account to pay-as-you-go and 402s paying customers. See publish_plan_roll.
        """
        timeout = httpx.Timeout(self._config.plans_api_timeout_seconds, connect=5.0)

        if next_url:
            page = urlparse(next_url)
            base = urlparse(str(self._client.base_url))
            # Absolute, so httpx uses it as-is rather than re-merging it under base_url's /api.
            target = urlunparse((base.scheme, base.netloc, page.path, "", page.query, ""))
            response = await self._client.get(target, headers=self._get_headers(), timeout=timeout)
        else:
            response = await self._client.get(
                "s3/plans/accounts/",
                params={"page": 1, "page_size": page_size},
                headers=self._get_headers(),
                timeout=timeout,
            )

        response.raise_for_status()
        return S3PlanAccountsResponse.model_validate(response.json())
