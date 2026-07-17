"""
Hippius API Client for interacting with the Hippius API.

This module provides an HTTP-based client that replaces direct blockchain
interactions with API calls authenticated via HIPPIUS_KEY.

API Documentation: https://api.hippius.com/?format=openapi
"""

import asyncio
import functools
import logging
from collections.abc import AsyncIterator
from typing import Any
from typing import Callable
from typing import Coroutine
from typing import Dict
from typing import TypeVar

import httpx
from pydantic import BaseModel

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
    valid: bool
    status: str
    account_address: str
    token_type: str
    encrypted_secret: str
    nonce: str


class UploadResponse(BaseModel):
    upload_id: str
    timestamp: int
    size_bytes: int = 0
    file_id: str

    @property
    def cid(self) -> str:
        return self.upload_id

    @property
    def id(self) -> str:
        return self.file_id

    @property
    def status(self) -> str:
        return "uploaded"


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


class DeleteResult(BaseModel):
    status: str
    file_id: str
    folder_hash: str | None = None
    user_id: str | None = None


class DeleteSuccessResponse(BaseModel):
    Success: DeleteResult


class BatchDeleteItem(BaseModel):
    file_id: str
    status: str  # "deleted" | "already_deleted"


class BatchDeleteError(BaseModel):
    file_id: str
    code: str
    message: str | None = None


class BatchDeleteResult(BaseModel):
    deleted: list[BatchDeleteItem] = []
    errors: list[BatchDeleteError] = []
    files_deleted: int = 0


class BatchDeleteResponse(BaseModel):
    Success: BatchDeleteResult


class DownloadMetadata(BaseModel):
    file_id: str
    user_id: str
    size_bytes: str
    revision_seq: str
    revision_id: str


class HippiusAPIError(Exception):
    """Raised when there's an authentication issue with the API."""

    pass


class HippiusAuthenticationError(HippiusAPIError):
    """Raised when there's an authentication issue with the API."""

    pass


# NOT a HippiusAPIError subclass on purpose: retry_on_error retries HippiusAPIError, but a 404 on
# the batch endpoint means "old HCFS server without POST /delete_files" — the caller must fall back
# to per-file DELETE immediately, not burn the retry budget. As a plain Exception it hits
# retry_on_error's `except Exception: raise` and propagates on the first call.
class BatchEndpointUnavailable(Exception):
    """POST /delete_files returned 404 — the batch endpoint is not deployed on this HCFS server."""

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

                    # Don't retry on 507 Insufficient Storage - server is full, retrying won't help
                    if hasattr(e, "response") and e.response.status_code == 507:  # ty: ignore[unresolved-attribute]
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


class CanUploadRequest(BaseModel):
    user_id: str
    size_bytes: int


class CanUploadResponse(BaseModel):
    result: bool
    error: str | None = None


class ArionClient:
    """
    HTTP API client for Hippius API.
    """

    def __init__(
        self,
        base_url: str | None = None,
        service_key: str | None = None,
    ) -> None:
        """
        Initialize the Arion API client.

        Args:
            base_url: Optional Arion base URL. Falls back to config if not provided.
            service_key: Optional API service key. Falls back to config if not provided.
        """
        self._config = get_config()
        self.api_url = base_url or self._config.arion_base_url
        self._service_key = service_key if service_key is not None else self._config.arion_service_key
        self._client = httpx.AsyncClient(
            base_url=self.api_url,
            timeout=httpx.Timeout(
                60.0,
                connect=10.0,
            ),
            follow_redirects=True,
            verify=self._config.arion_verify_ssl,
        )

    async def __aenter__(self) -> "ArionClient":
        """Async context manager entry."""
        return self

    async def __aexit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        """Async context manager exit."""
        await self.close()

    async def close(self) -> None:
        """Close the HTTP client."""
        await self._client.aclose()

    def _get_headers(self, account_ss58: str) -> Dict[str, str]:
        """
        Get HTTP headers with authentication.

        Returns:
            Dict[str, str]: Headers with authentication token
        """
        headers = {
            "X-API-Key": self._config.arion_service_key,
            "Authorization": f"Bearer {self._config.arion_bearer_token}",
        }
        if self._config.arion_rate_limiting_proxy_bypass_key:
            headers["X-Hippius-Bypass-Rate-Limiting"] = self._config.arion_rate_limiting_proxy_bypass_key
        return headers

    @retry_on_error(retries=3, backoff=5.0)
    async def unpin_file(
        self,
        file_id: str,
        account_ss58: str,
    ) -> DeleteSuccessResponse:
        """
        Delete a file from storage (HCFS endpoint).

        Maps to: DELETE /delete/{user_id}/{file_id}

        Args:
            file_id: File identifier (64-character hex-encoded SHA-256 path hash)
            account_ss58: Account SS58 hash

        Returns:
            DeleteSuccessResponse: Response with deletion status

        Raises:
            HippiusAPIError: If the API request fails
        """

        headers = self._get_headers(account_ss58)
        response = await self._client.delete(
            f"/delete/{account_ss58}/{file_id}",
            headers=headers,
        )

        logger.info(f"Raw response content {response.content}")
        # B3: a DELETE of an already-absent pin is a SUCCESS (idempotent). Without this, a 404
        # raises → the unpinner (A9) re-raises → retries → re-DELETEs the now-gone object → 404
        # again, looping to the DLQ while the chunk_backend row stays deleted=false forever
        # (zombie). Returning success lets the unpinner proceed to soft-delete the row.
        if response.status_code == 404:
            logger.info(f"unpin_file: {file_id} already absent (404) — treating as deleted (idempotent)")
            return DeleteSuccessResponse(
                Success=DeleteResult(status="already_deleted", file_id=file_id, user_id=account_ss58)
            )
        response.raise_for_status()
        response_json = response.json()

        return DeleteSuccessResponse.model_validate(response_json)

    @retry_on_error(retries=3, backoff=5.0)
    async def unpin_files_batch(
        self,
        file_ids: list[str],
        account_ss58: str,
        folder_hash: str = "",
    ) -> BatchDeleteResult:
        """
        Batch-delete up to 1000 files in one call (HCFS endpoint).

        Maps to: POST /delete_files

        Args:
            file_ids: File identifiers (64-char hex path hashes), max 1000 per call.
            account_ss58: Account SS58 address.
            folder_hash: Folder the files live under. Our S3 objects are uploaded to HCFS with NO
                folder, so this is "" (root) by default. Confirmed by HCFS (2026-07-16):
                /delete_files accepts an empty folder_hash for our root-folder objects, matching the
                single-delete behavior (see docs/issues/unpinner-batch-delete.md).

        Returns:
            BatchDeleteResult: per-item deleted/errors plus files_deleted count (HTTP 200 even on
                partial failure).

        Raises:
            BatchEndpointUnavailable: the endpoint 404'd (old server) — caller falls back to per-file.
            HippiusAPIError / httpx errors: whole-request failure (auth/500/network) after retries.
        """
        if len(file_ids) > 1000:
            raise ValueError(f"unpin_files_batch called with {len(file_ids)} file_ids; HCFS caps at 1000 per call")

        headers = self._get_headers(account_ss58)
        body = {
            "ss58_address": account_ss58,
            "folder_hash": folder_hash,
            "file_ids": file_ids,
            "quiet": False,
        }
        response = await self._client.post(
            "/delete_files",
            json=body,
            headers=headers,
        )
        logger.info(f"Batch delete response status {response.status_code} for {len(file_ids)} file_ids")
        # Detect the missing endpoint BEFORE raise_for_status so the caller can fall back to per-file.
        if response.status_code == 404:
            raise BatchEndpointUnavailable("POST /delete_files returned 404 (batch endpoint not deployed)")
        response.raise_for_status()

        return BatchDeleteResponse.model_validate(response.json()).Success

    async def download_file(
        self,
        file_id: str,
        account_ss58: str,
        chunk_size: int = 65536,
    ) -> AsyncIterator[bytes]:
        """
        Download a file from storage as an async iterator.

        Maps to: GET /download/{file_id}

        Args:
            file_id: File identifier (CID or 64-character hex-encoded SHA-256 path hash)
            account_ss58: Size of chunks to yield (default 64KB)
            chunk_size: Chunk size in bytes (default 65536)

        Yields:
            bytes: Chunks of file content

        Raises:
            HippiusAPIError: If the API request fails
        """

        headers = self._get_headers(account_ss58)
        download_path = f"/download/{account_ss58}/{file_id}"

        async with self._client.stream(
            "GET",
            download_path,
            headers=headers,
        ) as response:
            logger.info(f"Raw response status {response.status_code} for {download_path}")
            response.raise_for_status()
            async for chunk in response.aiter_bytes(chunk_size):
                yield chunk

    @retry_on_error(retries=3, backoff=5.0)
    async def upload_file_and_get_cid(
        self,
        file_data: bytes,
        file_name: str,
        content_type: str,
        account_ss58: str,
        extra_headers: Dict[str, str] | None = None,
    ) -> UploadResponse:
        """
        Upload file using multipart form data.

        Maps to: POST /upload

        Args:
            file_data: Binary file data to upload
            file_name: Original filename
            content_type: MIME type of the file
            account_ss58: Account SS58 address

        Returns:
            UploadResponse: Response with CID hash

        Raises:
            HippiusAPIError: If the API request fails
        """
        files = {
            "file": (
                file_name,
                file_data,
                "application/octet-stream",
                {"Content-Length": str(len(file_data))},
            ),
        }
        data = {"account_ss58": account_ss58}

        headers = self._get_headers(account_ss58)
        if extra_headers:
            headers.update(extra_headers)
        response = await self._client.post(
            "/upload",
            files=files,
            data=data,
            headers=headers,
        )
        logger.info(f"Raw response content {response.content}")
        response.raise_for_status()
        response_json = response.json()

        upload_response = UploadResponse.model_validate(response_json)
        upload_response.size_bytes = len(file_data)
        return upload_response

    @retry_on_error(retries=3, backoff=5.0)
    async def can_upload(
        self,
        account_ss58: str,
        size_bytes: int,
    ) -> CanUploadResponse:
        """
        Check whether the account is allowed to upload a file of the given size.

        Maps to: POST /can_upload

        Args:
            account_ss58: Account SS58 address
            size_bytes: Size of the intended upload in bytes

        Returns:
            CanUploadResponse: Whether the upload is permitted

        Raises:
            HippiusAPIError: If the API request fails
        """
        headers = self._get_headers(account_ss58)
        payload = CanUploadRequest(user_id=account_ss58, size_bytes=size_bytes)
        response = await self._client.post(
            "/can_upload",
            json=payload.model_dump(),
            headers=headers,
        )
        logger.info(f"Raw response content {response.content}")
        response.raise_for_status()
        return CanUploadResponse.model_validate(response.json())
