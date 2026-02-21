from typing import Any

from hippius_s3.services.arion_service import CanUploadResponse


class MockArionService:
    """Configurable mock for ArionClient used in unit tests."""

    def __init__(
        self,
        allow_upload: bool = True,
        upload_error: str | None = None,
        raise_on_can_upload: Exception | None = None,
    ) -> None:
        self.allow_upload = allow_upload
        self.upload_error = upload_error
        self.raise_on_can_upload = raise_on_can_upload
        self.can_upload_calls: list[tuple[str, int]] = []

    async def can_upload(self, account_ss58: str, size_bytes: int) -> CanUploadResponse:
        self.can_upload_calls.append((account_ss58, size_bytes))
        if self.raise_on_can_upload is not None:
            raise self.raise_on_can_upload
        return CanUploadResponse(result=self.allow_upload, error=self.upload_error)

    async def upload_file_and_get_cid(self, *args: Any, **kwargs: Any) -> None:
        raise NotImplementedError

    async def download_file(self, *args: Any, **kwargs: Any) -> None:
        raise NotImplementedError

    async def unpin_file(self, *args: Any, **kwargs: Any) -> None:
        raise NotImplementedError

    async def close(self) -> None:
        pass

    async def __aenter__(self) -> "MockArionService":
        return self

    async def __aexit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        await self.close()
