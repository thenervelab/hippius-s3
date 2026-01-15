"""Unit tests for bucket creation validation."""

from contextlib import asynccontextmanager
from typing import Any
from unittest.mock import AsyncMock

import pytest
from fastapi import FastAPI
from fastapi import Request
from httpx import ASGITransport
from httpx import AsyncClient

from hippius_s3.api.s3.buckets.router import router
from hippius_s3.dependencies import get_postgres
from hippius_s3.models.account import HippiusAccount


@pytest.fixture
def bucket_app() -> Any:
    app = FastAPI()
    app.include_router(router)

    @app.middleware("http")
    async def inject_account(request: Request, call_next: Any) -> Any:
        request.state.account = HippiusAccount(
            id="test-subaccount",
            main_account="test-main-account",
            upload=True,
            delete=True,
            has_credits=True,
        )
        return await call_next(request)

    @asynccontextmanager
    async def dummy_transaction() -> Any:
        yield

    async def override_get_postgres() -> Any:
        mock_db = AsyncMock()
        mock_db.transaction = dummy_transaction
        mock_db.fetchrow = AsyncMock()
        yield mock_db

    app.dependency_overrides[get_postgres] = override_get_postgres

    return app


@pytest.mark.asyncio
async def test_create_bucket_rejects_ss58_address(bucket_app: Any) -> None:
    ss58_bucket = "5GrwvaEF5zXb26Fz9rcQpDWS57CtERHpNehXCPcNoHGKutQY"

    async with AsyncClient(transport=ASGITransport(app=bucket_app), base_url="http://test") as client:
        response = await client.put(f"/{ss58_bucket}")

    assert response.status_code == 400
    assert "InvalidBucketName" in response.text
    assert "SS58" in response.text
