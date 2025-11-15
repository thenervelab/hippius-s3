from typing import Any
from unittest.mock import AsyncMock

import pytest
from fastapi import FastAPI
from httpx import ASGITransport
from httpx import AsyncClient

from gateway.middlewares.acl import acl_middleware
from gateway.middlewares.acl import get_required_permission
from gateway.middlewares.acl import parse_s3_path
from hippius_s3.models.acl import Permission


class TestPathParsing:
    def test_root_path_returns_none(self) -> None:
        bucket, key = parse_s3_path("/")
        assert bucket is None
        assert key is None

    def test_empty_path_returns_none(self) -> None:
        bucket, key = parse_s3_path("")
        assert bucket is None
        assert key is None

    def test_bucket_only_path(self) -> None:
        bucket, key = parse_s3_path("/my-bucket")
        assert bucket == "my-bucket"
        assert key is None

    def test_object_path(self) -> None:
        bucket, key = parse_s3_path("/my-bucket/my-object.txt")
        assert bucket == "my-bucket"
        assert key == "my-object.txt"

    def test_multi_level_key_path(self) -> None:
        bucket, key = parse_s3_path("/my-bucket/folder/subfolder/file.txt")
        assert bucket == "my-bucket"
        assert key == "folder/subfolder/file.txt"

    def test_path_with_multiple_slashes(self) -> None:
        bucket, key = parse_s3_path("///my-bucket/my-key")
        assert bucket == "my-bucket"
        assert key == "my-key"


class TestPermissionMapping:
    def test_get_without_acl_maps_to_read(self) -> None:
        permission = get_required_permission("GET", {}, has_key=True)
        assert permission == Permission.READ

    def test_head_without_acl_maps_to_read(self) -> None:
        permission = get_required_permission("HEAD", {}, has_key=True)
        assert permission == Permission.READ

    def test_get_with_acl_maps_to_read_acp(self) -> None:
        permission = get_required_permission("GET", {"acl": ""}, has_key=True)
        assert permission == Permission.READ_ACP

    def test_put_without_acl_maps_to_write(self) -> None:
        permission = get_required_permission("PUT", {}, has_key=True)
        assert permission == Permission.WRITE

    def test_post_maps_to_write(self) -> None:
        permission = get_required_permission("POST", {}, has_key=False)
        assert permission == Permission.WRITE

    def test_delete_maps_to_write(self) -> None:
        permission = get_required_permission("DELETE", {}, has_key=True)
        assert permission == Permission.WRITE

    def test_put_with_acl_maps_to_write_acp(self) -> None:
        permission = get_required_permission("PUT", {"acl": ""}, has_key=True)
        assert permission == Permission.WRITE_ACP

    def test_unknown_method_raises_error(self) -> None:
        with pytest.raises(ValueError, match="Unknown HTTP method"):
            get_required_permission("PATCH", {}, has_key=True)

    def test_initiate_multipart_maps_to_write(self) -> None:
        permission = get_required_permission("POST", {"uploads": ""}, has_key=True)
        assert permission == Permission.WRITE

    def test_upload_part_maps_to_write(self) -> None:
        permission = get_required_permission("PUT", {"uploadId": "abc123", "partNumber": "1"}, has_key=True)
        assert permission == Permission.WRITE

    def test_complete_multipart_maps_to_write(self) -> None:
        permission = get_required_permission("POST", {"uploadId": "abc123"}, has_key=True)
        assert permission == Permission.WRITE


class TestACLMiddleware:
    @pytest.fixture
    def mock_acl_service(self) -> Any:
        service = AsyncMock()
        service.check_permission = AsyncMock(return_value=True)
        return service

    @pytest.fixture
    def acl_app(self, mock_acl_service: Any) -> Any:
        app = FastAPI()
        app.state.acl_service = mock_acl_service

        @app.get("/test")
        async def test_endpoint() -> dict[str, str]:
            return {"message": "ok"}

        @app.get("/{bucket}/{key:path}")
        async def get_object(bucket: str, key: str) -> dict[str, str]:
            return {"bucket": bucket, "key": key}

        @app.head("/{bucket}/{key:path}")
        async def head_object(bucket: str, key: str) -> dict[str, str]:
            return {"bucket": bucket, "key": key}

        @app.put("/{bucket}/{key:path}")
        async def put_object(bucket: str, key: str) -> dict[str, str]:
            return {"bucket": bucket, "key": key}

        @app.delete("/{bucket}/{key:path}")
        async def delete_object(bucket: str, key: str) -> dict[str, str]:
            return {"bucket": bucket, "key": key}

        @app.post("/{bucket}/{key:path}")
        async def post_object(bucket: str, key: str) -> dict[str, str]:
            return {"bucket": bucket, "key": key}

        @app.get("/health")
        async def health() -> dict[str, str]:
            return {"status": "healthy"}

        app.middleware("http")(acl_middleware)

        return app

    @pytest.mark.asyncio
    async def test_health_endpoint_skips_acl_check(self, acl_app: Any, mock_acl_service: Any) -> None:
        async with AsyncClient(transport=ASGITransport(app=acl_app), base_url="http://test") as client:
            response = await client.get("/health")

        assert response.status_code == 200
        mock_acl_service.check_permission.assert_not_called()

    @pytest.mark.asyncio
    async def test_options_request_skips_acl_check(self, acl_app: Any, mock_acl_service: Any) -> None:
        async with AsyncClient(transport=ASGITransport(app=acl_app), base_url="http://test") as client:
            _response = await client.options("/my-bucket/my-key")

        mock_acl_service.check_permission.assert_not_called()

    @pytest.mark.asyncio
    async def test_root_path_skips_acl_check(self, acl_app: Any, mock_acl_service: Any) -> None:
        async with AsyncClient(transport=ASGITransport(app=acl_app), base_url="http://test") as client:
            _response = await client.get("/")

        mock_acl_service.check_permission.assert_not_called()

    @pytest.mark.asyncio
    async def test_owner_allowed_for_private_bucket(self, acl_app: Any, mock_acl_service: Any) -> None:
        mock_acl_service.check_permission.return_value = True

        async with AsyncClient(transport=ASGITransport(app=acl_app), base_url="http://test") as client:
            response = await client.get("/my-bucket/my-key")

        assert response.status_code == 200
        mock_acl_service.check_permission.assert_called_once()
        call_args = mock_acl_service.check_permission.call_args
        assert call_args.kwargs["bucket"] == "my-bucket"
        assert call_args.kwargs["key"] == "my-key"
        assert call_args.kwargs["permission"] == Permission.READ

    @pytest.mark.asyncio
    async def test_non_owner_denied_for_private_bucket(self, acl_app: Any, mock_acl_service: Any) -> None:
        mock_acl_service.check_permission.return_value = False

        async with AsyncClient(transport=ASGITransport(app=acl_app), base_url="http://test") as client:
            response = await client.get("/my-bucket/my-key")

        assert response.status_code == 403
        assert b"AccessDenied" in response.content

    @pytest.mark.asyncio
    async def test_anonymous_allowed_for_public_read_bucket(self, acl_app: Any, mock_acl_service: Any) -> None:
        mock_acl_service.check_permission.return_value = True

        async with AsyncClient(transport=ASGITransport(app=acl_app), base_url="http://test") as client:
            response = await client.get("/public-bucket/my-key")

        assert response.status_code == 200
        mock_acl_service.check_permission.assert_called_once()
        call_args = mock_acl_service.check_permission.call_args
        assert call_args.kwargs["account_id"] is None

    @pytest.mark.asyncio
    async def test_anonymous_denied_for_write_operation(self, acl_app: Any, mock_acl_service: Any) -> None:
        mock_acl_service.check_permission.return_value = False

        async with AsyncClient(transport=ASGITransport(app=acl_app), base_url="http://test") as client:
            response = await client.put("/public-bucket/my-key", content=b"data")

        assert response.status_code == 403
        assert b"AccessDenied" in response.content

    @pytest.mark.asyncio
    async def test_write_operation_checks_object_level_permission(self, acl_app: Any, mock_acl_service: Any) -> None:
        mock_acl_service.check_permission.return_value = True

        async with AsyncClient(transport=ASGITransport(app=acl_app), base_url="http://test") as client:
            response = await client.put("/my-bucket/my-key", content=b"data")

        assert response.status_code == 200
        mock_acl_service.check_permission.assert_called_once()
        call_args = mock_acl_service.check_permission.call_args
        assert call_args.kwargs["bucket"] == "my-bucket"
        assert call_args.kwargs["key"] == "my-key"
        assert call_args.kwargs["permission"] == Permission.WRITE

    @pytest.mark.asyncio
    async def test_get_acl_operation_requires_read_acp(self, acl_app: Any, mock_acl_service: Any) -> None:
        mock_acl_service.check_permission.return_value = True

        async with AsyncClient(transport=ASGITransport(app=acl_app), base_url="http://test") as client:
            _response = await client.get("/my-bucket/my-key?acl")

        mock_acl_service.check_permission.assert_called_once()
        call_args = mock_acl_service.check_permission.call_args
        assert call_args.kwargs["permission"] == Permission.READ_ACP

    @pytest.mark.asyncio
    async def test_put_acl_operation_requires_write_acp(self, acl_app: Any, mock_acl_service: Any) -> None:
        mock_acl_service.check_permission.return_value = True

        async with AsyncClient(transport=ASGITransport(app=acl_app), base_url="http://test") as client:
            _response = await client.put("/my-bucket/my-key?acl", content=b"<ACL>...</ACL>")

        mock_acl_service.check_permission.assert_called_once()
        call_args = mock_acl_service.check_permission.call_args
        assert call_args.kwargs["permission"] == Permission.WRITE_ACP

    @pytest.mark.asyncio
    async def test_read_operation_checks_object_level_permission(self, acl_app: Any, mock_acl_service: Any) -> None:
        mock_acl_service.check_permission.return_value = True

        async with AsyncClient(transport=ASGITransport(app=acl_app), base_url="http://test") as client:
            response = await client.get("/my-bucket/my-key")

        assert response.status_code == 200
        mock_acl_service.check_permission.assert_called_once()
        call_args = mock_acl_service.check_permission.call_args
        assert call_args.kwargs["bucket"] == "my-bucket"
        assert call_args.kwargs["key"] == "my-key"
        assert call_args.kwargs["permission"] == Permission.READ

    @pytest.mark.asyncio
    async def test_head_operation_checks_object_level_permission(self, acl_app: Any, mock_acl_service: Any) -> None:
        mock_acl_service.check_permission.return_value = True

        async with AsyncClient(transport=ASGITransport(app=acl_app), base_url="http://test") as client:
            response = await client.head("/my-bucket/my-key")

        assert response.status_code == 200
        mock_acl_service.check_permission.assert_called_once()
        call_args = mock_acl_service.check_permission.call_args
        assert call_args.kwargs["bucket"] == "my-bucket"
        assert call_args.kwargs["key"] == "my-key"
        assert call_args.kwargs["permission"] == Permission.READ

    @pytest.mark.asyncio
    async def test_delete_operation_checks_object_level_permission(self, acl_app: Any, mock_acl_service: Any) -> None:
        mock_acl_service.check_permission.return_value = True

        async with AsyncClient(transport=ASGITransport(app=acl_app), base_url="http://test") as client:
            _response = await client.delete("/my-bucket/my-key")

        mock_acl_service.check_permission.assert_called_once()
        call_args = mock_acl_service.check_permission.call_args
        assert call_args.kwargs["bucket"] == "my-bucket"
        assert call_args.kwargs["key"] == "my-key"
        assert call_args.kwargs["permission"] == Permission.WRITE

    @pytest.mark.asyncio
    async def test_post_multipart_operation_checks_object_level_permission(
        self, acl_app: Any, mock_acl_service: Any
    ) -> None:
        mock_acl_service.check_permission.return_value = True

        async with AsyncClient(transport=ASGITransport(app=acl_app), base_url="http://test") as client:
            _response = await client.post("/my-bucket/my-key?uploads", content=b"data")

        mock_acl_service.check_permission.assert_called_once()
        call_args = mock_acl_service.check_permission.call_args
        assert call_args.kwargs["bucket"] == "my-bucket"
        assert call_args.kwargs["key"] == "my-key"
        assert call_args.kwargs["permission"] == Permission.WRITE

    @pytest.mark.asyncio
    async def test_read_acp_checks_object_level_permission(self, acl_app: Any, mock_acl_service: Any) -> None:
        mock_acl_service.check_permission.return_value = True

        async with AsyncClient(transport=ASGITransport(app=acl_app), base_url="http://test") as client:
            _response = await client.get("/my-bucket/my-key?acl")

        mock_acl_service.check_permission.assert_called_once()
        call_args = mock_acl_service.check_permission.call_args
        assert call_args.kwargs["bucket"] == "my-bucket"
        assert call_args.kwargs["key"] == "my-key"
        assert call_args.kwargs["permission"] == Permission.READ_ACP

    @pytest.mark.asyncio
    async def test_write_acp_checks_object_level_permission(self, acl_app: Any, mock_acl_service: Any) -> None:
        mock_acl_service.check_permission.return_value = True

        async with AsyncClient(transport=ASGITransport(app=acl_app), base_url="http://test") as client:
            _response = await client.put("/my-bucket/my-key?acl", content=b"<ACL>...</ACL>")

        mock_acl_service.check_permission.assert_called_once()
        call_args = mock_acl_service.check_permission.call_args
        assert call_args.kwargs["bucket"] == "my-bucket"
        assert call_args.kwargs["key"] == "my-key"
        assert call_args.kwargs["permission"] == Permission.WRITE_ACP

    @pytest.mark.asyncio
    async def test_bucket_acl_get_checks_bucket_level(self, acl_app: Any, mock_acl_service: Any) -> None:
        mock_acl_service.check_permission.return_value = True

        async with AsyncClient(transport=ASGITransport(app=acl_app), base_url="http://test") as client:
            _response = await client.get("/my-bucket?acl")

        mock_acl_service.check_permission.assert_called_once()
        call_args = mock_acl_service.check_permission.call_args
        assert call_args.kwargs["bucket"] == "my-bucket"
        assert call_args.kwargs["key"] is None
        assert call_args.kwargs["permission"] == Permission.READ_ACP

    @pytest.mark.asyncio
    async def test_bucket_acl_put_checks_bucket_level(self, acl_app: Any, mock_acl_service: Any) -> None:
        mock_acl_service.check_permission.return_value = True

        async with AsyncClient(transport=ASGITransport(app=acl_app), base_url="http://test") as client:
            _response = await client.put("/my-bucket?acl", content=b"<ACL>...</ACL>")

        mock_acl_service.check_permission.assert_called_once()
        call_args = mock_acl_service.check_permission.call_args
        assert call_args.kwargs["bucket"] == "my-bucket"
        assert call_args.kwargs["key"] is None
        assert call_args.kwargs["permission"] == Permission.WRITE_ACP

    @pytest.mark.asyncio
    async def test_bucket_operation_checks_bucket_level(self, acl_app: Any, mock_acl_service: Any) -> None:
        mock_acl_service.check_permission.return_value = True

        async with AsyncClient(transport=ASGITransport(app=acl_app), base_url="http://test") as client:
            _response = await client.get("/my-bucket")

        mock_acl_service.check_permission.assert_called_once()
        call_args = mock_acl_service.check_permission.call_args
        assert call_args.kwargs["bucket"] == "my-bucket"
        assert call_args.kwargs["key"] is None
        assert call_args.kwargs["permission"] == Permission.READ

    @pytest.mark.asyncio
    async def test_user_endpoint_skips_acl_check(self, acl_app: Any, mock_acl_service: Any) -> None:
        async with AsyncClient(transport=ASGITransport(app=acl_app), base_url="http://test") as client:
            _response = await client.get("/user/profile")

        mock_acl_service.check_permission.assert_not_called()

    @pytest.mark.asyncio
    async def test_error_response_is_xml_format(self, acl_app: Any, mock_acl_service: Any) -> None:
        mock_acl_service.check_permission.return_value = False

        async with AsyncClient(transport=ASGITransport(app=acl_app), base_url="http://test") as client:
            response = await client.get("/my-bucket/my-key")

        assert response.status_code == 403
        assert response.headers["content-type"] == "application/xml"
        assert b"<?xml version=" in response.content
        assert b"<Error>" in response.content
        assert b"<Code>AccessDenied</Code>" in response.content
        assert b"<Message>Access Denied</Message>" in response.content
        assert b"</Error>" in response.content
