import xml.etree.ElementTree as ET
from typing import Any
from unittest.mock import AsyncMock
from unittest.mock import MagicMock

import pytest
from fastapi import FastAPI
from fastapi.responses import Response
from httpx import ASGITransport
from httpx import AsyncClient

from gateway.routers.acl import acl_to_xml
from gateway.routers.acl import router
from gateway.routers.acl import xml_to_acl
from gateway.services.acl_service import ACLService
from hippius_s3.models.acl import ACL
from hippius_s3.models.acl import Grant
from hippius_s3.models.acl import Grantee
from hippius_s3.models.acl import GranteeType
from hippius_s3.models.acl import Owner
from hippius_s3.models.acl import Permission
from hippius_s3.models.acl import WellKnownGroups


class TestAclToXml:
    def test_acl_to_xml_with_canonical_user(self) -> None:
        acl = ACL(
            owner=Owner(id="owner-123", display_name="Owner Name"),
            grants=[
                Grant(
                    grantee=Grantee(
                        type=GranteeType.CANONICAL_USER,
                        id="user-456",
                        display_name="User Name",
                    ),
                    permission=Permission.FULL_CONTROL,
                )
            ],
        )

        xml = acl_to_xml(acl)

        assert '<?xml version="1.0" encoding="UTF-8"?>' in xml
        assert "<AccessControlPolicy" in xml
        assert 'xmlns="http://s3.amazonaws.com/doc/2006-03-01/"' in xml
        assert "<Owner>" in xml
        assert "<ID>owner-123</ID>" in xml
        assert "<DisplayName>Owner Name</DisplayName>" in xml
        assert '<Grantee xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xsi:type="CanonicalUser">' in xml
        assert "<ID>user-456</ID>" in xml
        assert "<Permission>FULL_CONTROL</Permission>" in xml

    def test_acl_to_xml_with_group(self) -> None:
        acl = ACL(
            owner=Owner(id="owner-123"),
            grants=[
                Grant(
                    grantee=Grantee(type=GranteeType.GROUP, uri=WellKnownGroups.ALL_USERS),
                    permission=Permission.READ,
                )
            ],
        )

        xml = acl_to_xml(acl)

        assert '<Grantee xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xsi:type="Group">' in xml
        assert f"<URI>{WellKnownGroups.ALL_USERS}</URI>" in xml
        assert "<Permission>READ</Permission>" in xml

    def test_acl_to_xml_with_email(self) -> None:
        acl = ACL(
            owner=Owner(id="owner-123"),
            grants=[
                Grant(
                    grantee=Grantee(
                        type=GranteeType.AMAZON_CUSTOMER_BY_EMAIL,
                        email_address="user@example.com",
                    ),
                    permission=Permission.WRITE,
                )
            ],
        )

        xml = acl_to_xml(acl)

        assert '<Grantee xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xsi:type="AmazonCustomerByEmail">' in xml
        assert "<EmailAddress>user@example.com</EmailAddress>" in xml
        assert "<Permission>WRITE</Permission>" in xml

    def test_acl_to_xml_with_multiple_grants(self) -> None:
        acl = ACL(
            owner=Owner(id="owner-123"),
            grants=[
                Grant(
                    grantee=Grantee(type=GranteeType.CANONICAL_USER, id="owner-123"),
                    permission=Permission.FULL_CONTROL,
                ),
                Grant(
                    grantee=Grantee(type=GranteeType.GROUP, uri=WellKnownGroups.ALL_USERS),
                    permission=Permission.READ,
                ),
            ],
        )

        xml = acl_to_xml(acl)

        assert xml.count("<Grant>") == 2
        assert "<Permission>FULL_CONTROL</Permission>" in xml
        assert "<Permission>READ</Permission>" in xml

    def test_acl_to_xml_without_display_names(self) -> None:
        acl = ACL(
            owner=Owner(id="owner-123"),
            grants=[
                Grant(
                    grantee=Grantee(type=GranteeType.CANONICAL_USER, id="user-456"),
                    permission=Permission.READ,
                )
            ],
        )

        xml = acl_to_xml(acl)

        assert "<ID>owner-123</ID>" in xml
        assert "<DisplayName>" not in xml


class TestXmlToAcl:
    def test_xml_to_acl_with_canonical_user(self) -> None:
        xml = """<?xml version="1.0" encoding="UTF-8"?>
        <AccessControlPolicy xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
          <Owner>
            <ID>owner-123</ID>
            <DisplayName>Owner Name</DisplayName>
          </Owner>
          <AccessControlList>
            <Grant>
              <Grantee xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xsi:type="CanonicalUser">
                <ID>user-456</ID>
                <DisplayName>User Name</DisplayName>
              </Grantee>
              <Permission>FULL_CONTROL</Permission>
            </Grant>
          </AccessControlList>
        </AccessControlPolicy>"""

        acl = xml_to_acl(xml)

        assert acl.owner.id == "owner-123"
        assert acl.owner.display_name == "Owner Name"
        assert len(acl.grants) == 1
        assert acl.grants[0].grantee.type == GranteeType.CANONICAL_USER
        assert acl.grants[0].grantee.id == "user-456"
        assert acl.grants[0].grantee.display_name == "User Name"
        assert acl.grants[0].permission == Permission.FULL_CONTROL

    def test_xml_to_acl_with_group(self) -> None:
        xml = f"""<?xml version="1.0" encoding="UTF-8"?>
        <AccessControlPolicy xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
          <Owner>
            <ID>owner-123</ID>
          </Owner>
          <AccessControlList>
            <Grant>
              <Grantee xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xsi:type="Group">
                <URI>{WellKnownGroups.ALL_USERS}</URI>
              </Grantee>
              <Permission>READ</Permission>
            </Grant>
          </AccessControlList>
        </AccessControlPolicy>"""

        acl = xml_to_acl(xml)

        assert len(acl.grants) == 1
        assert acl.grants[0].grantee.type == GranteeType.GROUP
        assert acl.grants[0].grantee.uri == WellKnownGroups.ALL_USERS
        assert acl.grants[0].permission == Permission.READ

    def test_xml_to_acl_with_email(self) -> None:
        xml = """<?xml version="1.0" encoding="UTF-8"?>
        <AccessControlPolicy xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
          <Owner>
            <ID>owner-123</ID>
          </Owner>
          <AccessControlList>
            <Grant>
              <Grantee xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xsi:type="AmazonCustomerByEmail">
                <EmailAddress>user@example.com</EmailAddress>
              </Grantee>
              <Permission>WRITE</Permission>
            </Grant>
          </AccessControlList>
        </AccessControlPolicy>"""

        acl = xml_to_acl(xml)

        assert len(acl.grants) == 1
        assert acl.grants[0].grantee.type == GranteeType.AMAZON_CUSTOMER_BY_EMAIL
        assert acl.grants[0].grantee.email_address == "user@example.com"
        assert acl.grants[0].permission == Permission.WRITE

    def test_xml_to_acl_missing_owner(self) -> None:
        xml = """<?xml version="1.0" encoding="UTF-8"?>
        <AccessControlPolicy xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
          <AccessControlList>
            <Grant>
              <Grantee xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xsi:type="CanonicalUser">
                <ID>user-456</ID>
              </Grantee>
              <Permission>READ</Permission>
            </Grant>
          </AccessControlList>
        </AccessControlPolicy>"""

        with pytest.raises(ValueError, match="Missing Owner element"):
            xml_to_acl(xml)

    def test_xml_to_acl_missing_owner_id(self) -> None:
        xml = """<?xml version="1.0" encoding="UTF-8"?>
        <AccessControlPolicy xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
          <Owner>
            <DisplayName>Owner Name</DisplayName>
          </Owner>
          <AccessControlList/>
        </AccessControlPolicy>"""

        with pytest.raises(ValueError, match="Missing Owner ID"):
            xml_to_acl(xml)

    def test_xml_to_acl_skips_invalid_grants(self) -> None:
        xml = """<?xml version="1.0" encoding="UTF-8"?>
        <AccessControlPolicy xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
          <Owner>
            <ID>owner-123</ID>
          </Owner>
          <AccessControlList>
            <Grant>
              <Grantee xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xsi:type="CanonicalUser">
              </Grantee>
              <Permission>READ</Permission>
            </Grant>
            <Grant>
              <Grantee xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xsi:type="CanonicalUser">
                <ID>valid-user</ID>
              </Grantee>
              <Permission>WRITE</Permission>
            </Grant>
          </AccessControlList>
        </AccessControlPolicy>"""

        acl = xml_to_acl(xml)

        assert len(acl.grants) == 1
        assert acl.grants[0].grantee.id == "valid-user"


class TestParseCannedAclHeader:
    @pytest.fixture
    def acl_service(self) -> Any:
        mock_db_pool = MagicMock()
        return ACLService(mock_db_pool)

    @pytest.mark.asyncio
    async def test_parse_private(self, acl_service: Any) -> None:
        acl = await acl_service.canned_acl_to_acl("private", "5FHneW46xGXgs5mUiveU4sbTyGBzmstUspZC92UhjJM694ty")

        assert acl.owner.id == "5FHneW46xGXgs5mUiveU4sbTyGBzmstUspZC92UhjJM694ty"
        assert len(acl.grants) == 1
        assert acl.grants[0].grantee.type == GranteeType.CANONICAL_USER
        assert acl.grants[0].grantee.id == "5FHneW46xGXgs5mUiveU4sbTyGBzmstUspZC92UhjJM694ty"
        assert acl.grants[0].permission == Permission.FULL_CONTROL

    @pytest.mark.asyncio
    async def test_parse_public_read(self, acl_service: Any) -> None:
        acl = await acl_service.canned_acl_to_acl("public-read", "5FHneW46xGXgs5mUiveU4sbTyGBzmstUspZC92UhjJM694ty")

        assert acl.owner.id == "5FHneW46xGXgs5mUiveU4sbTyGBzmstUspZC92UhjJM694ty"
        assert len(acl.grants) == 2
        assert acl.grants[0].permission == Permission.FULL_CONTROL
        assert acl.grants[1].grantee.type == GranteeType.GROUP
        assert acl.grants[1].grantee.uri == WellKnownGroups.ALL_USERS
        assert acl.grants[1].permission == Permission.READ

    @pytest.mark.asyncio
    async def test_parse_public_read_write(self, acl_service: Any) -> None:
        acl = await acl_service.canned_acl_to_acl(
            "public-read-write", "5FHneW46xGXgs5mUiveU4sbTyGBzmstUspZC92UhjJM694ty"
        )

        assert acl.owner.id == "5FHneW46xGXgs5mUiveU4sbTyGBzmstUspZC92UhjJM694ty"
        assert len(acl.grants) == 3
        assert acl.grants[0].permission == Permission.FULL_CONTROL
        assert acl.grants[1].permission == Permission.READ
        assert acl.grants[2].permission == Permission.WRITE

    @pytest.mark.asyncio
    async def test_parse_authenticated_read(self, acl_service: Any) -> None:
        acl = await acl_service.canned_acl_to_acl(
            "authenticated-read", "5FHneW46xGXgs5mUiveU4sbTyGBzmstUspZC92UhjJM694ty"
        )

        assert acl.owner.id == "5FHneW46xGXgs5mUiveU4sbTyGBzmstUspZC92UhjJM694ty"
        assert len(acl.grants) == 2
        assert acl.grants[1].grantee.uri == WellKnownGroups.AUTHENTICATED_USERS
        assert acl.grants[1].permission == Permission.READ

    @pytest.mark.asyncio
    async def test_parse_invalid_canned_acl(self, acl_service: Any) -> None:
        with pytest.raises(ValueError, match="Unknown canned ACL"):
            await acl_service.canned_acl_to_acl("invalid-acl", "5FHneW46xGXgs5mUiveU4sbTyGBzmstUspZC92UhjJM694ty")


@pytest.mark.asyncio
class TestGetBucketAcl:
    async def test_get_bucket_acl_returns_xml(self) -> None:
        mock_service = AsyncMock()
        mock_service.get_effective_acl.return_value = ACL(
            owner=Owner(id="test-owner"),
            grants=[
                Grant(
                    grantee=Grantee(type=GranteeType.CANONICAL_USER, id="test-owner"),
                    permission=Permission.FULL_CONTROL,
                )
            ],
        )

        app = FastAPI()
        app.state.acl_service = mock_service
        app.include_router(router)

        async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
            response = await client.get("/my-bucket?acl")

        assert response.status_code == 200
        assert response.headers["content-type"] == "application/xml"
        assert b'<?xml version="1.0" encoding="UTF-8"?>' in response.content
        assert b"<AccessControlPolicy" in response.content
        assert b"<Owner>" in response.content
        assert b"<ID>test-owner</ID>" in response.content

        mock_service.get_effective_acl.assert_called_once_with("my-bucket", None)

    async def test_get_bucket_acl_without_query_param_doesnt_call_service(self) -> None:
        mock_service = AsyncMock()
        mock_forward = AsyncMock()
        mock_forward.forward_request.return_value = Response(status_code=200)

        app = FastAPI()
        app.state.acl_service = mock_service
        app.state.forward_service = mock_forward
        app.include_router(router)

        @app.get("/{bucket}")
        async def fallback(bucket: str) -> dict[str, Any]:
            return {"bucket": bucket, "fallback": True}

        async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
            response = await client.get("/my-bucket")

        assert response.status_code == 200
        mock_service.get_effective_acl.assert_not_called()

    async def test_get_bucket_acl_with_multiple_grants(self) -> None:
        mock_service = AsyncMock()
        mock_service.get_effective_acl.return_value = ACL(
            owner=Owner(id="owner-123", display_name="Owner"),
            grants=[
                Grant(
                    grantee=Grantee(type=GranteeType.CANONICAL_USER, id="owner-123"),
                    permission=Permission.FULL_CONTROL,
                ),
                Grant(
                    grantee=Grantee(type=GranteeType.GROUP, uri=WellKnownGroups.ALL_USERS),
                    permission=Permission.READ,
                ),
            ],
        )

        app = FastAPI()
        app.state.acl_service = mock_service
        app.include_router(router)

        async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
            response = await client.get("/my-bucket?acl")

        assert response.status_code == 200
        root = ET.fromstring(response.content)
        ns = {"s3": "http://s3.amazonaws.com/doc/2006-03-01/"}
        grants = root.findall(".//s3:Grant", ns)
        assert len(grants) == 2


@pytest.mark.asyncio
class TestGetObjectAcl:
    async def test_get_object_acl_returns_xml(self) -> None:
        mock_service = AsyncMock()
        mock_service.get_effective_acl.return_value = ACL(
            owner=Owner(id="test-owner"),
            grants=[
                Grant(
                    grantee=Grantee(type=GranteeType.CANONICAL_USER, id="test-owner"),
                    permission=Permission.FULL_CONTROL,
                )
            ],
        )

        app = FastAPI()
        app.state.acl_service = mock_service
        app.include_router(router)

        async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
            response = await client.get("/my-bucket/my-key?acl")

        assert response.status_code == 200
        assert response.headers["content-type"] == "application/xml"
        assert b"<AccessControlPolicy" in response.content

        mock_service.get_effective_acl.assert_called_once_with("my-bucket", "my-key")

    async def test_get_object_acl_without_query_param_doesnt_call_service(self) -> None:
        mock_service = AsyncMock()
        mock_forward = AsyncMock()
        mock_forward.forward_request.return_value = Response(status_code=200)

        app = FastAPI()
        app.state.acl_service = mock_service
        app.state.forward_service = mock_forward
        app.include_router(router)

        @app.get("/{bucket}/{key:path}")
        async def fallback(bucket: str, key: str) -> dict[str, Any]:
            return {"bucket": bucket, "key": key, "fallback": True}

        async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
            response = await client.get("/my-bucket/my-key")

        assert response.status_code == 200
        mock_service.get_effective_acl.assert_not_called()

    async def test_get_object_acl_with_nested_path(self) -> None:
        mock_service = AsyncMock()
        mock_service.get_effective_acl.return_value = ACL(
            owner=Owner(id="test-owner"),
            grants=[
                Grant(
                    grantee=Grantee(type=GranteeType.CANONICAL_USER, id="test-owner"),
                    permission=Permission.FULL_CONTROL,
                )
            ],
        )

        app = FastAPI()
        app.state.acl_service = mock_service
        app.include_router(router)

        async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
            response = await client.get("/my-bucket/folder/subfolder/key.txt?acl")

        assert response.status_code == 200
        mock_service.get_effective_acl.assert_called_once_with("my-bucket", "folder/subfolder/key.txt")


@pytest.mark.asyncio
class TestPutBucketAcl:
    async def test_put_bucket_acl_with_canned_acl_header(self) -> None:
        mock_db_pool = MagicMock()
        mock_service = ACLService(mock_db_pool)
        mock_service.acl_repo = AsyncMock()
        mock_service.invalidate_cache = AsyncMock()  # type: ignore[method-assign]

        app = FastAPI()
        app.state.acl_service = mock_service

        @app.middleware("http")
        async def mock_auth_middleware(request: Any, call_next: Any) -> Any:
            request.state.account_id = "5FHneW46xGXgs5mUiveU4sbTyGBzmstUspZC92UhjJM694ty"
            return await call_next(request)

        app.include_router(router)

        async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
            response = await client.put(
                "/my-bucket?acl",
                headers={"x-amz-acl": "public-read"},
            )

        assert response.status_code == 200
        mock_service.acl_repo.set_bucket_acl.assert_called_once()
        args = mock_service.acl_repo.set_bucket_acl.call_args[0]
        assert args[0] == "my-bucket"
        assert args[1] == "5FHneW46xGXgs5mUiveU4sbTyGBzmstUspZC92UhjJM694ty"
        assert len(args[2].grants) == 2

        mock_service.invalidate_cache.assert_called_once_with("my-bucket")

    async def test_put_bucket_acl_with_xml_body(self) -> None:
        mock_service = AsyncMock()
        mock_repo = AsyncMock()
        mock_service.acl_repo = mock_repo

        app = FastAPI()
        app.state.acl_service = mock_service

        @app.middleware("http")
        async def mock_auth_middleware(request: Any, call_next: Any) -> Any:
            request.state.account_id = "5FHneW46xGXgs5mUiveU4sbTyGBzmstUspZC92UhjJM694ty"
            return await call_next(request)

        app.include_router(router)

        xml_body = """<?xml version="1.0" encoding="UTF-8"?>
        <AccessControlPolicy xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
          <Owner>
            <ID>5FHneW46xGXgs5mUiveU4sbTyGBzmstUspZC92UhjJM694ty</ID>
          </Owner>
          <AccessControlList>
            <Grant>
              <Grantee xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xsi:type="CanonicalUser">
                <ID>5FHneW46xGXgs5mUiveU4sbTyGBzmstUspZC92UhjJM694ty</ID>
              </Grantee>
              <Permission>FULL_CONTROL</Permission>
            </Grant>
          </AccessControlList>
        </AccessControlPolicy>"""

        async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
            response = await client.put(
                "/my-bucket?acl",
                content=xml_body,
                headers={"Content-Type": "application/xml"},
            )

        assert response.status_code == 200
        mock_repo.set_bucket_acl.assert_called_once()

    async def test_put_bucket_acl_without_auth_returns_403(self) -> None:
        app = FastAPI()
        app.state.acl_service = AsyncMock()
        app.include_router(router)

        async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
            response = await client.put(
                "/my-bucket?acl",
                headers={"x-amz-acl": "private"},
            )

        assert response.status_code == 403
        assert b"<Code>AccessDenied</Code>" in response.content

    async def test_put_bucket_acl_owner_mismatch_returns_403(self) -> None:
        mock_service = AsyncMock()

        app = FastAPI()
        app.state.acl_service = mock_service
        app.include_router(router)

        xml_body = """<?xml version="1.0" encoding="UTF-8"?>
        <AccessControlPolicy xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
          <Owner>
            <ID>different-owner</ID>
          </Owner>
          <AccessControlList>
            <Grant>
              <Grantee xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xsi:type="CanonicalUser">
                <ID>different-owner</ID>
              </Grantee>
              <Permission>FULL_CONTROL</Permission>
            </Grant>
          </AccessControlList>
        </AccessControlPolicy>"""

        mock_request = MagicMock()
        mock_request.state.account_id = "owner-123"

        async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
            response = await client.put(
                "/my-bucket?acl",
                content=xml_body,
            )

        assert response.status_code == 403
        assert b"<Code>AccessDenied</Code>" in response.content

    async def test_put_bucket_acl_without_body_or_header_returns_400(self) -> None:
        app = FastAPI()
        app.state.acl_service = AsyncMock()

        @app.middleware("http")
        async def mock_auth_middleware(request: Any, call_next: Any) -> Any:
            request.state.account_id = "owner-123"
            return await call_next(request)

        app.include_router(router)

        async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
            response = await client.put("/my-bucket?acl")

        assert response.status_code == 400
        assert b"<Code>MalformedACLError</Code>" in response.content


@pytest.mark.asyncio
class TestPutObjectAcl:
    async def test_put_object_acl_with_canned_acl_header(self) -> None:
        mock_service = AsyncMock()
        mock_repo = AsyncMock()
        mock_service.acl_repo = mock_repo

        app = FastAPI()
        app.state.acl_service = mock_service

        @app.middleware("http")
        async def mock_auth_middleware(request: Any, call_next: Any) -> Any:
            request.state.account_id = "5FHneW46xGXgs5mUiveU4sbTyGBzmstUspZC92UhjJM694ty"
            return await call_next(request)

        app.include_router(router)

        async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
            response = await client.put(
                "/my-bucket/my-key?acl",
                headers={"x-amz-acl": "private"},
            )

        assert response.status_code == 200
        mock_repo.set_object_acl.assert_called_once()
        args = mock_repo.set_object_acl.call_args[0]
        assert args[0] == "my-bucket"
        assert args[1] == "my-key"
        assert args[2] == "5FHneW46xGXgs5mUiveU4sbTyGBzmstUspZC92UhjJM694ty"

        mock_service.invalidate_cache.assert_called_once_with("my-bucket", "my-key")

    async def test_put_object_acl_with_xml_body(self) -> None:
        mock_service = AsyncMock()
        mock_repo = AsyncMock()
        mock_service.acl_repo = mock_repo

        app = FastAPI()
        app.state.acl_service = mock_service

        @app.middleware("http")
        async def mock_auth_middleware(request: Any, call_next: Any) -> Any:
            request.state.account_id = "5FHneW46xGXgs5mUiveU4sbTyGBzmstUspZC92UhjJM694ty"
            return await call_next(request)

        app.include_router(router)

        xml_body = """<?xml version="1.0" encoding="UTF-8"?>
        <AccessControlPolicy xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
          <Owner>
            <ID>5FHneW46xGXgs5mUiveU4sbTyGBzmstUspZC92UhjJM694ty</ID>
          </Owner>
          <AccessControlList>
            <Grant>
              <Grantee xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xsi:type="CanonicalUser">
                <ID>5FHneW46xGXgs5mUiveU4sbTyGBzmstUspZC92UhjJM694ty</ID>
              </Grantee>
              <Permission>READ</Permission>
            </Grant>
          </AccessControlList>
        </AccessControlPolicy>"""

        async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
            response = await client.put(
                "/my-bucket/my-key?acl",
                content=xml_body,
            )

        assert response.status_code == 200
        mock_repo.set_object_acl.assert_called_once()

    async def test_put_object_acl_with_nested_path(self) -> None:
        mock_service = AsyncMock()
        mock_repo = AsyncMock()
        mock_service.acl_repo = mock_repo

        app = FastAPI()
        app.state.acl_service = mock_service

        @app.middleware("http")
        async def mock_auth_middleware(request: Any, call_next: Any) -> Any:
            request.state.account_id = "owner-123"
            return await call_next(request)

        app.include_router(router)

        async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
            response = await client.put(
                "/my-bucket/folder/subfolder/key.txt?acl",
                headers={"x-amz-acl": "private"},
            )

        assert response.status_code == 200
        args = mock_repo.set_object_acl.call_args[0]
        assert args[1] == "folder/subfolder/key.txt"

    async def test_put_object_acl_without_auth_returns_403(self) -> None:
        app = FastAPI()
        app.state.acl_service = AsyncMock()
        app.include_router(router)

        async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
            response = await client.put(
                "/my-bucket/my-key?acl",
                headers={"x-amz-acl": "private"},
            )

        assert response.status_code == 403
        assert b"<Code>AccessDenied</Code>" in response.content

    async def test_put_object_acl_owner_mismatch_returns_403(self) -> None:
        mock_service = AsyncMock()

        app = FastAPI()
        app.state.acl_service = mock_service
        app.include_router(router)

        xml_body = """<?xml version="1.0" encoding="UTF-8"?>
        <AccessControlPolicy xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
          <Owner>
            <ID>different-owner</ID>
          </Owner>
          <AccessControlList>
            <Grant>
              <Grantee xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xsi:type="CanonicalUser">
                <ID>different-owner</ID>
              </Grantee>
              <Permission>READ</Permission>
            </Grant>
          </AccessControlList>
        </AccessControlPolicy>"""

        async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
            response = await client.put(
                "/my-bucket/my-key?acl",
                content=xml_body,
            )

        assert response.status_code == 403
        assert b"<Code>AccessDenied</Code>" in response.content
