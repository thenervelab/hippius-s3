"""Bucket/object ?acl subresource handlers.

Dispatched from the bucket/object routers' query-param branches, exactly like
`tagging`/`lifecycle`/`policy`. (History: these were routes on the pre-merge
gateway app, then briefly a dispatcher middleware in the merged app; router
branches are the app's one idiom for S3 subresources.) The acl MIDDLEWARE is
what maps `?acl` to READ_ACP/WRITE_ACP and enforces permission before any of
these run; handlers only read/write grants.
"""

import logging
import xml.etree.ElementTree as ET

from fastapi import Request
from fastapi.responses import Response

from gateway.services.acl_service import ACLService
from gateway.utils.errors import s3_error_response
from hippius_s3.models.acl import ACL
from hippius_s3.models.acl import Grant
from hippius_s3.models.acl import Grantee
from hippius_s3.models.acl import GranteeType
from hippius_s3.models.acl import Owner
from hippius_s3.models.acl import Permission
from hippius_s3.models.acl import validate_grant_grantees


logger = logging.getLogger(__name__)

VALID_CANNED_ACLS = {
    "private",
    "public-read",
    "public-read-write",
    "authenticated-read",
    "log-delivery-write",
    "aws-exec-read",
    "bucket-owner-read",
    "bucket-owner-full-control",
}


def _find_element(parent: ET.Element, tag: str, ns: dict) -> ET.Element | None:
    """Find element with S3 namespace fallback."""
    elem = parent.find(f"s3:{tag}", ns)
    return elem if elem is not None else parent.find(tag)


def acl_to_xml(acl: ACL) -> str:
    """Convert ACL model to S3-compatible XML format."""
    root = ET.Element("AccessControlPolicy")
    root.set("xmlns", "http://s3.amazonaws.com/doc/2006-03-01/")

    owner_elem = ET.SubElement(root, "Owner")
    ET.SubElement(owner_elem, "ID").text = acl.owner.id
    if acl.owner.display_name:
        ET.SubElement(owner_elem, "DisplayName").text = acl.owner.display_name

    acl_list = ET.SubElement(root, "AccessControlList")

    for grant in acl.grants:
        grant_elem = ET.SubElement(acl_list, "Grant")

        grantee_elem = ET.SubElement(grant_elem, "Grantee")
        grantee_elem.set("xmlns:xsi", "http://www.w3.org/2001/XMLSchema-instance")

        if grant.grantee.type == GranteeType.CANONICAL_USER:
            grantee_elem.set("xsi:type", "CanonicalUser")
            ET.SubElement(grantee_elem, "ID").text = grant.grantee.id
            if grant.grantee.display_name:
                ET.SubElement(grantee_elem, "DisplayName").text = grant.grantee.display_name
        elif grant.grantee.type == GranteeType.ACCESS_KEY:
            grantee_elem.set("xsi:type", "AccessKey")
            ET.SubElement(grantee_elem, "ID").text = grant.grantee.id
        elif grant.grantee.type == GranteeType.GROUP:
            grantee_elem.set("xsi:type", "Group")
            ET.SubElement(grantee_elem, "URI").text = grant.grantee.uri
        elif grant.grantee.type == GranteeType.AMAZON_CUSTOMER_BY_EMAIL:
            grantee_elem.set("xsi:type", "AmazonCustomerByEmail")
            ET.SubElement(grantee_elem, "EmailAddress").text = grant.grantee.email_address

        ET.SubElement(grant_elem, "Permission").text = grant.permission.value

    xml_str = ET.tostring(root, encoding="unicode")
    return f'<?xml version="1.0" encoding="UTF-8"?>\n{xml_str}'


def parse_grant_header(header_value: str, permission: Permission) -> list[Grant]:
    """
    Parse grant header like: id="abc", uri="http://...", id="def"

    Returns list of Grant objects with the specified permission.
    Raises ValueError if format is invalid.
    """
    grants = []

    # Split by comma, but be careful about potential commas in values (though unlikely for IDs/URIs)
    # For now simple split is fine as per spec
    for part in header_value.split(","):
        part = part.strip()
        if not part:
            continue

        if part.startswith('id="') and part.endswith('"'):
            grantee_id = part[4:-1]
            if not grantee_id:
                raise ValueError("Grantee ID cannot be empty")
            grants.append(
                Grant(
                    grantee=Grantee(type=GranteeType.CANONICAL_USER, id=grantee_id),
                    permission=permission,
                )
            )

        elif part.startswith('uri="') and part.endswith('"'):
            uri = part[5:-1]
            if not uri:
                raise ValueError("Grantee URI cannot be empty")
            grants.append(
                Grant(
                    grantee=Grantee(type=GranteeType.GROUP, uri=uri),
                    permission=permission,
                )
            )

        elif part.startswith('emailAddress="') and part.endswith('"'):
            email = part[14:-1]
            if not email:
                raise ValueError("Grantee Email cannot be empty")
            grants.append(
                Grant(
                    grantee=Grantee(type=GranteeType.AMAZON_CUSTOMER_BY_EMAIL, email_address=email),
                    permission=permission,
                )
            )

        elif part.startswith('accessKey="') and part.endswith('"'):
            access_key_id = part[11:-1]
            if not access_key_id:
                raise ValueError("Access key ID cannot be empty")
            if not access_key_id.startswith("hip_"):
                raise ValueError(f"Invalid access key format: {access_key_id}")
            grants.append(
                Grant(
                    grantee=Grantee(type=GranteeType.ACCESS_KEY, id=access_key_id),
                    permission=permission,
                )
            )

        else:
            raise ValueError(f"Invalid grant header format: {part}")

    return grants


def xml_to_acl(xml_content: str) -> ACL:
    """Parse S3 ACL XML to ACL model."""
    root = ET.fromstring(xml_content)

    ns = {"s3": "http://s3.amazonaws.com/doc/2006-03-01/"}

    owner_elem = _find_element(root, "Owner", ns)
    if owner_elem is None:
        raise ValueError("Missing Owner element in ACL XML")

    owner_id_elem = _find_element(owner_elem, "ID", ns)
    if owner_id_elem is None or not (owner_id_elem.text and owner_id_elem.text.strip()):
        raise ValueError("Missing Owner ID in ACL XML")

    display_name_elem = _find_element(owner_elem, "DisplayName", ns)

    owner = Owner(
        id=owner_id_elem.text.strip(),
        display_name=display_name_elem.text.strip()
        if display_name_elem is not None and display_name_elem.text
        else None,
    )

    grants = []
    acl_list = _find_element(root, "AccessControlList", ns)

    if acl_list is not None:
        grant_list = acl_list.findall("s3:Grant", ns)
        if not grant_list:
            grant_list = acl_list.findall("Grant")

        for grant_elem in grant_list:
            grantee_elem = _find_element(grant_elem, "Grantee", ns)
            if grantee_elem is None:
                continue

            grantee_type = grantee_elem.get("{http://www.w3.org/2001/XMLSchema-instance}type")

            if grantee_type == "CanonicalUser":
                id_elem = _find_element(grantee_elem, "ID", ns)
                if id_elem is None or not id_elem.text:
                    continue

                display_elem = _find_element(grantee_elem, "DisplayName", ns)

                grantee = Grantee(
                    type=GranteeType.CANONICAL_USER,
                    id=id_elem.text.strip(),
                    display_name=display_elem.text.strip() if display_elem is not None and display_elem.text else None,
                )
            elif grantee_type == "Group":
                uri_elem = _find_element(grantee_elem, "URI", ns)
                if uri_elem is None or not uri_elem.text:
                    continue
                grantee = Grantee(
                    type=GranteeType.GROUP,
                    uri=uri_elem.text.strip(),
                )
            elif grantee_type == "AmazonCustomerByEmail":
                email_elem = _find_element(grantee_elem, "EmailAddress", ns)
                if email_elem is None or not email_elem.text:
                    continue
                grantee = Grantee(
                    type=GranteeType.AMAZON_CUSTOMER_BY_EMAIL,
                    email_address=email_elem.text.strip(),
                )
            elif grantee_type == "AccessKey":
                id_elem = _find_element(grantee_elem, "ID", ns)
                if id_elem is None or not id_elem.text:
                    continue
                grantee = Grantee(
                    type=GranteeType.ACCESS_KEY,
                    id=id_elem.text.strip(),
                )
            else:
                continue

            permission_elem = _find_element(grant_elem, "Permission", ns)
            if permission_elem is None or not permission_elem.text:
                continue

            permission = Permission(permission_elem.text.strip())

            grants.append(Grant(grantee=grantee, permission=permission))

    return ACL(owner=owner, grants=grants)


async def get_bucket_acl(bucket: str, request: Request) -> Response:
    """Get bucket ACL - S3 API compatible."""
    acl_service: ACLService = request.app.state.acl_service

    # get_effective_acl raises on unknown buckets — answer the S3 404 instead of a 500
    # (which doubled as a bucket-existence oracle for unauthenticated ?acl probes).
    if not await acl_service.get_bucket_owner(bucket):
        return s3_error_response(
            code="NoSuchBucket",
            message="The specified bucket does not exist",
            status_code=404,
        )

    effective_acl = await acl_service.get_effective_acl(bucket, None)

    xml_content = acl_to_xml(effective_acl)

    return Response(
        content=xml_content,
        media_type="application/xml",
        status_code=200,
    )


async def get_object_acl(bucket: str, key: str, request: Request) -> Response:
    """Get object ACL - S3 API compatible."""
    acl_service: ACLService = request.app.state.acl_service

    exists = await acl_service.acl_repo.object_exists(bucket, key)
    if not exists:
        return s3_error_response(
            code="NoSuchKey",
            message="The specified key does not exist.",
            status_code=404,
        )

    effective_acl = await acl_service.get_effective_acl(bucket, key)

    xml_content = acl_to_xml(effective_acl)

    return Response(
        content=xml_content,
        media_type="application/xml",
        status_code=200,
    )


async def put_bucket_acl(bucket: str, request: Request) -> Response:
    """Set bucket ACL - S3 API compatible. Handles both ?acl and x-amz-acl header."""
    x_amz_acl = request.headers.get("x-amz-acl")
    x_amz_grant_read = request.headers.get("x-amz-grant-read")
    x_amz_grant_write = request.headers.get("x-amz-grant-write")
    x_amz_grant_read_acp = request.headers.get("x-amz-grant-read-acp")
    x_amz_grant_write_acp = request.headers.get("x-amz-grant-write-acp")
    x_amz_grant_full_control = request.headers.get("x-amz-grant-full-control")

    account_id = getattr(request.state, "account_id", None)
    if not account_id:
        return s3_error_response(
            code="AccessDenied",
            message="Access Denied",
            status_code=403,
        )

    account = getattr(request.state, "account", None)
    if account and not account.delete:
        return s3_error_response(
            code="AccessDenied",
            message="Access Denied",
            status_code=403,
        )

    acl_service: ACLService = request.app.state.acl_service

    bucket_owner_id = await acl_service.get_bucket_owner(bucket)
    if not bucket_owner_id:
        return s3_error_response(
            code="NoSuchBucket",
            message="The specified bucket does not exist",
            status_code=404,
        )

    has_grant_headers = any(
        [x_amz_grant_read, x_amz_grant_write, x_amz_grant_read_acp, x_amz_grant_write_acp, x_amz_grant_full_control]
    )

    if x_amz_acl and has_grant_headers:
        return s3_error_response(
            code="InvalidRequest",
            message="Specifying both Canned ACLs and Header Grants is not allowed",
            status_code=400,
        )

    if x_amz_acl:
        try:
            new_acl = await acl_service.canned_acl_to_acl(x_amz_acl, bucket_owner_id, bucket)
        except ValueError as e:
            return s3_error_response(
                code="InvalidArgument",
                message=str(e),
                status_code=400,
            )
    elif has_grant_headers:
        grants = [
            Grant(
                grantee=Grantee(type=GranteeType.CANONICAL_USER, id=bucket_owner_id),
                permission=Permission.FULL_CONTROL,
            )
        ]

        try:
            if x_amz_grant_read:
                grants.extend(parse_grant_header(x_amz_grant_read, Permission.READ))
            if x_amz_grant_write:
                grants.extend(parse_grant_header(x_amz_grant_write, Permission.WRITE))
            if x_amz_grant_read_acp:
                grants.extend(parse_grant_header(x_amz_grant_read_acp, Permission.READ_ACP))
            if x_amz_grant_write_acp:
                grants.extend(parse_grant_header(x_amz_grant_write_acp, Permission.WRITE_ACP))
            if x_amz_grant_full_control:
                grants.extend(parse_grant_header(x_amz_grant_full_control, Permission.FULL_CONTROL))
        except ValueError as e:
            return s3_error_response(
                code="InvalidArgument",
                message=str(e),
                status_code=400,
            )

        new_acl = ACL(owner=Owner(id=bucket_owner_id), grants=grants)

        if len(new_acl.grants) > 100:
            return s3_error_response(
                code="InvalidArgument",
                message="ACL cannot have more than 100 grants",
                status_code=400,
            )
    else:
        body = await request.body()
        if not body:
            return s3_error_response(
                code="MalformedACLError",
                message="ACL XML body or x-amz-acl header required",
                status_code=400,
            )

        try:
            new_acl = xml_to_acl(body.decode("utf-8"))
        except ET.ParseError:
            return s3_error_response(
                code="MalformedACLError",
                message="The XML you provided was not well-formed",
                status_code=400,
            )
        except ValueError as e:
            return s3_error_response(
                code="MalformedACLError",
                message=str(e),
                status_code=400,
            )

        if len(new_acl.grants) > 100:
            return s3_error_response(
                code="InvalidArgument",
                message="ACL cannot have more than 100 grants",
                status_code=400,
            )

        # Preserve original owner - ownership is immutable in S3
        if new_acl.owner.id != bucket_owner_id:
            logger.warning(
                f"ACL owner mismatch for bucket {bucket}: XML has {new_acl.owner.id}, preserving original owner {bucket_owner_id}"
            )
            new_acl.owner.id = bucket_owner_id

    try:
        validate_grant_grantees(new_acl)
    except ValueError as e:
        return s3_error_response(
            code="InvalidArgument",
            message=str(e),
            status_code=400,
        )

    await acl_service.acl_repo.set_bucket_acl(bucket, bucket_owner_id, new_acl)

    await acl_service.invalidate_cache(bucket)

    return Response(status_code=200)


async def put_object_acl(bucket: str, key: str, request: Request) -> Response:
    """Set object ACL - S3 API compatible. Handles both ?acl and x-amz-acl header."""
    x_amz_acl = request.headers.get("x-amz-acl")
    x_amz_grant_read = request.headers.get("x-amz-grant-read")
    x_amz_grant_write = request.headers.get("x-amz-grant-write")
    x_amz_grant_read_acp = request.headers.get("x-amz-grant-read-acp")
    x_amz_grant_write_acp = request.headers.get("x-amz-grant-write-acp")
    x_amz_grant_full_control = request.headers.get("x-amz-grant-full-control")

    account_id = getattr(request.state, "account_id", None)
    if not account_id:
        return s3_error_response(
            code="AccessDenied",
            message="Access Denied",
            status_code=403,
        )

    account = getattr(request.state, "account", None)
    if account and not account.delete:
        return s3_error_response(
            code="AccessDenied",
            message="Access Denied",
            status_code=403,
        )

    acl_service: ACLService = request.app.state.acl_service

    exists = await acl_service.acl_repo.object_exists(bucket, key)
    if not exists:
        return s3_error_response(
            code="NoSuchKey",
            message="The specified key does not exist.",
            status_code=404,
        )

    current_acl = await acl_service.get_effective_acl(bucket, key)
    original_owner_id = current_acl.owner.id

    has_grant_headers = any(
        [x_amz_grant_read, x_amz_grant_write, x_amz_grant_read_acp, x_amz_grant_write_acp, x_amz_grant_full_control]
    )

    if x_amz_acl and has_grant_headers:
        return s3_error_response(
            code="InvalidRequest",
            message="Specifying both Canned ACLs and Header Grants is not allowed",
            status_code=400,
        )

    if x_amz_acl:
        try:
            new_acl = await acl_service.canned_acl_to_acl(x_amz_acl, original_owner_id, bucket)
        except ValueError as e:
            return s3_error_response(
                code="InvalidArgument",
                message=str(e),
                status_code=400,
            )
    elif has_grant_headers:
        grants = [
            Grant(
                grantee=Grantee(type=GranteeType.CANONICAL_USER, id=original_owner_id),
                permission=Permission.FULL_CONTROL,
            )
        ]

        try:
            if x_amz_grant_read:
                grants.extend(parse_grant_header(x_amz_grant_read, Permission.READ))
            if x_amz_grant_write:
                grants.extend(parse_grant_header(x_amz_grant_write, Permission.WRITE))
            if x_amz_grant_read_acp:
                grants.extend(parse_grant_header(x_amz_grant_read_acp, Permission.READ_ACP))
            if x_amz_grant_write_acp:
                grants.extend(parse_grant_header(x_amz_grant_write_acp, Permission.WRITE_ACP))
            if x_amz_grant_full_control:
                grants.extend(parse_grant_header(x_amz_grant_full_control, Permission.FULL_CONTROL))
        except ValueError as e:
            return s3_error_response(
                code="InvalidArgument",
                message=str(e),
                status_code=400,
            )

        new_acl = ACL(owner=Owner(id=original_owner_id), grants=grants)

        if len(new_acl.grants) > 100:
            return s3_error_response(
                code="InvalidArgument",
                message="ACL cannot have more than 100 grants",
                status_code=400,
            )
    else:
        body = await request.body()
        if not body:
            return s3_error_response(
                code="MalformedACLError",
                message="ACL XML body or x-amz-acl header required",
                status_code=400,
            )

        try:
            new_acl = xml_to_acl(body.decode("utf-8"))
        except ET.ParseError:
            return s3_error_response(
                code="MalformedACLError",
                message="The XML you provided was not well-formed",
                status_code=400,
            )
        except ValueError as e:
            return s3_error_response(
                code="MalformedACLError",
                message=str(e),
                status_code=400,
            )

        if len(new_acl.grants) > 100:
            return s3_error_response(
                code="InvalidArgument",
                message="ACL cannot have more than 100 grants",
                status_code=400,
            )

        # Preserve original owner - ownership is immutable in S3
        if new_acl.owner.id != original_owner_id:
            logger.warning(
                f"ACL owner mismatch for object {bucket}/{key}: XML has {new_acl.owner.id}, preserving original owner {original_owner_id}"
            )
            new_acl.owner.id = original_owner_id

    try:
        validate_grant_grantees(new_acl)
    except ValueError as e:
        return s3_error_response(
            code="InvalidArgument",
            message=str(e),
            status_code=400,
        )

    await acl_service.acl_repo.set_object_acl(bucket, key, original_owner_id, new_acl)

    await acl_service.invalidate_cache(bucket, key)

    return Response(status_code=200)


def invalid_canned_acl_response(x_amz_acl: str | None) -> Response | None:
    """400 for an unknown canned ACL, BEFORE the write happens — an orphan
    bucket/object with no ACL is worse than a 400. None when the header is
    absent or valid."""
    if x_amz_acl and x_amz_acl not in VALID_CANNED_ACLS:
        return s3_error_response(
            code="InvalidArgument",
            message=f"Invalid canned ACL: {x_amz_acl}",
            status_code=400,
        )
    return None


async def materialize_canned_object_acl(request: Request, bucket: str, key: str, x_amz_acl: str) -> None:
    """After a successful object write that carried x-amz-acl: persist the canned
    ACL for the object. Best-effort — the object exists either way and defaults to
    private, so a cache/DB blip here must not fail the completed write."""
    account_id = getattr(request.state, "account_id", None)
    if not account_id:
        return
    try:
        acl_svc: ACLService = request.app.state.acl_service
        new_acl = await acl_svc.canned_acl_to_acl(x_amz_acl, account_id, bucket)
        await acl_svc.acl_repo.set_object_acl(bucket, key, account_id, new_acl)
        await acl_svc.invalidate_cache(bucket, key)
        logger.info(f"Created {x_amz_acl} ACL for object {bucket}/{key}")
    except Exception as e:
        logger.error(f"Failed to create ACL for object {bucket}/{key}, defaulting to private: {e}")
