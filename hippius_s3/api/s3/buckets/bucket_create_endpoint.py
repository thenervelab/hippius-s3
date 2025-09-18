from __future__ import annotations

import json
import logging
import uuid
from datetime import datetime
from datetime import timezone
from typing import Any

import asyncpg
from fastapi import Request
from fastapi import Response
from lxml import etree as ET

from hippius_s3.api.s3 import errors
from hippius_s3.api.s3.buckets.bucket_policy_endpoint import set_bucket_policy
from hippius_s3.utils import get_query
from hippius_s3.utils import get_request_body


logger = logging.getLogger(__name__)


async def handle_create_bucket(bucket_name: str, request: Request, db: Any) -> Response:
    """
    Create a new bucket using S3 protocol (PUT /{bucket_name}).
    Also handles setting bucket tags (PUT /{bucket_name}?tagging=).
    Also handles setting bucket lifecycle (PUT /{bucket_name}?lifecycle=).

    This endpoint is compatible with the S3 protocol used by MinIO and other S3 clients.
    """
    # Check if this is a request to set bucket lifecycle
    if "lifecycle" in request.query_params:
        try:
            # Get user for user-scoped bucket lookup
            user = await db.fetchrow(
                get_query("get_or_create_user_by_main_account"),
                request.state.account.main_account,
                datetime.now(timezone.utc),
            )

            bucket = await db.fetchrow(
                get_query("get_bucket_by_name_and_owner"),
                bucket_name,
                user["main_account_id"],
            )

            if not bucket:
                return errors.s3_error_response(
                    "NoSuchBucket",
                    f"The specified bucket {bucket_name} does not exist",
                    status_code=404,
                    BucketName=bucket_name,
                )

            # Parse the XML lifecycle configuration
            xml_data = await get_request_body(request)

            # If no XML data provided, return a MalformedXML error
            if not xml_data:
                return errors.s3_error_response(
                    "MalformedXML",
                    "The XML you provided was not well-formed or did not validate against our published schema.",
                    status_code=400,
                )

            try:
                parsed_xml = ET.fromstring(xml_data)
                # Accept namespaced and non-namespaced lifecycle XML
                rules = parsed_xml.xpath(".//*[local-name()='Rule']")  # type: ignore[attr-defined]
                rule_ids = []
                for rule in rules:
                    id_nodes = rule.xpath("./*[local-name()='ID']")  # type: ignore[attr-defined]
                    if id_nodes and id_nodes[0] is not None and id_nodes[0].text:
                        rule_ids.append(id_nodes[0].text)

                # todo: For now, just acknowledge receipt
                logger.info(f"Received lifecycle configuration with {len(rules)} rules: {rule_ids}")
                logger.info(f"Setting lifecycle configuration for bucket '{bucket_name}' via S3 protocol")

                # Return success response - no content needed for PUT lifecycle
                return Response(status_code=200)

            except ET.XMLSyntaxError:
                return errors.s3_error_response(
                    "MalformedXML",
                    "The XML you provided was not well-formed or did not validate against our published schema.",
                    status_code=400,
                )

        except Exception:
            logger.exception("Error setting bucket lifecycle")
            return errors.s3_error_response(
                "InternalError",
                "We encountered an internal error. Please try again.",
                status_code=500,
            )

    # Check if this is a request to set bucket tags
    elif "tagging" in request.query_params:
        try:
            # Get user for user-scoped bucket lookup
            user = await db.fetchrow(
                get_query("get_or_create_user_by_main_account"),
                request.state.account.main_account,
                datetime.now(timezone.utc),
            )

            # First check if the bucket exists
            bucket = await db.fetchrow(
                get_query("get_bucket_by_name_and_owner"),
                bucket_name,
                user["main_account_id"],
            )

            if not bucket:
                return errors.s3_error_response(
                    "NoSuchBucket",
                    f"The specified bucket {bucket_name} does not exist",
                    status_code=404,
                    BucketName=bucket_name,
                )

            # Parse the XML tag data from the request
            xml_data = await get_request_body(request)
            tags: dict[str, str] = {}
            if xml_data:
                try:
                    # Parse XML using lxml with S3 namespace
                    root = ET.fromstring(xml_data)
                    ns = {"s3": "http://s3.amazonaws.com/doc/2006-03-01/"}

                    # Namespace-qualified selection
                    tag_elements = root.xpath(".//s3:Tag", namespaces=ns)  # type: ignore[attr-defined]

                    for tag_elem in tag_elements:
                        key_nodes = tag_elem.xpath("./s3:Key", namespaces=ns)  # type: ignore[attr-defined]
                        value_nodes = tag_elem.xpath("./s3:Value", namespaces=ns)  # type: ignore[attr-defined]
                        if (
                            key_nodes
                            and value_nodes
                            and key_nodes[0] is not None
                            and value_nodes[0] is not None
                            and key_nodes[0].text
                            and value_nodes[0].text
                        ):
                            tags[str(key_nodes[0].text)] = str(value_nodes[0].text)
                except Exception:
                    return errors.s3_error_response(
                        "MalformedXML",
                        "The XML you provided was not well-formed",
                        status_code=400,
                    )

            logger.info(f"Setting tags for bucket '{bucket_name}' via S3 protocol: {tags}")
            await db.fetchrow(
                get_query("update_bucket_tags"),
                bucket["bucket_id"],
                json.dumps(tags),
            )

            return Response(status_code=200)

        except Exception:
            logger.exception("Error setting bucket tags via S3 protocol")
            return errors.s3_error_response(
                "InternalError",
                "We encountered an internal error. Please try again.",
                status_code=500,
            )

    # Check if this is a request to set bucket policy
    elif "policy" in request.query_params:
        return await set_bucket_policy(bucket_name, request, db)

    # Handle standard bucket creation if not a tagging, lifecycle, or policy request
    else:
        try:
            # Reject ACLs to match AWS when ObjectOwnership is BucketOwnerEnforced
            acl_header = request.headers.get("x-amz-acl")
            if acl_header:
                return errors.s3_error_response(
                    "InvalidBucketAclWithObjectOwnership",
                    "Bucket cannot have ACLs set with ObjectOwnership's BucketOwnerEnforced setting",
                    status_code=400,
                )

            bucket_id = str(uuid.uuid4())
            created_at = datetime.now(timezone.utc)

            # Bucket public/private is managed via policy, not ACL
            is_public = False

            # Get or create user record for the main account
            main_account_id = request.state.account.main_account
            await db.fetchrow(
                get_query("get_or_create_user_by_main_account"),
                main_account_id,
                created_at,
            )

            logger.info(f"Creating bucket '{bucket_name}' via S3 protocol for account {main_account_id}")

            query = get_query("create_bucket")
            await db.fetchrow(
                query,
                bucket_id,
                bucket_name,
                created_at,
                is_public,
                main_account_id,
            )

            return Response(status_code=200)

        except asyncpg.UniqueViolationError:
            return errors.s3_error_response(
                "BucketAlreadyExists",
                f"The requested bucket {bucket_name} already exists",
                status_code=409,
                BucketName=bucket_name,
            )
        except Exception:
            logger.exception("Error creating bucket via S3 protocol")
            return errors.s3_error_response(
                "InternalError",
                "We encountered an internal error. Please try again.",
                status_code=500,
            )
