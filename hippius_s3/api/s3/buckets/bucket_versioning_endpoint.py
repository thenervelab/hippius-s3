from __future__ import annotations

import logging
from typing import Any

from fastapi import Request
from fastapi import Response

from hippius_s3.api.s3 import errors
from hippius_s3.api.s3.common.req import expected_bucket_owner_mismatch
from hippius_s3.repositories.buckets import BucketRepository
from hippius_s3.utils import get_query
from hippius_s3.xml_helpers import add_subelement
from hippius_s3.xml_helpers import create_element
from hippius_s3.xml_helpers import parse_untrusted_xml
from hippius_s3.xml_helpers import to_xml_bytes


logger = logging.getLogger(__name__)

S3_NS = "http://s3.amazonaws.com/doc/2006-03-01/"

# AWS's three states. `Suspended` is accepted by the parser so it can be rejected with a precise
# 501 rather than a generic "unknown status" — see handle_put_bucket_versioning.
STATUS_ENABLED = "Enabled"
STATUS_SUSPENDED = "Suspended"


def _no_such_bucket(bucket_name: str) -> Response:
    return errors.s3_error_response(
        "NoSuchBucket",
        f"The specified bucket {bucket_name} does not exist",
        status_code=404,
        BucketName=bucket_name,
    )


async def handle_get_bucket_versioning(
    bucket_name: str, db: Any, main_account_id: str, request: Request | None = None
) -> Response:
    bucket = await BucketRepository(db).get_by_name_and_owner(bucket_name, main_account_id)
    if not bucket:
        return _no_such_bucket(bucket_name)

    if request is not None and (denied := expected_bucket_owner_mismatch(request, bucket["main_account_id"])):
        return denied

    root = create_element("VersioningConfiguration", xmlns=S3_NS)
    # AWS omits <Status> entirely for a bucket that never enabled versioning; boto3 surfaces that
    # as a response dict with no "Status" key, which is what callers check.
    status = bucket["versioning_status"]
    if status:
        add_subelement(root, "Status", str(status))

    return Response(
        content=to_xml_bytes(root, pretty_print=False),
        media_type="application/xml",
        status_code=200,
    )


async def handle_put_bucket_versioning(bucket_name: str, request: Request, db: Any) -> Response:
    # NB: `main_account_id` is the STORAGE-ATTRIBUTION account — the bucket owner as resolved by the
    # ACL middleware, falling back to the caller (see api/middlewares/request_context.py). So this
    # lookup RESOLVES the bucket; it is not an ownership test, and cannot be used as one: for an
    # existing bucket it compares the owner against itself and always matches. Authorization for
    # this operation is the ACL middleware's WRITE_ACP grade — which is exactly what was missing
    # while `versioning` was absent from BUCKET_PUT_SUBRESOURCES and the request took the
    # CreateBucket bypass. Deliberately NOT tightened to the caller: a WRITE_ACP grantee who is not
    # the owner is legitimate delegation, and the ACL layer is the right place to decide that.
    bucket = await BucketRepository(db).get_by_name_and_owner(bucket_name, request.state.main_account_id)
    if not bucket:
        return _no_such_bucket(bucket_name)

    if denied := expected_bucket_owner_mismatch(request, bucket["main_account_id"]):
        return denied

    body = await request.body()
    if not body:
        return errors.s3_error_response(
            "MalformedXML",
            "The XML you provided was not well-formed or did not validate against our published schema.",
            status_code=400,
        )

    try:
        root = parse_untrusted_xml(body)
    except ValueError:
        logger.exception("Malformed XML for PutBucketVersioning")
        return errors.s3_error_response(
            "MalformedXML",
            "The XML you provided was not well-formed or did not validate against our published schema.",
            status_code=400,
        )

    # Match on local-name(): botocore namespaces the body, minio-go sends it bare, and real S3
    # accepts both. A namespace-qualified path would silently find no <Status>.
    nodes = root.xpath("./*[local-name()='Status']")
    status = str(nodes[0].text).strip() if nodes and nodes[0].text else ""

    if status == STATUS_SUSPENDED:
        return errors.s3_error_response(
            "NotImplemented",
            "Suspending versioning is not supported. A suspended bucket must replace its null "
            "version in place on write, which this gateway does not implement yet.",
            status_code=501,
            BucketName=bucket_name,
        )

    if status != STATUS_ENABLED:
        # Covers both an unknown status and an omitted one. An empty configuration would mean
        # "return to unversioned", which AWS forbids once a bucket has been enabled.
        return errors.s3_error_response(
            "IllegalVersioningConfigurationException",
            "The versioning configuration specified in the request is invalid.",
            status_code=400,
            BucketName=bucket_name,
        )

    await db.execute(get_query("set_bucket_versioning"), bucket["bucket_id"], STATUS_ENABLED)
    logger.info(f"Enabled versioning on bucket {bucket_name}")

    return Response(status_code=200)
