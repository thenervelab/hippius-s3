"""Bucket Object Lock configuration (Tier 1).

GET /{bucket}?object-lock and PUT /{bucket}?object-lock.

Persistence only — no enforcement of retention/legal hold on DELETE/PUT (that's Tier 2,
which additionally requires S3 versioning). The configuration round-trips so backup
tools and SDKs that probe for Object Lock support get a believable answer. See
specs/s3-object-lock.md.

Storage shape in `buckets.object_lock` JSONB column:

  {"enabled": true}                               # bucket born with the lock-enabled header
  {"enabled": true, "mode": "GOVERNANCE",
   "days": 30}                                    # bucket with default retention rule
  {"enabled": true, "mode": "COMPLIANCE",
   "years": 1}                                    # default rule in years
  null                                            # never configured
"""

from __future__ import annotations

import json
import logging
from typing import Any
from typing import Final

from fastapi import Request
from fastapi import Response
from lxml import etree as ET  # ty: ignore[unresolved-import]

from hippius_s3.api.s3 import errors
from hippius_s3.repositories.buckets import BucketRepository
from hippius_s3.utils import get_query


logger = logging.getLogger(__name__)

_VALID_MODES: Final[frozenset[str]] = frozenset({"GOVERNANCE", "COMPLIANCE"})
_S3_NS: Final[str] = "http://s3.amazonaws.com/doc/2006-03-01/"


def _parse_object_lock_jsonb(raw: Any) -> dict[str, Any] | None:
    """Normalise the JSONB column value into a dict, regardless of asyncpg variant."""
    if raw is None:
        return None
    if isinstance(raw, dict):
        return raw
    if isinstance(raw, str):
        try:
            value = json.loads(raw)
        except json.JSONDecodeError:
            return None
        return value if isinstance(value, dict) else None
    return None


def _config_to_xml(config: dict[str, Any]) -> bytes:
    """Serialise the stored config dict into the AWS ObjectLockConfiguration XML."""
    root = ET.Element("ObjectLockConfiguration", xmlns=_S3_NS)
    if config.get("enabled"):
        ET.SubElement(root, "ObjectLockEnabled").text = "Enabled"
    if config.get("mode"):
        rule = ET.SubElement(root, "Rule")
        default_retention = ET.SubElement(rule, "DefaultRetention")
        ET.SubElement(default_retention, "Mode").text = str(config["mode"])
        if "days" in config:
            ET.SubElement(default_retention, "Days").text = str(config["days"])
        elif "years" in config:
            ET.SubElement(default_retention, "Years").text = str(config["years"])
    return ET.tostring(root, encoding="UTF-8", xml_declaration=True, pretty_print=True)


def _parse_request_xml(body: bytes) -> tuple[dict[str, Any] | None, Response | None]:
    """Parse the PUT request body. Returns (config, error_response).

    On success, `config` is the normalised dict to persist and `error_response` is None.
    On failure, `config` is None and `error_response` is the appropriate 400.
    """
    if not body:
        return None, errors.s3_error_response(
            "MalformedXML",
            "The XML you provided was not well-formed or did not validate against our published schema.",
            status_code=400,
        )
    try:
        root = ET.fromstring(body)
    except ET.XMLSyntaxError:
        return None, errors.s3_error_response(
            "MalformedXML",
            "The XML you provided was not well-formed or did not validate against our published schema.",
            status_code=400,
        )
    if ET.QName(root).localname != "ObjectLockConfiguration":
        return None, errors.s3_error_response(
            "MalformedXML",
            "The XML you provided was not well-formed or did not validate against our published schema.",
            status_code=400,
        )

    enabled_node = root.find(f"{{{_S3_NS}}}ObjectLockEnabled")
    if enabled_node is None:
        enabled_node = root.find("ObjectLockEnabled")
    rule_node = root.find(f"{{{_S3_NS}}}Rule")
    if rule_node is None:
        rule_node = root.find("Rule")

    config: dict[str, Any] = {}

    if enabled_node is not None:
        if (enabled_node.text or "").strip() != "Enabled":
            return None, errors.s3_error_response(
                "MalformedXML",
                "ObjectLockEnabled must be 'Enabled'.",
                status_code=400,
            )
        config["enabled"] = True

    if rule_node is not None:
        default_retention = rule_node.find(f"{{{_S3_NS}}}DefaultRetention")
        if default_retention is None:
            default_retention = rule_node.find("DefaultRetention")
        if default_retention is None:
            return None, errors.s3_error_response(
                "MalformedXML",
                "Rule requires a DefaultRetention element.",
                status_code=400,
            )

        mode_node = default_retention.find(f"{{{_S3_NS}}}Mode")
        if mode_node is None:
            mode_node = default_retention.find("Mode")
        days_node = default_retention.find(f"{{{_S3_NS}}}Days")
        if days_node is None:
            days_node = default_retention.find("Days")
        years_node = default_retention.find(f"{{{_S3_NS}}}Years")
        if years_node is None:
            years_node = default_retention.find("Years")

        mode_text = (mode_node.text or "").strip() if mode_node is not None else ""
        if mode_text not in _VALID_MODES:
            return None, errors.s3_error_response(
                "MalformedXML",
                "DefaultRetention.Mode must be GOVERNANCE or COMPLIANCE.",
                status_code=400,
            )

        if (days_node is not None) == (years_node is not None):
            return None, errors.s3_error_response(
                "MalformedXML",
                "DefaultRetention requires exactly one of Days or Years.",
                status_code=400,
            )

        period_value: int
        try:
            if days_node is not None:
                period_value = int((days_node.text or "").strip())
                if period_value <= 0:
                    raise ValueError
                config["mode"] = mode_text
                config["days"] = period_value
            else:
                assert years_node is not None
                period_value = int((years_node.text or "").strip())
                if period_value <= 0:
                    raise ValueError
                config["mode"] = mode_text
                config["years"] = period_value
        except (TypeError, ValueError):
            return None, errors.s3_error_response(
                "MalformedXML",
                "DefaultRetention.Days/Years must be a positive integer.",
                status_code=400,
            )

        # Implicit Enabled when a Rule is set (AWS behaviour).
        config["enabled"] = True

    if not config:
        return None, errors.s3_error_response(
            "MalformedXML",
            "ObjectLockConfiguration body did not contain ObjectLockEnabled or Rule.",
            status_code=400,
        )

    return config, None


async def handle_get_bucket_object_lock(bucket_name: str, db: Any, main_account_id: str) -> Response:
    bucket = await BucketRepository(db).get_by_name_and_owner(bucket_name, main_account_id)
    if not bucket:
        return errors.s3_error_response(
            "NoSuchBucket",
            f"The specified bucket {bucket_name} does not exist",
            status_code=404,
            BucketName=bucket_name,
        )

    config = _parse_object_lock_jsonb(bucket.get("object_lock"))
    if config is None or not config.get("enabled"):
        return errors.s3_error_response(
            "ObjectLockConfigurationNotFoundError",
            "Object Lock configuration does not exist for this bucket",
            status_code=404,
            BucketName=bucket_name,
        )

    xml_content = _config_to_xml(config)
    return Response(content=xml_content, media_type="application/xml", status_code=200)


async def handle_put_bucket_object_lock(bucket_name: str, request: Request, db: Any) -> Response:
    main_account_id = request.state.account.main_account
    bucket = await BucketRepository(db).get_by_name_and_owner(bucket_name, main_account_id)
    if not bucket:
        return errors.s3_error_response(
            "NoSuchBucket",
            f"The specified bucket {bucket_name} does not exist",
            status_code=404,
            BucketName=bucket_name,
        )

    body = await request.body()
    config, error_response = _parse_request_xml(body)
    if error_response is not None:
        return error_response
    assert config is not None

    await db.fetchrow(get_query("update_bucket_object_lock"), bucket["bucket_id"], json.dumps(config))
    logger.info("Set Object Lock configuration for bucket '%s': %s", bucket_name, config)
    return Response(status_code=200)
