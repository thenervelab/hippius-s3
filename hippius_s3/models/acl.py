import re
from dataclasses import dataclass
from enum import Enum


class WellKnownGroups:
    """AWS S3 predefined group URIs for ACL grants."""

    ALL_USERS = "http://acs.amazonaws.com/groups/global/AllUsers"
    AUTHENTICATED_USERS = "http://acs.amazonaws.com/groups/global/AuthenticatedUsers"
    LOG_DELIVERY = "http://acs.amazonaws.com/groups/s3/LogDelivery"
    AWS_EC2 = "http://acs.amazonaws.com/groups/ec2/AmazonEC2"


class Permission(str, Enum):
    READ = "READ"
    WRITE = "WRITE"
    READ_ACP = "READ_ACP"
    WRITE_ACP = "WRITE_ACP"
    FULL_CONTROL = "FULL_CONTROL"


class GranteeType(str, Enum):
    CANONICAL_USER = "CanonicalUser"
    GROUP = "Group"
    AMAZON_CUSTOMER_BY_EMAIL = "AmazonCustomerByEmail"
    ACCESS_KEY = "AccessKey"


@dataclass
class Owner:
    id: str
    display_name: str | None = None


@dataclass
class Grantee:
    type: GranteeType
    id: str | None = None
    uri: str | None = None
    email_address: str | None = None
    display_name: str | None = None

    def __post_init__(self) -> None:
        if self.type == GranteeType.CANONICAL_USER and not self.id:
            raise ValueError("CanonicalUser grantee must have id")
        if self.type == GranteeType.ACCESS_KEY and not self.id:
            raise ValueError("AccessKey grantee must have id")
        if self.type == GranteeType.GROUP and not self.uri:
            raise ValueError("Group grantee must have uri")
        if self.type == GranteeType.AMAZON_CUSTOMER_BY_EMAIL and not self.email_address:
            raise ValueError("AmazonCustomerByEmail grantee must have email_address")


@dataclass
class Grant:
    grantee: Grantee
    permission: Permission


@dataclass
class ACL:
    owner: Owner
    grants: list[Grant]

    def to_dict(self) -> dict:
        return {
            "owner": {
                "id": self.owner.id,
                "display_name": self.owner.display_name,
            },
            "grants": [
                {
                    "grantee": {
                        "type": grant.grantee.type.value,
                        "id": grant.grantee.id,
                        "uri": grant.grantee.uri,
                        "email_address": grant.grantee.email_address,
                        "display_name": grant.grantee.display_name,
                    },
                    "permission": grant.permission.value,
                }
                for grant in self.grants
            ],
        }

    @classmethod
    def from_dict(cls, data: dict) -> "ACL":
        owner = Owner(
            id=data["owner"]["id"],
            display_name=data["owner"].get("display_name"),
        )
        grants = []
        for grant_data in data["grants"]:
            grantee_data = grant_data["grantee"]
            grantee = Grantee(
                type=GranteeType(grantee_data["type"]),
                id=grantee_data.get("id"),
                uri=grantee_data.get("uri"),
                email_address=grantee_data.get("email_address"),
                display_name=grantee_data.get("display_name"),
            )
            grant = Grant(
                grantee=grantee,
                permission=Permission(grant_data["permission"]),
            )
            grants.append(grant)
        return cls(owner=owner, grants=grants)


def validate_grant_grantees(acl: ACL) -> None:
    """Validate that all grantees in an ACL have valid IDs and URIs.

    Raises ValueError if any grantee has invalid data.
    """
    aws_canonical_id_pattern = re.compile(r"^[a-f0-9]{64}$")
    substrate_account_id_pattern = re.compile(r"^[1-9A-HJ-NP-Za-km-z]{47,48}$")
    access_key_pattern = re.compile(r"^hip_[a-zA-Z0-9_-]{1,240}$")
    valid_group_uris = {
        WellKnownGroups.ALL_USERS,
        WellKnownGroups.AUTHENTICATED_USERS,
        WellKnownGroups.LOG_DELIVERY,
        WellKnownGroups.AWS_EC2,
    }

    for grant in acl.grants:
        grantee = grant.grantee

        if grantee.type == GranteeType.CANONICAL_USER:
            if not grantee.id:
                raise ValueError("CanonicalUser grantee must have id")
            if not (aws_canonical_id_pattern.match(grantee.id) or substrate_account_id_pattern.match(grantee.id)):
                raise ValueError(
                    f"Invalid canonical user ID: {grantee.id}. Must be 64-character hex string or Substrate account ID."
                )

        elif grantee.type == GranteeType.ACCESS_KEY:
            if not grantee.id:
                raise ValueError("AccessKey grantee must have id")
            if not access_key_pattern.match(grantee.id):
                raise ValueError(
                    f"Invalid access key format: {grantee.id}. Must match pattern hip_[a-zA-Z0-9_-]{{1,240}}"
                )

        elif grantee.type == GranteeType.GROUP:
            if not grantee.uri:
                raise ValueError("Group grantee must have uri")
            if grantee.uri not in valid_group_uris:
                raise ValueError(f"Invalid group URI: {grantee.uri}. Must be one of: {', '.join(valid_group_uris)}")
