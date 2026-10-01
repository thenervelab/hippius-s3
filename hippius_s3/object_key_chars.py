from __future__ import annotations


# Object key characters to avoid (non-printable ASCII and problematic chars).
#
# `#` and `?` are here for the same concrete reason rather than as style: `ForwardService`
# interpolates the decoded path into a URL *string*, which httpx then re-parses, and both are
# delimiters there. A key sent as `report%3Fv1.txt` arrives at the api as `report` — so
# `report%3Fv1.txt` and `report%3Fv2.txt` are two distinct keys that both answer 200 and land on
# one object. `#` was already covered; `?` behaves identically and was not.
#
# This list lives here, not in input_validation. That module calls get_config() at import, and
# the prefix policy is imported by acl_service before the integration conftest loads dotenv.
OBJECT_KEY_AVOID_CHARS = (
    ["\\", "{", "}", "^", "%", "`", "[", "]", '"', "<", ">", "~", "#", "?", "|"]
    + [chr(i) for i in range(0, 32)]
    + [chr(127)]
)
