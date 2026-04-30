from datetime import datetime


def format_s3_timestamp(dt: datetime) -> str:
    """ISO-8601 with millisecond precision and trailing Z, matching AWS S3 responses."""
    return dt.strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"
