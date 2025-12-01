import json
import logging
from typing import Any


class AuditLogger:
    def __init__(
        self, logger_name: str = "audit", logger: logging.Logger | logging.LoggerAdapter | None = None
    ) -> None:
        self.logger = logger if logger is not None else logging.getLogger(logger_name)

    def should_skip(self, path: str, client_ip: str) -> bool:
        if path == "/metrics":
            return True

        is_internal_ip = client_ip in ("127.0.0.1", "localhost") or client_ip.startswith("172.")
        return bool(is_internal_ip and path in ("/", "/health"))

    def log_request(
        self,
        client_ip: str,
        user_agent: str,
        account_id: str,
        method: str,
        path: str,
        query_params: dict[str, Any],
        status_code: int,
        processing_time_ms: float,
        content_length: int | str,
        timestamp: float,
        ray_id: str = "no-ray-id",
    ) -> None:
        if self.should_skip(path, client_ip):
            return

        audit_data = {
            "ray_id": ray_id,
            "timestamp": timestamp,
            "client_ip": client_ip,
            "user_agent": user_agent,
            "account_id": account_id,
            "method": method,
            "path": path,
            "query_params": query_params,
            "status_code": status_code,
            "processing_time_ms": round(processing_time_ms, 2),
            "content_length": content_length,
        }

        if status_code >= 500:
            self.logger.error(f"S3_OPERATION_ERROR: {json.dumps(audit_data)}")
        elif status_code >= 400:
            self.logger.warning(f"S3_OPERATION_CLIENT_ERROR: {json.dumps(audit_data)}")
        else:
            self.logger.info(f"S3_OPERATION_SUCCESS: {json.dumps(audit_data)}")
