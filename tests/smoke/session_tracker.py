import json
from datetime import datetime
from datetime import timedelta
from datetime import timezone


class SessionTracker:
    def __init__(self, s3_client, session_id, bucket_name):
        self.s3 = s3_client
        self.session_id = session_id
        self.bucket = bucket_name
        self.files = []

    def add_file(self, key, file_type, size, hash_md5, upload_time):
        self.files.append(
            {
                "key": key,
                "type": file_type,
                "size": size,
                "hash": hash_md5,
                "upload_time": upload_time,
            }
        )

    def get_files(self):
        return self.files

    def list_historical_sessions(self, max_count=20, retention_days=30):
        # Manifests outlive their data files (cleanup deletes data >retention_days
        # but keeps the manifest), so filter by session age here to avoid handing
        # back manifests pointing at already-swept files. Paginate so we don't
        # silently miss recent sessions once the bucket grows past 1000 keys.
        cutoff = datetime.now(timezone.utc) - timedelta(days=retention_days)
        paginator = self.s3.get_paginator("list_objects_v2")

        fresh_keys = []
        for page in paginator.paginate(Bucket=self.bucket, Prefix="smoke-test/.index/"):
            for obj in page.get("Contents", []) or []:
                key = obj["Key"]
                session_id = key.rsplit("/", 1)[-1].removesuffix(".json")
                try:
                    ts = datetime.strptime(session_id, "%Y%m%d-%H%M%S").replace(tzinfo=timezone.utc)
                except ValueError:
                    continue
                if ts >= cutoff:
                    fresh_keys.append(key)

        return sorted(fresh_keys)[-max_count:]

    def get_manifest(self, session_id):
        key = f"smoke-test/.index/{session_id}.json"
        obj = self.s3.get_object(Bucket=self.bucket, Key=key)
        return json.loads(obj["Body"].read())

    def save_manifest(self):
        manifest = {
            "session_id": self.session_id,
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "files": self.files,
        }

        key = f"smoke-test/.index/{self.session_id}.json"
        self.s3.put_object(
            Bucket=self.bucket, Key=key, Body=json.dumps(manifest, indent=2), ContentType="application/json"
        )
