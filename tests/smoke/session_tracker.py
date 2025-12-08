import json
from datetime import datetime


class SessionTracker:
    def __init__(self, s3_client, session_id, bucket_name):
        self.s3 = s3_client
        self.session_id = session_id
        self.bucket = bucket_name
        self.files = []

    def add_file(self, key, file_type, size, hash_md5, upload_time):
        self.files.append({
            "key": key,
            "type": file_type,
            "size": size,
            "hash": hash_md5,
            "upload_time": upload_time,
        })

    def get_files(self):
        return self.files

    def list_historical_sessions(self, max_count=20):
        response = self.s3.list_objects_v2(Bucket=self.bucket, Prefix="smoke-test/.index/")

        if "Contents" not in response:
            return []

        keys = sorted([obj["Key"] for obj in response["Contents"]])
        return keys[-max_count:]

    def get_manifest(self, session_id):
        key = f"smoke-test/.index/{session_id}.json"
        obj = self.s3.get_object(Bucket=self.bucket, Key=key)
        return json.loads(obj["Body"].read())

    def save_manifest(self):
        manifest = {"session_id": self.session_id, "timestamp": datetime.utcnow().isoformat(), "files": self.files}

        key = f"smoke-test/.index/{self.session_id}.json"
        self.s3.put_object(Bucket=self.bucket, Key=key, Body=json.dumps(manifest, indent=2), ContentType="application/json")
