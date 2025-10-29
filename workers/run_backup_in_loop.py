#!/usr/bin/env python3
import logging
import os
import subprocess
import sys
import time
from datetime import datetime
from datetime import timezone
from pathlib import Path

import boto3
from botocore.exceptions import ClientError


sys.path.insert(0, str(Path(__file__).parent.parent))

from hippius_s3.config import get_config
from hippius_s3.monitoring import SimpleMetricsCollector
from hippius_s3.monitoring import initialize_metrics_simple


logging.basicConfig(level=logging.INFO, format="[%(asctime)s] %(levelname)s - %(message)s", datefmt="%Y-%m-%d %H:%M:%S")
logger = logging.getLogger(__name__)


class DatabaseBackup:
    def __init__(self, metrics_collector: SimpleMetricsCollector):
        self.metrics = metrics_collector
        self.db_host = os.getenv("DB_HOST", "db")
        self.db_port = os.getenv("DB_PORT", "5432")
        self.db_user = os.getenv("DB_USER", "postgres")
        self.db_password = os.getenv("DB_PASSWORD", "postgres")
        self.databases = ["hippius", "hippius_keys"]

        self.aws_access_key = os.getenv("AWS_BACKUP_ACCESS_KEY_ID")
        self.aws_secret_key = os.getenv("AWS_BACKUP_SECRET_ACCESS_KEY")
        self.aws_region = os.getenv("AWS_BACKUP_REGION", "us-east-1")
        self.aws_endpoint_url = os.getenv("AWS_BACKUP_ENDPOINT_URL") or None

        config = get_config()
        self.environment = config.environment

        self.bucket_name = f"s3-backups-{self.environment}"
        self.retention_count = int(os.getenv("BACKUP_RETENTION_COUNT", "30"))
        self.interval_hours = int(os.getenv("BACKUP_INTERVAL_HOURS", "24"))

        self._validate_config()
        self.s3_client = self._init_s3_client()

    def _validate_config(self):
        if not self.aws_access_key:
            logger.error("AWS_BACKUP_ACCESS_KEY_ID is required")
            sys.exit(1)
        if not self.aws_secret_key:
            logger.error("AWS_BACKUP_SECRET_ACCESS_KEY is required")
            sys.exit(1)

    def _init_s3_client(self):
        session = boto3.Session(
            aws_access_key_id=self.aws_access_key,
            aws_secret_access_key=self.aws_secret_key,
            region_name=self.aws_region,
        )

        client_kwargs = {}
        if self.aws_endpoint_url:
            client_kwargs["endpoint_url"] = self.aws_endpoint_url

        return session.client("s3", **client_kwargs)

    def ensure_bucket_exists(self):
        try:
            self.s3_client.head_bucket(Bucket=self.bucket_name)
            logger.info(f"Bucket '{self.bucket_name}' exists")
        except ClientError as e:
            error_code = e.response["Error"]["Code"]
            if error_code == "404":
                logger.info(f"Creating bucket '{self.bucket_name}'")
                create_kwargs = {"Bucket": self.bucket_name}
                if self.aws_region != "us-east-1":
                    create_kwargs["CreateBucketConfiguration"] = {"LocationConstraint": self.aws_region}
                self.s3_client.create_bucket(**create_kwargs)
                logger.info(f"Bucket '{self.bucket_name}' created")
            else:
                logger.error(f"Error checking bucket: {e}")
                raise

    def create_backup(self, database: str) -> tuple[str, Path]:
        timestamp = datetime.now(tz=timezone.utc).strftime("%Y%m%d_%H%M%S")
        backup_filename = f"{timestamp}_{database}.bak"
        backup_path = Path(f"/tmp/{backup_filename}")

        logger.info(f"Creating backup for database: {database}")

        start_time = time.time()
        env = os.environ.copy()
        env["PGPASSWORD"] = self.db_password

        cmd = [
            "pg_dump",
            "-h",
            self.db_host,
            "-p",
            self.db_port,
            "-U",
            self.db_user,
            "-d",
            database,
            "-f",
            str(backup_path),
        ]

        result = subprocess.run(cmd, env=env, capture_output=True, text=True)

        if result.returncode != 0:
            logger.error(f"Failed to create backup for {database}: {result.stderr}")
            raise Exception(f"pg_dump failed: {result.stderr}")

        backup_duration = time.time() - start_time
        backup_size = backup_path.stat().st_size
        size_mb = backup_size / (1024 * 1024)
        logger.info(f"Backup created: {backup_filename} ({size_mb:.2f} MB) in {backup_duration:.2f}s")

        return backup_filename, backup_path

    def upload_to_s3(self, backup_filename: str, backup_path: Path) -> float:
        logger.info(f"Uploading {backup_filename} to S3")

        start_time = time.time()
        self.s3_client.upload_file(str(backup_path), self.bucket_name, backup_filename)
        upload_duration = time.time() - start_time

        logger.info(
            f"Successfully uploaded {backup_filename} to s3://{self.bucket_name}/{backup_filename} in {upload_duration:.2f}s"
        )
        return upload_duration

    def cleanup_local_backup(self, backup_path: Path):
        if backup_path.exists():
            backup_path.unlink()
            logger.info(f"Cleaned up local backup: {backup_path.name}")

    def cleanup_old_backups(self, database: str):
        logger.info(f"Cleaning up old backups for {database} (keeping latest {self.retention_count})")

        response = self.s3_client.list_objects_v2(Bucket=self.bucket_name, Prefix="")

        if "Contents" not in response:
            logger.info("No backups found in S3")
            self.metrics.record_backup_cleanup(database, 0)
            return

        db_backups = [obj for obj in response["Contents"] if obj["Key"].endswith(f"_{database}.bak")]

        db_backups.sort(key=lambda x: x["LastModified"], reverse=True)

        if len(db_backups) <= self.retention_count:
            logger.info(f"Total backups: {len(db_backups)} (within retention limit)")
            self.metrics.record_backup_cleanup(database, 0)
            return

        backups_to_delete = db_backups[self.retention_count :]
        logger.info(f"Deleting {len(backups_to_delete)} old backups")

        for backup in backups_to_delete:
            self.s3_client.delete_object(Bucket=self.bucket_name, Key=backup["Key"])
            logger.info(f"Deleted old backup: {backup['Key']}")

        self.metrics.record_backup_cleanup(database, len(backups_to_delete))

    def run_backup_cycle(self):
        logger.info("Starting backup cycle")

        self.ensure_bucket_exists()

        cycle_success = True
        for database in self.databases:
            try:
                start_time = time.time()
                backup_filename, backup_path = self.create_backup(database)
                backup_duration = time.time() - start_time
                backup_size = backup_path.stat().st_size

                upload_duration = self.upload_to_s3(backup_filename, backup_path)

                self.metrics.record_backup_operation(
                    database_name=database,
                    success=True,
                    backup_duration=backup_duration,
                    backup_size_bytes=backup_size,
                    upload_duration=upload_duration,
                )

                self.cleanup_local_backup(backup_path)
                self.cleanup_old_backups(database)

            except Exception as e:
                logger.error(f"Failed to backup database {database}: {e}")
                cycle_success = False

                self.metrics.record_backup_operation(
                    database_name=database,
                    success=False,
                )

        if cycle_success:
            logger.info(
                f"Backup cycle completed. Total backups in bucket: {len(self.databases) * self.retention_count} (max)"
            )

        self.metrics.record_backup_cycle(success=cycle_success, num_databases=len(self.databases))

    def run(self):
        logger.info("Database Backup Service Started")
        logger.info(f"Environment: {self.environment}")
        logger.info(f"S3 Bucket: {self.bucket_name}")
        logger.info(f"Databases: {', '.join(self.databases)}")
        logger.info(f"Backup Interval: {self.interval_hours} hours")
        logger.info(f"Retention Count: {self.retention_count}")

        while True:
            self.run_backup_cycle()

            sleep_seconds = self.interval_hours * 3600
            logger.info(f"Sleeping for {self.interval_hours} hours until next backup")
            time.sleep(sleep_seconds)


if __name__ == "__main__":
    metrics = initialize_metrics_simple("hippius-s3-backup")
    backup = DatabaseBackup(metrics)
    backup.run()
