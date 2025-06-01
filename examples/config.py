"""
Configuration settings for MinIO client examples.
"""

# MinIO/S3 server configuration
MINIO_URL = "localhost:8000"
# MINIO_URL = "s3.hippius.com"

# Security settings
MINIO_SECURE = False  # Set to True for HTTPS
# MINIO_SECURE = True  # Uncomment for production/HTTPS

# Region setting
MINIO_REGION = "decentralized"
