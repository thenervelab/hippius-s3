#!/usr/bin/env python3
"""Migration script for Hippius S3 database setup.

Handles both application database migrations and Hippius SDK keystore initialization.
"""

import logging
import os
import subprocess
import sys
import time
from pathlib import Path
from typing import Optional


# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="[%(levelname)s] %(message)s",
    stream=sys.stdout,
)
logger = logging.getLogger(__name__)


def wait_for_database(database_url: str, timeout: int = 60, interval: int = 2) -> None:
    """Wait for database to be available."""
    logger.info(f"Waiting for database at {mask_database_url(database_url)}")

    start_time = time.time()
    while time.time() - start_time < timeout:
        try:
            # Try to connect and run a simple query
            import asyncio

            import asyncpg

            async def test_connection() -> bool:
                conn = await asyncpg.connect(database_url)
                await conn.close()
                return True

            result = asyncio.run(test_connection())
            if result:
                logger.info("Database is ready")
                return

        except Exception:
            logger.debug("Database not ready yet, retrying...")

        time.sleep(interval)

    raise RuntimeError(f"Database not available after {timeout} seconds")


def run_command(command: list[str], cwd: Optional[str] = None, env: Optional[dict] = None) -> None:
    """Run a command and raise on failure."""
    logger.info(f"Running: {' '.join(command)}")
    result = subprocess.run(command, cwd=cwd, env=env, capture_output=True, text=True)

    if result.returncode != 0:
        logger.error(f"Command failed with return code {result.returncode}")
        if result.stdout:
            logger.error(f"STDOUT: {result.stdout}")
        if result.stderr:
            logger.error(f"STDERR: {result.stderr}")
        raise subprocess.CalledProcessError(result.returncode, command)


def mask_database_url(url: str) -> str:
    """Mask credentials in database URL for logging."""
    # Simple masking - replace password part
    if "://" in url and "@" in url:
        prefix, rest = url.split("://", 1)
        if ":" in rest and "@" in rest:
            user_pass, host_db = rest.split("@", 1)
            if ":" in user_pass:
                user, _ = user_pass.split(":", 1)
                return f"{prefix}://{user}:***@{host_db}"
    return url


def parse_database_url(url: str) -> dict[str, str]:
    """Parse database URL into individual components."""
    # Handle postgresql://user:pass@host:port/db?sslmode=disable
    if "://" not in url:
        raise ValueError(f"Invalid database URL format: {url}")

    prefix, rest = url.split("://", 1)
    if "@" not in rest:
        raise ValueError(f"Database URL missing credentials: {url}")

    user_pass, host_db = rest.split("@", 1)

    # Parse user and password
    if ":" in user_pass:
        user, password = user_pass.split(":", 1)
    else:
        user = user_pass
        password = ""

    # Parse host, port, and database
    if "/" in host_db:
        host_port, database = host_db.split("/", 1)
    else:
        host_port = host_db
        database = ""

    # Parse host and port
    if ":" in host_port:
        host, port = host_port.split(":", 1)
    else:
        host = host_port
        port = "5432"  # Default PostgreSQL port

    # Handle query parameters
    sslmode = "disable"  # Default
    if "?" in database:
        database, query = database.split("?", 1)
        if "sslmode=" in query:
            sslmode = query.split("sslmode=")[1].split("&")[0]

    return {
        "DB_HOST": host,
        "DB_PORT": port,
        "DB_USER": user,
        "DB_PASSWORD": password,
        "DB_NAME": database,
        "DB_SSLMODE": sslmode,
    }


def setup_keystore(key_store_url: str) -> None:
    """Set up Hippius SDK keystore."""
    logger.info(f"Setting up keystore with database: {mask_database_url(key_store_url)}")

    # Configure keystore
    run_command(["hippius", "config", "set", "key_storage", "enabled", "True"])
    run_command(["hippius", "config", "set", "key_storage", "database_url", key_store_url])

    # Initialize keystore schema via dbmate directly
    logger.info("Initializing keystore schema with dbmate")

    try:
        import hippius_sdk  # type: ignore

        migrations_dir = Path(hippius_sdk.__file__).parent / "db" / "migrations"
        if not migrations_dir.exists():
            raise FileNotFoundError(f"Keystore migrations directory not found at {migrations_dir}")

        # Run dbmate against the keystore migrations using the keystore DATABASE_URL
        env = os.environ.copy()
        env["DATABASE_URL"] = key_store_url
        run_command(["dbmate", f"--migrations-dir={str(migrations_dir)}", "up"], env=env)
        logger.info("Keystore migrations applied successfully")
    except Exception as e:
        logger.error(f"Keystore migration failed: {e}")
        raise


def main() -> None:
    """Main migration function."""
    logger.info("Starting Hippius S3 migration process")

    # Get environment variables
    database_url = os.environ.get("DATABASE_URL")
    if not database_url:
        logger.error("DATABASE_URL environment variable is required")
        sys.exit(1)

    key_store_url = os.environ.get("HIPPIUS_KEYSTORE_DATABASE_URL") or database_url
    skip_keystore = os.environ.get("SKIP_KEYSTORE_SETUP", "false").lower() == "true"

    # Log configuration
    logger.info(f"App database: {mask_database_url(database_url)}")
    logger.info(f"Keystore database: {mask_database_url(key_store_url)}")
    logger.info(f"Skip keystore setup: {skip_keystore}")

    # Wait for database
    wait_for_database(database_url)

    # Run application database migrations
    logger.info("Running application database migrations")
    run_command(["dbmate", "up"])

    # Set up keystore (unless skipped)
    if not skip_keystore:
        setup_keystore(key_store_url)
    else:
        logger.info("Skipping keystore setup as requested")

    # Configure IPFS if environment variables are set
    ipfs_api_urls = (os.environ.get("HIPPIUS_IPFS_API_URLS") or "").strip()
    if ipfs_api_urls:
        first_api_url = ipfs_api_urls.split(",", 1)[0].strip()
        logger.info(f"Setting IPFS API URL (first of HIPPIUS_IPFS_API_URLS): {first_api_url}")
        run_command(["hippius", "config", "set", "ipfs", "api_url", first_api_url])

    logger.info("Migration process completed successfully")


if __name__ == "__main__":
    main()
