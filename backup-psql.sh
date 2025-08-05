#!/bin/bash

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ENV_FILE="$SCRIPT_DIR/.env.db"
BACKUP_DIR="/home/ubuntu/hippius-s3/backups"

if [ ! -f "$ENV_FILE" ]; then
    echo "Error: .env.db file not found at $ENV_FILE"
    exit 1
fi

source "$ENV_FILE"

if [ -z "$DB_HOST" ] || [ -z "$DB_PORT" ] || [ -z "$DB_USER" ] || [ -z "$DB_PASSWORD" ]; then
    echo "Error: Missing database credentials in .env.db file"
    exit 1
fi

mkdir -p "$BACKUP_DIR"
LOG_FILE="$BACKUP_DIR/backup.log"

export PGPASSWORD="$DB_PASSWORD"

TIMESTAMP=$(date +"%Y%m%d_%H%M%S")

log_message() {
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] $1" | tee -a "$LOG_FILE"
}

log_message "Starting PostgreSQL backup"

DATABASES=("hippius" "hippius_keys")

for DB in "${DATABASES[@]}"; do
    BACKUP_FILE="$BACKUP_DIR/${TIMESTAMP}_${DB}.bak"
    log_message "Backing up database: $DB"

    if docker exec hippius-s3-db-1 pg_dump -U "$DB_USER" -d "$DB" > "$BACKUP_FILE" 2>/dev/null; then
        log_message "Successfully backed up $DB ($(du -sh "$BACKUP_FILE" | cut -f1))"
    else
        log_message "Error: Failed to create backup for $DB"
        rm -f "$BACKUP_FILE"
    fi
done

log_message "Backup completed"

BACKUP_COUNT=$(find "$BACKUP_DIR" -name "*.bak" 2>/dev/null | wc -l)
log_message "Total backups in directory: $BACKUP_COUNT"

if [ "$BACKUP_COUNT" -gt 30 ]; then
    log_message "Cleaning up old backups (keeping latest 30)..."
    find "$BACKUP_DIR" -name "*.bak" -type f -printf '%T@ %p\n' | sort -rn | tail -n +31 | cut -d' ' -f2- | xargs -r rm
    log_message "Cleanup completed"
fi

unset PGPASSWORD
