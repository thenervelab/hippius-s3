#!/bin/bash
set -e

# Deploy migration files to s3-staging and run the migration

echo "========================================="
echo "Deploying Migration Files to s3-staging"
echo "========================================="
echo ""

# Copy files to staging
echo "Copying files..."
scp migrate-wrapper.sh migration-objects-staging.txt s3-staging:~/hippius-s3/

echo "Making script executable..."
ssh s3-staging "chmod +x ~/hippius-s3/migrate-wrapper.sh"

echo ""
echo "✓ Files deployed successfully!"
echo ""
echo "========================================="
echo "Next Steps"
echo "========================================="
echo ""
echo "1. SSH to staging:"
echo "   ssh s3-staging"
echo ""
echo "2. Go to project directory:"
echo "   cd hippius-s3"
echo ""
echo "3. Run dry-run first:"
echo "   ./migrate-wrapper.sh migration-objects-staging.txt --dry-run"
echo ""
echo "4. Run actual migration:"
echo "   ./migrate-wrapper.sh migration-objects-staging.txt"
echo ""
echo "5. Verify results:"
echo "   docker compose exec -T db psql -U postgres -d hippius -c \\"
echo "   \"SELECT storage_version, COUNT(*) FROM object_versions ov"
echo "   JOIN objects o ON o.object_id = ov.object_id"
echo "   AND o.current_object_version = ov.object_version"
echo "   GROUP BY storage_version ORDER BY storage_version;\""
echo ""
echo "========================================="
