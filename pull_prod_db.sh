#!/bin/bash

# Script to pull production database locally
# Usage: ./pull_prod_db.sh

set -e

echo "🚀 Pulling production database locally..."

# Configuration
PROD_SERVER="root@134.209.13.85"
PROD_PATH="/var/www/workcomp-rates"
LOCAL_BACKUP_DIR="db_backups"
TIMESTAMP=$(date +%Y%m%d_%H%M%S)

# Create backup directory
mkdir -p $LOCAL_BACKUP_DIR

echo "📦 Creating backup of current local database..."
if [ -f "db.sqlite3" ]; then
    cp db.sqlite3 "$LOCAL_BACKUP_DIR/db.sqlite3.local.$TIMESTAMP"
    echo "✅ Local database backed up to $LOCAL_BACKUP_DIR/db.sqlite3.local.$TIMESTAMP"
else
    echo "⚠️  No local database found to backup"
fi

echo "🔄 Downloading production database..."
scp "$PROD_SERVER:$PROD_PATH/db.sqlite3" "db.sqlite3.prod"

if [ $? -eq 0 ]; then
    echo "✅ Production database downloaded successfully"
    
    echo "🔄 Replacing local database with production data..."
    cp "db.sqlite3.prod" "db.sqlite3"
    
    echo "🧹 Cleaning up temporary files..."
    rm "db.sqlite3.prod"
    
    echo "✅ Production database successfully pulled locally!"
    echo "📊 You can now inspect users and sign-ins locally"
    echo "💾 Local backup saved in: $LOCAL_BACKUP_DIR/"
    
    # Show database info
    echo ""
    echo "📋 Database information:"
    python manage.py shell -c "
from django.contrib.auth.models import User
from django.db import connection
cursor = connection.cursor()
cursor.execute('SELECT COUNT(*) FROM auth_user')
user_count = cursor.fetchone()[0]
print(f'Total users: {user_count}')
print(f'Recent users:')
for user in User.objects.order_by('-date_joined')[:5]:
    print(f'  - {user.username} ({user.email}) - Joined: {user.date_joined}')
"
    
else
    echo "❌ Failed to download production database"
    exit 1
fi
