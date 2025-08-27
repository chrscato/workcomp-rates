#!/bin/bash

# WorkComp Rates Deployment Script
set -e

echo "🚀 Starting deployment..."

# Variables
PROJECT_DIR="/var/www/workcomp-rates"
BACKUP_DIR="/var/backups/workcomp-rates"
DATE=$(date +%Y%m%d_%H%M%S)

# Create backup
echo "📦 Creating backup..."
mkdir -p $BACKUP_DIR
if [ -d "$PROJECT_DIR" ]; then
    tar -czf "$BACKUP_DIR/backup_$DATE.tar.gz" -C $PROJECT_DIR .
fi

# Update code
echo " Updating code..."
cd $PROJECT_DIR
git pull origin main

# Install dependencies
echo "📦 Installing dependencies..."
uv sync

# Run migrations
echo "🗄️ Running migrations..."
uv run python manage.py migrate

# Collect static files
echo "📁 Collecting static files..."
uv run python manage.py collectstatic --noinput

# Restart services
echo "🔄 Restarting services..."
sudo systemctl restart workcomp-rates
sudo systemctl reload nginx

echo "✅ Deployment completed successfully!"