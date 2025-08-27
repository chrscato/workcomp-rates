#!/bin/bash

# WorkComp Rates Server Setup Script
set -e

echo "🖥️ Setting up WorkComp Rates server..."

# Update system
echo "📦 Updating system packages..."
apt update && apt upgrade -y

# Install required packages
echo "📦 Installing required packages..."
apt install -y python3.12 python3.12-venv python3-pip nginx git curl wget

# Install uv
echo "📦 Installing uv package manager..."
curl -LsSf https://astral.sh/uv/install.sh | sh
source ~/.bashrc

# Create application user
echo "👤 Creating application user..."
adduser --disabled-password --gecos "" www-data || true
usermod -aG sudo www-data

# Create application directory
echo "📁 Creating application directories..."
mkdir -p /var/www/workcomp-rates
chown www-data:www-data /var/www/workcomp-rates

# Create log directories
mkdir -p /var/log/workcomp-rates
chown www-data:www-data /var/log/workcomp-rates

# Create backup directory
mkdir -p /var/backups/workcomp-rates
chown www-data:www-data /var/backups/workcomp-rates

# Install Certbot for SSL
echo "🔒 Installing SSL certificate tools..."
apt install -y certbot python3-certbot-nginx

echo "✅ Server setup completed!"
echo "📝 Next steps:"
echo "1. Clone your repository to /var/www/workcomp-rates"
echo "2. Set up SSL certificates with: certbot --nginx -d workcomp-rates.com -d www.workcomp-rates.com"
echo "3. Configure and start services"