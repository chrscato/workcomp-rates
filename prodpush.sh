#!/bin/bash

# Production Push Script for WorkComp Rates
# This script pushes to GitHub and deploys to production server

set -e  # Exit on any error

# Configuration
REPO_NAME="workcomp-rates"
SERVER_IP="134.209.13.85"
SERVER_USER="root"
SERVER_PATH="/var/www/workcomp-rates"
BRANCH="master"
TMUX_SESSION="workcomp-rates"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Function to print colored output
print_status() {
    echo -e "${BLUE}[INFO]${NC} $1"
}

print_success() {
    echo -e "${GREEN}[SUCCESS]${NC} $1"
}

print_warning() {
    echo -e "${YELLOW}[WARNING]${NC} $1"
}

print_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

# Check if we're in the right directory
if [ ! -f "manage.py" ]; then
    print_error "This script must be run from the Django project root directory"
    exit 1
fi

# Check if git is clean
if [ -n "$(git status --porcelain)" ]; then
    print_warning "You have uncommitted changes. Please commit them first."
    git status --short
    read -p "Do you want to continue anyway? (y/N): " -n 1 -r
    echo
    if [[ ! $REPLY =~ ^[Yy]$ ]]; then
        exit 1
    fi
fi

# Step 1: Push to GitHub
print_status "Pushing to GitHub..."
git add .
git commit -m "Auto-deploy: $(date '+%Y-%m-%d %H:%M:%S')" || {
    print_warning "No changes to commit"
}
git push origin $BRANCH
print_success "GitHub push completed"

# Step 2: Deploy to server
print_status "Deploying to production server..."
print_status "You will be prompted for the server password"

ssh $SERVER_USER@$SERVER_IP << 'EOF'
    set -e
    
    # Colors for server output
    RED='\033[0;31m'
    GREEN='\033[0;32m'
    YELLOW='\033[1;33m'
    BLUE='\033[0;34m'
    NC='\033[0m'
    
    print_status() {
        echo -e "${BLUE}[SERVER]${NC} $1"
    }
    
    print_success() {
        echo -e "${GREEN}[SERVER]${NC} $1"
    }
    
    print_error() {
        echo -e "${RED}[SERVER]${NC} $1"
    }
    
    cd /var/www/workcomp-rates
    
    # Check if git repository exists, if not clone it
    if [ ! -d ".git" ]; then
        print_status "Git repository not found. Cloning from GitHub..."
        cd /var/www
        rm -rf workcomp-rates
        git clone https://github.com/chrscato/workcomp-rates.git
        cd workcomp-rates
    fi
    
    # Backup current state
    print_status "Creating backup..."
    cp -r . ../workcomp-rates-backup-$(date +%Y%m%d-%H%M%S) 2>/dev/null || true
    
    # Pull latest changes
    print_status "Pulling latest changes from GitHub..."
    git fetch origin
    git reset --hard origin/master
    
    # Check if virtual environment exists, if not create it
    if [ ! -d ".venv" ]; then
        print_status "Virtual environment not found. Creating new one..."
        uv venv .venv
    fi
    
    # Install/update dependencies
    print_status "Updating dependencies..."
    source .venv/bin/activate
    uv sync
    
    # Run migrations
    print_status "Running database migrations..."
    python manage.py migrate --noinput
    
    # Collect static files
    print_status "Collecting static files..."
    python manage.py collectstatic --noinput
    
    # Restart Django in tmux session
    print_status "Restarting Django application in tmux..."
    
    # Kill existing tmux session if it exists
    tmux kill-session -t workcomp-rates 2>/dev/null || true
    
    # Create new tmux session and start Django
    tmux new-session -d -s workcomp-rates -c /var/www/workcomp-rates
    tmux send-keys -t workcomp-rates "source .venv/bin/activate" Enter
    tmux send-keys -t workcomp-rates "python manage.py runserver 0.0.0.0:8000" Enter
    
    print_success "Django restarted in tmux session 'workcomp-rates'"
    
    # Reload Caddy
    print_status "Reloading Caddy..."
    systemctl reload caddy
    
    # Health check
    print_status "Performing health check..."
    sleep 5
    if curl -f -s https://workcomp-rates.com > /dev/null; then
        print_success "Health check passed - site is responding"
    else
        print_error "Health check failed - site may not be responding"
        print_status "Checking tmux session status..."
        tmux list-sessions
        exit 1
    fi
    
    print_success "Deployment completed successfully!"
EOF

print_success "Production deployment completed!"
print_status "Site should be live at: https://workcomp-rates.com"

# Optional: Open the site
read -p "Do you want to open the site in your browser? (y/N): " -n 1 -r
echo
if [[ $REPLY =~ ^[Yy]$ ]]; then
    if command -v xdg-open > /dev/null; then
        xdg-open https://workcomp-rates.com
    elif command -v open > /dev/null; then
        open https://workcomp-rates.com
    else
        print_status "Please visit: https://workcomp-rates.com"
    fi
fi