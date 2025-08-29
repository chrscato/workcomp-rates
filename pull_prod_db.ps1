# PowerShell script to pull production database locally
# Usage: .\pull_prod_db.ps1

param(
    [string]$ProdServer = "root@134.209.13.85",
    [string]$ProdPath = "/var/www/workcomp-rates",
    [string]$LocalBackupDir = "db_backups"
)

Write-Host "🚀 Pulling production database locally..." -ForegroundColor Green

# Create backup directory
if (!(Test-Path $LocalBackupDir)) {
    New-Item -ItemType Directory -Path $LocalBackupDir | Out-Null
}

$Timestamp = Get-Date -Format "yyyyMMdd_HHmmss"

# Create backup of current local database
Write-Host "📦 Creating backup of current local database..." -ForegroundColor Yellow
if (Test-Path "db.sqlite3") {
    $BackupPath = Join-Path $LocalBackupDir "db.sqlite3.local.$Timestamp"
    Copy-Item "db.sqlite3" $BackupPath
    Write-Host "✅ Local database backed up to $BackupPath" -ForegroundColor Green
} else {
    Write-Host "⚠️  No local database found to backup" -ForegroundColor Yellow
}

# Download production database
Write-Host "🔄 Downloading production database..." -ForegroundColor Yellow
try {
    # Use scp to download the database
    $ScpCommand = "scp $ProdServer`:$ProdPath/db.sqlite3 db.sqlite3.prod"
    Invoke-Expression $ScpCommand
    
    if ($LASTEXITCODE -eq 0) {
        Write-Host "✅ Production database downloaded successfully" -ForegroundColor Green
        
        Write-Host "🔄 Replacing local database with production data..." -ForegroundColor Yellow
        Copy-Item "db.sqlite3.prod" "db.sqlite3"
        
        Write-Host "🧹 Cleaning up temporary files..." -ForegroundColor Yellow
        Remove-Item "db.sqlite3.prod"
        
        Write-Host "✅ Production database successfully pulled locally!" -ForegroundColor Green
        Write-Host "📊 You can now inspect users and sign-ins locally" -ForegroundColor Cyan
        Write-Host "💾 Local backup saved in: $LocalBackupDir" -ForegroundColor Cyan
        
        # Show database info
        Write-Host ""
        Write-Host "📋 Database information:" -ForegroundColor Cyan
        python manage.py shell -c "
from django.contrib.auth.models import User
from django.db import connection
import pytz
est_tz = pytz.timezone('America/New_York')

cursor = connection.cursor()
cursor.execute('SELECT COUNT(*) FROM auth_user')
user_count = cursor.fetchone()[0]
print(f'Total users: {user_count}')
print(f'Recent users:')
for user in User.objects.order_by('-date_joined')[:5]:
    est_joined = user.date_joined.astimezone(est_tz)
    print(f'  - {user.username} ({user.email}) - Joined: {est_joined.strftime(\"%Y-%m-%d %I:%M %p EST\")}')
"
        
    } else {
        Write-Host "❌ Failed to download production database" -ForegroundColor Red
        exit 1
    }
} catch {
    Write-Host "❌ Error downloading production database: $_" -ForegroundColor Red
    exit 1
}
