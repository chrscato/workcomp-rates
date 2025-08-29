# User Activity Tracking System

This system automatically tracks user actions and page views with timestamps and URLs (no IP addresses).

## What Gets Tracked

### Automatic Tracking
- ✅ **Login/Logout Events**: Every time a user logs in or out
- ✅ **Page Views**: Every page a user visits (excluding static files)
- ✅ **Timestamps**: Precise timestamps for all activities
- ✅ **URLs**: The specific pages users visit
- ✅ **User Context**: Which user performed each action

### Action Types
- `login` - User login events
- `logout` - User logout events  
- `page_view` - Page navigation
- `search` - Search actions (when implemented)
- `download` - File downloads (when implemented)
- `export` - Data exports (when implemented)
- `rate_lookup` - Rate lookup actions (when implemented)
- `insight_view` - Insight page views (when implemented)

## How to Use

### 1. Pull Production Database Locally

**Option A: Git Bash (Windows)**
```bash
./pull_prod_db.sh
```

**Option B: PowerShell (Windows)**
```powershell
.\pull_prod_db.ps1
```

**Option C: Manual**
```bash
# SSH to production
ssh root@134.209.13.85

# Create backup
cd /var/www/workcomp-rates
cp db.sqlite3 db.sqlite3.backup.$(date +%Y%m%d_%H%M%S)

# From your local machine, download
scp root@134.209.13.85:/var/www/workcomp-rates/db.sqlite3 ./db.sqlite3.prod

# Replace local database
cp db.sqlite3.prod db.sqlite3
```

### 2. View User Activity Data

#### Web Dashboard (Staff Only)
- Navigate to `/admin/activity/` or use the "Activity Dashboard" link in your user menu
- Filter by date range (1, 7, 30, or 90 days)
- View summary statistics, action breakdowns, and recent activities

#### Django Admin
- Go to `/admin/` and look for "User Activities" section
- Filter, search, and export activity data
- View detailed information about each activity

#### Command Line
```bash
# View recent activity (last 7 days)
python manage.py view_user_activity

# View activity for specific user
python manage.py view_user_activity --user username

# View specific action type
python manage.py view_user_activity --action login

# View activity from last 30 days
python manage.py view_user_activity --days 30

# Show last 50 activities
python manage.py view_user_activity --recent 50
```

### 3. Database Queries

You can also query the data directly:

```python
# In Django shell: python manage.py shell
from core.models import UserActivity
from django.contrib.auth.models import User

# Get all activities for a user
user = User.objects.get(username='username')
activities = user.activities.all()

# Get recent logins
logins = UserActivity.objects.filter(action='login').order_by('-timestamp')[:10]

# Get page views from today
from django.utils import timezone
from datetime import timedelta
today = timezone.now().date()
page_views = UserActivity.objects.filter(
    action='page_view',
    timestamp__date=today
)

# Get most active users
from django.db.models import Count
active_users = UserActivity.objects.values('user__username').annotate(
    count=Count('id')
).order_by('-count')[:5]
```

## Database Schema

The `UserActivity` model stores:

- **user**: Foreign key to Django User
- **action**: Type of activity performed
- **timestamp**: When the activity occurred (stored in UTC, displayed in EST)
- **page_url**: URL of the page (for page views)
- **page_title**: Title of the page (for page views)
- **additional_data**: JSON field for extra context

## Timezone Information

- **Database Storage**: All timestamps are stored in UTC (Django best practice)
- **Display**: All timestamps are automatically converted to US Eastern Time (EST/EDT)
- **Timezone**: America/New_York (automatically handles daylight saving time)
- **Format**: MMM DD, YYYY HH:MM AM/PM EST (e.g., "Jan 15, 2025 02:30 PM EST")

## Adding Custom Tracking

To track additional user actions, use the `UserActivity.log_activity()` method:

```python
from core.models import UserActivity

# Track a custom action
UserActivity.log_activity(
    user=request.user,
    action='rate_lookup',
    page_url=request.path,
    page_title='Rate Lookup',
    search_term='workers comp',
    state='CA'
)
```

## Privacy & Security

- **No IP addresses** are stored
- **No personal data** beyond username is captured
- **Staff access only** for viewing activity data
- **Automatic cleanup** can be implemented for old records

## Performance Considerations

- Database indexes are created for efficient querying
- Middleware only tracks authenticated users
- Static files and admin pages are excluded from tracking
- Consider implementing data retention policies for production

## Troubleshooting

### No Activities Showing
1. Check if the middleware is enabled in `settings.py`
2. Verify signals are properly registered in `core/apps.py`
3. Ensure the database migration has been applied

### High Database Usage
1. Consider implementing data retention policies
2. Monitor the size of the `UserActivity` table
3. Implement cleanup tasks for old records

### Missing Page Titles
1. Set `request.page_title` in your views for custom titles
2. Default to URL path if no title is available
