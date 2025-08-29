from django.utils import timezone
import pytz

def timezone_context(request):
    """Add timezone context to all templates."""
    est_tz = pytz.timezone('America/New_York')
    current_est_time = timezone.now().astimezone(est_tz)
    
    return {
        'current_est_time': current_est_time,
        'est_timezone': 'America/New_York',
        'est_timezone_name': 'EST/EDT',
    }
