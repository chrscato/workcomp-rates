from django.utils.deprecation import MiddlewareMixin
from .models import UserActivity

class UserActivityMiddleware(MiddlewareMixin):
    """Middleware to track user page views and actions."""
    
    def process_request(self, request):
        """Track page views for authenticated users."""
        if request.user.is_authenticated:
            # Skip tracking for static files and admin
            if not any(path in request.path for path in ['/static/', '/admin/', '/media/']):
                # Get page title from request if available
                page_title = getattr(request, 'page_title', '')
                
                UserActivity.log_activity(
                    user=request.user,
                    action='page_view',
                    page_url=request.path,
                    page_title=page_title or request.path
                )
        
        return None
