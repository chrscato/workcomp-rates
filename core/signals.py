from django.db.models.signals import post_save
from django.dispatch import receiver
from django.contrib.auth.signals import user_logged_in, user_logged_out
from django.contrib.auth.models import User
from .models import UserActivity

@receiver(user_logged_in)
def log_user_login(sender, request, user, **kwargs):
    """Log when a user logs in."""
    UserActivity.log_activity(
        user=user,
        action='login',
        page_url=request.path if request else '',
        page_title='User Login'
    )

@receiver(user_logged_out)
def log_user_logout(sender, request, user, **kwargs):
    """Log when a user logs out."""
    UserActivity.log_activity(
        user=user,
        action='logout',
        page_url=request.path if request else '',
        page_title='User Logout'
    )
