from django.db import models
from django.contrib.auth.models import User
from django.utils import timezone
from django.conf import settings

# Create your models here.

class UserActivity(models.Model):
    """Track user actions and page views with timestamps and URLs."""
    
    ACTION_CHOICES = [
        ('login', 'Login'),
        ('logout', 'Logout'),
        ('page_view', 'Page View'),
        ('search', 'Search'),
        ('download', 'Download'),
        ('export', 'Export Data'),
        ('rate_lookup', 'Rate Lookup'),
        ('insight_view', 'Insight View'),
    ]
    
    user = models.ForeignKey(User, on_delete=models.CASCADE, related_name='activities')
    action = models.CharField(max_length=50, choices=ACTION_CHOICES)
    timestamp = models.DateTimeField(auto_now_add=True, db_index=True)
    page_url = models.URLField(max_length=500, blank=True)
    page_title = models.CharField(max_length=200, blank=True)
    additional_data = models.JSONField(default=dict, blank=True)  # For storing extra context
    
    class Meta:
        verbose_name_plural = "User Activities"
        ordering = ['-timestamp']
        indexes = [
            models.Index(fields=['user', 'timestamp']),
            models.Index(fields=['action', 'timestamp']),
            models.Index(fields=['timestamp']),
        ]
    
    def __str__(self):
        return f"{self.user.username} - {self.action} - {self.timestamp}"
    
    @classmethod
    def log_activity(cls, user, action, page_url='', page_title='', **kwargs):
        """Convenience method to log user activity."""
        return cls.objects.create(
            user=user,
            action=action,
            page_url=page_url,
            page_title=page_title,
            additional_data=kwargs
        )
