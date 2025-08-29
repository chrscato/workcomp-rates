from django.contrib import admin
from .models import UserActivity

@admin.register(UserActivity)
class UserActivityAdmin(admin.ModelAdmin):
    """Admin interface for UserActivity model."""
    
    list_display = ['user', 'action', 'timestamp', 'page_url', 'page_title']
    list_filter = ['action', 'timestamp', 'user']
    search_fields = ['user__username', 'user__email', 'page_url', 'page_title']
    readonly_fields = ['timestamp']
    date_hierarchy = 'timestamp'
    
    fieldsets = (
        ('User Information', {
            'fields': ('user', 'action')
        }),
        ('Page Information', {
            'fields': ('page_url', 'page_title')
        }),
        ('Timing', {
            'fields': ('timestamp',)
        }),
        ('Additional Data', {
            'fields': ('additional_data',),
            'classes': ('collapse',)
        }),
    )
    
    def has_add_permission(self, request):
        """Disable manual creation of activity records."""
        return False
    
    def has_change_permission(self, request, obj=None):
        """Disable editing of activity records."""
        return False
    
    def has_delete_permission(self, request, obj=None):
        """Allow deletion of activity records."""
        return True
