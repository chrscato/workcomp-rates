from django.core.management.base import BaseCommand
from django.contrib.auth.models import User
from core.models import UserActivity
from django.utils import timezone
from datetime import timedelta
from django.db import models

class Command(BaseCommand):
    help = 'View user activity data and statistics'
    
    def add_arguments(self, parser):
        parser.add_argument(
            '--user',
            type=str,
            help='Filter by specific username'
        )
        parser.add_argument(
            '--action',
            type=str,
            help='Filter by specific action type'
        )
        parser.add_argument(
            '--days',
            type=int,
            default=7,
            help='Show activity from last N days (default: 7)'
        )
        parser.add_argument(
            '--recent',
            type=int,
            default=20,
            help='Show N most recent activities (default: 20)'
        )
    
    def handle(self, *args, **options):
        username = options['user']
        action = options['action']
        days = options['days']
        recent = options['recent']
        
        # Calculate date range
        end_date = timezone.now()
        start_date = end_date - timedelta(days=days)
        
        # Build queryset
        activities = UserActivity.objects.filter(
            timestamp__gte=start_date,
            timestamp__lte=end_date
        )
        
        if username:
            activities = activities.filter(user__username=username)
        
        if action:
            activities = activities.filter(action=action)
        
        # Get recent activities
        recent_activities = activities.order_by('-timestamp')[:recent]
        
        # Get summary statistics
        total_activities = activities.count()
        unique_users = activities.values('user').distinct().count()
        action_counts = activities.values('action').annotate(
            count=models.Count('id')
        ).order_by('-count')
        
        self.stdout.write(
            self.style.SUCCESS(
                f'\n📊 User Activity Report (Last {days} days)'
            )
        )
        self.stdout.write('=' * 50)
        
        # Summary
        self.stdout.write(f'Total Activities: {total_activities}')
        self.stdout.write(f'Unique Users: {unique_users}')
        
        # Action breakdown
        self.stdout.write('\n📈 Action Breakdown:')
        for action_data in action_counts:
            self.stdout.write(
                f'  {action_data["action"]}: {action_data["count"]}'
            )
        
        # Recent activities
        self.stdout.write(f'\n🕒 Recent Activities (Last {recent}):')
        for activity in recent_activities:
            self.stdout.write(
                f'  {activity.timestamp.strftime("%Y-%m-%d %H:%M")} | '
                f'{activity.user.username} | {activity.action} | '
                f'{activity.page_url}'
            )
        
        # Top active users
        top_users = activities.values('user__username').annotate(
            count=models.Count('id')
        ).order_by('-count')[:5]
        
        self.stdout.write('\n👥 Top Active Users:')
        for user_data in top_users:
            self.stdout.write(
                f'  {user_data["user__username"]}: {user_data["count"]} activities'
            )
