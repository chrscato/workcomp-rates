from django.db import models
from django.contrib.auth.models import User
from django.utils import timezone
from django.conf import settings
import sqlite3
import os
from django.db import connection

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


class TinRecord:
    """
    Model for tin_records table in tin_index.db
    This is not a Django model but a data access class for the external SQLite database
    """
    
    def __init__(self, tin_value, tin_type=None, organization_name=None, partition_path=None,
                 payer_slug=None, state=None, billing_class=None, procedure_set=None,
                 procedure_class=None, taxonomy_code=None, stat_area_name=None,
                 year=None, month=None, file_size_bytes=None, last_modified=None,
                 first_seen=None, last_seen=None):
        self.tin_value = tin_value
        self.tin_type = tin_type
        self.organization_name = organization_name
        self.partition_path = partition_path
        self.payer_slug = payer_slug
        self.state = state
        self.billing_class = billing_class
        self.procedure_set = procedure_set
        self.procedure_class = procedure_class
        self.taxonomy_code = taxonomy_code
        self.stat_area_name = stat_area_name
        self.year = year
        self.month = month
        self.file_size_bytes = file_size_bytes
        self.last_modified = last_modified
        self.first_seen = first_seen
        self.last_seen = last_seen
    
    @classmethod
    def get_db_connection(cls):
        """Get connection to tin_index.db"""
        db_path = os.path.join(settings.BASE_DIR, 'core', 'data', 'tin_index.db')
        return sqlite3.connect(db_path)
    
    @classmethod
    def search_by_tin(cls, tin_value):
        """
        Search for records by TIN value
        Converts TIN to 9-digit format and searches
        """
        # Clean and format TIN to 9 digits
        clean_tin = ''.join(filter(str.isdigit, str(tin_value)))
        if len(clean_tin) < 9:
            clean_tin = clean_tin.zfill(9)
        elif len(clean_tin) > 9:
            clean_tin = clean_tin[-9:]  # Take last 9 digits
        
        conn = cls.get_db_connection()
        cursor = conn.cursor()
        
        try:
            cursor.execute("""
                SELECT * FROM tin_records 
                WHERE tin_value = ? 
                ORDER BY last_seen DESC
            """, (clean_tin,))
            
            rows = cursor.fetchall()
            records = []
            
            for row in rows:
                record = cls(
                    tin_value=row[0],
                    tin_type=row[1],
                    organization_name=row[2],
                    partition_path=row[3],
                    payer_slug=row[4],
                    state=row[5],
                    billing_class=row[6],
                    procedure_set=row[7],
                    procedure_class=row[8],
                    taxonomy_code=row[9],
                    stat_area_name=row[10],
                    year=row[11],
                    month=row[12],
                    file_size_bytes=row[13],
                    last_modified=row[14],
                    first_seen=row[15],
                    last_seen=row[16]
                )
                records.append(record)
            
            return records
            
        finally:
            conn.close()
    
    @classmethod
    def search_by_organization_name(cls, organization_name, limit=50):
        """
        Search for records by organization name (partial match)
        """
        conn = cls.get_db_connection()
        cursor = conn.cursor()
        
        try:
            cursor.execute("""
                SELECT DISTINCT organization_name, tin_value, tin_type, payer_slug, 
                       billing_class, procedure_set, procedure_class, taxonomy_code, state
                FROM tin_records 
                WHERE organization_name LIKE ? 
                AND organization_name IS NOT NULL 
                AND organization_name != 'NaN'
                ORDER BY organization_name
                LIMIT ?
            """, (f'%{organization_name}%', limit))
            
            rows = cursor.fetchall()
            results = []
            
            for row in rows:
                result = {
                    'organization_name': row[0],
                    'tin_value': row[1],
                    'tin_type': row[2],
                    'payer_slug': row[3],
                    'billing_class': row[4],
                    'procedure_set': row[5],
                    'procedure_class': row[6],
                    'taxonomy_code': row[7],
                    'state': row[8]
                }
                results.append(result)
            
            return results
            
        finally:
            conn.close()
    
    @classmethod
    def get_provider_summary(cls, tin_value):
        """
        Get comprehensive summary of a provider by TIN
        """
        records = cls.search_by_tin(tin_value)
        
        if not records:
            return None
        
        # Aggregate data from all records
        summary = {
            'tin_value': tin_value,
            'tin_type': records[0].tin_type,
            'organization_name': records[0].organization_name,
            'payer_slugs': list(set([r.payer_slug for r in records if r.payer_slug])),
            'states': list(set([r.state for r in records if r.state])),
            'billing_classes': list(set([r.billing_class for r in records if r.billing_class])),
            'procedure_sets': list(set([r.procedure_set for r in records if r.procedure_set])),
            'procedure_classes': list(set([r.procedure_class for r in records if r.procedure_class])),
            'taxonomy_codes': list(set([r.taxonomy_code for r in records if r.taxonomy_code])),
            'stat_areas': list(set([r.stat_area_name for r in records if r.stat_area_name and r.stat_area_name != '__NULL__'])),
            'years': sorted(list(set([r.year for r in records if r.year]))),
            'months': sorted(list(set([r.month for r in records if r.month]))),
            'total_records': len(records),
            'first_seen': min([r.first_seen for r in records if r.first_seen]),
            'last_seen': max([r.last_seen for r in records if r.last_seen])
        }
        
        return summary
