from django.test import TestCase, Client
from django.contrib.auth import get_user_model
from django.urls import reverse
from unittest.mock import patch, MagicMock

User = get_user_model()

class OverviewPageTests(TestCase):
    def setUp(self):
        self.client = Client()
        self.user = User.objects.create_user(
            username='testuser',
            password='testpass123'
        )
        self.client.login(username='testuser', password='testpass123')
        
    def test_overview_page_requires_login(self):
        """Test that overview page requires authentication"""
        self.client.logout()
        response = self.client.get(reverse('commercial_rate_insights_overview', args=['GA']))
        self.assertEqual(response.status_code, 302)  # Redirect to login
        
    def test_overview_page_url_pattern(self):
        """Test that overview page URL pattern works"""
        response = self.client.get(reverse('commercial_rate_insights_overview', args=['GA']))
        self.assertEqual(response.status_code, 200)
        
    def test_overview_page_template(self):
        """Test that overview page uses correct template"""
        response = self.client.get(reverse('commercial_rate_insights_overview', args=['GA']))
        self.assertTemplateUsed(response, 'core/commercial_rate_insights_overview.html')
        
    def test_overview_page_context(self):
        """Test that overview page has expected context variables"""
        with patch('core.views.ParquetDataManager') as mock_manager:
            # Mock the data manager to return sample data
            mock_instance = MagicMock()
            mock_manager.return_value = mock_instance
            mock_instance.get_overview_statistics.return_value = {
                'summary': {
                    'total_records': 1000,
                    'distinct_payers': 10,
                    'distinct_organizations': 50,
                    'distinct_procedure_sets': 25
                },
                'top_payers': [],
                'top_organizations': [],
                'top_procedure_sets': [],
                'data_coverage': {}
            }
            mock_instance.get_unique_values.return_value = ['Test1', 'Test2']
            mock_instance.get_sample_records.return_value = []
            
            # Mock static methods
            mock_manager.get_available_states.return_value = {'GA': 'available'}
            mock_manager.get_state_name.return_value = 'Georgia'
            
            response = self.client.get(reverse('commercial_rate_insights_overview', args=['GA']))
            
            self.assertEqual(response.status_code, 200)
            self.assertIn('overview_stats', response.context)
            self.assertIn('filtered_options', response.context)
            self.assertIn('state_code', response.context)
            self.assertIn('state_name', response.context)
            
    def test_overview_page_with_prefilters(self):
        """Test that overview page handles prefilter parameters"""
        with patch('core.views.ParquetDataManager') as mock_manager:
            mock_instance = MagicMock()
            mock_manager.return_value = mock_instance
            mock_instance.get_overview_statistics.return_value = {
                'summary': {'total_records': 1000},
                'top_payers': [],
                'top_organizations': [],
                'top_procedure_sets': [],
                'data_coverage': {}
            }
            mock_instance.get_unique_values.return_value = ['Test1']
            mock_instance.get_sample_records.return_value = []
            mock_manager.get_available_states.return_value = {'GA': 'available'}
            mock_manager.get_state_name.return_value = 'Georgia'
            
            # Test with prefilter parameters
            response = self.client.get(
                reverse('commercial_rate_insights_overview', args=['GA']),
                {'payer': 'TestPayer', 'org_name': 'TestOrg'}
            )
            
            self.assertEqual(response.status_code, 200)
            self.assertIn('active_prefilters', response.context)
            self.assertEqual(response.context['active_prefilters']['payer'], 'TestPayer')
            self.assertEqual(response.context['active_prefilters']['org_name'], 'TestOrg')

class URLPatternTests(TestCase):
    def test_overview_url_pattern(self):
        """Test that overview URL pattern is correctly configured"""
        from django.urls import reverse
        try:
            url = reverse('commercial_rate_insights_overview', args=['GA'])
            self.assertIn('/commercial/insights/GA/overview/', url)
        except:
            self.fail("Overview URL pattern not found")
            
    def test_url_ordering(self):
        """Test that overview URL comes before state insights URL"""
        from core.urls import urlpatterns
        
        overview_pattern = None
        state_pattern = None
        
        for pattern in urlpatterns:
            if 'overview' in str(pattern.pattern):
                overview_pattern = pattern
            elif 'state_code' in str(pattern.pattern) and 'overview' not in str(pattern.pattern):
                state_pattern = pattern
                
        self.assertIsNotNone(overview_pattern, "Overview pattern not found")
        self.assertIsNotNone(state_pattern, "State pattern not found")
        
        # Overview should come before state insights in URL patterns
        overview_index = list(urlpatterns).index(overview_pattern)
        state_index = list(urlpatterns).index(state_pattern)
        self.assertLess(overview_index, state_index, "Overview should come before state insights")
