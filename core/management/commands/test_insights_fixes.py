"""
Django management command to test insights system fixes
"""

from django.core.management.base import BaseCommand
from django.test import Client
from django.contrib.auth.models import User
from core.utils.parquet_utils import ParquetDataManager
import time

class Command(BaseCommand):
    help = 'Test insights system fixes for navigation issues'

    def add_arguments(self, parser):
        parser.add_argument(
            '--state',
            type=str,
            default='GA',
            help='State code to test (default: GA)'
        )

    def handle(self, *args, **options):
        state_code = options['state'].upper()
        
        self.stdout.write(f"🧪 Testing insights system fixes for {state_code}")
        self.stdout.write("=" * 60)
        
        # Test 1: Data Manager Initialization
        self.stdout.write("\n1. Testing data manager initialization...")
        try:
            data_manager = ParquetDataManager(state=state_code)
            if data_manager.has_data:
                self.stdout.write(self.style.SUCCESS("   ✅ Data manager initialized successfully"))
            else:
                self.stdout.write(self.style.ERROR("   ❌ Data file not found"))
                return
        except Exception as e:
            self.stdout.write(self.style.ERROR(f"   ❌ Data manager initialization failed: {str(e)}"))
            return
        
        # Test 2: Connection Pool
        self.stdout.write("\n2. Testing connection pool...")
        try:
            # Test multiple data manager instances
            dm1 = ParquetDataManager(state=state_code)
            dm2 = ParquetDataManager(state=state_code)
            
            # Test connection
            con1 = dm1._get_connection()
            con2 = dm2._get_connection()
            
            if con1 and con2:
                self.stdout.write(self.style.SUCCESS("   ✅ Connection pool working"))
            else:
                self.stdout.write(self.style.ERROR("   ❌ Connection pool failed"))
        except Exception as e:
            self.stdout.write(self.style.ERROR(f"   ❌ Connection pool test failed: {str(e)}"))
        
        # Test 3: Cache Key Generation
        self.stdout.write("\n3. Testing cache key generation...")
        try:
            filters1 = {"payer": ["Aetna"], "procedure_class": ["Surgery"]}
            filters2 = {"procedure_class": ["Surgery"], "payer": ["Aetna"]}
            
            key1 = ParquetDataManager.generate_cache_key(state_code, filters1)
            key2 = ParquetDataManager.generate_cache_key(state_code, filters2)
            
            if key1 == key2:
                self.stdout.write(self.style.SUCCESS("   ✅ Cache key generation is consistent"))
            else:
                self.stdout.write(self.style.ERROR("   ❌ Cache key generation is inconsistent"))
        except Exception as e:
            self.stdout.write(self.style.ERROR(f"   ❌ Cache key generation test failed: {str(e)}"))
        
        # Test 4: Data Loading
        self.stdout.write("\n4. Testing data loading...")
        try:
            # Test getting unique values
            payers = data_manager.get_unique_values('payer')
            orgs = data_manager.get_unique_values('org_name')
            
            if payers and orgs:
                self.stdout.write(self.style.SUCCESS(f"   ✅ Data loading successful ({len(payers)} payers, {len(orgs)} orgs)"))
            else:
                self.stdout.write(self.style.ERROR("   ❌ Data loading failed"))
        except Exception as e:
            self.stdout.write(self.style.ERROR(f"   ❌ Data loading test failed: {str(e)}"))
        
        # Test 5: Web Interface (if user exists)
        self.stdout.write("\n5. Testing web interface...")
        try:
            client = Client()
            
            # Create a test user if none exists
            if not User.objects.exists():
                User.objects.create_user('testuser', 'test@example.com', 'testpass')
                self.stdout.write("   Created test user")
            
            # Login
            login_success = client.login(username='testuser', password='testpass')
            if not login_success:
                self.stdout.write(self.style.ERROR("   ❌ Login failed"))
                return
            
            # Test insights page
            response = client.get(f'/commercial/insights/{state_code}/')
            
            if response.status_code == 200:
                content = response.content.decode('utf-8')
                if "An error occurred while processing the data" in content:
                    self.stdout.write(self.style.ERROR("   ❌ Error message found in response"))
                else:
                    self.stdout.write(self.style.SUCCESS("   ✅ Web interface working"))
            elif response.status_code == 302:
                self.stdout.write(self.style.WARNING("   ⚠️  Redirect (302) - likely authentication issue"))
            else:
                self.stdout.write(self.style.ERROR(f"   ❌ Web interface failed: {response.status_code}"))
                
        except Exception as e:
            self.stdout.write(self.style.ERROR(f"   ❌ Web interface test failed: {str(e)}"))
        
        # Test 6: Navigation Simulation
        self.stdout.write("\n6. Testing navigation simulation...")
        try:
            # Simulate navigation: insights -> overview -> insights
            response1 = client.get(f'/commercial/insights/{state_code}/')
            response2 = client.get(f'/commercial/overview/{state_code}/simple/')  # Use correct URL
            response3 = client.get(f'/commercial/insights/{state_code}/')
            
            self.stdout.write(f"   Response 1 (insights): {response1.status_code}")
            self.stdout.write(f"   Response 2 (overview): {response2.status_code}")
            self.stdout.write(f"   Response 3 (insights): {response3.status_code}")
            
            if (response1.status_code == 200 and 
                response2.status_code == 200 and 
                response3.status_code == 200):
                
                content3 = response3.content.decode('utf-8')
                if "An error occurred while processing the data" in content3:
                    self.stdout.write(self.style.ERROR("   ❌ Navigation simulation failed - error message found"))
                else:
                    self.stdout.write(self.style.SUCCESS("   ✅ Navigation simulation successful"))
            else:
                self.stdout.write(self.style.ERROR("   ❌ Navigation simulation failed - HTTP errors"))
                
        except Exception as e:
            self.stdout.write(self.style.ERROR(f"   ❌ Navigation simulation test failed: {str(e)}"))
        
        # Cleanup
        self.stdout.write("\n7. Cleaning up...")
        try:
            ParquetDataManager.cleanup_connections()
            self.stdout.write(self.style.SUCCESS("   ✅ Cleanup completed"))
        except Exception as e:
            self.stdout.write(self.style.ERROR(f"   ❌ Cleanup failed: {str(e)}"))
        
        self.stdout.write("\n" + "=" * 60)
        self.stdout.write("🎯 Test completed! Check the results above.")
        self.stdout.write("\nIf all tests passed, the navigation issue should be fixed.")
        self.stdout.write("If any tests failed, check the Django logs for more details.")
