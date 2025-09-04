#!/usr/bin/env python3
"""
Debug script to test insights navigation issues
Tests the specific scenario where user leaves insights and comes back
"""

import os
import sys
import time
import json
import urllib.request
import urllib.parse
import urllib.error

# Add the project root to the path
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

class InsightsNavigationDebugger:
    def __init__(self, base_url="http://localhost:8000"):
        self.base_url = base_url
        self.cookie_jar = {}
    
    def make_request(self, url, timeout=30):
        """Make HTTP request using urllib"""
        try:
            req = urllib.request.Request(url)
            
            # Add cookies if we have them
            if self.cookie_jar:
                cookie_header = '; '.join([f"{k}={v}" for k, v in self.cookie_jar.items()])
                req.add_header('Cookie', cookie_header)
            
            start_time = time.time()
            with urllib.request.urlopen(req, timeout=timeout) as response:
                end_time = time.time()
                content = response.read().decode('utf-8')
                
                # Store cookies from response
                if 'Set-Cookie' in response.headers:
                    for cookie in response.headers.get_all('Set-Cookie'):
                        if '=' in cookie:
                            key, value = cookie.split('=', 1)
                            self.cookie_jar[key] = value.split(';')[0]
                
                return {
                    'status_code': response.status,
                    'content': content,
                    'response_time': end_time - start_time,
                    'success': True
                }
        except urllib.error.HTTPError as e:
            return {
                'status_code': e.code,
                'content': e.read().decode('utf-8') if e.fp else '',
                'response_time': 0,
                'success': False,
                'error': str(e)
            }
        except Exception as e:
            return {
                'status_code': 0,
                'content': '',
                'response_time': 0,
                'success': False,
                'error': str(e)
            }
        
    def test_navigation_flow(self, state_code="GA"):
        """Test the navigation flow that's causing issues"""
        print(f"🔍 Testing navigation flow for {state_code}...")
        
        # Step 1: Go to insights page
        print("\n1. Navigating to insights page...")
        insights_url = f"{self.base_url}/commercial/insights/{state_code}/"
        response = self.make_request(insights_url)
        print(f"   Status: {response['status_code']}")
        print(f"   Response time: {response['response_time']:.3f}s")
        
        if response['success'] and response['status_code'] == 200:
            print("   ✅ Insights page loaded successfully")
        else:
            print(f"   ❌ Insights page failed: {response['status_code']}")
            if 'error' in response:
                print(f"   Error: {response['error']}")
            return False
        
        # Step 2: Navigate away (simulate going to overview)
        print("\n2. Navigating away to overview page...")
        overview_url = f"{self.base_url}/commercial/overview/{state_code}/"
        try:
            response = self.session.get(overview_url, timeout=30)
            print(f"   Status: {response.status_code}")
            print(f"   Response time: {response.elapsed.total_seconds():.3f}s")
            
            if response.status_code == 200:
                print("   ✅ Overview page loaded successfully")
            else:
                print(f"   ❌ Overview page failed: {response.status_code}")
                
        except Exception as e:
            print(f"   ❌ Error loading overview page: {str(e)}")
        
        # Step 3: Navigate back to insights (this is where the issue occurs)
        print("\n3. Navigating back to insights page...")
        try:
            response = self.session.get(insights_url, timeout=30)
            print(f"   Status: {response.status_code}")
            print(f"   Response time: {response.elapsed.total_seconds():.3f}s")
            
            if response.status_code == 200:
                print("   ✅ Insights page loaded successfully on return")
                
                # Check if the page has the expected content
                if "filterForm" in response.text:
                    print("   ✅ Filter form found in response")
                else:
                    print("   ❌ Filter form NOT found in response")
                    
                if "Active Filters" in response.text:
                    print("   ✅ Active filters section found")
                else:
                    print("   ❌ Active filters section NOT found")
                    
                if "An error occurred while processing the data" in response.text:
                    print("   ❌ ERROR MESSAGE FOUND in response!")
                    return False
                else:
                    print("   ✅ No error message found")
                    
                return True
            else:
                print(f"   ❌ Insights page failed on return: {response.status_code}")
                return False
                
        except Exception as e:
            print(f"   ❌ Error loading insights page on return: {str(e)}")
            return False
    
    def test_with_filters(self, state_code="GA"):
        """Test navigation with filters applied"""
        print(f"\n🔍 Testing navigation with filters for {state_code}...")
        
        # Step 1: Go to insights page with filters
        print("\n1. Navigating to insights page with filters...")
        insights_url = f"{self.base_url}/commercial/insights/{state_code}/?payer=Aetna"
        try:
            response = self.session.get(insights_url, timeout=30)
            print(f"   Status: {response.status_code}")
            print(f"   Response time: {response.elapsed.total_seconds():.3f}s")
            
            if response.status_code == 200:
                print("   ✅ Filtered insights page loaded successfully")
            else:
                print(f"   ❌ Filtered insights page failed: {response.status_code}")
                return False
                
        except Exception as e:
            print(f"   ❌ Error loading filtered insights page: {str(e)}")
            return False
        
        # Step 2: Navigate away
        print("\n2. Navigating away...")
        overview_url = f"{self.base_url}/commercial/overview/{state_code}/"
        try:
            response = self.session.get(overview_url, timeout=30)
            print(f"   Status: {response.status_code}")
        except Exception as e:
            print(f"   ❌ Error: {str(e)}")
        
        # Step 3: Navigate back to insights (without filters)
        print("\n3. Navigating back to insights page (no filters)...")
        insights_url = f"{self.base_url}/commercial/insights/{state_code}/"
        try:
            response = self.session.get(insights_url, timeout=30)
            print(f"   Status: {response.status_code}")
            print(f"   Response time: {response.elapsed.total_seconds():.3f}s")
            
            if response.status_code == 200:
                print("   ✅ Insights page loaded successfully on return")
                
                if "An error occurred while processing the data" in response.text:
                    print("   ❌ ERROR MESSAGE FOUND in response!")
                    return False
                else:
                    print("   ✅ No error message found")
                    
                return True
            else:
                print(f"   ❌ Insights page failed on return: {response.status_code}")
                return False
                
        except Exception as e:
            print(f"   ❌ Error loading insights page on return: {str(e)}")
            return False
    
    def test_connection_recovery(self, state_code="GA"):
        """Test connection recovery after errors"""
        print(f"\n🔍 Testing connection recovery for {state_code}...")
        
        # Make multiple requests to test connection stability
        for i in range(5):
            print(f"\nRequest {i+1}/5...")
            try:
                url = f"{self.base_url}/commercial/insights/{state_code}/"
                response = self.session.get(url, timeout=30)
                print(f"   Status: {response.status_code}")
                print(f"   Response time: {response.elapsed.total_seconds():.3f}s")
                
                if response.status_code != 200:
                    print(f"   ❌ Request failed: {response.status_code}")
                    return False
                    
            except Exception as e:
                print(f"   ❌ Request error: {str(e)}")
                return False
            
            # Small delay between requests
            time.sleep(0.5)
        
        print("   ✅ All requests successful")
        return True
    
    def run_all_tests(self, state_code="GA"):
        """Run all navigation tests"""
        print(f"🚀 Starting Insights Navigation Debug Tests for {state_code}")
        print("=" * 60)
        
        results = []
        
        # Test 1: Basic navigation flow
        result1 = self.test_navigation_flow(state_code)
        results.append(("Basic Navigation", result1))
        
        # Test 2: Navigation with filters
        result2 = self.test_with_filters(state_code)
        results.append(("Navigation with Filters", result2))
        
        # Test 3: Connection recovery
        result3 = self.test_connection_recovery(state_code)
        results.append(("Connection Recovery", result3))
        
        # Summary
        print("\n" + "=" * 60)
        print("📋 NAVIGATION DEBUG SUMMARY")
        print("=" * 60)
        
        for test_name, result in results:
            status = "✅ PASS" if result else "❌ FAIL"
            print(f"{test_name}: {status}")
        
        total_passed = sum(1 for _, result in results if result)
        total_tests = len(results)
        
        print(f"\nOverall: {total_passed}/{total_tests} tests passed")
        
        if total_passed == total_tests:
            print("🎉 All tests passed! Navigation should work correctly.")
        else:
            print("⚠️  Some tests failed. Check the logs above for details.")
        
        return results

if __name__ == "__main__":
    # Check if server is running
    try:
        response = requests.get("http://localhost:8000", timeout=5)
        print("✅ Server is running")
    except:
        print("❌ Server is not running. Please start the Django server first.")
        print("   Run: python manage.py runserver")
        sys.exit(1)
    
    # Run navigation debug tests
    debugger = InsightsNavigationDebugger()
    results = debugger.run_all_tests("GA")
    
    print("\n🔧 If tests are failing, check:")
    print("1. Django server logs for error messages")
    print("2. Browser console for JavaScript errors")
    print("3. Network tab for failed requests")
    print("4. Database connection status")
