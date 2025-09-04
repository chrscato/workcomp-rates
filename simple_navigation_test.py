#!/usr/bin/env python3
"""
Simple navigation test for insights system
Tests the specific scenario where user leaves insights and comes back
"""

import os
import sys
import time
import urllib.request
import urllib.error

def test_server_connection():
    """Test if the server is running"""
    try:
        with urllib.request.urlopen("http://localhost:8000", timeout=5) as response:
            if response.status == 200:
                print("✅ Server is running")
                return True
    except Exception as e:
        print(f"❌ Server is not running: {str(e)}")
        return False

def test_insights_page(state_code="GA"):
    """Test if the insights page loads correctly"""
    url = f"http://localhost:8000/commercial/insights/{state_code}/"
    
    try:
        print(f"🔍 Testing insights page: {url}")
        start_time = time.time()
        
        with urllib.request.urlopen(url, timeout=30) as response:
            end_time = time.time()
            content = response.read().decode('utf-8')
            
            print(f"   Status: {response.status}")
            print(f"   Response time: {end_time - start_time:.3f}s")
            
            if response.status == 200:
                # Check for key elements
                if "filterForm" in content:
                    print("   ✅ Filter form found")
                else:
                    print("   ❌ Filter form NOT found")
                
                if "Active Filters" in content:
                    print("   ✅ Active filters section found")
                else:
                    print("   ❌ Active filters section NOT found")
                
                if "An error occurred while processing the data" in content:
                    print("   ❌ ERROR MESSAGE FOUND!")
                    return False
                else:
                    print("   ✅ No error message found")
                
                return True
            else:
                print(f"   ❌ Page failed with status: {response.status}")
                return False
                
    except urllib.error.HTTPError as e:
        print(f"   ❌ HTTP Error: {e.code} - {e.reason}")
        return False
    except Exception as e:
        print(f"   ❌ Error: {str(e)}")
        return False

def test_navigation_flow(state_code="GA"):
    """Test the navigation flow that was causing issues"""
    print(f"\n🚀 Testing navigation flow for {state_code}")
    print("=" * 50)
    
    # Test 1: Initial insights page load
    print("\n1. Initial insights page load...")
    if not test_insights_page(state_code):
        print("❌ Initial load failed")
        return False
    
    # Test 2: Navigate away (to overview)
    print("\n2. Navigating away to overview...")
    overview_url = f"http://localhost:8000/commercial/overview/{state_code}/"
    try:
        with urllib.request.urlopen(overview_url, timeout=30) as response:
            print(f"   Status: {response.status}")
            if response.status == 200:
                print("   ✅ Overview page loaded")
            else:
                print(f"   ❌ Overview page failed: {response.status}")
    except Exception as e:
        print(f"   ❌ Overview error: {str(e)}")
    
    # Test 3: Navigate back to insights (this is where the issue occurred)
    print("\n3. Navigating back to insights...")
    if not test_insights_page(state_code):
        print("❌ Return navigation failed")
        return False
    
    print("\n✅ Navigation flow test completed successfully!")
    return True

def test_multiple_requests(state_code="GA"):
    """Test multiple consecutive requests to check for connection issues"""
    print(f"\n🔄 Testing multiple consecutive requests for {state_code}")
    print("=" * 50)
    
    url = f"http://localhost:8000/commercial/insights/{state_code}/"
    success_count = 0
    
    for i in range(5):
        print(f"\nRequest {i+1}/5...")
        try:
            start_time = time.time()
            with urllib.request.urlopen(url, timeout=30) as response:
                end_time = time.time()
                content = response.read().decode('utf-8')
                
                print(f"   Status: {response.status}")
                print(f"   Response time: {end_time - start_time:.3f}s")
                
                if response.status == 200 and "An error occurred while processing the data" not in content:
                    success_count += 1
                    print("   ✅ Success")
                else:
                    print("   ❌ Failed")
                    
        except Exception as e:
            print(f"   ❌ Error: {str(e)}")
        
        # Small delay between requests
        time.sleep(0.5)
    
    print(f"\n📊 Results: {success_count}/5 requests successful")
    return success_count == 5

def main():
    """Run all tests"""
    print("🧪 Insights Navigation Test")
    print("=" * 60)
    
    # Check server
    if not test_server_connection():
        print("\n❌ Server is not running. Please start Django server first:")
        print("   python manage.py runserver")
        return
    
    # Run tests
    tests_passed = 0
    total_tests = 2
    
    # Test 1: Navigation flow
    if test_navigation_flow("GA"):
        tests_passed += 1
    
    # Test 2: Multiple requests
    if test_multiple_requests("GA"):
        tests_passed += 1
    
    # Summary
    print("\n" + "=" * 60)
    print("📋 TEST SUMMARY")
    print("=" * 60)
    print(f"Tests passed: {tests_passed}/{total_tests}")
    
    if tests_passed == total_tests:
        print("🎉 All tests passed! Navigation should work correctly.")
    else:
        print("⚠️  Some tests failed. Check the output above for details.")
        print("\n🔧 If tests are failing, check:")
        print("1. Django server logs for error messages")
        print("2. Browser console for JavaScript errors")
        print("3. Network tab for failed requests")

if __name__ == "__main__":
    main()
