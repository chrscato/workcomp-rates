#!/usr/bin/env python3
"""
Simple test script to validate the insights system fixes
No external dependencies required - uses only built-in Python modules
"""

import os
import sys
import time
import urllib.request
import urllib.error

def test_server():
    """Test if the Django server is running"""
    try:
        with urllib.request.urlopen("http://localhost:8000", timeout=5) as response:
            if response.status == 200:
                print("✅ Server is running")
                return True
    except Exception as e:
        print(f"❌ Server is not running: {str(e)}")
        print("   Please start the Django server: python manage.py runserver")
        return False

def test_insights_page(state_code="GA"):
    """Test the insights page for the specific error"""
    url = f"http://localhost:8000/commercial/insights/{state_code}/"
    
    try:
        print(f"🔍 Testing: {url}")
        start_time = time.time()
        
        with urllib.request.urlopen(url, timeout=30) as response:
            end_time = time.time()
            content = response.read().decode('utf-8')
            
            print(f"   Status: {response.status}")
            print(f"   Time: {end_time - start_time:.3f}s")
            
            if response.status == 200:
                # Check for the specific error message
                if "An error occurred while processing the data" in content:
                    print("   ❌ ERROR MESSAGE FOUND!")
                    print("   The navigation issue is still present.")
                    return False
                elif "The data for Georgia (GA) is not currently available" in content:
                    print("   ❌ DATA UNAVAILABLE MESSAGE FOUND!")
                    print("   The data loading issue is still present.")
                    return False
                else:
                    print("   ✅ No error messages found")
                    
                    # Check for expected content
                    if "filterForm" in content:
                        print("   ✅ Filter form present")
                    else:
                        print("   ⚠️  Filter form not found")
                    
                    return True
            else:
                print(f"   ❌ HTTP Error: {response.status}")
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
    
    # Step 1: Initial load
    print("\n1. Initial insights page load...")
    if not test_insights_page(state_code):
        return False
    
    # Step 2: Navigate away
    print("\n2. Navigating away to overview...")
    overview_url = f"http://localhost:8000/commercial/overview/{state_code}/simple/"
    try:
        with urllib.request.urlopen(overview_url, timeout=30) as response:
            print(f"   Status: {response.status}")
            if response.status == 200:
                print("   ✅ Overview loaded")
            else:
                print(f"   ❌ Overview failed: {response.status}")
    except Exception as e:
        print(f"   ❌ Overview error: {str(e)}")
    
    # Step 3: Navigate back (this is where the issue occurred)
    print("\n3. Navigating back to insights...")
    if not test_insights_page(state_code):
        print("❌ Return navigation failed - the issue is still present!")
        return False
    
    print("✅ Navigation flow test passed!")
    return True

def test_multiple_requests(state_code="GA"):
    """Test multiple requests to check for connection issues"""
    print(f"\n🔄 Testing multiple requests for {state_code}")
    print("=" * 50)
    
    url = f"http://localhost:8000/commercial/insights/{state_code}/"
    success_count = 0
    
    for i in range(3):
        print(f"\nRequest {i+1}/3...")
        try:
            start_time = time.time()
            with urllib.request.urlopen(url, timeout=30) as response:
                end_time = time.time()
                content = response.read().decode('utf-8')
                
                print(f"   Status: {response.status}")
                print(f"   Time: {end_time - start_time:.3f}s")
                
                if (response.status == 200 and 
                    "An error occurred while processing the data" not in content):
                    success_count += 1
                    print("   ✅ Success")
                else:
                    print("   ❌ Failed")
                    
        except Exception as e:
            print(f"   ❌ Error: {str(e)}")
        
        time.sleep(0.5)
    
    print(f"\n📊 Results: {success_count}/3 requests successful")
    return success_count == 3

def main():
    """Run all tests"""
    print("🧪 Insights System Fix Validation")
    print("=" * 60)
    
    # Check server
    if not test_server():
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
        print("🎉 All tests passed!")
        print("✅ The navigation issue has been fixed!")
        print("✅ Users can now navigate away and back without errors.")
    else:
        print("⚠️  Some tests failed.")
        print("❌ The navigation issue may still be present.")
        print("\n🔧 Next steps:")
        print("1. Check Django server logs for error details")
        print("2. Verify the fixes were applied correctly")
        print("3. Test manually in the browser")

if __name__ == "__main__":
    main()
