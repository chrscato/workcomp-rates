#!/usr/bin/env python3
"""
Stress test for insights system caching and data reloading
Tests the issues identified in the caching and filtering system
"""

import os
import sys
import time
import requests
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
import json

# Add the project root to the path
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

class InsightsStressTest:
    def __init__(self, base_url="http://localhost:8000"):
        self.base_url = base_url
        self.session = requests.Session()
        self.results = []
        
    def test_rapid_filter_changes(self, state_code="GA", num_requests=20):
        """Test rapid filter changes to identify caching issues"""
        print(f"\n🧪 Testing rapid filter changes for {state_code}...")
        
        # Test different filter combinations
        filter_combinations = [
            {"payer": ["Aetna"]},
            {"payer": ["Aetna"], "procedure_class": ["Surgery"]},
            {"payer": ["Aetna"], "procedure_class": ["Surgery"], "org_name": ["Hospital A"]},
            {"procedure_class": ["Medicine"]},
            {"procedure_class": ["Medicine"], "billing_code": ["99213"]},
            {"org_name": ["Hospital B"]},
            {"org_name": ["Hospital B"], "payer": ["Blue Cross"]},
            {"cbsa": ["Atlanta"]},
            {"cbsa": ["Atlanta"], "procedure_set": ["Cardiology"]},
            {"tin_value": ["123456789"]},
        ]
        
        start_time = time.time()
        successful_requests = 0
        failed_requests = 0
        cache_hits = 0
        
        for i in range(num_requests):
            filter_combo = filter_combinations[i % len(filter_combinations)]
            
            try:
                # Build URL with filters
                params = []
                for key, values in filter_combo.items():
                    for value in values:
                        params.append(f"{key}={value}")
                
                url = f"{self.base_url}/commercial/insights/{state_code}/"
                if params:
                    url += "?" + "&".join(params)
                
                # Make request
                response = self.session.get(url, timeout=30)
                
                if response.status_code == 200:
                    successful_requests += 1
                    # Check if response time suggests cache hit (very fast)
                    if response.elapsed.total_seconds() < 0.1:
                        cache_hits += 1
                else:
                    failed_requests += 1
                    print(f"❌ Request {i+1} failed: {response.status_code}")
                
                # Small delay to simulate user behavior
                time.sleep(0.1)
                
            except Exception as e:
                failed_requests += 1
                print(f"❌ Request {i+1} error: {str(e)}")
        
        end_time = time.time()
        total_time = end_time - start_time
        
        print(f"📊 Rapid Filter Changes Results:")
        print(f"   Total Requests: {num_requests}")
        print(f"   Successful: {successful_requests}")
        print(f"   Failed: {failed_requests}")
        print(f"   Cache Hits: {cache_hits}")
        print(f"   Total Time: {total_time:.2f}s")
        print(f"   Avg Response Time: {total_time/num_requests:.3f}s")
        
        return {
            "test": "rapid_filter_changes",
            "total_requests": num_requests,
            "successful": successful_requests,
            "failed": failed_requests,
            "cache_hits": cache_hits,
            "total_time": total_time,
            "avg_response_time": total_time/num_requests
        }
    
    def test_concurrent_users(self, state_code="GA", num_users=10, requests_per_user=5):
        """Test concurrent users accessing the same data"""
        print(f"\n🧪 Testing {num_users} concurrent users...")
        
        def user_session(user_id):
            """Simulate a single user session"""
            user_results = []
            session = requests.Session()
            
            # Different filter combinations for each user
            filter_combinations = [
                {"payer": ["Aetna"]},
                {"procedure_class": ["Surgery"]},
                {"org_name": ["Hospital A"]},
                {"cbsa": ["Atlanta"]},
                {"procedure_set": ["Cardiology"]},
            ]
            
            for i in range(requests_per_user):
                filter_combo = filter_combinations[i % len(filter_combinations)]
                
                try:
                    # Build URL with filters
                    params = []
                    for key, values in filter_combo.items():
                        for value in values:
                            params.append(f"{key}={value}")
                    
                    url = f"{self.base_url}/commercial/insights/{state_code}/"
                    if params:
                        url += "?" + "&".join(params)
                    
                    start_time = time.time()
                    response = session.get(url, timeout=30)
                    end_time = time.time()
                    
                    user_results.append({
                        "user_id": user_id,
                        "request_id": i,
                        "status_code": response.status_code,
                        "response_time": end_time - start_time,
                        "success": response.status_code == 200
                    })
                    
                except Exception as e:
                    user_results.append({
                        "user_id": user_id,
                        "request_id": i,
                        "error": str(e),
                        "success": False
                    })
                
                # Small delay between requests
                time.sleep(0.2)
            
            return user_results
        
        # Run concurrent user sessions
        start_time = time.time()
        all_results = []
        
        with ThreadPoolExecutor(max_workers=num_users) as executor:
            futures = [executor.submit(user_session, i) for i in range(num_users)]
            
            for future in as_completed(futures):
                user_results = future.result()
                all_results.extend(user_results)
        
        end_time = time.time()
        total_time = end_time - start_time
        
        # Analyze results
        successful_requests = sum(1 for r in all_results if r.get("success", False))
        failed_requests = len(all_results) - successful_requests
        avg_response_time = sum(r.get("response_time", 0) for r in all_results) / len(all_results)
        
        print(f"📊 Concurrent Users Results:")
        print(f"   Users: {num_users}")
        print(f"   Requests per User: {requests_per_user}")
        print(f"   Total Requests: {len(all_results)}")
        print(f"   Successful: {successful_requests}")
        print(f"   Failed: {failed_requests}")
        print(f"   Total Time: {total_time:.2f}s")
        print(f"   Avg Response Time: {avg_response_time:.3f}s")
        
        return {
            "test": "concurrent_users",
            "users": num_users,
            "requests_per_user": requests_per_user,
            "total_requests": len(all_results),
            "successful": successful_requests,
            "failed": failed_requests,
            "total_time": total_time,
            "avg_response_time": avg_response_time
        }
    
    def test_cache_consistency(self, state_code="GA"):
        """Test cache consistency with identical requests"""
        print(f"\n🧪 Testing cache consistency...")
        
        # Make identical requests multiple times
        filter_params = "payer=Aetna&procedure_class=Surgery"
        url = f"{self.base_url}/commercial/insights/{state_code}/?{filter_params}"
        
        response_times = []
        cache_hits = 0
        
        for i in range(10):
            try:
                start_time = time.time()
                response = self.session.get(url, timeout=30)
                end_time = time.time()
                
                response_time = end_time - start_time
                response_times.append(response_time)
                
                # Consider response time < 0.1s as potential cache hit
                if response_time < 0.1:
                    cache_hits += 1
                
                print(f"   Request {i+1}: {response_time:.3f}s (Status: {response.status_code})")
                
            except Exception as e:
                print(f"   Request {i+1}: Error - {str(e)}")
        
        avg_response_time = sum(response_times) / len(response_times)
        cache_hit_rate = cache_hits / len(response_times) * 100
        
        print(f"📊 Cache Consistency Results:")
        print(f"   Total Requests: {len(response_times)}")
        print(f"   Cache Hits: {cache_hits}")
        print(f"   Cache Hit Rate: {cache_hit_rate:.1f}%")
        print(f"   Avg Response Time: {avg_response_time:.3f}s")
        
        return {
            "test": "cache_consistency",
            "total_requests": len(response_times),
            "cache_hits": cache_hits,
            "cache_hit_rate": cache_hit_rate,
            "avg_response_time": avg_response_time
        }
    
    def test_memory_usage(self, state_code="GA"):
        """Test for memory leaks during repeated requests"""
        print(f"\n🧪 Testing memory usage...")
        
        # This would require server-side monitoring
        # For now, we'll test response consistency
        filter_combinations = [
            {"payer": ["Aetna"]},
            {"procedure_class": ["Surgery"]},
            {"org_name": ["Hospital A"]},
            {"cbsa": ["Atlanta"]},
            {"procedure_set": ["Cardiology"]},
        ]
        
        response_sizes = []
        
        for i in range(20):
            filter_combo = filter_combinations[i % len(filter_combinations)]
            
            try:
                # Build URL with filters
                params = []
                for key, values in filter_combo.items():
                    for value in values:
                        params.append(f"{key}={value}")
                
                url = f"{self.base_url}/commercial/insights/{state_code}/"
                if params:
                    url += "?" + "&".join(params)
                
                response = self.session.get(url, timeout=30)
                response_sizes.append(len(response.content))
                
                print(f"   Request {i+1}: {len(response.content)} bytes")
                
            except Exception as e:
                print(f"   Request {i+1}: Error - {str(e)}")
        
        avg_response_size = sum(response_sizes) / len(response_sizes)
        
        print(f"📊 Memory Usage Results:")
        print(f"   Total Requests: {len(response_sizes)}")
        print(f"   Avg Response Size: {avg_response_size:.0f} bytes")
        print(f"   Min Response Size: {min(response_sizes)} bytes")
        print(f"   Max Response Size: {max(response_sizes)} bytes")
        
        return {
            "test": "memory_usage",
            "total_requests": len(response_sizes),
            "avg_response_size": avg_response_size,
            "min_response_size": min(response_sizes),
            "max_response_size": max(response_sizes)
        }
    
    def run_all_tests(self, state_code="GA"):
        """Run all stress tests"""
        print(f"🚀 Starting Insights System Stress Tests for {state_code}")
        print("=" * 60)
        
        results = []
        
        # Test 1: Rapid filter changes
        results.append(self.test_rapid_filter_changes(state_code))
        
        # Test 2: Concurrent users
        results.append(self.test_concurrent_users(state_code))
        
        # Test 3: Cache consistency
        results.append(self.test_cache_consistency(state_code))
        
        # Test 4: Memory usage
        results.append(self.test_memory_usage(state_code))
        
        # Summary
        print("\n" + "=" * 60)
        print("📋 STRESS TEST SUMMARY")
        print("=" * 60)
        
        total_requests = sum(r.get("total_requests", 0) for r in results)
        total_successful = sum(r.get("successful", 0) for r in results)
        total_failed = sum(r.get("failed", 0) for r in results)
        
        print(f"Total Requests: {total_requests}")
        print(f"Successful: {total_successful}")
        print(f"Failed: {total_failed}")
        print(f"Success Rate: {total_successful/total_requests*100:.1f}%")
        
        # Save results
        with open("stress_test_results.json", "w") as f:
            json.dump(results, f, indent=2)
        
        print(f"\n💾 Results saved to stress_test_results.json")
        
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
    
    # Run stress tests
    tester = InsightsStressTest()
    results = tester.run_all_tests("GA")
    
    print("\n🎯 Key Issues to Address:")
    print("1. DuckDB connection management")
    print("2. Cache key consistency")
    print("3. Filter submission debouncing")
    print("4. Service worker cache handling")
    print("5. Loading state management")
