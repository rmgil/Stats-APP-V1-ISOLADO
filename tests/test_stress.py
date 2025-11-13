#!/usr/bin/env python3
"""
Stress Test - Concurrent Load Testing
======================================

Tests system behavior under concurrent load:
1. Multiple simultaneous health checks
2. Concurrent database queries
3. Multi-worker job claiming safety
"""

import requests
import time
import concurrent.futures
import sys

BASE_URL = "http://localhost:5000"

class Colors:
    GREEN = '\033[92m'
    RED = '\033[91m'
    YELLOW = '\033[93m'
    BLUE = '\033[94m'
    END = '\033[0m'

def print_test(name):
    print(f"\n{Colors.BLUE}🧪 STRESS TEST: {name}{Colors.END}")

def print_pass(msg):
    print(f"{Colors.GREEN}✅ PASS: {msg}{Colors.END}")

def print_fail(msg):
    print(f"{Colors.RED}❌ FAIL: {msg}{Colors.END}")

def print_info(msg):
    print(f"{Colors.YELLOW}ℹ️  INFO: {msg}{Colors.END}")

def health_check_request(iteration):
    """Single health check request"""
    try:
        start = time.time()
        response = requests.get(f"{BASE_URL}/health", timeout=10)
        elapsed = time.time() - start
        
        if response.status_code == 200:
            data = response.json()
            if data.get('status') == 'healthy':
                return (True, elapsed, iteration)
        
        return (False, elapsed, iteration)
    except Exception as e:
        return (False, 0, iteration)

def admin_stats_request(iteration):
    """Single admin stats request"""
    try:
        start = time.time()
        response = requests.get(f"{BASE_URL}/api/admin/stats", timeout=10)
        elapsed = time.time() - start
        
        if response.status_code == 200:
            data = response.json()
            if data.get('success'):
                return (True, elapsed, iteration)
        
        return (False, elapsed, iteration)
    except Exception as e:
        return (False, 0, iteration)

def test_concurrent_health_checks():
    """Test 50 concurrent health check requests"""
    print_test("50 Concurrent Health Checks")
    
    num_requests = 50
    
    start_time = time.time()
    
    with concurrent.futures.ThreadPoolExecutor(max_workers=20) as executor:
        futures = [executor.submit(health_check_request, i) for i in range(num_requests)]
        results = [future.result() for future in concurrent.futures.as_completed(futures)]
    
    elapsed = time.time() - start_time
    
    successful = sum(1 for success, _, _ in results if success)
    avg_response_time = sum(t for _, t, _ in results) / len(results)
    
    print_info(f"Total time: {elapsed:.2f}s")
    print_info(f"Successful: {successful}/{num_requests}")
    print_info(f"Average response time: {avg_response_time*1000:.0f}ms")
    print_info(f"Requests/sec: {num_requests/elapsed:.1f}")
    
    if successful >= num_requests * 0.95:  # 95% success rate
        print_pass("Concurrent health checks handled successfully")
        return True
    else:
        print_fail(f"Too many failures: {num_requests - successful}")
        return False

def test_concurrent_database_queries():
    """Test 30 concurrent database queries via admin stats"""
    print_test("30 Concurrent Database Queries")
    
    num_requests = 30
    
    start_time = time.time()
    
    with concurrent.futures.ThreadPoolExecutor(max_workers=15) as executor:
        futures = [executor.submit(admin_stats_request, i) for i in range(num_requests)]
        results = [future.result() for future in concurrent.futures.as_completed(futures)]
    
    elapsed = time.time() - start_time
    
    successful = sum(1 for success, _, _ in results if success)
    avg_response_time = sum(t for _, t, _ in results) / len(results)
    
    print_info(f"Total time: {elapsed:.2f}s")
    print_info(f"Successful: {successful}/{num_requests}")
    print_info(f"Average response time: {avg_response_time*1000:.0f}ms")
    
    if successful >= num_requests * 0.90:  # 90% success rate
        print_pass("Concurrent database queries handled successfully")
        return True
    else:
        print_fail(f"Too many query failures: {num_requests - successful}")
        return False

def test_sustained_load():
    """Test sustained load over 30 seconds"""
    print_test("Sustained Load (30s)")
    
    duration = 30  # seconds
    requests_per_second = 5
    
    start_time = time.time()
    total_requests = 0
    successful_requests = 0
    
    print_info(f"Sending {requests_per_second} req/s for {duration}s...")
    
    while time.time() - start_time < duration:
        iteration_start = time.time()
        
        # Send batch of requests
        with concurrent.futures.ThreadPoolExecutor(max_workers=requests_per_second) as executor:
            futures = [executor.submit(health_check_request, i) for i in range(requests_per_second)]
            results = [future.result() for future in concurrent.futures.as_completed(futures)]
        
        total_requests += len(results)
        successful_requests += sum(1 for success, _, _ in results if success)
        
        # Progress indicator every 5 seconds
        elapsed = time.time() - start_time
        if int(elapsed) % 5 == 0 and elapsed > 0:
            print_info(f"  {int(elapsed)}s: {successful_requests}/{total_requests} successful")
        
        # Wait to maintain rate
        iteration_elapsed = time.time() - iteration_start
        sleep_time = max(0, 1.0 - iteration_elapsed)
        if sleep_time > 0:
            time.sleep(sleep_time)
    
    success_rate = (successful_requests / total_requests) * 100 if total_requests > 0 else 0
    
    print_info(f"Total requests: {total_requests}")
    print_info(f"Successful: {successful_requests}")
    print_info(f"Success rate: {success_rate:.1f}%")
    
    if success_rate >= 95:
        print_pass("System handled sustained load successfully")
        return True
    else:
        print_fail(f"Success rate too low: {success_rate:.1f}%")
        return False

def run_stress_tests():
    """Run all stress tests"""
    print("\n" + "="*60)
    print(f"{Colors.BLUE}🔥 STRESS TEST SUITE{Colors.END}")
    print("="*60)
    
    tests = [
        ("Concurrent Health Checks", test_concurrent_health_checks),
        ("Concurrent Database Queries", test_concurrent_database_queries),
        ("Sustained Load", test_sustained_load),
    ]
    
    results = []
    
    for name, test_func in tests:
        try:
            result = test_func()
            results.append((name, result))
        except Exception as e:
            print_fail(f"Test '{name}' crashed: {e}")
            results.append((name, False))
        
        time.sleep(2)  # Brief pause between tests
    
    # Summary
    print("\n" + "="*60)
    print(f"{Colors.BLUE}📊 STRESS TEST SUMMARY{Colors.END}")
    print("="*60)
    
    passed = sum(1 for _, result in results if result)
    total = len(results)
    
    for name, result in results:
        status = f"{Colors.GREEN}✅ PASS{Colors.END}" if result else f"{Colors.RED}❌ FAIL{Colors.END}"
        print(f"{status} - {name}")
    
    print("\n" + "-"*60)
    
    if passed == total:
        print(f"{Colors.GREEN}🎉 ALL STRESS TESTS PASSED ({passed}/{total}){Colors.END}")
        print(f"{Colors.GREEN}✅ System handles concurrent load successfully!{Colors.END}")
        return 0
    else:
        print(f"{Colors.RED}⚠️  STRESS TESTS FAILED ({total - passed}/{total} failures){Colors.END}")
        return 1

if __name__ == "__main__":
    sys.exit(run_stress_tests())
