#!/usr/bin/env python3
"""
🚀 API Optimization Test Suite
Tests the new optimized analytics endpoints for performance and accuracy.
"""

import requests
import time
import json
from datetime import datetime, timedelta

# Test configuration
BASE_URL = "http://localhost:5000"  # Adjust as needed
TEST_DATES = {
    "start_date": "2024-07-01",
    "end_date": "2024-07-31"
}

def test_endpoint_performance(endpoint_path, payload, test_name):
    """Test endpoint performance and response format"""
    print(f"\n🧪 Testing {test_name}")
    print(f"   Endpoint: {endpoint_path}")
    print(f"   Payload: {payload}")
    
    try:
        start_time = time.time()
        response = requests.post(f"{BASE_URL}{endpoint_path}", json=payload)
        end_time = time.time()
        
        response_time = (end_time - start_time) * 1000  # Convert to milliseconds
        
        print(f"   ⏱️  Response Time: {response_time:.2f}ms")
        print(f"   📊 Status Code: {response.status_code}")
        
        if response.status_code == 200:
            data = response.json()
            if data.get('success'):
                record_count = len(data.get('data', []))
                print(f"   📈 Records Returned: {record_count}")
                print(f"   ✅ Success: {data.get('success')}")
                
                # Show sample fields from first record if available
                if data.get('data') and len(data['data']) > 0:
                    first_record = data['data'][0]
                    sample_fields = {
                        'id': first_record.get('id'),
                        'entity_type': first_record.get('entity_type'),
                        'trials': first_record.get('mixpanel_trials_started'),
                        'revenue': first_record.get('estimated_revenue_adjusted'),
                        'spend': first_record.get('spend')
                    }
                    print(f"   📝 Sample Fields: {sample_fields}")
                
                return {
                    'success': True,
                    'response_time': response_time,
                    'record_count': record_count,
                    'data': data
                }
            else:
                print(f"   ❌ API Error: {data.get('error')}")
                return {'success': False, 'error': data.get('error')}
        else:
            print(f"   ❌ HTTP Error: {response.status_code}")
            return {'success': False, 'error': f"HTTP {response.status_code}"}
            
    except Exception as e:
        print(f"   💥 Exception: {str(e)}")
        return {'success': False, 'error': str(e)}

def compare_endpoints():
    """Compare legacy vs optimized endpoints"""
    print("\n" + "="*60)
    print("🔬 PERFORMANCE COMPARISON: Legacy vs Optimized")
    print("="*60)
    
    test_payload = {
        "start_date": TEST_DATES["start_date"],
        "end_date": TEST_DATES["end_date"],
        "breakdown": "all",
        "group_by": "campaign",
        "include_mixpanel": True
    }
    
    # Test legacy endpoint
    legacy_result = test_endpoint_performance(
        "/api/dashboard/analytics/data",
        test_payload,
        "LEGACY ENDPOINT"
    )
    
    # Test optimized endpoint
    optimized_result = test_endpoint_performance(
        "/api/dashboard/analytics/data/optimized",
        test_payload,
        "OPTIMIZED ENDPOINT"
    )
    
    # Performance comparison
    if legacy_result.get('success') and optimized_result.get('success'):
        legacy_time = legacy_result['response_time']
        optimized_time = optimized_result['response_time']
        
        if optimized_time > 0:
            improvement = ((legacy_time - optimized_time) / legacy_time) * 100
            speedup = legacy_time / optimized_time
            
            print(f"\n🚀 PERFORMANCE RESULTS:")
            print(f"   Legacy Time:    {legacy_time:.2f}ms")
            print(f"   Optimized Time: {optimized_time:.2f}ms")
            print(f"   Improvement:    {improvement:.1f}% faster")
            print(f"   Speedup:        {speedup:.1f}x faster")
            
            # Validate improvement target
            if improvement >= 90:
                print(f"   ✅ TARGET MET: >90% improvement achieved!")
            else:
                print(f"   ⚠️  TARGET MISSED: <90% improvement")
        
        # Record count comparison
        legacy_count = legacy_result['record_count']
        optimized_count = optimized_result['record_count']
        
        print(f"\n📊 DATA ACCURACY:")
        print(f"   Legacy Records:    {legacy_count}")
        print(f"   Optimized Records: {optimized_count}")
        
        if legacy_count == optimized_count:
            print(f"   ✅ RECORD COUNT MATCH")
        else:
            print(f"   ⚠️  RECORD COUNT MISMATCH")

def test_breakdown_functionality():
    """Test breakdown functionality with optimized endpoint"""
    print("\n" + "="*60)
    print("🔍 BREAKDOWN FUNCTIONALITY TEST")
    print("="*60)
    
    breakdowns = ['all', 'country', 'device']
    
    for breakdown in breakdowns:
        payload = {
            "start_date": TEST_DATES["start_date"],
            "end_date": TEST_DATES["end_date"],
            "breakdown": breakdown,
            "group_by": "campaign",
            "include_mixpanel": True
        }
        
        result = test_endpoint_performance(
            "/api/dashboard/analytics/data/optimized",
            payload,
            f"BREAKDOWN: {breakdown.upper()}"
        )

def test_hierarchy_levels():
    """Test different hierarchy levels (campaign, adset, ad)"""
    print("\n" + "="*60)
    print("🏗️ HIERARCHY LEVELS TEST")
    print("="*60)
    
    levels = ['campaign', 'adset', 'ad']
    
    for level in levels:
        payload = {
            "start_date": TEST_DATES["start_date"],
            "end_date": TEST_DATES["end_date"],
            "breakdown": "all",
            "group_by": level,
            "include_mixpanel": True
        }
        
        result = test_endpoint_performance(
            "/api/dashboard/analytics/data/optimized",
            payload,
            f"HIERARCHY: {level.upper()}"
        )

def test_error_handling():
    """Test error handling and validation"""
    print("\n" + "="*60)
    print("🛡️ ERROR HANDLING TEST")
    print("="*60)
    
    # Test missing parameters
    invalid_payloads = [
        {
            "end_date": TEST_DATES["end_date"],
            "breakdown": "all"
        },
        {
            "start_date": TEST_DATES["start_date"],
            "breakdown": "invalid_breakdown"
        },
        {
            "start_date": TEST_DATES["start_date"],
            "end_date": TEST_DATES["end_date"],
            "group_by": "invalid_group"
        }
    ]
    
    for i, payload in enumerate(invalid_payloads):
        result = test_endpoint_performance(
            "/api/dashboard/analytics/data/optimized",
            payload,
            f"ERROR TEST {i+1}"
        )

def main():
    """Run all tests"""
    print("🚀 API OPTIMIZATION TEST SUITE")
    print(f"Testing against: {BASE_URL}")
    print(f"Date Range: {TEST_DATES['start_date']} to {TEST_DATES['end_date']}")
    
    # Run all test suites
    compare_endpoints()
    test_breakdown_functionality()
    test_hierarchy_levels()
    test_error_handling()
    
    print("\n" + "="*60)
    print("✅ TEST SUITE COMPLETED")
    print("="*60)

if __name__ == "__main__":
    main()