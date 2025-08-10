#!/usr/bin/env python3
"""
Complete end-to-end validation test for the pre-computation dashboard system
Tests all key functionality including performance benchmarks
"""

import sys
import os
import time
sys.path.append('/Users/joshuakaufman/Atly Cursor Projects/Ads-Dashboard-Final')

from orchestrator.dashboard.services.analytics_query_service import AnalyticsQueryService
from datetime import datetime, timedelta
import json

def test_performance_benchmark():
    """Test query performance against specification targets"""
    print("\n🏃‍♂️ PERFORMANCE BENCHMARKS")
    print("-" * 40)
    
    service = AnalyticsQueryService()
    
    # Test dates
    end_date = datetime.now().strftime('%Y-%m-%d')
    start_date = (datetime.now() - timedelta(days=13)).strftime('%Y-%m-%d')  # 14 days
    overview_start = (datetime.now() - timedelta(days=27)).strftime('%Y-%m-%d')  # 28 days
    
    benchmarks = []
    
    # 1. Test single entity row sparkline (14-day) - target <20ms per spec
    print("1. Testing row-level sparkline (14-day target: <20ms)...")
    start_time = time.time()
    result = service.get_chart_data('campaign', '23863000648590385', start_date, end_date, 'all')
    row_sparkline_time = (time.time() - start_time) * 1000
    benchmarks.append(('Row Sparkline (14-day)', row_sparkline_time, 20))
    print(f"   ⏱️  {row_sparkline_time:.1f}ms - {'✅ PASS' if row_sparkline_time < 20 else '⚠️  SLOW'}")
    
    # 2. Test overview sparkline (28-day) - target <30ms per spec
    print("2. Testing overview sparkline (28-day target: <30ms)...")
    start_time = time.time()
    result = service.get_overview_roas_chart_data(overview_start, end_date, 'all')
    overview_time = (time.time() - start_time) * 1000
    benchmarks.append(('Overview Sparkline (28-day)', overview_time, 30))
    print(f"   ⏱️  {overview_time:.1f}ms - {'✅ PASS' if overview_time < 30 else '⚠️  SLOW'}")
    
    # 3. Test dashboard analytics query - target <1000ms per spec
    print("3. Testing dashboard analytics query (target: <1000ms)...")
    start_time = time.time()
    result = service.get_analytics_data({
        'entity_type': 'campaign',
        'start_date': start_date,
        'end_date': end_date,
        'breakdown': 'all'
    })
    dashboard_time = (time.time() - start_time) * 1000
    benchmarks.append(('Dashboard Analytics', dashboard_time, 1000))
    print(f"   ⏱️  {dashboard_time:.1f}ms - {'✅ PASS' if dashboard_time < 1000 else '⚠️  SLOW'}")
    
    # 4. Test country breakdown query - target <1000ms per spec
    print("4. Testing country breakdown query (target: <1000ms)...")
    start_time = time.time()
    result = service.get_analytics_data({
        'entity_type': 'campaign',
        'start_date': start_date,
        'end_date': end_date,
        'breakdown': 'country'
    })
    breakdown_time = (time.time() - start_time) * 1000
    benchmarks.append(('Country Breakdown', breakdown_time, 1000))
    print(f"   ⏱️  {breakdown_time:.1f}ms - {'✅ PASS' if breakdown_time < 1000 else '⚠️  SLOW'}")
    
    # Summary
    print(f"\n🎯 PERFORMANCE SUMMARY")
    total_pass = 0
    for name, actual, target in benchmarks:
        status = "✅ PASS" if actual < target else "❌ FAIL"
        improvement = f"{target/actual:.1f}x faster than target" if actual < target else f"{actual/target:.1f}x slower than target"
        print(f"   {name}: {actual:.1f}ms (target: {target}ms) {status} - {improvement}")
        if actual < target:
            total_pass += 1
    
    print(f"\n📊 Overall: {total_pass}/{len(benchmarks)} tests passed performance targets")
    return total_pass == len(benchmarks)

def test_data_integrity():
    """Test data integrity and completeness"""
    print("\n🔍 DATA INTEGRITY CHECKS")
    print("-" * 40)
    
    import sqlite3
    from utils.database_utils import get_database_connection
    
    integrity_checks = []
    
    try:
        with get_database_connection('mixpanel_data') as conn:
            cursor = conn.cursor()
            
            # 1. Check main table has data
            cursor.execute("SELECT COUNT(*) FROM daily_mixpanel_metrics")
            main_count = cursor.fetchone()[0]
            integrity_checks.append(('Main table populated', main_count > 0, f"{main_count:,} records"))
            
            # 2. Check breakdown table has data
            cursor.execute("SELECT COUNT(*) FROM daily_mixpanel_metrics_breakdown")
            breakdown_count = cursor.fetchone()[0]
            integrity_checks.append(('Breakdown table populated', breakdown_count > 0, f"{breakdown_count:,} records"))
            
            # 3. Check all entity types present
            cursor.execute("SELECT DISTINCT entity_type FROM daily_mixpanel_metrics")
            entity_types = [row[0] for row in cursor.fetchall()]
            expected_types = ['campaign', 'adset', 'ad']
            has_all_types = all(etype in entity_types for etype in expected_types)
            integrity_checks.append(('All entity types present', has_all_types, f"Found: {entity_types}"))
            
            # 4. Check recent data exists (last 3 days)
            recent_date = (datetime.now() - timedelta(days=3)).strftime('%Y-%m-%d')
            cursor.execute("SELECT COUNT(*) FROM daily_mixpanel_metrics WHERE date >= ?", [recent_date])
            recent_count = cursor.fetchone()[0]
            integrity_checks.append(('Recent data available', recent_count > 0, f"{recent_count} records in last 3 days"))
            
            # 5. Check user lists are valid JSON and counts match
            cursor.execute("""
                SELECT entity_type, entity_id, date, trial_users_count, trial_users_list 
                FROM daily_mixpanel_metrics 
                WHERE trial_users_count > 0 
                LIMIT 5
            """)
            json_valid = True
            count_valid = True
            for row in cursor.fetchall():
                try:
                    user_list = json.loads(row[4] or '[]')
                    if len(user_list) != row[3]:
                        count_valid = False
                        break
                except json.JSONDecodeError:
                    json_valid = False
                    break
            
            integrity_checks.append(('User lists JSON valid', json_valid, 'All user lists parse correctly'))
            integrity_checks.append(('User counts match lists', count_valid, 'Count fields match user list lengths'))
            
            # 6. Check breakdown data consistency
            cursor.execute("""
                SELECT COUNT(*) FROM daily_mixpanel_metrics_breakdown 
                WHERE breakdown_type = 'country' AND breakdown_value != ''
            """)
            country_breakdown_count = cursor.fetchone()[0]
            integrity_checks.append(('Country breakdown data', country_breakdown_count > 0, f"{country_breakdown_count:,} country records"))
            
    except Exception as e:
        integrity_checks.append(('Database connection', False, f"Error: {e}"))
    
    # Report results
    passed = 0
    for name, success, details in integrity_checks:
        status = "✅ PASS" if success else "❌ FAIL"
        print(f"   {name}: {status} - {details}")
        if success:
            passed += 1
    
    print(f"\n📊 Integrity: {passed}/{len(integrity_checks)} checks passed")
    return passed == len(integrity_checks)

def test_api_response_formats():
    """Test API response formats for frontend compatibility"""
    print("\n📡 API RESPONSE FORMAT TESTS")
    print("-" * 40)
    
    service = AnalyticsQueryService()
    end_date = datetime.now().strftime('%Y-%m-%d')
    start_date = (datetime.now() - timedelta(days=13)).strftime('%Y-%m-%d')
    
    format_tests = []
    
    # 1. Test analytics data response format
    result = service.get_analytics_data({
        'entity_type': 'campaign',
        'start_date': start_date,
        'end_date': end_date,
        'breakdown': 'all'
    })
    
    required_keys = ['success', 'data', 'breakdown_data', 'metadata']
    has_required = all(key in result for key in required_keys)
    format_tests.append(('Analytics data keys', has_required, f"Has: {list(result.keys())}"))
    
    if result.get('data'):
        sample_row = result['data'][0]
        required_row_keys = ['id', 'spend', 'mixpanel_trials_started', 'estimated_revenue_adjusted']
        has_row_keys = all(key in sample_row for key in required_row_keys)
        format_tests.append(('Row data format', has_row_keys, f"Sample keys: {list(sample_row.keys())[:5]}..."))
    
    # 2. Test chart data response format
    chart_result = service.get_chart_data('campaign', '23863000648590385', start_date, end_date, 'all')
    chart_required = ['success', 'chart_data', 'metadata']
    has_chart_keys = all(key in chart_result for key in chart_required)
    format_tests.append(('Chart data keys', has_chart_keys, f"Has: {list(chart_result.keys())}"))
    
    if chart_result.get('chart_data'):
        sample_chart = chart_result['chart_data'][0]
        chart_data_keys = ['date', 'rolling_1d_roas', 'rolling_1d_spend', 'rolling_1d_revenue']
        has_chart_data = all(key in sample_chart for key in chart_data_keys)
        format_tests.append(('Chart data format', has_chart_data, f"Sample keys: {list(sample_chart.keys())[:5]}..."))
    
    # 3. Test overview response format
    overview_result = service.get_overview_roas_chart_data(start_date, end_date, 'all')
    overview_required = ['success', 'chart_data', 'metadata']
    has_overview_keys = all(key in overview_result for key in overview_required)
    format_tests.append(('Overview data keys', has_overview_keys, f"Has: {list(overview_result.keys())}"))
    
    # Report results
    passed = 0
    for name, success, details in format_tests:
        status = "✅ PASS" if success else "❌ FAIL"
        print(f"   {name}: {status} - {details}")
        if success:
            passed += 1
    
    print(f"\n📊 API Format: {passed}/{len(format_tests)} tests passed")
    return passed == len(format_tests)

def test_breakdown_functionality():
    """Test breakdown functionality across different dimensions"""
    print("\n🌍 BREAKDOWN FUNCTIONALITY TESTS")
    print("-" * 40)
    
    service = AnalyticsQueryService()
    end_date = datetime.now().strftime('%Y-%m-%d')
    start_date = (datetime.now() - timedelta(days=6)).strftime('%Y-%m-%d')  # 7 days
    
    breakdown_tests = []
    
    # Test different breakdown types
    breakdown_types = ['all', 'country']
    
    for breakdown in breakdown_types:
        try:
            result = service.get_analytics_data({
                'entity_type': 'campaign',
                'start_date': start_date,
                'end_date': end_date,
                'breakdown': breakdown
            })
            
            success = result.get('success', False)
            has_data = len(result.get('data', [])) > 0
            
            if breakdown == 'country':
                has_breakdown = len(result.get('breakdown_data', {})) > 0
                breakdown_tests.append((f'Breakdown: {breakdown}', success and has_data and has_breakdown, 
                                      f"Success: {success}, Data: {len(result.get('data', []))}, Breakdown: {len(result.get('breakdown_data', {}))}"))
            else:
                breakdown_tests.append((f'Breakdown: {breakdown}', success and has_data, 
                                      f"Success: {success}, Data: {len(result.get('data', []))}"))
                
        except Exception as e:
            breakdown_tests.append((f'Breakdown: {breakdown}', False, f"Error: {e}"))
    
    # Report results
    passed = 0
    for name, success, details in breakdown_tests:
        status = "✅ PASS" if success else "❌ FAIL"
        print(f"   {name}: {status} - {details}")
        if success:
            passed += 1
    
    print(f"\n📊 Breakdown: {passed}/{len(breakdown_tests)} tests passed")
    return passed == len(breakdown_tests)

def main():
    """Run complete system validation"""
    print("🚀 COMPLETE PRE-COMPUTATION SYSTEM VALIDATION")
    print("=" * 60)
    
    test_results = []
    
    # Run all test suites
    test_results.append(("Performance Benchmarks", test_performance_benchmark()))
    test_results.append(("Data Integrity", test_data_integrity()))
    test_results.append(("API Response Formats", test_api_response_formats()))
    test_results.append(("Breakdown Functionality", test_breakdown_functionality()))
    
    # Final summary
    print(f"\n🎯 FINAL VALIDATION SUMMARY")
    print("=" * 60)
    
    total_passed = 0
    for test_name, passed in test_results:
        status = "✅ PASS" if passed else "❌ FAIL"
        print(f"   {test_name}: {status}")
        if passed:
            total_passed += 1
    
    overall_success = total_passed == len(test_results)
    print(f"\n📊 OVERALL RESULT: {total_passed}/{len(test_results)} test suites passed")
    
    if overall_success:
        print("\n🎉 SYSTEM READY FOR PRODUCTION! 🎉")
        print("✅ All tests passed - pre-computation dashboard is fully functional")
        print("✅ Performance exceeds specification targets")
        print("✅ Data integrity validated")
        print("✅ API compatibility maintained")
        print("✅ Breakdown functionality working")
    else:
        print("\n⚠️  SYSTEM NEEDS ATTENTION")
        print("Some test suites failed - please review before production deployment")
    
    return overall_success

if __name__ == "__main__":
    main()