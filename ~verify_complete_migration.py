#!/usr/bin/env python3
"""
Complete Migration Verification

Verifies that ALL dashboard refresh functionality has been properly moved 
from analytics_query_service.py to dashboard_refresh_service.py.

Checks:
1. All required methods exist in dashboard_refresh_service.py
2. All routes use dashboard_refresh_service instead of analytics_service
3. Deprecated methods are marked properly in analytics_query_service.py
4. No hardcoded dates remain
"""

import logging
from pathlib import Path

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def check_dashboard_refresh_service_methods():
    """Check that all required methods exist in dashboard_refresh_service.py"""
    logger.info("=== CHECKING DASHBOARD REFRESH SERVICE METHODS ===")
    
    required_methods = [
        'execute_optimized_dashboard_refresh',
        'get_available_date_range', 
        'get_earliest_meta_date',
        'get_segment_performance',
        'get_overview_roas_chart_data',
        'get_table_name',
        'execute_analytics_query_optimized'  # Compatibility wrapper
    ]
    
    service_path = "orchestrator/dashboard/services/dashboard_refresh_service.py"
    
    if not Path(service_path).exists():
        logger.error(f"❌ Dashboard refresh service not found: {service_path}")
        return False
    
    try:
        with open(service_path, 'r') as f:
            content = f.read()
        
        all_methods_found = True
        for method in required_methods:
            if f"def {method}" in content:
                logger.info(f"✅ Method found: {method}")
            else:
                logger.error(f"❌ Method missing: {method}")
                all_methods_found = False
        
        return all_methods_found
        
    except Exception as e:
        logger.error(f"❌ Error reading dashboard refresh service: {e}")
        return False

def check_routes_use_dashboard_refresh_service():
    """Check that all routes use dashboard_refresh_service instead of analytics_service"""
    logger.info("=== CHECKING DASHBOARD ROUTES ===")
    
    routes_path = "orchestrator/dashboard/api/dashboard_routes.py"
    
    if not Path(routes_path).exists():
        logger.error(f"❌ Dashboard routes not found: {routes_path}")
        return False
    
    try:
        with open(routes_path, 'r') as f:
            content = f.read()
        
        # Check that analytics_service method calls are only in deprecated/commented sections
        # Count actual method calls (analytics_service.method_name)
        import re
        method_call_pattern = r'analytics_service\.\w+'
        all_method_calls = re.findall(method_call_pattern, content)
        commented_method_calls = re.findall(r'#.*' + method_call_pattern, content)
        
        analytics_service_calls = len(all_method_calls)
        commented_calls = len(commented_method_calls)
        
        # Allow for some commented out calls, but no active ones
        active_analytics_calls = analytics_service_calls - commented_calls
        
        logger.info(f"📊 Total analytics_service calls: {analytics_service_calls}")
        logger.info(f"📊 Commented analytics_service calls: {commented_calls}")  
        logger.info(f"📊 Active analytics_service calls: {active_analytics_calls}")
        
        # Check dashboard_refresh_service calls
        refresh_service_calls = content.count('dashboard_refresh_service.')
        logger.info(f"📊 Dashboard refresh service calls: {refresh_service_calls}")
        
        if active_analytics_calls == 0 and refresh_service_calls >= 4:
            logger.info("✅ All routes properly use dashboard_refresh_service!")
            return True
        else:
            logger.error(f"❌ Found {active_analytics_calls} active analytics_service calls")
            logger.error(f"❌ Found only {refresh_service_calls} dashboard_refresh_service calls (expected 4+)")
            return False
        
    except Exception as e:
        logger.error(f"❌ Error reading dashboard routes: {e}")
        return False

def check_deprecated_methods_marked():
    """Check that deprecated methods are properly marked in analytics_query_service.py"""
    logger.info("=== CHECKING DEPRECATED METHODS MARKINGS ===")
    
    analytics_path = "orchestrator/dashboard/services/analytics_query_service.py"
    
    if not Path(analytics_path).exists():
        logger.error(f"❌ Analytics service not found: {analytics_path}")
        return False
    
    try:
        with open(analytics_path, 'r') as f:
            content = f.read()
        
        deprecated_methods = [
            'execute_analytics_query_optimized',
            'get_segment_performance', 
            'get_overview_roas_chart_data',
            'get_available_date_range',
            'get_earliest_meta_date'
        ]
        
        all_marked = True
        for method in deprecated_methods:
            if f"def {method}" in content:
                # Check if it has DEPRECATED in docstring
                method_start = content.find(f"def {method}")
                if method_start != -1:
                    # Look for the docstring after the method definition
                    method_section = content[method_start:method_start + 1000]  # Look at next 1000 chars
                    if "DEPRECATED" in method_section:
                        logger.info(f"✅ Method properly marked deprecated: {method}")
                    else:
                        logger.error(f"❌ Method not marked deprecated: {method}")
                        all_marked = False
                else:
                    logger.warning(f"⚠️ Method not found: {method}")
        
        return all_marked
        
    except Exception as e:
        logger.error(f"❌ Error reading analytics service: {e}")
        return False

def check_no_hardcoded_dates():
    """Check that no hardcoded dates remain in critical files"""
    logger.info("=== CHECKING FOR HARDCODED DATES ===")
    
    critical_files = [
        "orchestrator/dashboard/services/dashboard_refresh_service.py",
        "orchestrator/dashboard/api/dashboard_routes.py",
        "orchestrator/dashboard/client/src/services/dashboardApi.js"
    ]
    
    hardcoded_patterns = [
        "'2024-01-01'", '"2024-01-01"',
        "'2025-01-01'", '"2025-01-01"',
        "'2025-04-01'", '"2025-04-01"'
    ]
    
    all_clean = True
    
    for file_path in critical_files:
        if Path(file_path).exists():
            try:
                with open(file_path, 'r') as f:
                    content = f.read()
                
                found_hardcoded = False
                for pattern in hardcoded_patterns:
                    if pattern in content:
                        logger.error(f"❌ HARDCODED DATE in {file_path}: {pattern}")
                        found_hardcoded = True
                        all_clean = False
                
                if not found_hardcoded:
                    logger.info(f"✅ {file_path} - No hardcoded dates!")
                    
            except Exception as e:
                logger.error(f"❌ Error reading {file_path}: {e}")
                all_clean = False
        else:
            logger.warning(f"⚠️ File not found: {file_path}")
    
    return all_clean

def main():
    """Run all verification checks"""
    logger.info("🚀 STARTING COMPLETE MIGRATION VERIFICATION")
    
    all_tests_passed = True
    
    # Test 1: Dashboard Refresh Service Methods
    if not check_dashboard_refresh_service_methods():
        all_tests_passed = False
    
    # Test 2: Routes Use Dashboard Refresh Service
    if not check_routes_use_dashboard_refresh_service():
        all_tests_passed = False
    
    # Test 3: Deprecated Methods Marked
    if not check_deprecated_methods_marked():
        all_tests_passed = False
    
    # Test 4: No Hardcoded Dates
    if not check_no_hardcoded_dates():
        all_tests_passed = False
    
    if all_tests_passed:
        logger.info("🎉 ALL VERIFICATION TESTS PASSED!")
        logger.info("✅ Dashboard refresh functionality completely migrated to dashboard_refresh_service")
        logger.info("✅ All routes updated to use dashboard_refresh_service")
        logger.info("✅ Deprecated methods properly marked in analytics_query_service")
        logger.info("✅ No hardcoded dates found in critical files")
        logger.info("")
        logger.info("🚀 COMPLETE MIGRATION SUCCESSFULLY VERIFIED!")
    else:
        logger.error("❌ Some verification tests failed. Review the errors above.")
    
    return all_tests_passed

if __name__ == "__main__":
    success = main()
    exit(0 if success else 1)
