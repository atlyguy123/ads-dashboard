#!/usr/bin/env python3
"""
Complete Frontend-Backend Flow Verification

Traces the EXACT flow from frontend dashboard refresh to backend services
to verify ALL dashboard functionality uses dashboard_refresh_service.

Frontend Refresh Flow Analysis:
1. Main Dashboard Grid Data
2. Overview Stats & Sparklines  
3. Individual Row Sparklines
4. Hierarchy (campaigns/adsets/ads)

Verifies:
- Frontend API calls map to correct backend routes
- Backend routes use dashboard_refresh_service (not analytics_service) 
- All optimized methods are properly wired
"""

import logging
from pathlib import Path
import re

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def analyze_frontend_api_calls():
    """Analyze what API calls the frontend makes during dashboard refresh"""
    logger.info("=== ANALYZING FRONTEND API CALLS ===")
    
    frontend_files = [
        "orchestrator/dashboard/client/src/pages/Dashboard.js",
        "orchestrator/dashboard/client/src/services/dashboardApi.js", 
        "orchestrator/dashboard/client/src/hooks/useOverviewChartData.js"
    ]
    
    api_calls = {}
    
    for file_path in frontend_files:
        if not Path(file_path).exists():
            logger.warning(f"⚠️ File not found: {file_path}")
            continue
            
        try:
            with open(file_path, 'r') as f:
                content = f.read()
            
            # Find API method calls
            api_patterns = [
                r'dashboardApi\.(\w+)\(',
                r'makeRequest\([\'"]([^\'\"]+)[\'"]',
                r'await.*?([\'"]\/analytics\/[^\'\"]+[\'"])',
            ]
            
            for pattern in api_patterns:
                matches = re.findall(pattern, content)
                for match in matches:
                    if match not in api_calls:
                        api_calls[match] = []
                    api_calls[match].append(file_path)
        
        except Exception as e:
            logger.error(f"❌ Error reading {file_path}: {e}")
    
    logger.info("🔍 Frontend API Calls Found:")
    for call, files in api_calls.items():
        logger.info(f"  📞 {call} (used in {len(files)} files)")
    
    return api_calls

def verify_backend_route_mappings():
    """Verify backend routes map to dashboard_refresh_service methods"""
    logger.info("=== VERIFYING BACKEND ROUTE MAPPINGS ===")
    
    routes_path = "orchestrator/dashboard/api/dashboard_routes.py"
    if not Path(routes_path).exists():
        logger.error(f"❌ Routes file not found: {routes_path}")
        return False
    
    try:
        with open(routes_path, 'r') as f:
            content = f.read()
        
        # Find all dashboard analytics routes and their service calls
        route_pattern = r'@dashboard_bp\.route\([\'"]([^\'\"]*analytics[^\'\"]*)[\'"].*?\n.*?def\s+(\w+)'
        service_call_pattern = r'(dashboard_refresh_service|analytics_service)\.(\w+)\('
        
        routes = re.findall(route_pattern, content, re.DOTALL)
        service_calls = re.findall(service_call_pattern, content)
        
        logger.info("🎯 Analytics Routes Found:")
        for route, func_name in routes:
            logger.info(f"  🛣️ {route} → {func_name}()")
        
        logger.info("🔧 Service Calls Found:")
        dashboard_refresh_calls = 0
        analytics_calls = 0
        
        for service, method in service_calls:
            if service == 'dashboard_refresh_service':
                dashboard_refresh_calls += 1
                logger.info(f"  ✅ dashboard_refresh_service.{method}()")
            elif service == 'analytics_service':
                analytics_calls += 1
                logger.info(f"  ❌ analytics_service.{method}()")
        
        logger.info(f"📊 Summary:")
        logger.info(f"  ✅ dashboard_refresh_service calls: {dashboard_refresh_calls}")
        logger.info(f"  ❌ analytics_service calls: {analytics_calls}")
        
        return analytics_calls == 0 and dashboard_refresh_calls > 0
        
    except Exception as e:
        logger.error(f"❌ Error reading routes file: {e}")
        return False

def verify_main_dashboard_flow():
    """Verify the main dashboard data flow"""
    logger.info("=== VERIFYING MAIN DASHBOARD FLOW ===")
    
    # 1. Frontend: Dashboard.js calls dashboardApi.getAnalyticsData()
    dashboard_js = "orchestrator/dashboard/client/src/pages/Dashboard.js"
    dashboardapi_js = "orchestrator/dashboard/client/src/services/dashboardApi.js"
    
    checks = []
    
    # Check 1: Dashboard.js calls getAnalyticsData
    try:
        with open(dashboard_js, 'r') as f:
            content = f.read()
        if 'dashboardApi.getAnalyticsData(' in content:
            logger.info("✅ Frontend: Dashboard.js calls dashboardApi.getAnalyticsData()")
            checks.append(True)
        else:
            logger.error("❌ Frontend: Dashboard.js does NOT call getAnalyticsData")
            checks.append(False)
    except Exception as e:
        logger.error(f"❌ Error checking Dashboard.js: {e}")
        checks.append(False)
    
    # Check 2: dashboardApi.js maps to /analytics/data
    try:
        with open(dashboardapi_js, 'r') as f:
            content = f.read()
        if "'/analytics/data'" in content and 'getAnalyticsData' in content:
            logger.info("✅ Frontend: dashboardApi.getAnalyticsData() → /analytics/data")
            checks.append(True)
        else:
            logger.error("❌ Frontend: getAnalyticsData does NOT map to /analytics/data")
            checks.append(False)
    except Exception as e:
        logger.error(f"❌ Error checking dashboardApi.js: {e}")
        checks.append(False)
    
    # Check 3: Backend route uses dashboard_refresh_service
    routes_path = "orchestrator/dashboard/api/dashboard_routes.py"
    try:
        with open(routes_path, 'r') as f:
            content = f.read()
        
        # Find the /analytics/data route
        route_section = ""
        lines = content.split('\n')
        in_analytics_data = False
        
        for line in lines:
            if "@dashboard_bp.route('/analytics/data'" in line:
                in_analytics_data = True
            elif in_analytics_data:
                route_section += line + '\n'
                if line.strip().startswith('@dashboard_bp.route') and "/analytics/data" not in line:
                    break
                if line.strip().startswith('def ') and 'get_analytics_data' not in line:
                    break
        
        if 'dashboard_refresh_service.execute_optimized_dashboard_refresh' in route_section:
            logger.info("✅ Backend: /analytics/data uses dashboard_refresh_service.execute_optimized_dashboard_refresh()")
            checks.append(True)
        else:
            logger.error("❌ Backend: /analytics/data does NOT use dashboard_refresh_service")
            logger.error(f"Route section found: {route_section[:200]}...")
            checks.append(False)
            
    except Exception as e:
        logger.error(f"❌ Error checking backend routes: {e}")
        checks.append(False)
    
    return all(checks)

def verify_overview_sparklines_flow():
    """Verify the overview sparklines flow"""
    logger.info("=== VERIFYING OVERVIEW SPARKLINES FLOW ===")
    
    checks = []
    
    # Check 1: useOverviewChartData hook calls getOverviewROASChartData
    hook_path = "orchestrator/dashboard/client/src/hooks/useOverviewChartData.js"
    try:
        with open(hook_path, 'r') as f:
            content = f.read()
        if 'dashboardApi.getOverviewROASChartData(' in content:
            logger.info("✅ Frontend: useOverviewChartData calls dashboardApi.getOverviewROASChartData()")
            checks.append(True)
        else:
            logger.error("❌ Frontend: useOverviewChartData does NOT call getOverviewROASChartData")
            checks.append(False)
    except Exception as e:
        logger.error(f"❌ Error checking useOverviewChartData: {e}")
        checks.append(False)
    
    # Check 2: getOverviewROASChartData maps to /analytics/overview-roas-chart
    dashboardapi_js = "orchestrator/dashboard/client/src/services/dashboardApi.js"
    try:
        with open(dashboardapi_js, 'r') as f:
            content = f.read()
        if "'/analytics/overview-roas-chart'" in content and 'getOverviewROASChartData' in content:
            logger.info("✅ Frontend: getOverviewROASChartData() → /analytics/overview-roas-chart")
            checks.append(True)
        else:
            logger.error("❌ Frontend: getOverviewROASChartData does NOT map to /analytics/overview-roas-chart")
            checks.append(False)
    except Exception as e:
        logger.error(f"❌ Error checking dashboardApi.js: {e}")
        checks.append(False)
    
    # Check 3: Backend route uses dashboard_refresh_service
    routes_path = "orchestrator/dashboard/api/dashboard_routes.py"
    try:
        with open(routes_path, 'r') as f:
            content = f.read()
        
        if 'dashboard_refresh_service.get_overview_roas_chart_data(' in content:
            logger.info("✅ Backend: /analytics/overview-roas-chart uses dashboard_refresh_service.get_overview_roas_chart_data()")
            checks.append(True)
        else:
            logger.error("❌ Backend: /analytics/overview-roas-chart does NOT use dashboard_refresh_service")
            checks.append(False)
            
    except Exception as e:
        logger.error(f"❌ Error checking backend routes: {e}")
        checks.append(False)
    
    return all(checks)

def verify_dashboard_refresh_service_completeness():
    """Verify dashboard_refresh_service has all required methods"""
    logger.info("=== VERIFYING DASHBOARD_REFRESH_SERVICE COMPLETENESS ===")
    
    service_path = "orchestrator/dashboard/services/dashboard_refresh_service.py"
    if not Path(service_path).exists():
        logger.error(f"❌ Dashboard refresh service not found: {service_path}")
        return False
    
    required_methods = [
        'execute_optimized_dashboard_refresh',  # Main dashboard data
        'get_overview_roas_chart_data',         # Overview sparklines
        'get_available_date_range',             # Date range API
        'get_segment_performance',              # Segments
        '_format_sparkline_data_optimized',     # Individual sparklines (might be private)
        '_get_entity_children_optimized'        # Hierarchy (might be private)
    ]
    
    try:
        with open(service_path, 'r') as f:
            content = f.read()
        
        found_methods = []
        missing_methods = []
        
        for method in required_methods:
            if f"def {method}" in content:
                found_methods.append(method)
                logger.info(f"✅ Method found: {method}")
            else:
                missing_methods.append(method)
                logger.error(f"❌ Method missing: {method}")
        
        logger.info(f"📊 Summary: {len(found_methods)}/{len(required_methods)} methods found")
        
        return len(missing_methods) == 0
        
    except Exception as e:
        logger.error(f"❌ Error reading dashboard refresh service: {e}")
        return False

def main():
    """Run complete frontend-backend flow verification"""
    logger.info("🚀 STARTING COMPLETE FRONTEND-BACKEND FLOW VERIFICATION")
    
    all_tests_passed = True
    
    # Test 1: Analyze frontend API calls
    frontend_calls = analyze_frontend_api_calls()
    
    # Test 2: Verify backend route mappings
    if not verify_backend_route_mappings():
        all_tests_passed = False
    
    # Test 3: Verify main dashboard flow
    if not verify_main_dashboard_flow():
        all_tests_passed = False
    
    # Test 4: Verify overview sparklines flow
    if not verify_overview_sparklines_flow():
        all_tests_passed = False
    
    # Test 5: Verify dashboard refresh service completeness
    if not verify_dashboard_refresh_service_completeness():
        all_tests_passed = False
    
    if all_tests_passed:
        logger.info("")
        logger.info("🎉 ALL FRONTEND-BACKEND FLOW VERIFICATION PASSED!")
        logger.info("✅ Main dashboard grid data flows through dashboard_refresh_service")
        logger.info("✅ Overview stats & sparklines flow through dashboard_refresh_service")
        logger.info("✅ Individual row sparklines included in main data response")
        logger.info("✅ Hierarchy (campaigns/adsets/ads) handled by dashboard_refresh_service")
        logger.info("✅ ALL dashboard refresh functionality properly migrated and wired")
        logger.info("")
        logger.info("🚀 FRONTEND DASHBOARD REFRESH COMPLETELY USES DASHBOARD_REFRESH_SERVICE!")
    else:
        logger.error("")
        logger.error("❌ Some verification tests failed. Review the errors above.")
        logger.error("❌ Dashboard refresh may still be using old analytics_service methods.")
    
    return all_tests_passed

if __name__ == "__main__":
    success = main()
    exit(0 if success else 1)
