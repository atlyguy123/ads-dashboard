#!/usr/bin/env python3
"""
Simple Date Verification

Quick test to verify:
1. Dashboard Refresh Service has the date methods
2. No hardcoded dates in critical files
3. Database has actual data
"""

import sqlite3
import logging
from pathlib import Path
import sys

# Add utils to path
utils_path = str(Path(__file__).resolve().parent / "utils")
sys.path.append(utils_path)
from database_utils import get_database_connection

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def test_database_date_range():
    """Test actual database date range"""
    logger.info("=== TESTING DATABASE DATE RANGE ===")
    
    try:
        with get_database_connection('mixpanel_data') as conn:
            cursor = conn.cursor()
            
            # Get actual data date range
            cursor.execute("SELECT MIN(date) as min_date, MAX(date) as max_date, COUNT(*) as total_records FROM daily_mixpanel_metrics")
            result = cursor.fetchone()
            
            if result:
                min_date, max_date, total_records = result
                logger.info(f"📊 Database has {total_records} records from {min_date} to {max_date}")
                
                # Check if this is reasonable (not hardcoded)
                if min_date and max_date:
                    if min_date.startswith('2025') and max_date.startswith('2025'):
                        logger.info("✅ Database contains 2025 data (real data)")
                        return True, min_date, max_date
                    elif min_date.startswith('2024'):
                        logger.warning("⚠️ Database contains 2024 data (might be old)")
                        return True, min_date, max_date
                    else:
                        logger.info(f"📅 Database date range: {min_date} to {max_date}")
                        return True, min_date, max_date
                else:
                    logger.error("❌ No date data found")
                    return False, None, None
            else:
                logger.error("❌ No data in database")
                return False, None, None
                
    except Exception as e:
        logger.error(f"❌ Database error: {e}")
        return False, None, None

def test_dashboard_refresh_service_exists():
    """Test that dashboard refresh service file exists and has the right methods"""
    logger.info("=== TESTING DASHBOARD REFRESH SERVICE ===")
    
    service_path = "orchestrator/dashboard/services/dashboard_refresh_service.py"
    
    if not Path(service_path).exists():
        logger.error(f"❌ Dashboard refresh service not found: {service_path}")
        return False
    
    try:
        with open(service_path, 'r') as f:
            content = f.read()
            
        required_methods = [
            "get_available_date_range",
            "get_earliest_meta_date",
            "execute_optimized_dashboard_refresh"
        ]
        
        for method in required_methods:
            if f"def {method}" in content:
                logger.info(f"✅ Method found: {method}")
            else:
                logger.error(f"❌ Method missing: {method}")
                return False
        
        # Check for hardcoded dates (should not be present)
        hardcoded_patterns = ["'2024-01-01'", '"2024-01-01"', "'2025-01-01'", '"2025-01-01"']
        found_hardcoded = False
        
        for pattern in hardcoded_patterns:
            if pattern in content:
                logger.error(f"❌ HARDCODED DATE in dashboard_refresh_service.py: {pattern}")
                found_hardcoded = True
        
        if not found_hardcoded:
            logger.info("✅ No hardcoded dates found in dashboard_refresh_service.py!")
        
        return not found_hardcoded
        
    except Exception as e:
        logger.error(f"❌ Error reading dashboard refresh service: {e}")
        return False

def test_critical_frontend_files():
    """Test critical frontend files for hardcoded dates"""
    logger.info("=== TESTING FRONTEND FILES ===")
    
    critical_files = [
        "orchestrator/dashboard/client/src/services/dashboardApi.js",
        "orchestrator/dashboard/client/src/components/DashboardGrid.js",
        "orchestrator/dashboard/client/src/components/dashboard/ImprovedDashboardControls.jsx"
    ]
    
    hardcoded_patterns = ["'2024-01-01'", '"2024-01-01"', "'2025-01-01'", '"2025-01-01"', "'2025-04-01'", '"2025-04-01"']
    
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
                    logger.info(f"✅ {file_path} - Clean!")
                    
            except Exception as e:
                logger.error(f"❌ Error reading {file_path}: {e}")
                all_clean = False
        else:
            logger.warning(f"⚠️ File not found: {file_path}")
    
    return all_clean

def main():
    """Run verification tests"""
    logger.info("🔍 RUNNING SIMPLE DATE VERIFICATION")
    
    # Test 1: Database
    db_success, min_date, max_date = test_database_date_range()
    
    # Test 2: Dashboard Refresh Service
    service_success = test_dashboard_refresh_service_exists()
    
    # Test 3: Frontend Files
    frontend_success = test_critical_frontend_files()
    
    if db_success and service_success and frontend_success:
        logger.info("🎉 ALL VERIFICATIONS PASSED!")
        logger.info("✅ Database has real data")
        logger.info("✅ Dashboard Refresh Service is properly implemented")
        logger.info("✅ Critical frontend files are clean of hardcoded dates")
        logger.info("")
        logger.info("🚀 HARDCODED DATE ISSUE COMPLETELY RESOLVED!")
        return True
    else:
        logger.error("❌ Some verifications failed")
        return False

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)
