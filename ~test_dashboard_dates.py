#!/usr/bin/env python3
"""
Test Dashboard Date Handling

Tests what dates are actually being used in dashboard queries
and verifies the hierarchy child methods get the correct dates.
"""

import sqlite3
import logging
from pathlib import Path
import sys
from datetime import datetime

# Add utils and dashboard services to path
utils_path = str(Path(__file__).resolve().parent / "utils")
dashboard_path = str(Path(__file__).resolve().parent / "orchestrator" / "dashboard" / "services")
sys.path.append(utils_path)
sys.path.append(dashboard_path)

from database_utils import get_database_connection
from analytics_query_service import AnalyticsQueryService, QueryConfig

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def test_current_date_ranges():
    """Test what date ranges exist in the data and what should be used"""
    logger.info("=== TESTING CURRENT DATA DATE RANGES ===")
    
    with get_database_connection('mixpanel_data') as conn:
        cursor = conn.cursor()
        
        # Check actual date ranges in daily_mixpanel_metrics
        cursor.execute("""
        SELECT 
            entity_type,
            MIN(date) as min_date,
            MAX(date) as max_date,
            COUNT(DISTINCT entity_id) as unique_entities,
            COUNT(*) as total_records
        FROM daily_mixpanel_metrics
        GROUP BY entity_type
        ORDER BY entity_type
        """)
        
        logger.info("📊 Current data ranges in daily_mixpanel_metrics:")
        for entity_type, min_date, max_date, unique_entities, total_records in cursor.fetchall():
            logger.info(f"   {entity_type.upper()}: {min_date} to {max_date} ({unique_entities} entities, {total_records} records)")
        
        # Get the overall date range
        cursor.execute("SELECT MIN(date), MAX(date) FROM daily_mixpanel_metrics")
        overall_min, overall_max = cursor.fetchone()
        logger.info(f"📅 OVERALL DATA RANGE: {overall_min} to {overall_max}")
        
        return overall_min, overall_max

def test_dashboard_query_with_correct_dates():
    """Test dashboard query with actual data date range"""
    logger.info("=== TESTING DASHBOARD QUERY WITH CORRECT DATES ===")
    
    # Get the actual data date range
    min_date, max_date = test_current_date_ranges()
    
    # Create analytics service and config with actual data dates
    analytics_service = AnalyticsQueryService()
    
    config = QueryConfig(
        breakdown='all',
        start_date=min_date,
        end_date=max_date,
        group_by='campaign',
        include_mixpanel=True
    )
    
    logger.info(f"🎯 Testing with config dates: {config.start_date} to {config.end_date}")
    logger.info(f"🎯 Group by: {config.group_by}")
    
    # Execute the query (just to see if children are populated)
    result = analytics_service.execute_analytics_query(config)
    
    if result.get('success'):
        data = result.get('data', [])
        logger.info(f"📊 Query successful: {len(data)} campaigns returned")
        
        # Check if any campaigns have children
        campaigns_with_children = 0
        total_children = 0
        
        for campaign in data:
            children = campaign.get('children', [])
            if children:
                campaigns_with_children += 1
                total_children += len(children)
                
                # Log details for first campaign with children
                if campaigns_with_children == 1:
                    campaign_id = campaign.get('campaign_id', 'Unknown')
                    logger.info(f"🎯 First campaign with children: {campaign_id}")
                    logger.info(f"   📊 Campaign: {campaign.get('campaign_name', 'Unknown')}")
                    logger.info(f"   👥 Children: {len(children)} adsets")
                    
                    for i, child in enumerate(children[:3]):  # Show first 3 children
                        child_id = child.get('adset_id', 'Unknown')
                        child_name = child.get('adset_name', 'Unknown')
                        logger.info(f"      AdSet {i+1}: {child_id} ({child_name})")
                        
                        # Check if adsets have children (ads)
                        grandchildren = child.get('children', [])
                        if grandchildren:
                            logger.info(f"         └─ {len(grandchildren)} ads under this adset")
        
        logger.info(f"✅ SUCCESS: {campaigns_with_children}/{len(data)} campaigns have children")
        logger.info(f"✅ SUCCESS: {total_children} total adset children found")
        
        if campaigns_with_children == 0:
            logger.error("❌ NO CHILDREN FOUND! Issue persists even with correct dates")
            return False
        else:
            logger.info("🎉 CHILDREN FOUND! Hierarchy is working with correct dates")
            return True
            
    else:
        error = result.get('error', 'Unknown error')
        logger.error(f"❌ Query failed: {error}")
        return False

def test_specific_child_method():
    """Test the child method directly with correct dates"""
    logger.info("=== TESTING CHILD METHOD DIRECTLY ===")
    
    min_date, max_date = test_current_date_ranges()
    
    analytics_service = AnalyticsQueryService()
    config = QueryConfig(
        breakdown='all',
        start_date=min_date,
        end_date=max_date,
        group_by='campaign',
        include_mixpanel=True
    )
    
    # Get a sample campaign that should have children
    with get_database_connection('mixpanel_data') as conn:
        cursor = conn.cursor()
        
        # Find a campaign that has adsets in hierarchy mapping and metrics data
        cursor.execute("""
        SELECT DISTINCT hm.campaign_id
        FROM id_hierarchy_mapping hm
        INNER JOIN daily_mixpanel_metrics dmm ON hm.adset_id = dmm.entity_id
        WHERE dmm.entity_type = 'adset'
          AND dmm.date BETWEEN ? AND ?
        LIMIT 1
        """, [min_date, max_date])
        
        result = cursor.fetchone()
        if not result:
            logger.error("❌ No campaign found with adsets that have metrics in date range")
            return False
        
        test_campaign_id = result[0]
        logger.info(f"🎯 Testing child method with campaign: {test_campaign_id}")
        
        # Test the child method directly
        children = analytics_service._get_child_adsets_for_campaign(test_campaign_id, config)
        
        logger.info(f"📊 Direct child method result: {len(children)} adsets found")
        
        if children:
            for i, child in enumerate(children[:3]):
                child_id = child.get('adset_id', 'Unknown')
                child_name = child.get('adset_name', 'Unknown')
                trials = child.get('mixpanel_trials_started', 0)
                logger.info(f"   AdSet {i+1}: {child_id} ({child_name}) - {trials} trials")
            
            logger.info("✅ Child method working correctly!")
            return True
        else:
            logger.error("❌ Child method returned no results")
            return False

def main():
    """Test dashboard date handling"""
    logger.info("🔍 TESTING DASHBOARD DATE HANDLING")
    logger.info("=" * 60)
    
    # Test 1: Check current data ranges
    logger.info("Step 1: Checking current data ranges...")
    min_date, max_date = test_current_date_ranges()
    
    # Test 2: Test full dashboard query with correct dates
    logger.info("Step 2: Testing full dashboard query...")
    dashboard_success = test_dashboard_query_with_correct_dates()
    
    # Test 3: Test child method directly
    logger.info("Step 3: Testing child method directly...")
    child_method_success = test_specific_child_method()
    
    logger.info("=" * 60)
    logger.info("🎯 TEST SUMMARY:")
    logger.info(f"   Data Date Range: {min_date} to {max_date}")
    logger.info(f"   Dashboard Query: {'✅' if dashboard_success else '❌'}")
    logger.info(f"   Child Method: {'✅' if child_method_success else '❌'}")
    
    if dashboard_success and child_method_success:
        logger.info("🎉 All tests passed! Hierarchy should be working.")
    else:
        logger.error("❌ Tests failed. Further investigation needed.")
    
    return 0

if __name__ == "__main__":
    exit(main())
