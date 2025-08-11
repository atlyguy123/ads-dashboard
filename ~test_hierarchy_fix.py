#!/usr/bin/env python3
"""
Test Hierarchy Fix with Correct Date Ranges

Direct SQL testing to verify hierarchy works with proper dates.
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

def test_hierarchy_with_correct_dates():
    """Test the hierarchy query with the actual data date range"""
    logger.info("=== TESTING HIERARCHY WITH CORRECT DATES ===")
    
    with get_database_connection('mixpanel_data') as conn:
        cursor = conn.cursor()
        
        # Step 1: Get actual data date range
        cursor.execute("SELECT MIN(date), MAX(date) FROM daily_mixpanel_metrics")
        min_date, max_date = cursor.fetchone()
        logger.info(f"📅 Actual data range: {min_date} to {max_date}")
        
        # Step 2: Get a sample campaign
        cursor.execute("""
        SELECT DISTINCT entity_id 
        FROM daily_mixpanel_metrics 
        WHERE entity_type = 'campaign' 
        LIMIT 1
        """)
        
        campaign_result = cursor.fetchone()
        if not campaign_result:
            logger.error("❌ No campaign data found!")
            return False
        
        sample_campaign_id = campaign_result[0]
        logger.info(f"🎯 Testing with campaign: {sample_campaign_id}")
        
        # Step 3: Test child adsets query with CORRECT dates
        correct_child_query = """
        SELECT 
            adset_data.entity_id as adset_id,
            COALESCE(nm.canonical_name, 'Unknown Adset (' || adset_data.entity_id || ')') as adset_name,
            adset_data.total_users,
            adset_data.mixpanel_trials_started,
            adset_data.mixpanel_purchases,
            adset_data.estimated_revenue_usd
        FROM (
            SELECT 
                entity_id,
                SUM(trial_users_count) as total_users,
                SUM(trial_users_count) as mixpanel_trials_started,
                SUM(purchase_users_count) as mixpanel_purchases,
                SUM(estimated_revenue_usd) as estimated_revenue_usd
            FROM daily_mixpanel_metrics
            WHERE entity_type = 'adset'
              AND date BETWEEN ? AND ?
            GROUP BY entity_id
        ) adset_data
        LEFT JOIN id_name_mapping nm ON adset_data.entity_id = nm.entity_id AND nm.entity_type = 'adset'
        LEFT JOIN id_hierarchy_mapping hm ON adset_data.entity_id = hm.adset_id
        WHERE hm.campaign_id = ?
        ORDER BY adset_data.estimated_revenue_usd DESC
        """
        
        cursor.execute(correct_child_query, [min_date, max_date, sample_campaign_id])
        child_adsets = cursor.fetchall()
        
        logger.info(f"🔍 Child adsets with CORRECT dates: {len(child_adsets)} found")
        
        if child_adsets:
            logger.info("✅ SUCCESS! Hierarchy works with correct dates")
            for i, adset in enumerate(child_adsets[:3]):
                adset_id, adset_name, total_users, trials, purchases, revenue = adset
                logger.info(f"   AdSet {i+1}: {adset_id} ({adset_name}) - {trials} trials, ${revenue:.2f}")
            
            # Test child ads for the first adset
            if child_adsets:
                test_adset_id = child_adsets[0][0]
                logger.info(f"🎯 Testing child ads for adset: {test_adset_id}")
                
                child_ads_query = """
                SELECT 
                    ad_data.entity_id as ad_id,
                    COALESCE(nm.canonical_name, 'Unknown Ad (' || ad_data.entity_id || ')') as ad_name,
                    ad_data.total_users,
                    ad_data.mixpanel_trials_started,
                    ad_data.mixpanel_purchases,
                    ad_data.estimated_revenue_usd
                FROM (
                    SELECT 
                        entity_id,
                        SUM(trial_users_count) as total_users,
                        SUM(trial_users_count) as mixpanel_trials_started,
                        SUM(purchase_users_count) as mixpanel_purchases,
                        SUM(estimated_revenue_usd) as estimated_revenue_usd
                    FROM daily_mixpanel_metrics
                    WHERE entity_type = 'ad'
                      AND date BETWEEN ? AND ?
                    GROUP BY entity_id
                ) ad_data
                LEFT JOIN id_name_mapping nm ON ad_data.entity_id = nm.entity_id AND nm.entity_type = 'ad'
                LEFT JOIN id_hierarchy_mapping hm ON ad_data.entity_id = hm.ad_id
                WHERE hm.adset_id = ?
                ORDER BY ad_data.estimated_revenue_usd DESC
                """
                
                cursor.execute(child_ads_query, [min_date, max_date, test_adset_id])
                child_ads = cursor.fetchall()
                
                logger.info(f"🔍 Child ads for adset {test_adset_id}: {len(child_ads)} found")
                
                if child_ads:
                    logger.info("✅ SUCCESS! Ad children also work with correct dates")
                    for i, ad in enumerate(child_ads[:3]):
                        ad_id, ad_name, total_users, trials, purchases, revenue = ad
                        logger.info(f"      Ad {i+1}: {ad_id} ({ad_name}) - {trials} trials, ${revenue:.2f}")
                else:
                    logger.warning("⚠️ No child ads found, but adsets work")
            
            return True
        else:
            logger.error("❌ FAILED! Still no children even with correct dates")
            
            # Debug: Check what's in hierarchy mapping for this campaign
            cursor.execute("SELECT COUNT(*) FROM id_hierarchy_mapping WHERE campaign_id = ?", [sample_campaign_id])
            hierarchy_count = cursor.fetchone()[0]
            logger.info(f"   📊 Hierarchy mappings for campaign: {hierarchy_count}")
            
            return False

def check_frontend_default_dates():
    """Check what default dates the frontend might be using"""
    logger.info("=== CHECKING LIKELY FRONTEND DATE ISSUES ===")
    
    # The issue is likely that the frontend is using default dates that don't match our data
    # Let's see what happens if we test with common default date ranges
    
    test_ranges = [
        ("2024-01-01", "2024-12-31", "Full year 2024"),
        ("2024-07-01", "2024-07-31", "July 2024 (common default)"),
        ("2025-01-01", "2025-12-31", "Full year 2025"),
        ("2025-07-01", "2025-07-31", "July 2025"),
    ]
    
    with get_database_connection('mixpanel_data') as conn:
        cursor = conn.cursor()
        
        # Get our data range for comparison
        cursor.execute("SELECT MIN(date), MAX(date) FROM daily_mixpanel_metrics")
        actual_min, actual_max = cursor.fetchone()
        logger.info(f"📅 Actual data range: {actual_min} to {actual_max}")
        
        for start_date, end_date, description in test_ranges:
            cursor.execute("""
            SELECT COUNT(*) 
            FROM daily_mixpanel_metrics 
            WHERE date BETWEEN ? AND ?
            """, [start_date, end_date])
            
            count = cursor.fetchone()[0]
            overlap = "✅ OVERLAP" if count > 0 else "❌ NO DATA"
            logger.info(f"   {description} ({start_date} to {end_date}): {count} records {overlap}")
        
        # Recommendation
        logger.info(f"🎯 RECOMMENDATION: Frontend should use {actual_min} to {actual_max}")

def main():
    """Test hierarchy fix"""
    logger.info("🔍 TESTING HIERARCHY FIX WITH CORRECT DATES")
    logger.info("=" * 60)
    
    # Test 1: Check hierarchy with correct dates
    hierarchy_works = test_hierarchy_with_correct_dates()
    
    # Test 2: Check likely frontend date issues  
    check_frontend_default_dates()
    
    logger.info("=" * 60)
    logger.info("🎯 CONCLUSION:")
    
    if hierarchy_works:
        logger.info("✅ HIERARCHY WORKS! The issue is date range mismatch between frontend and data.")
        logger.info("🔧 SOLUTION: Update frontend to use correct date ranges or set proper defaults.")
    else:
        logger.error("❌ Hierarchy still broken. Need to investigate further.")
    
    return 0

if __name__ == "__main__":
    exit(main())
