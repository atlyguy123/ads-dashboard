#!/usr/bin/env python3
"""
Debug Hierarchy JOIN Issue

Investigates why the JOIN between daily_mixpanel_metrics and id_hierarchy_mapping
is failing despite both tables having the expected data.
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

def debug_specific_campaign():
    """Debug the specific failing campaign"""
    logger.info("=== DEBUGGING SPECIFIC CAMPAIGN JOIN ISSUE ===")
    
    with get_database_connection('mixpanel_data') as conn:
        cursor = conn.cursor()
        
        campaign_id = "120213243563310178"
        logger.info(f"🎯 Debugging campaign: {campaign_id}")
        
        # Step 1: Check hierarchy mappings for this campaign
        cursor.execute("""
        SELECT ad_id, adset_id, campaign_id 
        FROM id_hierarchy_mapping 
        WHERE campaign_id = ?
        LIMIT 5
        """, [campaign_id])
        
        mappings = cursor.fetchall()
        logger.info(f"📊 Hierarchy mappings for campaign {campaign_id}:")
        for ad_id, adset_id, camp_id in mappings:
            logger.info(f"   Ad {ad_id} → AdSet {adset_id} → Campaign {camp_id}")
        
        # Step 2: Check what adsets this campaign should have
        cursor.execute("""
        SELECT DISTINCT adset_id 
        FROM id_hierarchy_mapping 
        WHERE campaign_id = ?
        """, [campaign_id])
        
        expected_adsets = [row[0] for row in cursor.fetchall()]
        logger.info(f"📊 Expected adsets for campaign {campaign_id}: {len(expected_adsets)}")
        for adset_id in expected_adsets[:3]:  # Show first 3
            logger.info(f"   Expected AdSet: {adset_id}")
        
        # Step 3: Check if these adsets exist in daily_mixpanel_metrics
        logger.info("🔍 Checking if expected adsets exist in daily_mixpanel_metrics:")
        for adset_id in expected_adsets[:5]:  # Check first 5
            cursor.execute("""
            SELECT COUNT(*), MIN(date), MAX(date)
            FROM daily_mixpanel_metrics 
            WHERE entity_type = 'adset' AND entity_id = ?
            """, [adset_id])
            
            result = cursor.fetchone()
            count, min_date, max_date = result
            
            if count > 0:
                logger.info(f"   ✅ AdSet {adset_id}: {count} metrics ({min_date} to {max_date})")
            else:
                logger.info(f"   ❌ AdSet {adset_id}: NO METRICS FOUND")
        
        # Step 4: Test the exact problematic query
        logger.info("🔧 Testing the exact child adset query:")
        
        problem_query = """
        SELECT 
            adset_data.entity_id as adset_id,
            COALESCE(nm.canonical_name, 'Unknown Adset (' || adset_data.entity_id || ')') as adset_name,
            hm.campaign_id as mapped_campaign_id
        FROM (
            SELECT 
                entity_id,
                SUM(trial_users_count) as total_users,
                SUM(trial_users_count) as mixpanel_trials_started,
                SUM(purchase_users_count) as mixpanel_purchases,
                SUM(estimated_revenue_usd) as estimated_revenue_usd
            FROM daily_mixpanel_metrics
            WHERE entity_type = 'adset'
              AND date BETWEEN '2024-01-01' AND '2024-12-31'
            GROUP BY entity_id
        ) adset_data
        LEFT JOIN id_name_mapping nm ON adset_data.entity_id = nm.entity_id AND nm.entity_type = 'adset'
        LEFT JOIN id_hierarchy_mapping hm ON adset_data.entity_id = hm.adset_id
        WHERE hm.campaign_id = ?
        LIMIT 10
        """
        
        cursor.execute(problem_query, [campaign_id])
        results = cursor.fetchall()
        
        logger.info(f"🔍 Query results: {len(results)} rows returned")
        for adset_id, adset_name, mapped_campaign in results:
            logger.info(f"   Result: AdSet {adset_id} ({adset_name}) → Campaign {mapped_campaign}")
        
        # Step 5: Test the JOIN separately to isolate the issue
        logger.info("🔧 Testing JOIN components separately:")
        
        # Test the subquery first
        cursor.execute("""
        SELECT entity_id, SUM(trial_users_count) as trials
        FROM daily_mixpanel_metrics
        WHERE entity_type = 'adset'
          AND date BETWEEN '2024-01-01' AND '2024-12-31'
        GROUP BY entity_id
        HAVING entity_id IN (
            SELECT DISTINCT adset_id 
            FROM id_hierarchy_mapping 
            WHERE campaign_id = ?
        )
        """, [campaign_id])
        
        subquery_results = cursor.fetchall()
        logger.info(f"📊 Subquery results (adsets with metrics for this campaign): {len(subquery_results)}")
        for entity_id, trials in subquery_results:
            logger.info(f"   AdSet {entity_id}: {trials} trials")
        
        # Test the hierarchy join directly
        cursor.execute("""
        SELECT DISTINCT dmm.entity_id, hm.adset_id, hm.campaign_id
        FROM daily_mixpanel_metrics dmm
        INNER JOIN id_hierarchy_mapping hm ON dmm.entity_id = hm.adset_id
        WHERE dmm.entity_type = 'adset'
          AND hm.campaign_id = ?
        LIMIT 10
        """, [campaign_id])
        
        join_results = cursor.fetchall()
        logger.info(f"📊 Direct JOIN results: {len(join_results)}")
        for dmm_id, hm_adset_id, hm_campaign_id in join_results:
            logger.info(f"   DMM AdSet {dmm_id} ↔ HM AdSet {hm_adset_id} → Campaign {hm_campaign_id}")

def debug_general_join_issues():
    """Look for general JOIN alignment issues"""
    logger.info("=== DEBUGGING GENERAL JOIN ALIGNMENT ===")
    
    with get_database_connection('mixpanel_data') as conn:
        cursor = conn.cursor()
        
        # Check for adsets that exist in metrics but not in hierarchy
        cursor.execute("""
        SELECT dmm.entity_id
        FROM daily_mixpanel_metrics dmm
        LEFT JOIN id_hierarchy_mapping hm ON dmm.entity_id = hm.adset_id
        WHERE dmm.entity_type = 'adset'
          AND hm.adset_id IS NULL
        GROUP BY dmm.entity_id
        LIMIT 10
        """)
        
        orphaned_adsets = cursor.fetchall()
        logger.info(f"🔍 Adsets with metrics but no hierarchy mapping: {len(orphaned_adsets)}")
        for adset_id, in orphaned_adsets:
            logger.info(f"   Orphaned AdSet: {adset_id}")
        
        # Check for adsets that exist in hierarchy but not in metrics
        cursor.execute("""
        SELECT hm.adset_id
        FROM id_hierarchy_mapping hm
        LEFT JOIN daily_mixpanel_metrics dmm ON hm.adset_id = dmm.entity_id AND dmm.entity_type = 'adset'
        WHERE dmm.entity_id IS NULL
        GROUP BY hm.adset_id
        LIMIT 10
        """)
        
        missing_adsets = cursor.fetchall()
        logger.info(f"🔍 Adsets in hierarchy but no metrics: {len(missing_adsets)}")
        for adset_id, in missing_adsets:
            logger.info(f"   Missing AdSet: {adset_id}")
        
        # Check for successful joins
        cursor.execute("""
        SELECT COUNT(DISTINCT dmm.entity_id)
        FROM daily_mixpanel_metrics dmm
        INNER JOIN id_hierarchy_mapping hm ON dmm.entity_id = hm.adset_id
        WHERE dmm.entity_type = 'adset'
        """)
        
        successful_joins = cursor.fetchone()[0]
        logger.info(f"✅ Successful adset joins: {successful_joins}")

def main():
    """Debug the hierarchy JOIN issues"""
    logger.info("🔍 DEBUGGING HIERARCHY JOIN ISSUES")
    logger.info("=" * 60)
    
    debug_specific_campaign()
    debug_general_join_issues()
    
    logger.info("=" * 60)
    logger.info("🎯 Debug complete")
    
    return 0

if __name__ == "__main__":
    exit(main())
