#!/usr/bin/env python3
"""
Comprehensive Hierarchy Diagnostic Script

Analyzes the current state of hierarchy data and identifies why children 
are not being displayed in the dashboard.

Checks:
1. id_hierarchy_mapping table completeness
2. daily_mixpanel_metrics data availability
3. Hierarchy relationship integrity
4. Sample queries for debugging
"""

import sqlite3
import logging
import json
from pathlib import Path
import sys
from datetime import datetime

# Add utils to path
utils_path = str(Path(__file__).resolve().parent / "utils")
sys.path.append(utils_path)
from database_utils import get_database_connection

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def analyze_hierarchy_mapping():
    """Analyze the id_hierarchy_mapping table"""
    logger.info("=== ANALYZING HIERARCHY MAPPING TABLE ===")
    
    with get_database_connection('mixpanel_data') as conn:
        cursor = conn.cursor()
        
        # Check if table exists
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='id_hierarchy_mapping'")
        if not cursor.fetchone():
            logger.error("❌ id_hierarchy_mapping table does not exist!")
            return False
        
        # Count total mappings
        cursor.execute("SELECT COUNT(*) FROM id_hierarchy_mapping")
        total_mappings = cursor.fetchone()[0]
        logger.info(f"📊 Total hierarchy mappings: {total_mappings}")
        
        if total_mappings == 0:
            logger.error("❌ No hierarchy mappings found! Need to run meta pipeline module 03.")
            return False
        
        # Sample some mappings
        cursor.execute("""
        SELECT ad_id, adset_id, campaign_id, relationship_confidence 
        FROM id_hierarchy_mapping 
        LIMIT 5
        """)
        samples = cursor.fetchall()
        logger.info("🔍 Sample hierarchy mappings:")
        for ad_id, adset_id, campaign_id, confidence in samples:
            logger.info(f"   Ad {ad_id} → AdSet {adset_id} → Campaign {campaign_id} (confidence: {confidence})")
        
        # Check for unique campaigns and adsets
        cursor.execute("SELECT COUNT(DISTINCT campaign_id) FROM id_hierarchy_mapping")
        unique_campaigns = cursor.fetchone()[0]
        cursor.execute("SELECT COUNT(DISTINCT adset_id) FROM id_hierarchy_mapping")
        unique_adsets = cursor.fetchone()[0]
        cursor.execute("SELECT COUNT(DISTINCT ad_id) FROM id_hierarchy_mapping")
        unique_ads = cursor.fetchone()[0]
        
        logger.info(f"📈 Hierarchy structure: {unique_campaigns} campaigns → {unique_adsets} adsets → {unique_ads} ads")
        
        return True

def analyze_precomputed_metrics():
    """Analyze the daily_mixpanel_metrics table"""
    logger.info("=== ANALYZING PRECOMPUTED METRICS TABLE ===")
    
    with get_database_connection('mixpanel_data') as conn:
        cursor = conn.cursor()
        
        # Check if table exists
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='daily_mixpanel_metrics'")
        if not cursor.fetchone():
            logger.error("❌ daily_mixpanel_metrics table does not exist!")
            return False
        
        # Count metrics by entity type
        for entity_type in ['campaign', 'adset', 'ad']:
            cursor.execute("""
            SELECT COUNT(*), COUNT(DISTINCT entity_id), MIN(date), MAX(date)
            FROM daily_mixpanel_metrics 
            WHERE entity_type = ?
            """, (entity_type,))
            
            result = cursor.fetchone()
            count, unique_entities, min_date, max_date = result
            logger.info(f"📊 {entity_type.upper()}: {count} metrics for {unique_entities} entities ({min_date} to {max_date})")
            
            if count == 0:
                logger.warning(f"⚠️ No precomputed data for {entity_type} entities!")
        
        return True

def test_hierarchy_child_queries():
    """Test the actual child queries used by the dashboard"""
    logger.info("=== TESTING HIERARCHY CHILD QUERIES ===")
    
    with get_database_connection('mixpanel_data') as conn:
        cursor = conn.cursor()
        
        # Get a sample campaign
        cursor.execute("""
        SELECT DISTINCT entity_id 
        FROM daily_mixpanel_metrics 
        WHERE entity_type = 'campaign' 
        LIMIT 1
        """)
        
        campaign_result = cursor.fetchone()
        if not campaign_result:
            logger.error("❌ No campaign data found in precomputed metrics!")
            return False
        
        sample_campaign_id = campaign_result[0]
        logger.info(f"🎯 Testing with sample campaign: {sample_campaign_id}")
        
        # Test the same query used by _get_child_adsets_for_campaign
        child_adsets_query = """
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
              AND date BETWEEN '2024-01-01' AND '2024-12-31'
            GROUP BY entity_id
        ) adset_data
        LEFT JOIN id_name_mapping nm ON adset_data.entity_id = nm.entity_id AND nm.entity_type = 'adset'
        LEFT JOIN id_hierarchy_mapping hm ON adset_data.entity_id = hm.adset_id
        WHERE hm.campaign_id = ?
        ORDER BY adset_data.estimated_revenue_usd DESC
        """
        
        cursor.execute(child_adsets_query, [sample_campaign_id])
        child_adsets = cursor.fetchall()
        
        logger.info(f"🔍 Child adsets for campaign {sample_campaign_id}: {len(child_adsets)} found")
        
        if len(child_adsets) == 0:
            logger.error(f"❌ No child adsets found for campaign {sample_campaign_id}!")
            
            # Debug: Check if this campaign exists in hierarchy mapping
            cursor.execute("SELECT COUNT(*) FROM id_hierarchy_mapping WHERE campaign_id = ?", [sample_campaign_id])
            hierarchy_count = cursor.fetchone()[0]
            logger.info(f"   📊 Campaign {sample_campaign_id} has {hierarchy_count} entries in hierarchy mapping")
            
            # Debug: Check if we have adset data for this campaign's adsets
            cursor.execute("""
            SELECT hm.adset_id, COUNT(dmm.entity_id) as metric_count
            FROM id_hierarchy_mapping hm
            LEFT JOIN daily_mixpanel_metrics dmm ON hm.adset_id = dmm.entity_id AND dmm.entity_type = 'adset'
            WHERE hm.campaign_id = ?
            GROUP BY hm.adset_id
            """, [sample_campaign_id])
            
            adset_debug = cursor.fetchall()
            logger.info(f"   🔍 Debug - adsets for this campaign and their metric counts:")
            for adset_id, metric_count in adset_debug:
                logger.info(f"      AdSet {adset_id}: {metric_count} metrics")
        else:
            logger.info("✅ Child adsets found successfully!")
            for adset in child_adsets[:3]:  # Show first 3
                logger.info(f"   📊 AdSet: {adset[0]} ({adset[1]}) - {adset[2]} trials, ${adset[5]:.2f} revenue")
        
        # Test child ads query if we found adsets
        if child_adsets:
            sample_adset_id = child_adsets[0][0]
            logger.info(f"🎯 Testing child ads for sample adset: {sample_adset_id}")
            
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
                  AND date BETWEEN '2024-01-01' AND '2024-12-31'
                GROUP BY entity_id
            ) ad_data
            LEFT JOIN id_name_mapping nm ON ad_data.entity_id = nm.entity_id AND nm.entity_type = 'ad'
            LEFT JOIN id_hierarchy_mapping hm ON ad_data.entity_id = hm.ad_id
            WHERE hm.adset_id = ?
            ORDER BY ad_data.estimated_revenue_usd DESC
            """
            
            cursor.execute(child_ads_query, [sample_adset_id])
            child_ads = cursor.fetchall()
            
            logger.info(f"🔍 Child ads for adset {sample_adset_id}: {len(child_ads)} found")
            
            if len(child_ads) == 0:
                logger.error(f"❌ No child ads found for adset {sample_adset_id}!")
            else:
                logger.info("✅ Child ads found successfully!")
                for ad in child_ads[:3]:  # Show first 3
                    logger.info(f"   📊 Ad: {ad[0]} ({ad[1]}) - {ad[2]} trials, ${ad[5]:.2f} revenue")
        
        return True

def analyze_name_mappings():
    """Analyze the id_name_mapping table"""
    logger.info("=== ANALYZING NAME MAPPING TABLE ===")
    
    with get_database_connection('mixpanel_data') as conn:
        cursor = conn.cursor()
        
        # Check if table exists
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='id_name_mapping'")
        if not cursor.fetchone():
            logger.error("❌ id_name_mapping table does not exist!")
            return False
        
        # Count mappings by entity type
        for entity_type in ['campaign', 'adset', 'ad']:
            cursor.execute("SELECT COUNT(*) FROM id_name_mapping WHERE entity_type = ?", (entity_type,))
            count = cursor.fetchone()[0]
            logger.info(f"📊 {entity_type.upper()} name mappings: {count}")
        
        return True

def check_meta_database():
    """Check the Meta database for source data"""
    logger.info("=== CHECKING META DATABASE ===")
    
    try:
        with get_database_connection('meta_analytics') as conn:
            cursor = conn.cursor()
            
            # Check if ad_performance_daily exists
            cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='ad_performance_daily'")
            if not cursor.fetchone():
                logger.error("❌ ad_performance_daily table does not exist in meta database!")
                return False
            
            # Count rows
            cursor.execute("SELECT COUNT(*) FROM ad_performance_daily")
            total_rows = cursor.fetchone()[0]
            logger.info(f"📊 Meta ad_performance_daily table: {total_rows} rows")
            
            if total_rows == 0:
                logger.error("❌ No data in ad_performance_daily table!")
                return False
            
            # Check hierarchy completeness
            cursor.execute("""
            SELECT 
                COUNT(*) as total,
                COUNT(DISTINCT ad_id) as unique_ads,
                COUNT(DISTINCT adset_id) as unique_adsets,
                COUNT(DISTINCT campaign_id) as unique_campaigns
            FROM ad_performance_daily
            WHERE ad_id IS NOT NULL AND adset_id IS NOT NULL AND campaign_id IS NOT NULL
            """)
            
            result = cursor.fetchone()
            total, unique_ads, unique_adsets, unique_campaigns = result
            logger.info(f"📈 Meta hierarchy: {unique_campaigns} campaigns → {unique_adsets} adsets → {unique_ads} ads")
            logger.info(f"📊 Complete hierarchy rows: {total}")
            
            return True
            
    except Exception as e:
        logger.error(f"❌ Error accessing Meta database: {e}")
        return False

def main():
    """Run comprehensive hierarchy diagnostics"""
    logger.info("🔍 STARTING COMPREHENSIVE HIERARCHY DIAGNOSTICS")
    logger.info("=" * 80)
    
    # Step 1: Check Meta source data
    meta_ok = check_meta_database()
    
    # Step 2: Check hierarchy mapping
    hierarchy_ok = analyze_hierarchy_mapping()
    
    # Step 3: Check name mappings
    names_ok = analyze_name_mappings()
    
    # Step 4: Check precomputed metrics
    metrics_ok = analyze_precomputed_metrics()
    
    # Step 5: Test actual hierarchy queries
    queries_ok = test_hierarchy_child_queries()
    
    logger.info("=" * 80)
    logger.info("🎯 DIAGNOSTIC SUMMARY:")
    logger.info(f"   Meta Database: {'✅' if meta_ok else '❌'}")
    logger.info(f"   Hierarchy Mapping: {'✅' if hierarchy_ok else '❌'}")
    logger.info(f"   Name Mappings: {'✅' if names_ok else '❌'}")
    logger.info(f"   Precomputed Metrics: {'✅' if metrics_ok else '❌'}")
    logger.info(f"   Child Queries: {'✅' if queries_ok else '❌'}")
    
    if all([meta_ok, hierarchy_ok, names_ok, metrics_ok, queries_ok]):
        logger.info("🎉 All diagnostics passed! Hierarchy should be working.")
    else:
        logger.error("❌ Issues found. Hierarchy may not work properly.")
        
        # Provide specific recommendations
        if not meta_ok:
            logger.error("   🔧 Recommendation: Run meta pipeline to populate Meta database")
        if not hierarchy_ok:
            logger.error("   🔧 Recommendation: Run meta pipeline module 03_create_hierarchy_mapping.py")
        if not names_ok:
            logger.error("   🔧 Recommendation: Run meta pipeline module 02_create_id_name_mapping.py")
        if not metrics_ok:
            logger.error("   🔧 Recommendation: Run mixpanel pipeline module 08_compute_daily_metrics.py")
    
    return 0

if __name__ == "__main__":
    exit(main())
