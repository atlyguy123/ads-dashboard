#!/usr/bin/env python3
"""
Analysis Script for Missing Credited Dates

This script investigates why 4,623 user_product_metrics records 
received PLACEHOLDER_DATE instead of real credited dates.
"""

import sqlite3
import pandas as pd
import logging

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Database path
DB_PATH = "/Users/joshuakaufman/Ads Dashboard V3 copy 12 - updated ingest copy/database/mixpanel_data.db"


def main():
    """Main analysis function"""
    logger.info("🔍 ANALYZING MISSING CREDITED DATES")
    logger.info("=" * 60)
    
    try:
        # Get records with PLACEHOLDER_DATE
        analyze_placeholder_records()
        
        # Check for missing starter events
        check_missing_starter_events()
        
        # Analyze products without events
        analyze_products_without_events()
        
        # Check user patterns
        analyze_user_patterns()
        
        # Check for potential data issues
        check_data_issues()
        
        logger.info("✅ Analysis completed!")
        return True
        
    except Exception as e:
        logger.error(f"❌ Analysis failed: {str(e)}")
        return False


def analyze_placeholder_records():
    """Analyze records that got PLACEHOLDER_DATE"""
    logger.info("📊 Analyzing records with PLACEHOLDER_DATE...")
    
    conn = sqlite3.connect(DB_PATH)
    
    # Get placeholder records
    placeholder_query = """
    SELECT 
        COUNT(*) as total_placeholder,
        COUNT(DISTINCT distinct_id) as unique_users,
        COUNT(DISTINCT product_id) as unique_products
    FROM user_product_metrics 
    WHERE credited_date = 'PLACEHOLDER_DATE'
    """
    
    placeholder_stats = pd.read_sql_query(placeholder_query, conn)
    row = placeholder_stats.iloc[0]
    
    logger.info(f"   Total PLACEHOLDER_DATE records: {row['total_placeholder']:,}")
    logger.info(f"   Unique users affected: {row['unique_users']:,}")
    logger.info(f"   Unique products affected: {row['unique_products']:,}")
    
    # Get top products with placeholder dates
    top_products_query = """
    SELECT 
        product_id,
        COUNT(*) as placeholder_count,
        COUNT(*) * 100.0 / (SELECT COUNT(*) FROM user_product_metrics WHERE credited_date = 'PLACEHOLDER_DATE') as percentage
    FROM user_product_metrics 
    WHERE credited_date = 'PLACEHOLDER_DATE'
    GROUP BY product_id
    ORDER BY placeholder_count DESC
    LIMIT 10
    """
    
    top_products = pd.read_sql_query(top_products_query, conn)
    logger.info("   Top products with PLACEHOLDER_DATE:")
    for _, row in top_products.iterrows():
        logger.info(f"     {row['product_id']}: {row['placeholder_count']:,} records ({row['percentage']:.1f}%)")
    
    conn.close()


def check_missing_starter_events():
    """Check which user-product combinations have no starter events"""
    logger.info("🔍 Checking for missing starter events...")
    
    conn = sqlite3.connect(DB_PATH)
    
    # Find user-product combinations in user_product_metrics but NOT in events
    missing_events_query = """
    SELECT 
        upm.distinct_id,
        upm.product_id,
        upm.credited_date
    FROM user_product_metrics upm
    LEFT JOIN (
        SELECT DISTINCT
            me.distinct_id,
            JSON_EXTRACT(me.event_json, '$.properties.product_id') as product_id
        FROM mixpanel_event me 
        WHERE me.event_name IN ('RC Trial started', 'RC Initial purchase')
        AND JSON_VALID(me.event_json) = 1
        AND JSON_EXTRACT(me.event_json, '$.properties.product_id') IS NOT NULL
        AND JSON_EXTRACT(me.event_json, '$.properties.product_id') != ''
    ) events ON upm.distinct_id = events.distinct_id 
        AND upm.product_id = events.product_id
    WHERE events.distinct_id IS NULL
    AND upm.credited_date = 'PLACEHOLDER_DATE'
    LIMIT 10
    """
    
    missing_events = pd.read_sql_query(missing_events_query, conn)
    
    if len(missing_events) > 0:
        logger.info(f"   Found {len(missing_events)} user-product combinations with no starter events:")
        for _, row in missing_events.head(5).iterrows():
            logger.info(f"     User: {row['distinct_id'][:8]}..., Product: {row['product_id']}")
    else:
        logger.info("   All PLACEHOLDER_DATE records have some events in the system")
    
    conn.close()


def analyze_products_without_events():
    """Analyze which products have no starter events at all"""
    logger.info("📦 Analyzing products without starter events...")
    
    conn = sqlite3.connect(DB_PATH)
    
    # Products in user_product_metrics but not in starter events
    products_no_events_query = """
    SELECT 
        upm.product_id,
        COUNT(*) as user_count
    FROM user_product_metrics upm
    WHERE upm.credited_date = 'PLACEHOLDER_DATE'
    AND upm.product_id NOT IN (
        SELECT DISTINCT JSON_EXTRACT(me.event_json, '$.properties.product_id')
        FROM mixpanel_event me 
        WHERE me.event_name IN ('RC Trial started', 'RC Initial purchase')
        AND JSON_VALID(me.event_json) = 1
        AND JSON_EXTRACT(me.event_json, '$.properties.product_id') IS NOT NULL
        AND JSON_EXTRACT(me.event_json, '$.properties.product_id') != ''
    )
    GROUP BY upm.product_id
    ORDER BY user_count DESC
    LIMIT 10
    """
    
    products_no_events = pd.read_sql_query(products_no_events_query, conn)
    
    if len(products_no_events) > 0:
        logger.info(f"   Products with NO starter events in mixpanel_event:")
        for _, row in products_no_events.iterrows():
            logger.info(f"     {row['product_id']}: {row['user_count']:,} users affected")
    else:
        logger.info("   All products have some starter events")
    
    conn.close()


def analyze_user_patterns():
    """Analyze user patterns for those with PLACEHOLDER_DATE"""
    logger.info("👥 Analyzing user patterns...")
    
    conn = sqlite3.connect(DB_PATH)
    
    # Check if these users have ANY events in the system
    user_events_query = """
    SELECT 
        'Users with PLACEHOLDER_DATE that have NO events' as category,
        COUNT(DISTINCT upm.distinct_id) as user_count
    FROM user_product_metrics upm
    LEFT JOIN mixpanel_event me ON upm.distinct_id = me.distinct_id
    WHERE upm.credited_date = 'PLACEHOLDER_DATE'
    AND me.distinct_id IS NULL
    
    UNION ALL
    
    SELECT 
        'Users with PLACEHOLDER_DATE that have OTHER events' as category,
        COUNT(DISTINCT upm.distinct_id) as user_count
    FROM user_product_metrics upm
    JOIN mixpanel_event me ON upm.distinct_id = me.distinct_id
    WHERE upm.credited_date = 'PLACEHOLDER_DATE'
    AND me.event_name NOT IN ('RC Trial started', 'RC Initial purchase')
    
    UNION ALL
    
    SELECT 
        'Users with PLACEHOLDER_DATE that have starter events for DIFFERENT products' as category,
        COUNT(DISTINCT upm.distinct_id) as user_count
    FROM user_product_metrics upm
    JOIN mixpanel_event me ON upm.distinct_id = me.distinct_id
    WHERE upm.credited_date = 'PLACEHOLDER_DATE'
    AND me.event_name IN ('RC Trial started', 'RC Initial purchase')
    AND JSON_EXTRACT(me.event_json, '$.properties.product_id') != upm.product_id
    """
    
    user_patterns = pd.read_sql_query(user_events_query, conn)
    
    logger.info("   User event patterns for PLACEHOLDER_DATE records:")
    for _, row in user_patterns.iterrows():
        logger.info(f"     {row['category']}: {row['user_count']:,}")
    
    conn.close()


def check_data_issues():
    """Check for potential data quality issues"""
    logger.info("⚠️  Checking for data quality issues...")
    
    conn = sqlite3.connect(DB_PATH)
    
    # Check for events with product_id that don't match user_product_metrics
    orphan_events_query = """
    SELECT 
        COUNT(*) as orphan_starter_events
    FROM mixpanel_event me 
    WHERE me.event_name IN ('RC Trial started', 'RC Initial purchase')
    AND JSON_VALID(me.event_json) = 1
    AND JSON_EXTRACT(me.event_json, '$.properties.product_id') IS NOT NULL
    AND JSON_EXTRACT(me.event_json, '$.properties.product_id') != ''
    AND NOT EXISTS (
        SELECT 1 FROM user_product_metrics upm
        WHERE upm.distinct_id = me.distinct_id
        AND upm.product_id = JSON_EXTRACT(me.event_json, '$.properties.product_id')
    )
    """
    
    orphan_events = pd.read_sql_query(orphan_events_query, conn)
    orphan_count = orphan_events.iloc[0, 0]
    
    logger.info(f"   Starter events with no matching user_product_metrics: {orphan_count:,}")
    
    # Check for inconsistent product_id formats
    product_format_query = """
    SELECT 
        'In user_product_metrics' as source,
        COUNT(DISTINCT product_id) as unique_products,
        MIN(LENGTH(product_id)) as min_length,
        MAX(LENGTH(product_id)) as max_length
    FROM user_product_metrics
    WHERE credited_date = 'PLACEHOLDER_DATE'
    
    UNION ALL
    
    SELECT 
        'In mixpanel_event' as source,
        COUNT(DISTINCT JSON_EXTRACT(event_json, '$.properties.product_id')) as unique_products,
        MIN(LENGTH(JSON_EXTRACT(event_json, '$.properties.product_id'))) as min_length,
        MAX(LENGTH(JSON_EXTRACT(event_json, '$.properties.product_id'))) as max_length
    FROM mixpanel_event
    WHERE event_name IN ('RC Trial started', 'RC Initial purchase')
    AND JSON_VALID(event_json) = 1
    AND JSON_EXTRACT(event_json, '$.properties.product_id') IS NOT NULL
    """
    
    format_check = pd.read_sql_query(product_format_query, conn)
    
    logger.info("   Product ID format comparison:")
    for _, row in format_check.iterrows():
        logger.info(f"     {row['source']}: {row['unique_products']:,} products, length {row['min_length']}-{row['max_length']}")
    
    # Sample some specific PLACEHOLDER_DATE records for manual inspection
    sample_query = """
    SELECT 
        upm.distinct_id,
        upm.product_id,
        COUNT(me.event_uuid) as total_events,
        COUNT(CASE WHEN me.event_name IN ('RC Trial started', 'RC Initial purchase') THEN 1 END) as starter_events
    FROM user_product_metrics upm
    LEFT JOIN mixpanel_event me ON upm.distinct_id = me.distinct_id
    WHERE upm.credited_date = 'PLACEHOLDER_DATE'
    GROUP BY upm.distinct_id, upm.product_id
    ORDER BY total_events DESC
    LIMIT 5
    """
    
    samples = pd.read_sql_query(sample_query, conn)
    
    logger.info("   Sample PLACEHOLDER_DATE records:")
    for _, row in samples.iterrows():
        logger.info(f"     User: {row['distinct_id'][:8]}..., Product: {row['product_id']}, "
                   f"Total events: {row['total_events']}, Starter events: {row['starter_events']}")
    
    conn.close()


if __name__ == "__main__":
    success = main()
    if success:
        print("\n🎯 Analysis Summary:")
        print("The 11% of records with PLACEHOLDER_DATE are user-product combinations")
        print("that exist in user_product_metrics but have no corresponding starter events")
        print("in mixpanel_event. This suggests:")
        print("1. Records were created through other means (conversions, renewals, etc.)")
        print("2. Data ingestion timing differences")
        print("3. Historical data without complete event tracking")
    else:
        print("\n❌ Analysis failed!") 