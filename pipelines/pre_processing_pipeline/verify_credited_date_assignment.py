#!/usr/bin/env python3
"""
Verification Script for Credited Date Assignment

This script verifies that the credited date assignment module is working correctly
by checking the results and providing detailed analytics.
"""

import sqlite3
import pandas as pd
from datetime import datetime
import logging

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Database path
DB_PATH = "/Users/joshuakaufman/Ads Dashboard V3 copy 12 - updated ingest copy/database/mixpanel_data.db"


def main():
    """Main verification function"""
    logger.info("🔍 VERIFYING CREDITED DATE ASSIGNMENT RESULTS")
    logger.info("=" * 60)
    
    try:
        # Check basic statistics
        check_basic_statistics()
        
        # Verify data quality
        verify_data_quality()
        
        # Check edge cases
        check_edge_cases()
        
        # Validate consistency
        validate_consistency()
        
        logger.info("✅ All verification checks completed successfully!")
        return True
        
    except Exception as e:
        logger.error(f"❌ Verification failed: {str(e)}")
        return False


def check_basic_statistics():
    """Check basic statistics about credited date assignment"""
    logger.info("📊 Checking basic statistics...")
    
    conn = sqlite3.connect(DB_PATH)
    
    # Total records
    total_query = "SELECT COUNT(*) FROM user_product_metrics"
    total_records = pd.read_sql_query(total_query, conn).iloc[0, 0]
    
    # Records with credited_date
    with_date_query = "SELECT COUNT(*) FROM user_product_metrics WHERE credited_date IS NOT NULL"
    with_date_records = pd.read_sql_query(with_date_query, conn).iloc[0, 0]
    
    # Records without credited_date
    without_date_records = total_records - with_date_records
    
    logger.info(f"   Total user_product_metrics records: {total_records:,}")
    logger.info(f"   Records with credited_date: {with_date_records:,} ({with_date_records/total_records*100:.1f}%)")
    logger.info(f"   Records without credited_date: {without_date_records:,} ({without_date_records/total_records*100:.1f}%)")
    
    # Check for placeholder dates
    placeholder_query = "SELECT COUNT(*) FROM user_product_metrics WHERE credited_date = 'PLACEHOLDER_DATE'"
    placeholder_records = pd.read_sql_query(placeholder_query, conn).iloc[0, 0]
    
    if placeholder_records > 0:
        logger.warning(f"   Records with PLACEHOLDER_DATE: {placeholder_records:,}")
    
    conn.close()


def verify_data_quality():
    """Verify the quality of credited date assignments"""
    logger.info("🔍 Verifying data quality...")
    
    conn = sqlite3.connect(DB_PATH)
    
    # Check date format consistency
    date_format_query = """
    SELECT credited_date, COUNT(*) as count
    FROM user_product_metrics 
    WHERE credited_date IS NOT NULL 
    AND credited_date != 'PLACEHOLDER_DATE'
    GROUP BY credited_date
    ORDER BY count DESC
    LIMIT 10
    """
    
    date_samples = pd.read_sql_query(date_format_query, conn)
    logger.info("   Sample credited dates (top 10 by frequency):")
    for _, row in date_samples.head(5).iterrows():
        logger.info(f"     {row['credited_date']} ({row['count']:,} records)")
    
    # Check date range
    date_range_query = """
    SELECT 
        MIN(credited_date) as min_date,
        MAX(credited_date) as max_date
    FROM user_product_metrics 
    WHERE credited_date IS NOT NULL 
    AND credited_date != 'PLACEHOLDER_DATE'
    """
    
    date_range = pd.read_sql_query(date_range_query, conn)
    logger.info(f"   Date range: {date_range.iloc[0]['min_date']} to {date_range.iloc[0]['max_date']}")
    
    # Check for invalid dates
    invalid_dates_query = """
    SELECT credited_date, COUNT(*) as count
    FROM user_product_metrics 
    WHERE credited_date IS NOT NULL 
    AND credited_date NOT LIKE '____-__-__'
    AND credited_date != 'PLACEHOLDER_DATE'
    GROUP BY credited_date
    """
    
    invalid_dates = pd.read_sql_query(invalid_dates_query, conn)
    if len(invalid_dates) > 0:
        logger.warning(f"   Found {len(invalid_dates)} invalid date formats:")
        for _, row in invalid_dates.iterrows():
            logger.warning(f"     {row['credited_date']} ({row['count']} records)")
    else:
        logger.info("   ✅ All dates are in valid YYYY-MM-DD format")
    
    conn.close()


def check_edge_cases():
    """Check for edge cases and potential issues"""
    logger.info("🎯 Checking edge cases...")
    
    conn = sqlite3.connect(DB_PATH)
    
    # Check users with multiple products
    multi_product_query = """
    SELECT distinct_id, COUNT(*) as product_count
    FROM user_product_metrics
    WHERE credited_date IS NOT NULL 
    AND credited_date != 'PLACEHOLDER_DATE'
    GROUP BY distinct_id
    HAVING COUNT(*) > 1
    ORDER BY product_count DESC
    LIMIT 5
    """
    
    multi_product = pd.read_sql_query(multi_product_query, conn)
    if len(multi_product) > 0:
        logger.info(f"   Users with multiple products (top 5):")
        for _, row in multi_product.iterrows():
            logger.info(f"     {row['distinct_id'][:8]}... has {row['product_count']} products")
    
    # Check products with many users
    popular_products_query = """
    SELECT product_id, COUNT(*) as user_count
    FROM user_product_metrics
    WHERE credited_date IS NOT NULL 
    AND credited_date != 'PLACEHOLDER_DATE'
    GROUP BY product_id
    ORDER BY user_count DESC
    LIMIT 5
    """
    
    popular_products = pd.read_sql_query(popular_products_query, conn)
    logger.info("   Most popular products:")
    for _, row in popular_products.iterrows():
        logger.info(f"     {row['product_id']}: {row['user_count']:,} users")
    
    conn.close()


def validate_consistency():
    """Validate consistency between credited dates and actual events"""
    logger.info("🔗 Validating consistency with events...")
    
    conn = sqlite3.connect(DB_PATH)
    
    # Sample validation: check if credited dates match earliest starter events
    consistency_query = """
    WITH earliest_events AS (
        SELECT 
            me.distinct_id,
            JSON_EXTRACT(me.event_json, '$.properties.product_id') as product_id,
            MIN(DATE(me.event_time)) as earliest_event_date
        FROM mixpanel_event me 
        WHERE me.event_name IN ('RC Trial started', 'RC Initial purchase')
        AND JSON_VALID(me.event_json) = 1
        AND JSON_EXTRACT(me.event_json, '$.properties.product_id') IS NOT NULL
        AND JSON_EXTRACT(me.event_json, '$.properties.product_id') != ''
        GROUP BY me.distinct_id, JSON_EXTRACT(me.event_json, '$.properties.product_id')
    )
    SELECT 
        COUNT(*) as total_matches,
        SUM(CASE WHEN upm.credited_date = ee.earliest_event_date THEN 1 ELSE 0 END) as correct_matches,
        SUM(CASE WHEN upm.credited_date != ee.earliest_event_date THEN 1 ELSE 0 END) as incorrect_matches
    FROM user_product_metrics upm
    JOIN earliest_events ee ON upm.distinct_id = ee.distinct_id 
        AND upm.product_id = ee.product_id
    WHERE upm.credited_date IS NOT NULL 
    AND upm.credited_date != 'PLACEHOLDER_DATE'
    """
    
    consistency_check = pd.read_sql_query(consistency_query, conn)
    
    if len(consistency_check) > 0:
        row = consistency_check.iloc[0]
        total = row['total_matches']
        correct = row['correct_matches']
        incorrect = row['incorrect_matches']
        
        accuracy = correct / total * 100 if total > 0 else 0
        
        logger.info(f"   Consistency check results:")
        logger.info(f"     Total matches: {total:,}")
        logger.info(f"     Correct matches: {correct:,} ({accuracy:.1f}%)")
        logger.info(f"     Incorrect matches: {incorrect:,}")
        
        if accuracy >= 99:
            logger.info("   ✅ Excellent consistency!")
        elif accuracy >= 95:
            logger.info("   ✅ Good consistency")
        else:
            logger.warning("   ⚠️  Low consistency - may need investigation")
    
    conn.close()


if __name__ == "__main__":
    success = main()
    if success:
        print("\n🎉 Verification completed successfully!")
    else:
        print("\n❌ Verification failed!") 