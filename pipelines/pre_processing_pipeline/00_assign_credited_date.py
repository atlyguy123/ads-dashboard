#!/usr/bin/env python3
"""
Assign Credited Date Module - Pre-processing Pipeline

This module assigns credited_date to user_product_metrics records based on
starter events (RC Trial started or RC Initial purchase) from the mixpanel_event table.

Business Logic:
1. Find all starter events: 'RC Trial started' and 'RC Initial purchase'
2. For each user-product combination, identify the earliest starter event
3. Extract the date portion from the event_time
4. Update the credited_date field in user_product_metrics table

Starter Events Priority:
- Both 'RC Trial started' and 'RC Initial purchase' are valid starter events
- If multiple starter events exist for same user-product, use the earliest one
- credited_date is just the date (YYYY-MM-DD) extracted from event_time
"""

import os
import sys
import sqlite3
import logging
import pandas as pd
from datetime import datetime
from typing import Dict, List, Tuple, Optional
from collections import defaultdict
from pathlib import Path

# Add utils directory to path for database utilities
utils_path = str(Path(__file__).resolve().parent.parent.parent / "utils")
sys.path.append(utils_path)
from database_utils import get_database_path

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Configuration - use centralized database path discovery
DB_PATH = get_database_path('mixpanel_data')

# Starter event names
STARTER_EVENTS = ['RC Trial started', 'RC Initial purchase']

# Batch processing size
BATCH_SIZE = 1000


def main():
    """
    Main function for the credited date assignment module.
    """
    logger.info("🗓️  STARTING CREDITED DATE ASSIGNMENT")
    logger.info("=" * 60)
    
    try:
        # Verify database exists
        if not os.path.exists(DB_PATH):
            logger.error(f"❌ Database not found at {DB_PATH}")
            return False
        
        # Step 1: Get all starter events and group by user-product
        logger.info("📊 Extracting starter events...")
        starter_events = get_all_starter_events()
        
        if starter_events.empty:
            logger.warning("⚠️  No starter events found. Nothing to process.")
            return True
        
        # Step 2: Calculate credited dates for each user-product combination
        logger.info("🔄 Processing user-product combinations...")
        credited_dates = calculate_credited_dates(starter_events)
        
        if not credited_dates:
            logger.warning("⚠️  No credited dates calculated. Nothing to update.")
            return True
        
        # Step 3: Update user_product_metrics table
        logger.info("💾 Updating user_product_metrics table...")
        success = update_credited_dates_in_db(credited_dates)
        
        if success:
            logger.info("✅ Credited date assignment completed successfully")
            return True
        else:
            logger.error("❌ Failed to update credited dates in database")
            return False
        
    except Exception as e:
        logger.error(f"❌ Error in credited date assignment: {str(e)}")
        import traceback
        logger.error(f"Traceback: {traceback.format_exc()}")
        return False


def get_all_starter_events() -> pd.DataFrame:
    """
    Get all starter events (RC Trial started, RC Initial purchase) with user and product data.
    
    Returns:
        DataFrame with columns: distinct_id, product_id, event_time, event_name, event_date
    """
    logger.info("📊 Extracting starter events from mixpanel_event table...")
    
    # Query to get all starter events with product_id
    # Use CASE statement to handle malformed JSON gracefully
    query = """
    SELECT 
        me.distinct_id,
        CASE 
            WHEN JSON_VALID(me.event_json) = 1 
            THEN JSON_EXTRACT(me.event_json, '$.properties.product_id')
            ELSE NULL
        END as product_id,
        me.event_time,
        me.event_name,
        DATE(me.event_time) as event_date
    FROM mixpanel_event me 
    WHERE me.event_name IN ('RC Trial started', 'RC Initial purchase')
    AND JSON_VALID(me.event_json) = 1
    AND JSON_EXTRACT(me.event_json, '$.properties.product_id') IS NOT NULL
    AND JSON_EXTRACT(me.event_json, '$.properties.product_id') != ''
    ORDER BY me.distinct_id, product_id, me.event_time
    """
    
    try:
        conn = sqlite3.connect(DB_PATH)
        df = pd.read_sql_query(query, conn)
        conn.close()
        
        # Log summary statistics
        event_counts = df['event_name'].value_counts()
        logger.info(f"   Found {len(df):,} starter events")
        logger.info(f"   • RC Trial started: {event_counts.get('RC Trial started', 0):,}")
        logger.info(f"   • RC Initial purchase: {event_counts.get('RC Initial purchase', 0):,}")
        
        # Count unique user-product combinations
        unique_combos = df[['distinct_id', 'product_id']].drop_duplicates()
        logger.info(f"   • Unique user-product combinations: {len(unique_combos):,}")
        
        return df
        
    except Exception as e:
        logger.error(f"❌ Error retrieving starter events: {str(e)}")
        raise


def calculate_credited_dates(starter_events_df: pd.DataFrame) -> Dict[Tuple[str, str], str]:
    """
    Calculate credited_date for each user-product combination by finding the earliest starter event.
    
    Args:
        starter_events_df: DataFrame containing all starter events
        
    Returns:
        Dictionary mapping (distinct_id, product_id) tuples to credited_date strings
    """
    logger.info("🔄 Calculating credited dates for user-product combinations...")
    
    credited_dates = {}
    
    # Group by user-product combination and find earliest event
    for (distinct_id, product_id), group in starter_events_df.groupby(['distinct_id', 'product_id']):
        # Sort by event_time to get earliest event
        earliest_event = group.sort_values('event_time').iloc[0]
        credited_date = earliest_event['event_date']
        
        # Store the result
        credited_dates[(distinct_id, product_id)] = credited_date
        
        # Log details for first few combinations (for debugging)
        if len(credited_dates) <= 5:
            logger.info(f"   {distinct_id[:8]}... + {product_id} → {credited_date} (from {earliest_event['event_name']})")
    
    logger.info(f"✅ Calculated credited dates for {len(credited_dates):,} user-product combinations")
    return credited_dates


def update_credited_dates_in_db(credited_dates: Dict[Tuple[str, str], str]) -> bool:
    """
    Update the credited_date field in user_product_metrics table.
    
    Args:
        credited_dates: Dictionary mapping (distinct_id, product_id) to credited_date
        
    Returns:
        True if successful, False otherwise
    """
    logger.info("💾 Updating credited_date field in user_product_metrics table...")
    
    try:
        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()
        
        # Check if any user_product_metrics records exist
        cursor.execute("SELECT COUNT(*) FROM user_product_metrics")
        total_records = cursor.fetchone()[0]
        logger.info(f"   Found {total_records:,} existing user_product_metrics records")
        
        if total_records == 0:
            logger.warning("⚠️  No user_product_metrics records found. Nothing to update.")
            conn.close()
            return True
        
        # Prepare batch updates
        updates = []
        found_matches = 0
        
        for (distinct_id, product_id), credited_date in credited_dates.items():
            # Check if this user-product combination exists in user_product_metrics
            cursor.execute("""
                SELECT user_product_id FROM user_product_metrics 
                WHERE distinct_id = ? AND product_id = ?
            """, (distinct_id, product_id))
            
            result = cursor.fetchone()
            if result:
                updates.append((credited_date, distinct_id, product_id))
                found_matches += 1
        
        logger.info(f"   Found {found_matches:,} matching user_product_metrics records to update")
        
        if not updates:
            logger.warning("⚠️  No matching user_product_metrics records found for starter events")
            conn.close()
            return True
        
        # Perform batch update in chunks
        update_count = 0
        for i in range(0, len(updates), BATCH_SIZE):
            batch = updates[i:i + BATCH_SIZE]
            
            cursor.executemany("""
                UPDATE user_product_metrics 
                SET credited_date = ?
                WHERE distinct_id = ? AND product_id = ?
            """, batch)
            
            update_count += len(batch)
            
            if i // BATCH_SIZE % 10 == 0:  # Log every 10 batches
                logger.info(f"   Updated {update_count:,}/{len(updates):,} records...")
        
        # Commit changes
        conn.commit()
        
        # Verify the updates
        cursor.execute("""
            SELECT COUNT(*) FROM user_product_metrics 
            WHERE credited_date IS NOT NULL
        """)
        records_with_credited_date = cursor.fetchone()[0]
        
        logger.info(f"✅ Successfully updated {update_count:,} records")
        logger.info(f"   Total records with credited_date: {records_with_credited_date:,}")
        
        conn.close()
        return True
        
    except Exception as e:
        logger.error(f"❌ Error updating database: {str(e)}")
        if 'conn' in locals():
            conn.rollback()
            conn.close()
        return False


def verify_credited_dates() -> bool:
    """
    Verify that credited dates were assigned correctly by checking a sample of records.
    
    Returns:
        True if verification passes, False otherwise
    """
    logger.info("🔍 Verifying credited date assignments...")
    
    try:
        conn = sqlite3.connect(DB_PATH)
        
        # Check for any NULL credited_dates
        cursor = conn.cursor()
        cursor.execute("""
            SELECT COUNT(*) FROM user_product_metrics 
            WHERE credited_date IS NULL
        """)
        null_count = cursor.fetchone()[0]
        
        cursor.execute("SELECT COUNT(*) FROM user_product_metrics")
        total_count = cursor.fetchone()[0]
        
        logger.info(f"   Records with NULL credited_date: {null_count:,}/{total_count:,}")
        
        # Sample a few records to verify dates look reasonable
        cursor.execute("""
            SELECT distinct_id, product_id, credited_date
            FROM user_product_metrics 
            WHERE credited_date IS NOT NULL
            LIMIT 5
        """)
        samples = cursor.fetchall()
        
        logger.info("   Sample credited dates:")
        for distinct_id, product_id, credited_date in samples:
            logger.info(f"     {distinct_id[:8]}... + {product_id} → {credited_date}")
        
        conn.close()
        
        # Basic validation - check if dates are in reasonable format
        for _, _, credited_date in samples:
            try:
                datetime.strptime(credited_date, '%Y-%m-%d')
            except ValueError:
                logger.error(f"❌ Invalid date format found: {credited_date}")
                return False
        
        logger.info("✅ Verification passed")
        return True
        
    except Exception as e:
        logger.error(f"❌ Error during verification: {str(e)}")
        return False


if __name__ == "__main__":
    success = main()
    
    if success:
        # Run verification
        verify_credited_dates()
        logger.info("🎉 Credited date assignment module completed successfully")
        sys.exit(0)
    else:
        logger.error("💥 Credited date assignment module failed")
        sys.exit(1) 