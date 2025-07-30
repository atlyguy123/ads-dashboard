#!/usr/bin/env python3
"""
Test the exact current dashboard query to verify what it returns vs my testing
"""

import sqlite3
from typing import List, Dict, Any

# Ad Set Configuration
AD_SET_ID = "120223331225270178"
START_DATE = "2025-07-16"
END_DATE = "2025-07-29"

def get_database_path():
    return "database/mixpanel_data.db"

def test_current_dashboard_query():
    """Test the EXACT query currently in the dashboard code"""
    print("=== TESTING CURRENT DASHBOARD QUERY ===")
    
    db_path = get_database_path()
    
    with sqlite3.connect(db_path) as conn:
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()
        
        # Step 1: Get ad_ids for this ad set (just like the dashboard does)
        ad_ids_query = """
        SELECT DISTINCT u.abi_ad_id
        FROM mixpanel_user u
        WHERE u.abi_ad_set_id = ?
          AND u.abi_ad_id IS NOT NULL
          AND u.has_abi_attribution = TRUE
        """
        
        cursor.execute(ad_ids_query, (AD_SET_ID,))
        ad_ids = [row['abi_ad_id'] for row in cursor.fetchall()]
        
        print(f"🎯 Found {len(ad_ids)} ad IDs")
        
        if not ad_ids:
            print("❌ No ad IDs found")
            return
        
        # Step 2: Run the EXACT query from analytics_query_service.py line 1189-1199
        ad_placeholders = ','.join(['?' for _ in ad_ids])
        events_query = f"""
        SELECT 
            u.abi_ad_id,
            COUNT(DISTINCT CASE WHEN e.event_name = 'RC Trial started' AND DATE(e.event_time) >= ? AND DATE(e.event_time) <= ? THEN u.distinct_id END) as mixpanel_trials_started,
            COUNT(DISTINCT CASE WHEN e.event_name = 'RC Initial purchase' AND DATE(e.event_time) >= ? AND DATE(e.event_time) <= ? THEN u.distinct_id END) as mixpanel_purchases,
            COUNT(DISTINCT u.distinct_id) as total_attributed_users
        FROM mixpanel_user u
        LEFT JOIN mixpanel_event e ON u.distinct_id = e.distinct_id
        WHERE u.abi_ad_id IN ({ad_placeholders})
          AND u.has_abi_attribution = TRUE
        GROUP BY u.abi_ad_id
        """
        
        events_params = [
            START_DATE, END_DATE,  # trial date filter
            START_DATE, END_DATE,  # purchase date filter
            *list(ad_ids)
        ]
        
        cursor.execute(events_query, events_params)
        ad_results = cursor.fetchall()
        
        # Step 3: Aggregate the results (lines 1016-1022)
        total_trials = sum(row['mixpanel_trials_started'] for row in ad_results)
        total_purchases = sum(row['mixpanel_purchases'] for row in ad_results)
        total_users = sum(row['total_attributed_users'] for row in ad_results)
        
        print(f"📊 CURRENT DASHBOARD RESULTS:")
        print(f"   Total Trials (aggregated): {total_trials}")
        print(f"   Total Purchases (aggregated): {total_purchases}")
        print(f"   Total Users (aggregated): {total_users}")
        print(f"   Individual ad results: {len(ad_results)} ads")
        
        # Show a few ad-level results for debugging
        print(f"\n📋 Sample ad-level results:")
        for i, result in enumerate(ad_results[:5]):
            print(f"   Ad {i+1}: {result['mixpanel_trials_started']} trials, {result['total_attributed_users']} users")
        if len(ad_results) > 5:
            print(f"   ... and {len(ad_results) - 5} more ads")

def main():
    print("🔍 TESTING CURRENT DASHBOARD QUERY")
    print(f"Ad Set ID: {AD_SET_ID}")
    print(f"Date Range: {START_DATE} to {END_DATE}")
    print(f"Note: This tests the query AFTER my fix (>= and <=)")
    
    test_current_dashboard_query()

if __name__ == "__main__":
    main() 