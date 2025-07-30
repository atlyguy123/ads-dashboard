#!/usr/bin/env python3
"""
Fix the unique user counting logic for ad sets.
Instead of aggregating from individual ads (which double-counts users across ads),
count unique users directly at the ad set level.
"""

import sqlite3
from typing import List, Dict, Any

# Ad Set Configuration
AD_SET_ID = "120223331225270178"
START_DATE = "2025-07-16"
END_DATE = "2025-07-29"

def get_database_path():
    return "database/mixpanel_data.db"

def test_correct_adset_counting():
    """Test the CORRECT way to count unique users for an ad set"""
    print("=== CORRECT AD SET USER COUNTING ===")
    
    db_path = get_database_path()
    
    with sqlite3.connect(db_path) as conn:
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()
        
        # CORRECT: Count unique users directly at ad set level
        adset_query = """
        SELECT 
            u.abi_ad_set_id,
            COUNT(DISTINCT CASE WHEN e.event_name = 'RC Trial started' AND DATE(e.event_time) >= ? AND DATE(e.event_time) <= ? THEN u.distinct_id END) as mixpanel_trials_started,
            COUNT(DISTINCT CASE WHEN e.event_name = 'RC Initial purchase' AND DATE(e.event_time) >= ? AND DATE(e.event_time) <= ? THEN u.distinct_id END) as mixpanel_purchases,
            COUNT(DISTINCT u.distinct_id) as total_attributed_users
        FROM mixpanel_user u
        LEFT JOIN mixpanel_event e ON u.distinct_id = e.distinct_id
        WHERE u.abi_ad_set_id = ?
          AND u.has_abi_attribution = TRUE
        GROUP BY u.abi_ad_set_id
        """
        
        cursor.execute(adset_query, (START_DATE, END_DATE, START_DATE, END_DATE, AD_SET_ID))
        result = cursor.fetchone()
        
        if result:
            print(f"📊 CORRECT AD SET RESULTS:")
            print(f"   Trial Users (unique): {result['mixpanel_trials_started']}")
            print(f"   Purchase Users (unique): {result['mixpanel_purchases']}")
            print(f"   Total Users: {result['total_attributed_users']}")
        else:
            print("❌ No results from ad set query")

def test_wrong_aggregation_approach():
    """Test the WRONG way (current dashboard) that double-counts users"""
    print("\n=== WRONG AGGREGATION APPROACH (Current Dashboard) ===")
    
    db_path = get_database_path()
    
    with sqlite3.connect(db_path) as conn:
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()
        
        # Get all ad_ids for this ad set
        ad_ids_query = """
        SELECT DISTINCT u.abi_ad_id
        FROM mixpanel_user u
        WHERE u.abi_ad_set_id = ?
          AND u.abi_ad_id IS NOT NULL
          AND u.has_abi_attribution = TRUE
        """
        
        cursor.execute(ad_ids_query, (AD_SET_ID,))
        ad_ids = [row['abi_ad_id'] for row in cursor.fetchall()]
        
        if not ad_ids:
            print("❌ No ad IDs found")
            return
        
        # WRONG: Count users per ad, then aggregate (double-counts users across ads)
        ad_placeholders = ','.join(['?' for _ in ad_ids])
        events_query = f"""
        SELECT 
            u.abi_ad_id,
            COUNT(DISTINCT CASE WHEN e.event_name = 'RC Trial started' AND DATE(e.event_time) >= ? AND DATE(e.event_time) <= ? THEN u.distinct_id END) as mixpanel_trials_started
        FROM mixpanel_user u
        LEFT JOIN mixpanel_event e ON u.distinct_id = e.distinct_id
        WHERE u.abi_ad_id IN ({ad_placeholders})
          AND u.has_abi_attribution = TRUE
        GROUP BY u.abi_ad_id
        """
        
        events_params = [START_DATE, END_DATE] + ad_ids
        cursor.execute(events_query, events_params)
        ad_results = cursor.fetchall()
        
        # Aggregate (this is where double-counting happens)
        total_trials_wrong = sum(row['mixpanel_trials_started'] for row in ad_results)
        
        print(f"📊 WRONG AGGREGATION RESULTS:")
        print(f"   Trial Users (aggregated - WRONG): {total_trials_wrong}")
        print(f"   Why wrong: Users with events in multiple ads get counted multiple times")

def show_user_distribution_across_ads():
    """Show how users are distributed across ads to explain the double-counting"""
    print("\n=== USER DISTRIBUTION ACROSS ADS ===")
    
    db_path = get_database_path()
    
    with sqlite3.connect(db_path) as conn:
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()
        
        # Show which users appear in multiple ads
        cross_ad_query = """
        SELECT 
            u.distinct_id,
            COUNT(DISTINCT u.abi_ad_id) as ad_count,
            GROUP_CONCAT(DISTINCT u.abi_ad_id) as ad_ids
        FROM mixpanel_user u
        LEFT JOIN mixpanel_event e ON u.distinct_id = e.distinct_id
        WHERE u.abi_ad_set_id = ?
          AND u.has_abi_attribution = TRUE
          AND e.event_name = 'RC Trial started'
          AND DATE(e.event_time) >= ? AND DATE(e.event_time) <= ?
        GROUP BY u.distinct_id
        HAVING COUNT(DISTINCT u.abi_ad_id) > 1
        ORDER BY ad_count DESC
        LIMIT 10
        """
        
        cursor.execute(cross_ad_query, (AD_SET_ID, START_DATE, END_DATE))
        cross_ad_users = cursor.fetchall()
        
        if cross_ad_users:
            print(f"📋 Users appearing in multiple ads (causing double-counting):")
            for user in cross_ad_users:
                print(f"   User {user['distinct_id'][:30]}: {user['ad_count']} ads")
        else:
            print("✅ No users appear in multiple ads (no double-counting)")

def main():
    print("🔍 FIXING UNIQUE USER COUNTING")
    print(f"Ad Set ID: {AD_SET_ID}")
    print(f"Date Range: {START_DATE} to {END_DATE}")
    print(f"Goal: Count 47 unique users, not 49 events")
    
    test_correct_adset_counting()
    test_wrong_aggregation_approach()
    show_user_distribution_across_ads()

if __name__ == "__main__":
    main() 