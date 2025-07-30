#!/usr/bin/env python3
"""
Debug the dashboard query to see why it returns 42 instead of 47 users.
Test different query approaches used in the dashboard code.
"""

import sqlite3
from typing import List, Dict, Any

# Ad Set Configuration
AD_SET_ID = "120223331225270178"
START_DATE = "2025-07-16"
END_DATE = "2025-07-29"

def get_database_path():
    return "database/mixpanel_data.db"

def test_dashboard_query_approach_1():
    """
    Test the first approach from _get_mixpanel_adset_data() - SUM of events
    """
    print("=== APPROACH 1: SUM of events (from _get_mixpanel_adset_data) ===")
    
    db_path = get_database_path()
    
    with sqlite3.connect(db_path) as conn:
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()
        
        query = """
        SELECT 
            e.abi_ad_set_id as adset_id,
            COUNT(DISTINCT u.distinct_id) as total_users,
            SUM(CASE WHEN e.event_name = 'RC Trial started' AND e.event_time BETWEEN ? AND ? THEN 1 ELSE 0 END) as mixpanel_trials_started
        FROM mixpanel_user u
        LEFT JOIN mixpanel_event e ON u.distinct_id = e.distinct_id
        WHERE e.abi_ad_set_id = ?
        GROUP BY e.abi_ad_set_id
        """
        
        cursor.execute(query, (START_DATE, END_DATE, AD_SET_ID))
        result = cursor.fetchone()
        
        if result:
            print(f"📊 Results:")
            print(f"   Total Users: {result['total_users']}")
            print(f"   Trial Events (SUM): {result['mixpanel_trials_started']}")
        else:
            print("❌ No results found")

def test_dashboard_query_approach_2():
    """
    Test the second approach from _add_mixpanel_data_to_records() - COUNT DISTINCT
    """
    print("\n=== APPROACH 2: COUNT DISTINCT users (from _add_mixpanel_data_to_records) ===")
    
    db_path = get_database_path()
    
    with sqlite3.connect(db_path) as conn:
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()
        
        # First, get all ad_ids for this ad set
        ad_ids_query = """
        SELECT DISTINCT u.abi_ad_id
        FROM mixpanel_user u
        WHERE u.abi_ad_set_id = ?
          AND u.abi_ad_id IS NOT NULL
          AND u.has_abi_attribution = TRUE
        """
        
        cursor.execute(ad_ids_query, (AD_SET_ID,))
        ad_ids = [row['abi_ad_id'] for row in cursor.fetchall()]
        
        print(f"🎯 Found {len(ad_ids)} ad IDs for this ad set")
        
        if ad_ids:
            # Now run the actual dashboard query
            ad_placeholders = ','.join(['?' for _ in ad_ids])
            events_query = f"""
            SELECT 
                COUNT(DISTINCT CASE WHEN e.event_name = 'RC Trial started' AND DATE(e.event_time) BETWEEN ? AND ? THEN u.distinct_id END) as mixpanel_trials_started,
                COUNT(DISTINCT u.distinct_id) as total_attributed_users
            FROM mixpanel_user u
            LEFT JOIN mixpanel_event e ON u.distinct_id = e.distinct_id
            WHERE u.abi_ad_id IN ({ad_placeholders})
              AND u.has_abi_attribution = TRUE
            """
            
            params = [START_DATE, END_DATE] + ad_ids
            cursor.execute(events_query, params)
            result = cursor.fetchone()
            
            if result:
                print(f"📊 Results:")
                print(f"   Trial Users (COUNT DISTINCT): {result['mixpanel_trials_started']}")
                print(f"   Total Attributed Users: {result['total_attributed_users']}")
            else:
                print("❌ No results found")
        else:
            print("❌ No ad IDs found for this ad set")

def test_our_working_query():
    """
    Test our working query that correctly returns 47 users
    """
    print("\n=== OUR WORKING QUERY (returns 47) ===")
    
    db_path = get_database_path()
    
    with sqlite3.connect(db_path) as conn:
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()
        
        query = """
        SELECT 
            COUNT(DISTINCT u.distinct_id) as unique_users,
            COUNT(*) as total_events
        FROM mixpanel_user u
        LEFT JOIN mixpanel_event e ON u.distinct_id = e.distinct_id
        WHERE u.abi_ad_set_id = ?
          AND u.has_abi_attribution = TRUE
          AND e.event_name = 'RC Trial started'
          AND DATE(e.event_time) BETWEEN ? AND ?
        """
        
        cursor.execute(query, (AD_SET_ID, START_DATE, END_DATE))
        result = cursor.fetchone()
        
        if result:
            print(f"📊 Results:")
            print(f"   Unique Users: {result['unique_users']}")
            print(f"   Total Events: {result['total_events']}")
        else:
            print("❌ No results found")

def test_attribution_filtering():
    """
    Test what filtering is happening with has_abi_attribution and ad_id filters
    """
    print("\n=== TESTING ATTRIBUTION FILTERING ===")
    
    db_path = get_database_path()
    
    with sqlite3.connect(db_path) as conn:
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()
        
        # Check how many users have abi_ad_id vs abi_ad_set_id
        ad_id_query = """
        SELECT 
            COUNT(DISTINCT CASE WHEN u.abi_ad_id IS NOT NULL THEN u.distinct_id END) as users_with_ad_id,
            COUNT(DISTINCT CASE WHEN u.abi_ad_set_id = ? THEN u.distinct_id END) as users_with_adset_id,
            COUNT(DISTINCT CASE WHEN u.abi_ad_id IS NOT NULL AND u.abi_ad_set_id = ? THEN u.distinct_id END) as users_with_both
        FROM mixpanel_user u
        WHERE u.has_abi_attribution = TRUE
        """
        
        cursor.execute(ad_id_query, (AD_SET_ID, AD_SET_ID))
        result = cursor.fetchone()
        
        if result:
            print(f"📊 Attribution Analysis:")
            print(f"   Users with ad_id: {result['users_with_ad_id']}")
            print(f"   Users with our ad_set_id: {result['users_with_adset_id']}")
            print(f"   Users with both: {result['users_with_both']}")
        
        # Check which users are missing ad_id
        missing_query = """
        SELECT DISTINCT u.distinct_id
        FROM mixpanel_user u
        WHERE u.abi_ad_set_id = ?
          AND u.has_abi_attribution = TRUE
          AND u.abi_ad_id IS NULL
        ORDER BY u.distinct_id
        """
        
        cursor.execute(missing_query, (AD_SET_ID,))
        missing_users = [row['distinct_id'] for row in cursor.fetchall()]
        
        print(f"   Users missing ad_id: {len(missing_users)}")
        if missing_users and len(missing_users) <= 10:
            print(f"   Missing users: {missing_users}")

def main():
    print("🔍 DEBUGGING DASHBOARD QUERY DISCREPANCY")
    print(f"Expected: 47 users, Dashboard shows: 42 users")
    print(f"Ad Set ID: {AD_SET_ID}")
    print(f"Date Range: {START_DATE} to {END_DATE}")
    
    test_our_working_query()
    test_dashboard_query_approach_1() 
    test_dashboard_query_approach_2()
    test_attribution_filtering()

if __name__ == "__main__":
    main() 