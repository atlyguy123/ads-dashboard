#!/usr/bin/env python3
"""
Test the exact dashboard query logic including the critical first_install_date filter
"""

import sqlite3
import json
from typing import List, Dict, Any

# Ad Set Configuration
AD_SET_ID = "120223331225270178"
START_DATE = "2025-07-16"
END_DATE = "2025-07-29"

def get_database_path():
    return "database/mixpanel_data.db"

def test_dashboard_exact_query():
    """Test the exact query from _get_mixpanel_adset_data()"""
    print("=== TESTING EXACT DASHBOARD QUERY ===")
    
    db_path = get_database_path()
    
    with sqlite3.connect(db_path) as conn:
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()
        
        # This is the EXACT query from the dashboard (lines 576-593)
        adset_event_query = """
        SELECT 
            e.abi_ad_set_id as adset_id,
            'Unknown Adset' as adset_name,
            e.abi_campaign_id as campaign_id,
            'Unknown Campaign' as campaign_name,
            COUNT(DISTINCT u.distinct_id) as total_users,
            COUNT(DISTINCT CASE WHEN JSON_EXTRACT(u.profile_json, '$.first_install_date') BETWEEN ? AND ? THEN u.distinct_id END) as new_users,
            SUM(CASE WHEN e.event_name = 'RC Trial started' AND e.event_time BETWEEN ? AND ? THEN 1 ELSE 0 END) as mixpanel_trials_started,
            SUM(CASE WHEN e.event_name = 'RC Initial purchase' AND e.event_time BETWEEN ? AND ? THEN 1 ELSE 0 END) as mixpanel_purchases
        FROM mixpanel_user u
        LEFT JOIN mixpanel_event e ON u.distinct_id = e.distinct_id
        WHERE e.abi_ad_set_id = ?
          AND JSON_EXTRACT(u.profile_json, '$.first_install_date') BETWEEN ? AND ?
        GROUP BY e.abi_ad_set_id, e.abi_campaign_id
        """
        
        # Parameters match the dashboard exactly
        event_params = [
            START_DATE, END_DATE,  # first_install_date filter
            START_DATE, END_DATE,  # trial event time filter
            START_DATE, END_DATE,  # purchase event time filter
            AD_SET_ID,             # ad_set_id filter
            START_DATE, END_DATE   # first_install_date filter (again)
        ]
        
        cursor.execute(adset_event_query, event_params)
        result = cursor.fetchone()
        
        if result:
            print(f"📊 EXACT DASHBOARD RESULTS:")
            print(f"   Total Users: {result['total_users']}")
            print(f"   New Users: {result['new_users']}")
            print(f"   Trial Events: {result['mixpanel_trials_started']}")
            print(f"   Purchase Events: {result['mixpanel_purchases']}")
        else:
            print("❌ No results from exact dashboard query")

def test_without_first_install_filter():
    """Test the same query but WITHOUT the first_install_date filter"""
    print("\n=== TESTING WITHOUT FIRST_INSTALL_DATE FILTER ===")
    
    db_path = get_database_path()
    
    with sqlite3.connect(db_path) as conn:
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()
        
        # Same query but WITHOUT first_install_date filter
        query_no_install_filter = """
        SELECT 
            e.abi_ad_set_id as adset_id,
            COUNT(DISTINCT u.distinct_id) as total_users,
            SUM(CASE WHEN e.event_name = 'RC Trial started' AND e.event_time BETWEEN ? AND ? THEN 1 ELSE 0 END) as mixpanel_trials_started,
            SUM(CASE WHEN e.event_name = 'RC Initial purchase' AND e.event_time BETWEEN ? AND ? THEN 1 ELSE 0 END) as mixpanel_purchases
        FROM mixpanel_user u
        LEFT JOIN mixpanel_event e ON u.distinct_id = e.distinct_id
        WHERE e.abi_ad_set_id = ?
        GROUP BY e.abi_ad_set_id
        """
        
        params = [START_DATE, END_DATE, START_DATE, END_DATE, AD_SET_ID]
        cursor.execute(query_no_install_filter, params)
        result = cursor.fetchone()
        
        if result:
            print(f"📊 WITHOUT first_install_date filter:")
            print(f"   Total Users: {result['total_users']}")
            print(f"   Trial Events: {result['mixpanel_trials_started']}")
            print(f"   Purchase Events: {result['mixpanel_purchases']}")
        else:
            print("❌ No results without first_install_date filter")

def investigate_first_install_dates():
    """Investigate the first_install_date values for our users"""
    print("\n=== INVESTIGATING FIRST_INSTALL_DATES ===")
    
    db_path = get_database_path()
    
    with sqlite3.connect(db_path) as conn:
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()
        
        # Get first_install_date for users in our ad set
        query = """
        SELECT 
            u.distinct_id,
            JSON_EXTRACT(u.profile_json, '$.first_install_date') as first_install_date,
            u.first_seen
        FROM mixpanel_user u
        WHERE u.abi_ad_set_id = ?
          AND u.has_abi_attribution = TRUE
        ORDER BY u.first_seen
        LIMIT 10
        """
        
        cursor.execute(query, (AD_SET_ID,))
        results = cursor.fetchall()
        
        print(f"📊 Sample of first_install_dates for our ad set users:")
        for result in results:
            print(f"   User: {result['distinct_id'][:30]}...")
            print(f"   First Install: {result['first_install_date']}")
            print(f"   First Seen: {result['first_seen']}")
            print(f"   ---")
            
        # Count how many users have first_install_date in our range
        count_query = """
        SELECT 
            COUNT(DISTINCT u.distinct_id) as total_users,
            COUNT(DISTINCT CASE WHEN JSON_EXTRACT(u.profile_json, '$.first_install_date') BETWEEN ? AND ? THEN u.distinct_id END) as users_in_range,
            COUNT(DISTINCT CASE WHEN JSON_EXTRACT(u.profile_json, '$.first_install_date') IS NULL THEN u.distinct_id END) as users_no_install_date
        FROM mixpanel_user u
        WHERE u.abi_ad_set_id = ?
          AND u.has_abi_attribution = TRUE
        """
        
        cursor.execute(count_query, (START_DATE, END_DATE, AD_SET_ID))
        counts = cursor.fetchone()
        
        if counts:
            print(f"📊 First install date analysis:")
            print(f"   Total users in ad set: {counts['total_users']}")
            print(f"   Users with install date in range: {counts['users_in_range']}")
            print(f"   Users with no install date: {counts['users_no_install_date']}")

def test_aggregation_approach():
    """Test the aggregation approach used for Meta data"""
    print("\n=== TESTING AGGREGATION APPROACH ===")
    
    db_path = get_database_path()
    
    with sqlite3.connect(db_path) as conn:
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()
        
        # Get all ad_ids for this ad set first
        ad_ids_query = """
        SELECT DISTINCT u.abi_ad_id
        FROM mixpanel_user u
        WHERE u.abi_ad_set_id = ?
          AND u.abi_ad_id IS NOT NULL
          AND u.has_abi_attribution = TRUE
        """
        
        cursor.execute(ad_ids_query, (AD_SET_ID,))
        ad_ids = [row['abi_ad_id'] for row in cursor.fetchall()]
        
        print(f"🎯 Found {len(ad_ids)} ad IDs for aggregation")
        
        if ad_ids:
            # Run the aggregation query (lines 1189-1199)
            ad_placeholders = ','.join(['?' for _ in ad_ids])
            events_query = f"""
            SELECT 
                u.abi_ad_id,
                COUNT(DISTINCT CASE WHEN e.event_name = 'RC Trial started' AND DATE(e.event_time) BETWEEN ? AND ? THEN u.distinct_id END) as mixpanel_trials_started,
                COUNT(DISTINCT CASE WHEN e.event_name = 'RC Initial purchase' AND DATE(e.event_time) BETWEEN ? AND ? THEN u.distinct_id END) as mixpanel_purchases,
                COUNT(DISTINCT u.distinct_id) as total_attributed_users
            FROM mixpanel_user u
            LEFT JOIN mixpanel_event e ON u.distinct_id = e.distinct_id
            WHERE u.abi_ad_id IN ({ad_placeholders})
              AND u.has_abi_attribution = TRUE
            GROUP BY u.abi_ad_id
            """
            
            events_params = [START_DATE, END_DATE, START_DATE, END_DATE] + ad_ids
            cursor.execute(events_query, events_params)
            ad_results = cursor.fetchall()
            
            # Aggregate to ad set level
            total_trials = sum(row['mixpanel_trials_started'] for row in ad_results)
            total_purchases = sum(row['mixpanel_purchases'] for row in ad_results)
            total_users = sum(row['total_attributed_users'] for row in ad_results)
            
            print(f"📊 AGGREGATION RESULTS:")
            print(f"   Total Trials (aggregated): {total_trials}")
            print(f"   Total Purchases (aggregated): {total_purchases}")
            print(f"   Total Users (aggregated): {total_users}")
            print(f"   Individual ad results: {len(ad_results)} ads")

def main():
    print("🔍 TESTING EXACT DASHBOARD LOGIC")
    print(f"Ad Set ID: {AD_SET_ID}")
    print(f"Date Range: {START_DATE} to {END_DATE}")
    print(f"Expected: 47 users, Dashboard claims: 42 users")
    
    test_dashboard_exact_query()
    test_without_first_install_filter()
    investigate_first_install_dates()
    test_aggregation_approach()

if __name__ == "__main__":
    main() 