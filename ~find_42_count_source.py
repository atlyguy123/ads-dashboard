#!/usr/bin/env python3
"""
Find the exact filtering logic that produces 42 count matching the dashboard
"""

import sqlite3
from typing import List, Dict, Any

# Ad Set Configuration
AD_SET_ID = "120223331225270178"
START_DATE = "2025-07-16"
END_DATE = "2025-07-29"

def get_database_path():
    return "database/mixpanel_data.db"

def test_different_date_filters():
    """Test different date filtering approaches"""
    print("=== TESTING DIFFERENT DATE FILTERS ===")
    
    db_path = get_database_path()
    
    with sqlite3.connect(db_path) as conn:
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()
        
        # Test 1: Using e.event_time BETWEEN (datetime)
        query1 = """
        SELECT 
            COUNT(DISTINCT CASE WHEN e.event_name = 'RC Trial started' AND e.event_time BETWEEN ? AND ? THEN u.distinct_id END) as trial_users
        FROM mixpanel_user u
        LEFT JOIN mixpanel_event e ON u.distinct_id = e.distinct_id
        WHERE u.abi_ad_set_id = ?
          AND u.has_abi_attribution = TRUE
        """
        
        cursor.execute(query1, (START_DATE, END_DATE, AD_SET_ID))
        result1 = cursor.fetchone()
        print(f"📊 e.event_time BETWEEN (datetime): {result1['trial_users'] if result1 else 'None'}")
        
        # Test 2: Using DATE(e.event_time) BETWEEN (date only)
        query2 = """
        SELECT 
            COUNT(DISTINCT CASE WHEN e.event_name = 'RC Trial started' AND DATE(e.event_time) BETWEEN ? AND ? THEN u.distinct_id END) as trial_users
        FROM mixpanel_user u
        LEFT JOIN mixpanel_event e ON u.distinct_id = e.distinct_id
        WHERE u.abi_ad_set_id = ?
          AND u.has_abi_attribution = TRUE
        """
        
        cursor.execute(query2, (START_DATE, END_DATE, AD_SET_ID))
        result2 = cursor.fetchone()
        print(f"📊 DATE(e.event_time) BETWEEN (date): {result2['trial_users'] if result2 else 'None'}")
        
        # Test 3: Using event_time with datetime boundaries
        start_datetime = f"{START_DATE} 00:00:00"
        end_datetime = f"{END_DATE} 23:59:59"
        
        query3 = """
        SELECT 
            COUNT(DISTINCT CASE WHEN e.event_name = 'RC Trial started' AND e.event_time BETWEEN ? AND ? THEN u.distinct_id END) as trial_users
        FROM mixpanel_user u
        LEFT JOIN mixpanel_event e ON u.distinct_id = e.distinct_id
        WHERE u.abi_ad_set_id = ?
          AND u.has_abi_attribution = TRUE
        """
        
        cursor.execute(query3, (start_datetime, end_datetime, AD_SET_ID))
        result3 = cursor.fetchone()
        print(f"📊 e.event_time BETWEEN with full datetime: {result3['trial_users'] if result3 else 'None'}")

def test_first_install_date_filter():
    """Test if first_install_date filtering is affecting the count"""
    print("\n=== TESTING FIRST_INSTALL_DATE FILTER ===")
    
    db_path = get_database_path()
    
    with sqlite3.connect(db_path) as conn:
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()
        
        # Test with first_install_date filter (like in approach 1)
        query = """
        SELECT 
            COUNT(DISTINCT u.distinct_id) as total_users,
            COUNT(DISTINCT CASE WHEN JSON_EXTRACT(u.profile_json, '$.first_install_date') BETWEEN ? AND ? THEN u.distinct_id END) as new_users,
            COUNT(DISTINCT CASE WHEN e.event_name = 'RC Trial started' AND e.event_time BETWEEN ? AND ? THEN u.distinct_id END) as trial_users
        FROM mixpanel_user u
        LEFT JOIN mixpanel_event e ON u.distinct_id = e.distinct_id
        WHERE u.abi_ad_set_id = ?
          AND u.has_abi_attribution = TRUE
          AND JSON_EXTRACT(u.profile_json, '$.first_install_date') BETWEEN ? AND ?
        """
        
        cursor.execute(query, (START_DATE, END_DATE, START_DATE, END_DATE, AD_SET_ID, START_DATE, END_DATE))
        result = cursor.fetchone()
        
        if result:
            print(f"📊 With first_install_date filter:")
            print(f"   Total Users: {result['total_users']}")
            print(f"   New Users: {result['new_users']}")
            print(f"   Trial Users: {result['trial_users']}")
        else:
            print("❌ No results with first_install_date filter")

def test_exact_dashboard_logic():
    """Test the exact logic from the dashboard code"""
    print("\n=== TESTING EXACT DASHBOARD LOGIC ===")
    
    db_path = get_database_path()
    
    with sqlite3.connect(db_path) as conn:
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()
        
        # Get ad IDs for this ad set
        ad_ids_query = """
        SELECT DISTINCT u.abi_ad_id
        FROM mixpanel_user u
        WHERE u.abi_ad_set_id = ?
          AND u.abi_ad_id IS NOT NULL
          AND u.has_abi_attribution = TRUE
        """
        
        cursor.execute(ad_ids_query, (AD_SET_ID,))
        ad_ids = [row['abi_ad_id'] for row in cursor.fetchall()]
        
        if ad_ids:
            print(f"🎯 Testing with {len(ad_ids)} ad IDs")
            
            # Test exact dashboard query with COUNT DISTINCT on unique users
            ad_placeholders = ','.join(['?' for _ in ad_ids])
            events_query = f"""
            SELECT 
                COUNT(DISTINCT CASE WHEN e.event_name = 'RC Trial started' AND DATE(e.event_time) BETWEEN ? AND ? THEN u.distinct_id END) as mixpanel_trials_started
            FROM mixpanel_user u
            LEFT JOIN mixpanel_event e ON u.distinct_id = e.distinct_id
            WHERE u.abi_ad_id IN ({ad_placeholders})
              AND u.has_abi_attribution = TRUE
            """
            
            params = [START_DATE, END_DATE] + ad_ids
            cursor.execute(events_query, params)
            result = cursor.fetchone()
            
            print(f"📊 Dashboard logic result: {result['mixpanel_trials_started'] if result else 'None'}")
            
            # Now test what happens if we exclude some boundary conditions
            print("\n🔍 Testing boundary exclusions:")
            
            # Exclusive start date
            exclusive_start_query = f"""
            SELECT 
                COUNT(DISTINCT CASE WHEN e.event_name = 'RC Trial started' AND DATE(e.event_time) > ? AND DATE(e.event_time) <= ? THEN u.distinct_id END) as mixpanel_trials_started
            FROM mixpanel_user u
            LEFT JOIN mixpanel_event e ON u.distinct_id = e.distinct_id
            WHERE u.abi_ad_id IN ({ad_placeholders})
              AND u.has_abi_attribution = TRUE
            """
            
            cursor.execute(exclusive_start_query, params)
            result_exclusive_start = cursor.fetchone()
            print(f"   Exclusive start date: {result_exclusive_start['mixpanel_trials_started'] if result_exclusive_start else 'None'}")
            
            # Exclusive end date
            exclusive_end_query = f"""
            SELECT 
                COUNT(DISTINCT CASE WHEN e.event_name = 'RC Trial started' AND DATE(e.event_time) >= ? AND DATE(e.event_time) < ? THEN u.distinct_id END) as mixpanel_trials_started
            FROM mixpanel_user u
            LEFT JOIN mixpanel_event e ON u.distinct_id = e.distinct_id
            WHERE u.abi_ad_id IN ({ad_placeholders})
              AND u.has_abi_attribution = TRUE
            """
            
            cursor.execute(exclusive_end_query, params)
            result_exclusive_end = cursor.fetchone()
            print(f"   Exclusive end date: {result_exclusive_end['mixpanel_trials_started'] if result_exclusive_end else 'None'}")

def main():
    print("🔍 FINDING THE SOURCE OF COUNT = 42")
    print(f"Target: Find what produces 42 (dashboard count)")
    print(f"Expected: 47 unique users (Mixpanel truth)")
    print(f"Ad Set ID: {AD_SET_ID}")
    print(f"Date Range: {START_DATE} to {END_DATE}")
    
    test_different_date_filters()
    test_first_install_date_filter()
    test_exact_dashboard_logic()

if __name__ == "__main__":
    main() 