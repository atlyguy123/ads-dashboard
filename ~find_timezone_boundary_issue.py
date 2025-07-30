#!/usr/bin/env python3
"""
Test timezone boundary issues that might reduce trial count from 49 to 42
"""

import sqlite3
from typing import List, Dict, Any

# Ad Set Configuration  
AD_SET_ID = "120223331225270178"
START_DATE = "2025-07-16"
END_DATE = "2025-07-29"

def get_database_path():
    return "database/mixpanel_data.db"

def test_timezone_boundaries():
    """Test different timezone boundary approaches"""
    print("=== TESTING TIMEZONE BOUNDARY ISSUES ===")
    
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
            
        ad_placeholders = ','.join(['?' for _ in ad_ids])
        
        print(f"🎯 Testing with {len(ad_ids)} ad IDs")
        
        # Test 1: Inclusive boundaries (our current approach)
        query_inclusive = f"""
        SELECT 
            COUNT(DISTINCT CASE WHEN e.event_name = 'RC Trial started' AND DATE(e.event_time) BETWEEN ? AND ? THEN u.distinct_id END) as trials
        FROM mixpanel_user u
        LEFT JOIN mixpanel_event e ON u.distinct_id = e.distinct_id
        WHERE u.abi_ad_id IN ({ad_placeholders})
          AND u.has_abi_attribution = TRUE
        """
        
        params = [START_DATE, END_DATE] + ad_ids
        cursor.execute(query_inclusive, params)
        result = cursor.fetchone()
        print(f"📊 Inclusive boundaries (>= start, <= end): {result['trials'] if result else 'None'}")
        
        # Test 2: Exclusive start boundary  
        query_exclusive_start = f"""
        SELECT 
            COUNT(DISTINCT CASE WHEN e.event_name = 'RC Trial started' AND DATE(e.event_time) > ? AND DATE(e.event_time) <= ? THEN u.distinct_id END) as trials
        FROM mixpanel_user u
        LEFT JOIN mixpanel_event e ON u.distinct_id = e.distinct_id
        WHERE u.abi_ad_id IN ({ad_placeholders})
          AND u.has_abi_attribution = TRUE
        """
        
        cursor.execute(query_exclusive_start, params)
        result = cursor.fetchone()
        print(f"📊 Exclusive start (> start, <= end): {result['trials'] if result else 'None'}")
        
        # Test 3: Exclusive end boundary
        query_exclusive_end = f"""
        SELECT 
            COUNT(DISTINCT CASE WHEN e.event_name = 'RC Trial started' AND DATE(e.event_time) >= ? AND DATE(e.event_time) < ? THEN u.distinct_id END) as trials
        FROM mixpanel_user u
        LEFT JOIN mixpanel_event e ON u.distinct_id = e.distinct_id
        WHERE u.abi_ad_id IN ({ad_placeholders})
          AND u.has_abi_attribution = TRUE
        """
        
        cursor.execute(query_exclusive_end, params)
        result = cursor.fetchone()
        print(f"📊 Exclusive end (>= start, < end): {result['trials'] if result else 'None'}")
        
        # Test 4: Both boundaries exclusive
        query_both_exclusive = f"""
        SELECT 
            COUNT(DISTINCT CASE WHEN e.event_name = 'RC Trial started' AND DATE(e.event_time) > ? AND DATE(e.event_time) < ? THEN u.distinct_id END) as trials
        FROM mixpanel_user u
        LEFT JOIN mixpanel_event e ON u.distinct_id = e.distinct_id
        WHERE u.abi_ad_id IN ({ad_placeholders})
          AND u.has_abi_attribution = TRUE
        """
        
        cursor.execute(query_both_exclusive, params)
        result = cursor.fetchone()
        print(f"📊 Both exclusive (> start, < end): {result['trials'] if result else 'None'}")
        
        # Test 5: Using datetime boundaries instead of date
        start_datetime = f"{START_DATE}T00:00:00"
        end_datetime = f"{END_DATE}T23:59:59"
        
        query_datetime = f"""
        SELECT 
            COUNT(DISTINCT CASE WHEN e.event_name = 'RC Trial started' AND e.event_time BETWEEN ? AND ? THEN u.distinct_id END) as trials
        FROM mixpanel_user u
        LEFT JOIN mixpanel_event e ON u.distinct_id = e.distinct_id
        WHERE u.abi_ad_id IN ({ad_placeholders})
          AND u.has_abi_attribution = TRUE
        """
        
        datetime_params = [start_datetime, end_datetime] + ad_ids
        cursor.execute(query_datetime, datetime_params)
        result = cursor.fetchone()
        print(f"📊 Datetime boundaries: {result['trials'] if result else 'None'}")

def check_boundary_dates():
    """Check events on boundary dates specifically"""
    print("\n=== CHECKING BOUNDARY DATE EVENTS ===")
    
    db_path = get_database_path()
    
    with sqlite3.connect(db_path) as conn:
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()
        
        # Check events on first and last days
        boundary_query = """
        SELECT 
            DATE(e.event_time) as event_date,
            COUNT(DISTINCT u.distinct_id) as unique_users,
            COUNT(*) as total_events
        FROM mixpanel_user u
        LEFT JOIN mixpanel_event e ON u.distinct_id = e.distinct_id
        WHERE u.abi_ad_set_id = ?
          AND u.has_abi_attribution = TRUE
          AND e.event_name = 'RC Trial started'
          AND DATE(e.event_time) IN (?, ?)
        GROUP BY DATE(e.event_time)
        ORDER BY event_date
        """
        
        cursor.execute(boundary_query, (AD_SET_ID, START_DATE, END_DATE))
        results = cursor.fetchall()
        
        print(f"📊 Events on boundary dates:")
        for result in results:
            print(f"   {result['event_date']}: {result['unique_users']} users, {result['total_events']} events")
            
        total_boundary_users = sum(row['unique_users'] for row in results)
        print(f"   Total boundary users: {total_boundary_users}")

def analyze_date_distribution():
    """Analyze the distribution of trial events by date"""
    print("\n=== ANALYZING DATE DISTRIBUTION ===")
    
    db_path = get_database_path()
    
    with sqlite3.connect(db_path) as conn:
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()
        
        date_dist_query = """
        SELECT 
            DATE(e.event_time) as event_date,
            COUNT(DISTINCT u.distinct_id) as unique_users,
            COUNT(*) as total_events
        FROM mixpanel_user u
        LEFT JOIN mixpanel_event e ON u.distinct_id = e.distinct_id
        WHERE u.abi_ad_set_id = ?
          AND u.has_abi_attribution = TRUE
          AND e.event_name = 'RC Trial started'
          AND DATE(e.event_time) BETWEEN ? AND ?
        GROUP BY DATE(e.event_time)
        ORDER BY event_date
        """
        
        cursor.execute(date_dist_query, (AD_SET_ID, START_DATE, END_DATE))
        results = cursor.fetchall()
        
        print(f"📊 Daily trial distribution:")
        total_unique = 0
        total_events = 0
        
        for result in results:
            print(f"   {result['event_date']}: {result['unique_users']} users, {result['total_events']} events")
            total_unique += result['unique_users']
            total_events += result['total_events']
            
        print(f"   TOTAL: {total_unique} users, {total_events} events")
        print(f"   Note: Total users != sum of daily users due to users with multiple events")

def main():
    print("🔍 INVESTIGATING TIMEZONE/BOUNDARY ISSUES")
    print(f"Ad Set ID: {AD_SET_ID}")
    print(f"Date Range: {START_DATE} to {END_DATE}")
    print(f"Goal: Explain why dashboard shows 42 instead of 49")
    
    test_timezone_boundaries()
    check_boundary_dates()
    analyze_date_distribution()

if __name__ == "__main__":
    main() 