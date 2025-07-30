#!/usr/bin/env python3
"""
Find Actual Discrepancy

Get the exact user IDs and identify the real discrepancy
"""

import sqlite3
import sys
from pathlib import Path

# Add utils directory to path
utils_path = str(Path(__file__).resolve().parent / "utils")
sys.path.append(utils_path)
from database_utils import get_database_path

# Configuration
DB_PATH = get_database_path('mixpanel_data')
CAMPAIGN_ID = '120223331225260178'
START_DATE = '2025-07-16'
END_DATE = '2025-07-29'

def get_exact_user_lists():
    """
    Get the exact lists of users from both counts
    """
    with sqlite3.connect(DB_PATH) as conn:
        cursor = conn.cursor()
        
        # Get Mixpanel users
        print("🔍 Getting exact Mixpanel trial users...")
        cursor.execute("""
            SELECT DISTINCT u.distinct_id
            FROM mixpanel_user u
            LEFT JOIN mixpanel_event e ON u.distinct_id = e.distinct_id
            WHERE u.abi_campaign_id = ?
              AND u.has_abi_attribution = TRUE
              AND e.event_name = 'RC Trial started'
              AND DATE(e.event_time) BETWEEN ? AND ?
            ORDER BY u.distinct_id
        """, [CAMPAIGN_ID, START_DATE, END_DATE])
        
        mixpanel_users = [row[0] for row in cursor.fetchall()]
        print(f"   Found {len(mixpanel_users)} Mixpanel users")
        
        # Get Tooltip users
        print("\n🔍 Getting exact tooltip users...")
        cursor.execute("""
            SELECT DISTINCT upm.distinct_id
            FROM user_product_metrics upm
            JOIN mixpanel_user u ON upm.distinct_id = u.distinct_id
            WHERE u.abi_campaign_id = ?
              AND upm.credited_date BETWEEN ? AND ?
              AND upm.trial_conversion_rate IS NOT NULL
              AND upm.trial_converted_to_refund_rate IS NOT NULL  
              AND upm.initial_purchase_to_refund_rate IS NOT NULL
              AND EXISTS (
                  SELECT 1 FROM mixpanel_event e 
                  WHERE e.distinct_id = upm.distinct_id 
                  AND e.event_name = 'RC Trial started'
                  AND DATE(e.event_time) BETWEEN ? AND ?
              )
            ORDER BY upm.distinct_id
        """, [CAMPAIGN_ID, START_DATE, END_DATE, START_DATE, END_DATE])
        
        tooltip_users = [row[0] for row in cursor.fetchall()]
        print(f"   Found {len(tooltip_users)} tooltip users")
        
        return mixpanel_users, tooltip_users

def analyze_differences(mixpanel_users, tooltip_users):
    """
    Find the exact differences
    """
    mixpanel_set = set(mixpanel_users)
    tooltip_set = set(tooltip_users)
    
    only_in_mixpanel = mixpanel_set - tooltip_set
    only_in_tooltip = tooltip_set - mixpanel_set
    
    print(f"\n📊 ANALYSIS:")
    print(f"   Mixpanel count: {len(mixpanel_set)}")
    print(f"   Tooltip count: {len(tooltip_set)}")
    print(f"   Difference: {len(tooltip_set) - len(mixpanel_set)}")
    print(f"   Only in Mixpanel: {len(only_in_mixpanel)}")
    print(f"   Only in Tooltip: {len(only_in_tooltip)}")
    
    return only_in_mixpanel, only_in_tooltip

def investigate_specific_user(user_id):
    """
    Investigate a specific user
    """
    print(f"\n🕵️ INVESTIGATING: {user_id}")
    
    with sqlite3.connect(DB_PATH) as conn:
        cursor = conn.cursor()
        
        # Check if user exists in mixpanel_user
        cursor.execute("""
            SELECT abi_campaign_id, has_abi_attribution, country
            FROM mixpanel_user 
            WHERE distinct_id = ?
        """, [user_id])
        
        user_info = cursor.fetchone()
        if not user_info:
            print("   ❌ User NOT in mixpanel_user table")
            return
        
        print(f"   ✅ User in mixpanel_user: campaign={user_info[0]}, attribution={user_info[1]}")
        
        # Check trial events
        cursor.execute("""
            SELECT event_time, DATE(event_time) as event_date
            FROM mixpanel_event 
            WHERE distinct_id = ? 
              AND event_name = 'RC Trial started'
              AND DATE(event_time) BETWEEN ? AND ?
        """, [user_id, START_DATE, END_DATE])
        
        trial_events = cursor.fetchall()
        print(f"   🎯 Trial events in range: {len(trial_events)}")
        for event in trial_events:
            print(f"     {event[1]} ({event[0]})")
        
        # Check UPM records
        cursor.execute("""
            SELECT 
                credited_date,
                trial_conversion_rate,
                trial_converted_to_refund_rate,
                initial_purchase_to_refund_rate,
                product_id
            FROM user_product_metrics 
            WHERE distinct_id = ?
        """, [user_id])
        
        upm_records = cursor.fetchall()
        print(f"   📊 UPM records: {len(upm_records)}")
        for upm in upm_records:
            in_range = START_DATE <= upm[0] <= END_DATE if upm[0] else False
            has_rates = all([upm[1] is not None, upm[2] is not None, upm[3] is not None])
            print(f"     Credited: {upm[0]} ({'IN RANGE' if in_range else 'OUT OF RANGE'})")
            print(f"     Has rates: {has_rates}")
            print(f"     Product: {upm[4]}")

def main():
    """
    Main function
    """
    print("🔍 EXACT DISCREPANCY ANALYSIS")
    print("=" * 60)
    print(f"Campaign: {CAMPAIGN_ID}")
    print(f"Date Range: {START_DATE} to {END_DATE}")
    
    # Get exact user lists
    mixpanel_users, tooltip_users = get_exact_user_lists()
    
    # Analyze differences
    only_in_mixpanel, only_in_tooltip = analyze_differences(mixpanel_users, tooltip_users)
    
    # Investigate each discrepancy
    if only_in_mixpanel:
        print(f"\n🔍 USERS ONLY IN MIXPANEL COUNT ({len(only_in_mixpanel)}):")
        for user_id in only_in_mixpanel:
            investigate_specific_user(user_id)
    
    if only_in_tooltip:
        print(f"\n🔍 USERS ONLY IN TOOLTIP COUNT ({len(only_in_tooltip)}):")
        for user_id in only_in_tooltip:
            investigate_specific_user(user_id)
    
    if not only_in_mixpanel and not only_in_tooltip:
        print("\n✅ NO DISCREPANCY FOUND - Counts match!")

if __name__ == "__main__":
    main() 