#!/usr/bin/env python3
"""
Debug Trial Count Discrepancy

Investigates the discrepancy between:
- Mixpanel Trials count (42 users) - based on event_time 
- Tooltip users count (48 users) - based on credited_date

Campaign: ppc_atly_fb_advantage_tier1_ROAS_May_25 Campaign
Campaign ID: 120223331225260178
Date Range: July 16-29, 2025
"""

import sqlite3
import pandas as pd
import sys
from pathlib import Path
from datetime import datetime

# Add utils directory to path
utils_path = str(Path(__file__).resolve().parent / "utils")
sys.path.append(utils_path)
from database_utils import get_database_path

# Configuration
DB_PATH = get_database_path('mixpanel_data')
CAMPAIGN_ID = '120223331225260178'
START_DATE = '2025-07-16'
END_DATE = '2025-07-29'

def get_mixpanel_trial_users():
    """
    Get the 42 users counted by Mixpanel Trials column
    Uses event_time and actual trial events
    """
    print("🔍 Getting Mixpanel Trial users (the 42)...")
    
    query = """
    SELECT DISTINCT
        u.distinct_id,
        u.abi_campaign_id,
        e.event_time,
        DATE(e.event_time) as event_date,
        e.event_name
    FROM mixpanel_user u
    LEFT JOIN mixpanel_event e ON u.distinct_id = e.distinct_id
    WHERE u.abi_campaign_id = ?
      AND u.has_abi_attribution = TRUE
      AND e.event_name = 'RC Trial started'
      AND DATE(e.event_time) BETWEEN ? AND ?
    ORDER BY e.event_time
    """
    
    with sqlite3.connect(DB_PATH) as conn:
        df = pd.read_sql_query(query, conn, params=[CAMPAIGN_ID, START_DATE, END_DATE])
    
    print(f"✅ Found {len(df)} Mixpanel trial users")
    if len(df) > 0:
        print(f"   First event: {df.iloc[0]['event_date']}")
        print(f"   Last event: {df.iloc[-1]['event_date']}")
        print(f"   Sample users: {df['distinct_id'].head(3).tolist()}")
    
    return df

def get_tooltip_users():
    """
    Get the 48 users counted by tooltip logic
    Uses credited_date and user_product_metrics
    """
    print("\n🔍 Getting Tooltip users (the 48)...")
    
    query = """
    SELECT DISTINCT
        upm.distinct_id,
        u.abi_campaign_id,
        upm.credited_date,
        upm.product_id,
        upm.trial_conversion_rate,
        upm.trial_converted_to_refund_rate,
        upm.initial_purchase_to_refund_rate,
        upm.current_value
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
    ORDER BY upm.credited_date
    """
    
    with sqlite3.connect(DB_PATH) as conn:
        df = pd.read_sql_query(query, conn, params=[CAMPAIGN_ID, START_DATE, END_DATE, START_DATE, END_DATE])
    
    print(f"✅ Found {len(df)} tooltip users")
    if len(df) > 0:
        print(f"   First credited: {df.iloc[0]['credited_date']}")
        print(f"   Last credited: {df.iloc[-1]['credited_date']}")
        print(f"   Sample users: {df['distinct_id'].head(3).tolist()}")
    
    return df

def get_tooltip_users_without_trial_filter():
    """
    Get tooltip users WITHOUT the trial event existence filter
    This will help us understand if the issue is with the EXISTS clause
    """
    print("\n🔍 Getting Tooltip users WITHOUT trial event filter...")
    
    query = """
    SELECT DISTINCT
        upm.distinct_id,
        u.abi_campaign_id,
        upm.credited_date,
        upm.product_id,
        upm.trial_conversion_rate,
        upm.trial_converted_to_refund_rate,
        upm.initial_purchase_to_refund_rate,
        upm.current_value
    FROM user_product_metrics upm
    JOIN mixpanel_user u ON upm.distinct_id = u.distinct_id
    WHERE u.abi_campaign_id = ?
      AND upm.credited_date BETWEEN ? AND ?
      AND upm.trial_conversion_rate IS NOT NULL
      AND upm.trial_converted_to_refund_rate IS NOT NULL  
      AND upm.initial_purchase_to_refund_rate IS NOT NULL
    ORDER BY upm.credited_date
    """
    
    with sqlite3.connect(DB_PATH) as conn:
        df = pd.read_sql_query(query, conn, params=[CAMPAIGN_ID, START_DATE, END_DATE])
    
    print(f"✅ Found {len(df)} tooltip users (without trial filter)")
    return df

def analyze_discrepancy(mixpanel_users, tooltip_users):
    """
    Analyze the differences between the two user sets
    """
    print("\n📊 ANALYZING DISCREPANCY...")
    print("=" * 50)
    
    # Get user sets
    mixpanel_set = set(mixpanel_users['distinct_id'])
    tooltip_set = set(tooltip_users['distinct_id'])
    
    print(f"Mixpanel users: {len(mixpanel_set)}")
    print(f"Tooltip users: {len(tooltip_set)}")
    print(f"Discrepancy: {len(tooltip_set) - len(mixpanel_set)}")
    
    # Find differences
    only_in_tooltip = tooltip_set - mixpanel_set
    only_in_mixpanel = mixpanel_set - tooltip_set
    in_both = mixpanel_set & tooltip_set
    
    print(f"\n📋 BREAKDOWN:")
    print(f"   In both: {len(in_both)} users")
    print(f"   Only in tooltip: {len(only_in_tooltip)} users")
    print(f"   Only in mixpanel: {len(only_in_mixpanel)} users")
    
    if only_in_tooltip:
        print(f"\n🔍 EXTRA USERS IN TOOLTIP ({len(only_in_tooltip)}):")
        for user_id in only_in_tooltip:
            user_data = tooltip_users[tooltip_users['distinct_id'] == user_id].iloc[0]
            print(f"   {user_id[:12]}... credited: {user_data['credited_date']}")
    
    if only_in_mixpanel:
        print(f"\n🔍 EXTRA USERS IN MIXPANEL ({len(only_in_mixpanel)}):")
        for user_id in only_in_mixpanel:
            user_data = mixpanel_users[mixpanel_users['distinct_id'] == user_id].iloc[0]
            print(f"   {user_id[:12]}... event: {user_data['event_date']}")
    
    return only_in_tooltip, only_in_mixpanel

def investigate_extra_tooltip_users(extra_users):
    """
    Deep dive into why these users are in tooltip but not mixpanel count
    """
    if not extra_users:
        print("\n✅ No extra users to investigate")
        return
    
    print(f"\n🕵️ DEEP DIVE: Investigating {len(extra_users)} extra tooltip users...")
    print("=" * 60)
    
    for user_id in extra_users:
        print(f"\n👤 USER: {user_id}")
        
        # Get their trial events
        trial_query = """
        SELECT 
            event_name,
            event_time,
            DATE(event_time) as event_date,
            JSON_EXTRACT(event_json, '$.properties.product_id') as product_id
        FROM mixpanel_event 
        WHERE distinct_id = ? 
          AND event_name = 'RC Trial started'
        ORDER BY event_time
        """
        
        # Get their credited_date info
        credited_query = """
        SELECT 
            credited_date,
            product_id,
            trial_conversion_rate,
            current_value
        FROM user_product_metrics 
        WHERE distinct_id = ?
        """
        
        with sqlite3.connect(DB_PATH) as conn:
            trial_events = pd.read_sql_query(trial_query, conn, params=[user_id])
            credited_info = pd.read_sql_query(credited_query, conn, params=[user_id])
        
        print(f"   📅 Credited dates: {credited_info['credited_date'].tolist()}")
        
        if len(trial_events) > 0:
            print(f"   🎯 Trial events:")
            for _, event in trial_events.iterrows():
                print(f"      {event['event_date']} - {event['product_id']}")
                
            # Check if any trial events are in our date range
            in_range = trial_events[
                (trial_events['event_date'] >= START_DATE) & 
                (trial_events['event_date'] <= END_DATE)
            ]
            
            if len(in_range) == 0:
                print(f"   ❌ NO TRIAL EVENTS in range {START_DATE} to {END_DATE}")
                print(f"   🤔 But credited_date IS in range - WHY?")
                
                # Check if this could be a fallback case (conversion without start)
                conversion_query = """
                SELECT 
                    event_name,
                    event_time,
                    DATE(event_time) as event_date
                FROM mixpanel_event 
                WHERE distinct_id = ? 
                  AND event_name = 'RC Trial converted'
                ORDER BY event_time
                """
                
                conversions = pd.read_sql_query(conversion_query, conn, params=[user_id])
                if len(conversions) > 0:
                    print(f"   🔄 Found conversions: {conversions['event_date'].tolist()}")
                    print(f"   💡 This might be a FALLBACK case (conversion without start event)")
        else:
            print(f"   ❌ NO TRIAL EVENTS FOUND AT ALL")

def check_timezone_issues():
    """
    Check if timezone differences could explain the discrepancy
    """
    print(f"\n🌍 CHECKING TIMEZONE ISSUES...")
    print("=" * 40)
    
    # Check events right at the boundaries
    boundary_query = """
    SELECT 
        distinct_id,
        event_time,
        DATE(event_time) as event_date,
        TIME(event_time) as event_time_only
    FROM mixpanel_event e
    JOIN mixpanel_user u ON e.distinct_id = u.distinct_id
    WHERE u.abi_campaign_id = ?
      AND e.event_name = 'RC Trial started'
      AND (
          DATE(e.event_time) = ? OR
          DATE(e.event_time) = ? OR  
          DATE(e.event_time) = DATE(?, '-1 day') OR
          DATE(e.event_time) = DATE(?, '+1 day')
      )
    ORDER BY e.event_time
    """
    
    with sqlite3.connect(DB_PATH) as conn:
        boundary_events = pd.read_sql_query(
            boundary_query, 
            conn, 
            params=[CAMPAIGN_ID, START_DATE, END_DATE, START_DATE, END_DATE]
        )
    
    if len(boundary_events) > 0:
        print(f"Found {len(boundary_events)} events near boundaries:")
        for _, event in boundary_events.iterrows():
            print(f"   {event['distinct_id'][:12]}... {event['event_date']} {event['event_time_only']}")
    else:
        print("No events found near date boundaries")

def main():
    """
    Main diagnostic function
    """
    print("🔍 TRIAL COUNT DISCREPANCY INVESTIGATION")
    print("=" * 60)
    print(f"Campaign: {CAMPAIGN_ID}")
    print(f"Date Range: {START_DATE} to {END_DATE}")
    print(f"Expected: 42 vs 48 user discrepancy")
    
    # Get both user sets
    mixpanel_users = get_mixpanel_trial_users()
    tooltip_users = get_tooltip_users()
    tooltip_users_no_filter = get_tooltip_users_without_trial_filter()
    
    # Check if the issue is with the EXISTS filter
    print(f"\n🔍 Tooltip users without trial filter: {len(tooltip_users_no_filter)}")
    print(f"   Tooltip users with trial filter: {len(tooltip_users)}")
    print(f"   Difference: {len(tooltip_users_no_filter) - len(tooltip_users)}")
    
    # Analyze discrepancy
    extra_tooltip, extra_mixpanel = analyze_discrepancy(mixpanel_users, tooltip_users)
    
    # Deep dive into extra users
    investigate_extra_tooltip_users(extra_tooltip)
    
    # Check timezone issues
    check_timezone_issues()
    
    print(f"\n🎯 SUMMARY:")
    print(f"   Mixpanel count (correct): {len(mixpanel_users)}")
    print(f"   Tooltip count (incorrect): {len(tooltip_users)}")
    print(f"   Extra tooltip users: {len(extra_tooltip)}")
    print(f"   Extra mixpanel users: {len(extra_mixpanel)}")

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print(f"❌ Error: {e}")
        import traceback
        traceback.print_exc() 