#!/usr/bin/env python3
"""
Investigate Missing User

Focus on the specific user that appears in Mixpanel count but not tooltip count:
User ID: 197759f7b942... (event on 2025-07-29)
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
PROBLEM_USER = '197759f7b942e9ad03e27e03abb2c5d04f4ae24bd53a22e6ecba35ec49fef34b'  # Full ID

def investigate_user(user_id):
    """
    Deep dive into a specific user's data
    """
    print(f"🕵️ INVESTIGATING USER: {user_id}")
    print("=" * 80)
    
    with sqlite3.connect(DB_PATH) as conn:
        cursor = conn.cursor()
        
        # 1. Check user attribution
        print("\n1️⃣ USER ATTRIBUTION:")
        cursor.execute("""
            SELECT 
                distinct_id,
                abi_campaign_id,
                has_abi_attribution,
                country,
                region
            FROM mixpanel_user 
            WHERE distinct_id = ?
        """, [user_id])
        
        user_info = cursor.fetchone()
        if user_info:
            print(f"   ✅ Found user in mixpanel_user table")
            print(f"   Campaign ID: {user_info[1]}")
            print(f"   Has Attribution: {user_info[2]}")
            print(f"   Country: {user_info[3]}")
            print(f"   Region: {user_info[4]}")
        else:
            print(f"   ❌ User NOT found in mixpanel_user table")
            return
        
        # 2. Check trial events
        print("\n2️⃣ TRIAL EVENTS:")
        cursor.execute("""
            SELECT 
                event_name,
                event_time,
                DATE(event_time) as event_date,
                JSON_EXTRACT(event_json, '$.properties.product_id') as product_id
            FROM mixpanel_event 
            WHERE distinct_id = ? 
              AND event_name = 'RC Trial started'
            ORDER BY event_time
        """, [user_id])
        
        trial_events = cursor.fetchall()
        if trial_events:
            print(f"   ✅ Found {len(trial_events)} trial events:")
            for event in trial_events:
                in_range = START_DATE <= event[2] <= END_DATE
                range_indicator = "✅ IN RANGE" if in_range else "❌ OUT OF RANGE"
                print(f"     {event[2]} ({event[1]}) - {event[3]} - {range_indicator}")
        else:
            print(f"   ❌ No trial events found")
        
        # 3. Check user_product_metrics
        print("\n3️⃣ USER PRODUCT METRICS:")
        cursor.execute("""
            SELECT 
                user_product_id,
                product_id,
                credited_date,
                trial_conversion_rate,
                trial_converted_to_refund_rate,
                initial_purchase_to_refund_rate,
                current_value,
                current_status
            FROM user_product_metrics 
            WHERE distinct_id = ?
        """, [user_id])
        
        upm_records = cursor.fetchall()
        if upm_records:
            print(f"   ✅ Found {len(upm_records)} UPM records:")
            for upm in upm_records:
                in_range = START_DATE <= upm[2] <= END_DATE if upm[2] else False
                range_indicator = "✅ IN RANGE" if in_range else "❌ OUT OF RANGE"
                has_rates = all([upm[3] is not None, upm[4] is not None, upm[5] is not None])
                rates_indicator = "✅ HAS RATES" if has_rates else "❌ MISSING RATES"
                
                print(f"     Product: {upm[1]}")
                print(f"     Credited: {upm[2]} - {range_indicator}")
                print(f"     Rates: {rates_indicator}")
                print(f"       - Trial Conv: {upm[3]}")
                print(f"       - Trial Refund: {upm[4]}")
                print(f"       - Purchase Refund: {upm[5]}")
                print(f"     Value: ${upm[6]} ({upm[7]})")
                print()
        else:
            print(f"   ❌ No UPM records found")
        
        # 4. Test tooltip query conditions
        print("\n4️⃣ TOOLTIP QUERY TEST:")
        cursor.execute("""
            SELECT 
                upm.distinct_id,
                upm.credited_date,
                upm.trial_conversion_rate,
                upm.trial_converted_to_refund_rate,
                upm.initial_purchase_to_refund_rate,
                CASE WHEN EXISTS (
                    SELECT 1 FROM mixpanel_event e 
                    WHERE e.distinct_id = upm.distinct_id 
                    AND e.event_name = 'RC Trial started'
                    AND DATE(e.event_time) BETWEEN ? AND ?
                ) THEN 'YES' ELSE 'NO' END as has_trial_in_range
            FROM user_product_metrics upm
            JOIN mixpanel_user u ON upm.distinct_id = u.distinct_id
            WHERE upm.distinct_id = ?
              AND u.abi_campaign_id = ?
        """, [START_DATE, END_DATE, user_id, CAMPAIGN_ID])
        
        tooltip_test = cursor.fetchall()
        if tooltip_test:
            print(f"   Found {len(tooltip_test)} potential tooltip records:")
            for test in tooltip_test:
                credited_in_range = START_DATE <= test[1] <= END_DATE if test[1] else False
                has_all_rates = all([test[2] is not None, test[3] is not None, test[4] is not None])
                
                print(f"     Credited in range: {credited_in_range} ({test[1]})")
                print(f"     Has all rates: {has_all_rates}")
                print(f"     Has trial in range: {test[5]}")
                
                # Final eligibility
                eligible = credited_in_range and has_all_rates and test[5] == 'YES'
                print(f"     🎯 ELIGIBLE FOR TOOLTIP: {'✅ YES' if eligible else '❌ NO'}")
        else:
            print(f"   ❌ User doesn't match basic tooltip criteria")

def check_campaign_totals():
    """
    Double-check the campaign totals we're seeing
    """
    print(f"\n🔢 CAMPAIGN TOTALS VERIFICATION")
    print("=" * 50)
    
    with sqlite3.connect(DB_PATH) as conn:
        cursor = conn.cursor()
        
        # Mixpanel count
        cursor.execute("""
            SELECT COUNT(DISTINCT u.distinct_id)
            FROM mixpanel_user u
            LEFT JOIN mixpanel_event e ON u.distinct_id = e.distinct_id
            WHERE u.abi_campaign_id = ?
              AND u.has_abi_attribution = TRUE
              AND e.event_name = 'RC Trial started'
              AND DATE(e.event_time) BETWEEN ? AND ?
        """, [CAMPAIGN_ID, START_DATE, END_DATE])
        
        mixpanel_count = cursor.fetchone()[0]
        print(f"Mixpanel trial count: {mixpanel_count}")
        
        # Tooltip count
        cursor.execute("""
            SELECT COUNT(DISTINCT upm.distinct_id)
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
        """, [CAMPAIGN_ID, START_DATE, END_DATE, START_DATE, END_DATE])
        
        tooltip_count = cursor.fetchone()[0]
        print(f"Tooltip user count: {tooltip_count}")
        print(f"Discrepancy: {tooltip_count - mixpanel_count}")

def main():
    """
    Main investigation
    """
    print("🔍 MISSING USER INVESTIGATION")
    print("Campaign:", CAMPAIGN_ID)
    print("Date Range:", START_DATE, "to", END_DATE)
    
    # Check overall totals first
    check_campaign_totals()
    
    # Investigate the specific user
    investigate_user(PROBLEM_USER)

if __name__ == "__main__":
    main() 