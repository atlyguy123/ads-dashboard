#!/usr/bin/env python3
"""
Debug Modal and Discrepancy Analysis

This script investigates the discrepancy between:
1. MIXPANEL TRIALS count (displayed in dashboard column)
2. TOTAL USERS count (shown in conversion rate tooltips)

Root cause: Different data sources, date fields, and filtering criteria.
"""

import sqlite3
import sys
from datetime import datetime, timedelta

def debug_modal_and_discrepancy():
    """Analyze the discrepancy between trials count and tooltip user count"""
    
    # Try different database paths
    possible_db_paths = [
        "data/mixpanel_database.db",
        "database/mixpanel_data.db", 
        "mixpanel_data.db",
        "database/mixpanel_database.db"
    ]
    
    mixpanel_db = None
    for db_path in possible_db_paths:
        try:
            with sqlite3.connect(db_path) as test_conn:
                cursor = test_conn.cursor()
                cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='mixpanel_user'")
                if cursor.fetchone():
                    mixpanel_db = db_path
                    break
        except:
            continue
    
    if not mixpanel_db:
        print("❌ Could not find a database with mixpanel_user table")
        print("Available databases:")
        import os
        for db_path in possible_db_paths:
            if os.path.exists(db_path):
                print(f"   📁 {db_path} (exists)")
            else:
                print(f"   ❌ {db_path} (not found)")
        return
    
    print("🔍 DEBUGGING DASHBOARD TRIAL COUNT DISCREPANCY")
    print("=" * 60)
    print(f"📂 Using database: {mixpanel_db}")
    
    try:
        with sqlite3.connect(mixpanel_db) as conn:
            conn.row_factory = sqlite3.Row
            cursor = conn.cursor()
            
            # Get a campaign with both trial data AND UPM data (to test real scenarios)
            cursor.execute("""
                SELECT campaign_stats.abi_campaign_id, 
                       campaign_stats.trial_count,
                       campaign_stats.upm_count
                FROM (
                    SELECT u.abi_campaign_id, 
                           COUNT(DISTINCT CASE WHEN e.event_name = 'RC Trial started' THEN u.distinct_id END) as trial_count,
                           COUNT(DISTINCT upm.distinct_id) as upm_count
                    FROM mixpanel_user u
                    LEFT JOIN mixpanel_event e ON u.distinct_id = e.distinct_id
                    LEFT JOIN user_product_metrics upm ON u.distinct_id = upm.distinct_id
                    WHERE u.abi_campaign_id IS NOT NULL 
                      AND u.has_abi_attribution = TRUE
                    GROUP BY u.abi_campaign_id 
                    HAVING trial_count > 0 AND upm_count > 0
                ) campaign_stats
                ORDER BY campaign_stats.trial_count DESC 
                LIMIT 1
            """)
            result = cursor.fetchone()
            if not result:
                print("❌ No campaigns found with both trial and UPM data")
                return
                
            campaign_id = result['abi_campaign_id']
            trial_count = result['trial_count']
            upm_count = result['upm_count']
            print(f"🎯 Found campaign with {trial_count} trial users and {upm_count} UPM users")
            
            # Use recent date range from actual data
            start_date = "2025-07-01"  # Use recent data
            end_date = "2025-07-28"    # Based on max date available
            
            print(f"📊 ANALYZING CAMPAIGN: {campaign_id}")
            print(f"📅 DATE RANGE: {start_date} to {end_date}")
            print()
            
            # ============================================
            # 1. MIXPANEL TRIALS COUNT (Dashboard Column)
            # ============================================
            print("1️⃣ MIXPANEL TRIALS COUNT (Dashboard Column Logic)")
            print("-" * 50)
            
            mixpanel_trials_query = """
            SELECT 
                COUNT(DISTINCT CASE WHEN e.event_name = 'RC Trial started' AND DATE(e.event_time) BETWEEN ? AND ? THEN u.distinct_id END) as mixpanel_trials_started,
                COUNT(DISTINCT u.distinct_id) as total_attributed_users
            FROM mixpanel_user u
            LEFT JOIN mixpanel_event e ON u.distinct_id = e.distinct_id
            WHERE u.abi_campaign_id = ?
              AND u.has_abi_attribution = TRUE
            """
            
            cursor.execute(mixpanel_trials_query, [start_date, end_date, campaign_id])
            mixpanel_result = cursor.fetchone()
            
            mixpanel_trials = mixpanel_result['mixpanel_trials_started']
            total_attributed = mixpanel_result['total_attributed_users']
            
            print(f"   Mixpanel Trials (by event_time): {mixpanel_trials}")
            print(f"   Total Attributed Users: {total_attributed}")
            print()
            
            # ============================================
            # 2. TOOLTIP TOTAL USERS COUNT  
            # ============================================
            print("2️⃣ TOOLTIP TOTAL USERS COUNT (Conversion Rate Logic)")
            print("-" * 50)
            
            tooltip_users_query = """
            SELECT 
                COUNT(*) as total_users,
                COUNT(CASE WHEN upm.trial_conversion_rate IS NOT NULL THEN 1 END) as users_with_trial_rates,
                COUNT(CASE WHEN upm.trial_converted_to_refund_rate IS NOT NULL THEN 1 END) as users_with_refund_rates,
                COUNT(CASE WHEN upm.initial_purchase_to_refund_rate IS NOT NULL THEN 1 END) as users_with_purchase_rates
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
            """
            
            cursor.execute(tooltip_users_query, [campaign_id, start_date, end_date, start_date, end_date])
            tooltip_result = cursor.fetchone()
            
            tooltip_users = tooltip_result['total_users']
            users_with_trial_rates = tooltip_result['users_with_trial_rates']
            users_with_refund_rates = tooltip_result['users_with_refund_rates']
            users_with_purchase_rates = tooltip_result['users_with_purchase_rates']
            
            print(f"   Tooltip Total Users: {tooltip_users}")
            print(f"   Users with trial rates: {users_with_trial_rates}")
            print(f"   Users with refund rates: {users_with_refund_rates}")
            print(f"   Users with purchase rates: {users_with_purchase_rates}")
            print()
            
            # ============================================
            # 3. DISCREPANCY ANALYSIS
            # ============================================
            print("3️⃣ DISCREPANCY ANALYSIS")
            print("-" * 50)
            
            discrepancy = mixpanel_trials - tooltip_users
            if discrepancy != 0:
                print(f"   ❌ DISCREPANCY FOUND: {discrepancy} users")
                print(f"   📊 Mixpanel Trials: {mixpanel_trials}")
                print(f"   📊 Tooltip Users: {tooltip_users}")
                print(f"   📈 Difference: {abs(discrepancy)} ({abs(discrepancy/mixpanel_trials)*100:.1f}%)")
            else:
                print(f"   ✅ NO DISCREPANCY: Both counts match at {mixpanel_trials}")
            print()
            
            # ============================================
            # 4. ROOT CAUSE INVESTIGATION
            # ============================================
            print("4️⃣ ROOT CAUSE INVESTIGATION")
            print("-" * 50)
            
            # Users with trial events but missing from UPM
            missing_from_upm_query = """
            WITH trial_users AS (
                SELECT DISTINCT u.distinct_id, u.abi_campaign_id
                FROM mixpanel_user u
                JOIN mixpanel_event e ON u.distinct_id = e.distinct_id
                WHERE u.abi_campaign_id = ?
                  AND u.has_abi_attribution = TRUE
                  AND e.event_name = 'RC Trial started'
                  AND DATE(e.event_time) BETWEEN ? AND ?
            ),
            upm_users AS (
                SELECT DISTINCT upm.distinct_id
                FROM user_product_metrics upm
                JOIN mixpanel_user u ON upm.distinct_id = u.distinct_id
                WHERE u.abi_campaign_id = ?
                  AND upm.credited_date BETWEEN ? AND ?
            )
            SELECT COUNT(*) as missing_from_upm
            FROM trial_users tu
            LEFT JOIN upm_users uu ON tu.distinct_id = uu.distinct_id
            WHERE uu.distinct_id IS NULL
            """
            
            cursor.execute(missing_from_upm_query, [campaign_id, start_date, end_date, campaign_id, start_date, end_date])
            missing_from_upm = cursor.fetchone()['missing_from_upm']
            
            # Users in UPM but missing conversion rates
            missing_rates_query = """
            SELECT 
                COUNT(*) as total_in_upm,
                COUNT(CASE WHEN upm.trial_conversion_rate IS NULL THEN 1 END) as missing_trial_rates,
                COUNT(CASE WHEN upm.trial_converted_to_refund_rate IS NULL THEN 1 END) as missing_refund_rates,
                COUNT(CASE WHEN upm.initial_purchase_to_refund_rate IS NULL THEN 1 END) as missing_purchase_rates
            FROM user_product_metrics upm
            JOIN mixpanel_user u ON upm.distinct_id = u.distinct_id
            WHERE u.abi_campaign_id = ?
              AND upm.credited_date BETWEEN ? AND ?
              AND EXISTS (
                  SELECT 1 FROM mixpanel_event e 
                  WHERE e.distinct_id = upm.distinct_id 
                  AND e.event_name = 'RC Trial started'
                  AND DATE(e.event_time) BETWEEN ? AND ?
              )
            """
            
            cursor.execute(missing_rates_query, [campaign_id, start_date, end_date, start_date, end_date])
            rates_result = cursor.fetchone()
            
            total_in_upm = rates_result['total_in_upm']
            missing_trial_rates = rates_result['missing_trial_rates']
            missing_refund_rates = rates_result['missing_refund_rates']
            missing_purchase_rates = rates_result['missing_purchase_rates']
            
            # Date field differences
            date_field_diff_query = """
            WITH trial_events AS (
                SELECT DISTINCT 
                    u.distinct_id,
                    DATE(e.event_time) as event_date,
                    upm.credited_date
                FROM mixpanel_user u
                JOIN mixpanel_event e ON u.distinct_id = e.distinct_id
                LEFT JOIN user_product_metrics upm ON u.distinct_id = upm.distinct_id
                WHERE u.abi_campaign_id = ?
                  AND e.event_name = 'RC Trial started'
                  AND DATE(e.event_time) BETWEEN ? AND ?
            )
            SELECT 
                COUNT(*) as total_with_events,
                COUNT(CASE WHEN credited_date IS NULL THEN 1 END) as missing_credited_date,
                COUNT(CASE WHEN event_date != credited_date THEN 1 END) as date_mismatch
            FROM trial_events
            """
            
            cursor.execute(date_field_diff_query, [campaign_id, start_date, end_date])
            date_result = cursor.fetchone()
            
            total_with_events = date_result['total_with_events']
            missing_credited_date = date_result['missing_credited_date']
            date_mismatch = date_result['date_mismatch']
            
            print(f"   🔍 Users with trial events missing from UPM: {missing_from_upm}")
            print(f"   🔍 Users in UPM (total): {total_in_upm}")
            print(f"   🔍 Users missing trial conversion rates: {missing_trial_rates}")
            print(f"   🔍 Users missing refund rates: {missing_refund_rates}")
            print(f"   🔍 Users missing purchase rates: {missing_purchase_rates}")
            print(f"   🔍 Users with event but no credited_date: {missing_credited_date}")
            print(f"   🔍 Users with event_date ≠ credited_date: {date_mismatch}")
            print()
            
            # ============================================
            # 5. SUMMARY AND RECOMMENDATIONS
            # ============================================
            print("5️⃣ SUMMARY AND RECOMMENDATIONS")
            print("-" * 50)
            
            print("🔍 DISCREPANCY ROOT CAUSES:")
            print(f"   1. Different date fields: event_time vs credited_date")
            print(f"   2. Missing conversion rate calculations: {missing_trial_rates + missing_refund_rates + missing_purchase_rates} null values")
            print(f"   3. Users not processed into UPM table: {missing_from_upm} missing users")
            print(f"   4. Date field mismatches: {date_mismatch} users with different dates")
            print()
            
            print("💡 WHICH COUNT IS MORE ACCURATE:")
            print("   ✅ MIXPANEL TRIALS count is more accurate for actual trial activity")
            print("   ❌ TOOLTIP TOTAL USERS count is artificially reduced by processing pipeline issues")
            print()
            
            print("🔧 RECOMMENDED FIXES:")
            print("   1. Fix product ID mapping issues in preprocessing pipeline")
            print("   2. Ensure all users with trial events get processed into UPM")
            print("   3. Use consistent date field (event_time) for both calculations")
            print("   4. Add data quality validation checks")
            print("   5. Consider using mixpanel_trials_started as tooltip denominator")
            
    except sqlite3.Error as e:
        print(f"❌ Database error: {e}")
    except Exception as e:
        print(f"❌ Error: {e}")

if __name__ == "__main__":
    debug_modal_and_discrepancy() 