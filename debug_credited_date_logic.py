#!/usr/bin/env python3
"""
Debug Credited Date Logic Analysis

This script analyzes the discrepancy between:
1. Users with trial events in date range (dashboard count)
2. Users with credited_date in date range (tooltip count)

According to 00_assign_credited_date.py, credited_date should be derived from trial event dates,
so these counts should be nearly identical. If they're not, there's a logic bug.
"""

import sqlite3
import sys
from datetime import datetime, timedelta

def debug_credited_date_logic():
    """Analyze the specific users causing the discrepancy"""
    
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
        return
    
    print("🔍 DEBUGGING CREDITED DATE LOGIC DISCREPANCY")
    print("=" * 65)
    print(f"📂 Using database: {mixpanel_db}")
    
    try:
        with sqlite3.connect(mixpanel_db) as conn:
            conn.row_factory = sqlite3.Row
            cursor = conn.cursor()
            
            # Get the same campaign and date range as before
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
            
            campaign_id = result['abi_campaign_id']
            start_date = "2025-07-01"
            end_date = "2025-07-28"
            
            print(f"📊 ANALYZING CAMPAIGN: {campaign_id}")
            print(f"📅 DATE RANGE: {start_date} to {end_date}")
            print()
            
            # ============================================
            # 1. GET DASHBOARD TRIAL USERS (by event_time)
            # ============================================
            print("1️⃣ DASHBOARD TRIAL USERS (by event_time)")
            print("-" * 45)
            
            dashboard_users_query = """
            SELECT DISTINCT u.distinct_id, DATE(e.event_time) as trial_date
            FROM mixpanel_user u
            JOIN mixpanel_event e ON u.distinct_id = e.distinct_id
            WHERE u.abi_campaign_id = ?
              AND u.has_abi_attribution = TRUE
              AND e.event_name = 'RC Trial started'
              AND DATE(e.event_time) BETWEEN ? AND ?
            ORDER BY trial_date, u.distinct_id
            """
            
            cursor.execute(dashboard_users_query, [campaign_id, start_date, end_date])
            dashboard_users = cursor.fetchall()
            dashboard_user_ids = set(row['distinct_id'] for row in dashboard_users)
            
            print(f"   Dashboard trial users: {len(dashboard_users)}")
            print(f"   Unique user IDs: {len(dashboard_user_ids)}")
            print()
            
            # ============================================
            # 2. GET TOOLTIP USERS (by credited_date)
            # ============================================
            print("2️⃣ TOOLTIP USERS (by credited_date)")
            print("-" * 40)
            
            tooltip_users_query = """
            SELECT DISTINCT upm.distinct_id, upm.credited_date
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
            ORDER BY upm.credited_date, upm.distinct_id
            """
            
            cursor.execute(tooltip_users_query, [campaign_id, start_date, end_date, start_date, end_date])
            tooltip_users = cursor.fetchall()
            tooltip_user_ids = set(row['distinct_id'] for row in tooltip_users)
            
            print(f"   Tooltip users: {len(tooltip_users)}")
            print(f"   Unique user IDs: {len(tooltip_user_ids)}")
            print()
            
            # ============================================
            # 3. FIND DISCREPANCIES
            # ============================================
            print("3️⃣ DISCREPANCY ANALYSIS")
            print("-" * 35)
            
            # Users in dashboard but NOT in tooltip
            dashboard_only = dashboard_user_ids - tooltip_user_ids
            # Users in tooltip but NOT in dashboard  
            tooltip_only = tooltip_user_ids - dashboard_user_ids
            # Users in both
            both_sets = dashboard_user_ids & tooltip_user_ids
            
            print(f"   Users in BOTH sets: {len(both_sets)}")
            print(f"   Users ONLY in dashboard: {len(dashboard_only)}")
            print(f"   Users ONLY in tooltip: {len(tooltip_only)}")
            print(f"   Total discrepancy: {abs(len(dashboard_users) - len(tooltip_users))}")
            print()
            
            # ============================================
            # 4. ANALYZE DASHBOARD-ONLY USERS
            # ============================================
            if dashboard_only:
                print("4️⃣ USERS ONLY IN DASHBOARD (have trials, missing from tooltip)")
                print("-" * 65)
                
                dashboard_only_list = list(dashboard_only)[:10]  # Limit to first 10
                
                for user_id in dashboard_only_list:
                    # Get their trial event details
                    cursor.execute("""
                        SELECT DATE(e.event_time) as trial_date, e.event_time
                        FROM mixpanel_event e
                        WHERE e.distinct_id = ? AND e.event_name = 'RC Trial started'
                        AND DATE(e.event_time) BETWEEN ? AND ?
                        ORDER BY e.event_time
                        LIMIT 1
                    """, [user_id, start_date, end_date])
                    trial_info = cursor.fetchone()
                    
                    # Check if they exist in UPM table
                    cursor.execute("""
                        SELECT credited_date, trial_conversion_rate
                        FROM user_product_metrics upm
                        JOIN mixpanel_user u ON upm.distinct_id = u.distinct_id
                        WHERE upm.distinct_id = ? AND u.abi_campaign_id = ?
                    """, [user_id, campaign_id])
                    upm_info = cursor.fetchone()
                    
                    trial_date = trial_info['trial_date'] if trial_info else 'No trial found'
                    if upm_info:
                        credited_date = upm_info['credited_date'] or 'NULL'
                        conversion_rate = upm_info['trial_conversion_rate']
                        print(f"   🔍 {user_id[:12]}... | Trial: {trial_date} | Credited: {credited_date} | Rate: {conversion_rate}")
                    else:
                        print(f"   🔍 {user_id[:12]}... | Trial: {trial_date} | UPM: NOT FOUND")
                
                print()
            
            # ============================================
            # 5. ANALYZE TOOLTIP-ONLY USERS  
            # ============================================
            if tooltip_only:
                print("5️⃣ USERS ONLY IN TOOLTIP (have credited_date, missing from dashboard)")
                print("-" * 68)
                
                tooltip_only_list = list(tooltip_only)[:10]  # Limit to first 10
                
                for user_id in tooltip_only_list:
                    # Get their UPM details
                    cursor.execute("""
                        SELECT upm.credited_date, upm.trial_conversion_rate
                        FROM user_product_metrics upm
                        JOIN mixpanel_user u ON upm.distinct_id = u.distinct_id
                        WHERE upm.distinct_id = ? AND u.abi_campaign_id = ?
                        AND upm.credited_date BETWEEN ? AND ?
                    """, [user_id, campaign_id, start_date, end_date])
                    upm_info = cursor.fetchone()
                    
                    # Check their trial events (any date)
                    cursor.execute("""
                        SELECT DATE(e.event_time) as trial_date, e.event_time
                        FROM mixpanel_event e
                        WHERE e.distinct_id = ? AND e.event_name = 'RC Trial started'
                        ORDER BY e.event_time
                        LIMIT 1
                    """, [user_id])
                    trial_info = cursor.fetchone()
                    
                    # Check if trial is outside date range
                    cursor.execute("""
                        SELECT COUNT(*) as trials_in_range
                        FROM mixpanel_event e
                        WHERE e.distinct_id = ? AND e.event_name = 'RC Trial started'
                        AND DATE(e.event_time) BETWEEN ? AND ?
                    """, [user_id, start_date, end_date])
                    trials_in_range = cursor.fetchone()['trials_in_range']
                    
                    credited_date = upm_info['credited_date'] if upm_info else 'NULL'
                    trial_date = trial_info['trial_date'] if trial_info else 'No trial found'
                    
                    print(f"   🔍 {user_id[:12]}... | Credited: {credited_date} | Trial: {trial_date} | In Range: {trials_in_range}")
                
                print()
            
            # ============================================
            # 6. ROOT CAUSE SUMMARY
            # ============================================
            print("6️⃣ ROOT CAUSE ANALYSIS")
            print("-" * 30)
            
            print("💡 EXPECTED BEHAVIOR (per 00_assign_credited_date.py):")
            print("   - credited_date should = DATE(trial_event_time)")
            print("   - Users with trials on date X should have credited_date = X")
            print("   - Therefore: dashboard count should ≈ tooltip count")
            print()
            
            print("🔍 ACTUAL DISCREPANCY CAUSES:")
            if dashboard_only:
                print(f"   - {len(dashboard_only)} users have trials but missing from UPM/tooltip")
                print("     → Possible: UPM processing incomplete or filtering issues")
            if tooltip_only:
                print(f"   - {len(tooltip_only)} users in tooltip but no trials in date range")
                print("     → Possible: credited_date ≠ actual trial date")
            
            print()
            print("🔧 INVESTIGATION RECOMMENDATIONS:")
            print("   1. Check if credited_date assignment is working correctly")
            print("   2. Verify UPM processing completeness")
            print("   3. Check for date field mismatches")
            print("   4. Review filtering logic differences between dashboard and tooltip")
            
    except sqlite3.Error as e:
        print(f"❌ Database error: {e}")
    except Exception as e:
        print(f"❌ Error: {e}")

if __name__ == "__main__":
    debug_credited_date_logic() 