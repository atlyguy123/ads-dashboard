#!/usr/bin/env python3
"""
Diagnose Specific Campaign Discrepancy

Campaign: ppc_atly_fb_advantage_tier1_ROAS_May_25 Campaign  
Campaign ID: 120223331225260178
Date Range: July 22nd, 2025 to July 28th, 2025
Issue: 9 Mixpanel trials vs 14 tooltip users
"""

import sqlite3

def diagnose_campaign_discrepancy():
    """Diagnose the specific campaign discrepancy"""
    
    # Exact campaign details from user
    campaign_id = "120223331225260178"
    start_date = "2025-07-22"
    end_date = "2025-07-28"
    
    print("🔍 DIAGNOSING SPECIFIC CAMPAIGN DISCREPANCY")
    print("=" * 60)
    print(f"📊 Campaign: ppc_atly_fb_advantage_tier1_ROAS_May_25 Campaign")
    print(f"🆔 Campaign ID: {campaign_id}")
    print(f"📅 Date Range: {start_date} to {end_date}")
    print()
    
    try:
        with sqlite3.connect("database/mixpanel_data.db") as conn:
            conn.row_factory = sqlite3.Row
            cursor = conn.cursor()
            
            # ============================================
            # 1. MIXPANEL TRIALS COUNT (Dashboard Logic)
            # ============================================
            print("1️⃣ MIXPANEL TRIALS COUNT (Dashboard - should be 9)")
            print("-" * 55)
            
            dashboard_query = """
            SELECT 
                COUNT(DISTINCT CASE WHEN e.event_name = 'RC Trial started' AND DATE(e.event_time) BETWEEN ? AND ? THEN u.distinct_id END) as mixpanel_trials,
                COUNT(DISTINCT u.distinct_id) as total_attributed_users
            FROM mixpanel_user u
            LEFT JOIN mixpanel_event e ON u.distinct_id = e.distinct_id
            WHERE u.abi_campaign_id = ?
              AND u.has_abi_attribution = TRUE
            """
            
            cursor.execute(dashboard_query, [start_date, end_date, campaign_id])
            dashboard_result = cursor.fetchone()
            
            mixpanel_trials = dashboard_result['mixpanel_trials']
            total_attributed = dashboard_result['total_attributed_users']
            
            print(f"   ✅ Mixpanel Trials: {mixpanel_trials}")
            print(f"   📊 Total Attributed Users: {total_attributed}")
            print()
            
            # ============================================
            # 2. TOOLTIP QUERY (UPDATED Logic - should match dashboard)
            # ============================================
            print("2️⃣ TOOLTIP QUERY (Updated - should be 9)")
            print("-" * 45)
            
            # This is the UPDATED tooltip query (after our fix)
            tooltip_query = """
            SELECT 
                COUNT(*) as total_users,
                COUNT(DISTINCT upm.distinct_id) as unique_users
            FROM user_product_metrics upm
            JOIN mixpanel_user u ON upm.distinct_id = u.distinct_id
            WHERE u.abi_campaign_id = ?
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
            
            cursor.execute(tooltip_query, [campaign_id, start_date, end_date])
            tooltip_result = cursor.fetchone()
            
            tooltip_total = tooltip_result['total_users']
            tooltip_unique = tooltip_result['unique_users']
            
            print(f"   🔢 Tooltip Total Records: {tooltip_total}")
            print(f"   👤 Tooltip Unique Users: {tooltip_unique}")
            print()
            
            # ============================================
            # 3. IDENTIFY THE EXACT DISCREPANCY
            # ============================================
            print("3️⃣ DISCREPANCY ANALYSIS")
            print("-" * 35)
            
            dashboard_vs_tooltip_total = mixpanel_trials - tooltip_total
            dashboard_vs_tooltip_unique = mixpanel_trials - tooltip_unique
            
            print(f"   Dashboard Trials: {mixpanel_trials}")
            print(f"   Tooltip Total: {tooltip_total} (difference: {dashboard_vs_tooltip_total})")
            print(f"   Tooltip Unique: {tooltip_unique} (difference: {dashboard_vs_tooltip_unique})")
            print()
            
            if dashboard_vs_tooltip_unique == 0:
                print("   ✅ Unique users match! The issue is multiple lifecycles per user.")
                print("   💡 Need to run deduplication pipeline to fix.")
            elif dashboard_vs_tooltip_unique != 0:
                print("   ❌ Unique users don't match! The tooltip fix may not have worked.")
                print("   🔍 Need to investigate further...")
            
            # ============================================
            # 4. FIND USERS WITH MULTIPLE LIFECYCLES
            # ============================================
            print("4️⃣ USERS WITH MULTIPLE LIFECYCLES")
            print("-" * 45)
            
            multiple_lifecycles_query = """
            SELECT 
                upm.distinct_id, 
                COUNT(*) as lifecycle_count,
                GROUP_CONCAT(upm.product_id, ', ') as products
            FROM user_product_metrics upm
            JOIN mixpanel_user u ON upm.distinct_id = u.distinct_id
            WHERE u.abi_campaign_id = ?
              AND upm.trial_conversion_rate IS NOT NULL
              AND EXISTS (
                  SELECT 1 FROM mixpanel_event e 
                  WHERE e.distinct_id = upm.distinct_id 
                  AND e.event_name = 'RC Trial started'
                  AND DATE(e.event_time) BETWEEN ? AND ?
              )
            GROUP BY upm.distinct_id
            HAVING COUNT(*) > 1
            ORDER BY lifecycle_count DESC
            """
            
            cursor.execute(multiple_lifecycles_query, [campaign_id, start_date, end_date])
            multiple_users = cursor.fetchall()
            
            if multiple_users:
                print(f"   Found {len(multiple_users)} users with multiple lifecycles:")
                for user in multiple_users:
                    distinct_id = user['distinct_id']
                    count = user['lifecycle_count']
                    products = user['products']
                    print(f"   👤 {distinct_id[:15]}... | {count} lifecycles | Products: {products}")
                
                total_extra_lifecycles = sum(user['lifecycle_count'] - 1 for user in multiple_users)
                print(f"\n   📊 Extra lifecycles: {total_extra_lifecycles}")
                print(f"   🧮 Expected after deduplication: {tooltip_total - total_extra_lifecycles}")
            else:
                print("   ✅ No users with multiple lifecycles found")
            
            print()
            
            # ============================================
            # 5. RECOMMENDATION
            # ============================================
            print("5️⃣ DIAGNOSIS & RECOMMENDATION")
            print("-" * 45)
            
            if tooltip_unique == mixpanel_trials:
                print("   ✅ TOOLTIP FIX WORKED: Unique users match")
                print("   🔧 NEXT STEP: Run deduplication pipeline to eliminate duplicate lifecycles")
                print("   📈 Expected result: 9 = 9 (perfect match)")
            else:
                print("   ❌ TOOLTIP FIX ISSUE: Unique users don't match")
                print("   🔍 INVESTIGATION NEEDED: Check tooltip query implementation")
                
                # Let's check if there are users in tooltip that shouldn't be there
                print("\n   🔍 Checking for users in tooltip but not in dashboard...")
                
                users_query = """
                SELECT DISTINCT upm.distinct_id
                FROM user_product_metrics upm
                JOIN mixpanel_user u ON upm.distinct_id = u.distinct_id
                WHERE u.abi_campaign_id = ?
                  AND upm.trial_conversion_rate IS NOT NULL
                  AND EXISTS (
                      SELECT 1 FROM mixpanel_event e 
                      WHERE e.distinct_id = upm.distinct_id 
                      AND e.event_name = 'RC Trial started'
                      AND DATE(e.event_time) BETWEEN ? AND ?
                  )
                """
                
                cursor.execute(users_query, [campaign_id, start_date, end_date])
                tooltip_users = set(row['distinct_id'] for row in cursor.fetchall())
                
                dashboard_users_query = """
                SELECT DISTINCT u.distinct_id
                FROM mixpanel_user u
                JOIN mixpanel_event e ON u.distinct_id = e.distinct_id
                WHERE u.abi_campaign_id = ?
                  AND u.has_abi_attribution = TRUE
                  AND e.event_name = 'RC Trial started'
                  AND DATE(e.event_time) BETWEEN ? AND ?
                """
                
                cursor.execute(dashboard_users_query, [campaign_id, start_date, end_date])
                dashboard_users = set(row['distinct_id'] for row in cursor.fetchall())
                
                only_in_tooltip = tooltip_users - dashboard_users
                only_in_dashboard = dashboard_users - tooltip_users
                
                print(f"   Users only in tooltip: {len(only_in_tooltip)}")
                print(f"   Users only in dashboard: {len(only_in_dashboard)}")
                
                if only_in_tooltip:
                    print("   ⚠️  Some users in tooltip shouldn't be there!")
                if only_in_dashboard:
                    print("   ⚠️  Some users missing from tooltip!")
            
    except sqlite3.Error as e:
        print(f"❌ Database error: {e}")
    except Exception as e:
        print(f"❌ Error: {e}")

if __name__ == "__main__":
    diagnose_campaign_discrepancy() 