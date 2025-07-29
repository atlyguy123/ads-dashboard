#!/usr/bin/env python3
"""
Test Deduplication Logic

This script tests the deduplication logic to ensure it will work correctly
before running the full validation pipeline.
"""

import sqlite3

def test_deduplication_logic():
    """Test the deduplication logic on current data"""
    
    # Database path
    db_path = "database/mixpanel_data.db"
    
    print("🧪 TESTING DEDUPLICATION LOGIC")
    print("=" * 50)
    
    try:
        with sqlite3.connect(db_path) as conn:
            conn.row_factory = sqlite3.Row
            cursor = conn.cursor()
            
            # Test with the same campaign we analyzed before
            campaign_id = "120217904661980178"
            
            print(f"📊 Testing with campaign: {campaign_id}")
            print()
            
            # 1. Find users with multiple valid lifecycles in current data
            cursor.execute("""
                SELECT u.distinct_id, COUNT(*) as lifecycle_count
                FROM user_product_metrics upm
                JOIN mixpanel_user u ON upm.distinct_id = u.distinct_id
                WHERE u.abi_campaign_id = ?
                  AND upm.trial_conversion_rate IS NOT NULL
                GROUP BY u.distinct_id
                HAVING COUNT(*) > 1
                LIMIT 5
            """, [campaign_id])
            
            multi_lifecycle_users = cursor.fetchall()
            
            print(f"1️⃣ USERS WITH MULTIPLE LIFECYCLES")
            print(f"   Found {len(multi_lifecycle_users)} users with multiple lifecycles")
            
            if not multi_lifecycle_users:
                print("   ✅ No deduplication needed - each user has one lifecycle")
                return
            
            # 2. For each user, show their lifecycles and which would be kept
            for user_row in multi_lifecycle_users:
                distinct_id = user_row['distinct_id']
                lifecycle_count = user_row['lifecycle_count']
                
                print(f"\n   👤 User: {distinct_id[:20]}... ({lifecycle_count} lifecycles)")
                
                # Get their lifecycles with start dates
                cursor.execute("""
                    SELECT 
                        upm.product_id,
                        MIN(e.event_time) as first_event_time
                    FROM user_product_metrics upm
                    JOIN mixpanel_event e ON upm.distinct_id = e.distinct_id
                    WHERE upm.distinct_id = ?
                      AND e.event_name IN ('RC Trial started', 'RC Initial purchase')
                      AND JSON_EXTRACT(e.event_json, '$.properties.product_id') = upm.product_id
                    GROUP BY upm.product_id
                    ORDER BY first_event_time DESC
                """, [distinct_id])
                
                lifecycles = cursor.fetchall()
                
                for i, lifecycle in enumerate(lifecycles):
                    product_id = lifecycle['product_id']
                    start_time = lifecycle['first_event_time']
                    status = "🟢 KEEP" if i == 0 else "🔴 REMOVE"
                    
                    print(f"      {status} | {product_id} | Started: {start_time}")
            
            # 3. Calculate the impact
            total_before = 0
            total_after = 0
            
            cursor.execute("""
                SELECT COUNT(*) as total_lifecycles
                FROM user_product_metrics upm
                JOIN mixpanel_user u ON upm.distinct_id = u.distinct_id
                WHERE u.abi_campaign_id = ?
                  AND upm.trial_conversion_rate IS NOT NULL
            """, [campaign_id])
            
            total_before = cursor.fetchone()['total_lifecycles']
            
            cursor.execute("""
                SELECT COUNT(DISTINCT u.distinct_id) as unique_users
                FROM user_product_metrics upm
                JOIN mixpanel_user u ON upm.distinct_id = u.distinct_id
                WHERE u.abi_campaign_id = ?
                  AND upm.trial_conversion_rate IS NOT NULL
            """, [campaign_id])
            
            total_after = cursor.fetchone()['unique_users']
            
            print(f"\n2️⃣ DEDUPLICATION IMPACT")
            print(f"   Before: {total_before} UPM records")
            print(f"   After:  {total_after} UPM records (one per user)")
            print(f"   Reduction: {total_before - total_after} duplicate lifecycles")
            
            # 4. Verify this matches our dashboard vs tooltip discrepancy
            cursor.execute("""
                SELECT 
                    COUNT(DISTINCT CASE WHEN e.event_name = 'RC Trial started' AND DATE(e.event_time) BETWEEN '2025-07-01' AND '2025-07-28' THEN u.distinct_id END) as dashboard_trials,
                    COUNT(DISTINCT u.distinct_id) as total_users
                FROM mixpanel_user u
                LEFT JOIN mixpanel_event e ON u.distinct_id = e.distinct_id
                WHERE u.abi_campaign_id = ?
                  AND u.has_abi_attribution = TRUE
            """, [campaign_id])
            
            result = cursor.fetchone()
            dashboard_trials = result['dashboard_trials']
            
            print(f"\n3️⃣ VERIFICATION")
            print(f"   Dashboard trials (unique users): {dashboard_trials}")
            print(f"   After deduplication: {total_after}")
            print(f"   ✅ Match expected: {dashboard_trials == total_after}")
            
    except sqlite3.Error as e:
        print(f"❌ Database error: {e}")
    except Exception as e:
        print(f"❌ Error: {e}")

if __name__ == "__main__":
    test_deduplication_logic() 