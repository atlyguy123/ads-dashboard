#!/usr/bin/env python3
"""
Standalone User Lifecycle Deduplication

This script implements the "most recent active lifecycle wins" rule
without complex dependencies. Fixes the dashboard vs tooltip discrepancy.
"""

import sqlite3
from datetime import datetime

def deduplicate_user_lifecycles():
    """
    Standalone deduplication: Each user keeps only their most recent lifecycle.
    """
    
    db_path = "database/mixpanel_data.db"
    
    print("🔧 STANDALONE USER LIFECYCLE DEDUPLICATION")
    print("=" * 55)
    
    try:
        with sqlite3.connect(db_path) as conn:
            conn.row_factory = sqlite3.Row
            cursor = conn.cursor()
            
            print("📊 Finding users with multiple valid lifecycles...")
            
            # Find users with multiple valid lifecycles
            cursor.execute("""
                SELECT distinct_id, COUNT(*) as lifecycle_count
                FROM user_product_metrics
                WHERE valid_lifecycle = 1 OR valid_lifecycle IS NULL
                GROUP BY distinct_id
                HAVING COUNT(*) > 1
            """)
            
            users_with_multiple = cursor.fetchall()
            print(f"   Found {len(users_with_multiple):,} users with multiple lifecycles")
            
            if not users_with_multiple:
                print("   ✅ No deduplication needed")
                return
            
            deduplication_count = 0
            
            for user_row in users_with_multiple:
                distinct_id = user_row['distinct_id']
                lifecycle_count = user_row['lifecycle_count']
                
                # For each user, find their most recent lifecycle
                # (based on earliest trial start or purchase event)
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
                    LIMIT 1
                """, [distinct_id])
                
                most_recent = cursor.fetchone()
                if most_recent:
                    most_recent_product_id = most_recent['product_id']
                    
                    # Mark all other lifecycles for this user as invalid
                    cursor.execute("""
                        UPDATE user_product_metrics
                        SET valid_lifecycle = 0
                        WHERE distinct_id = ?
                          AND product_id != ?
                          AND (valid_lifecycle = 1 OR valid_lifecycle IS NULL)
                    """, [distinct_id, most_recent_product_id])
                    
                    # Ensure the kept lifecycle is marked as valid
                    cursor.execute("""
                        UPDATE user_product_metrics
                        SET valid_lifecycle = 1
                        WHERE distinct_id = ?
                          AND product_id = ?
                    """, [distinct_id, most_recent_product_id])
                    
                    deduplication_count += lifecycle_count - 1
                    
                    if len(users_with_multiple) <= 10:  # Show details for small numbers
                        print(f"   👤 {distinct_id[:15]}... | Kept: {most_recent_product_id} | Removed: {lifecycle_count-1}")
            
            conn.commit()
            
            print(f"\n✅ Deduplication completed:")
            print(f"   Users processed: {len(users_with_multiple):,}")
            print(f"   Duplicate lifecycles removed: {deduplication_count:,}")
            
            # Test our specific campaign to verify fix
            test_campaign_fix()
            
    except sqlite3.Error as e:
        print(f"❌ Database error: {e}")
    except Exception as e:
        print(f"❌ Error: {e}")

def test_campaign_fix():
    """Test the specific campaign to verify the fix worked"""
    
    db_path = "database/mixpanel_data.db"
    campaign_id = "120223331225260178"
    start_date = "2025-07-22"
    end_date = "2025-07-28"
    
    print(f"\n🧪 TESTING FIX ON SPECIFIC CAMPAIGN")
    print("-" * 40)
    
    try:
        with sqlite3.connect(db_path) as conn:
            conn.row_factory = sqlite3.Row
            cursor = conn.cursor()
            
            # Dashboard count
            cursor.execute("""
                SELECT 
                    COUNT(DISTINCT CASE WHEN e.event_name = 'RC Trial started' AND DATE(e.event_time) BETWEEN ? AND ? THEN u.distinct_id END) as mixpanel_trials
                FROM mixpanel_user u
                LEFT JOIN mixpanel_event e ON u.distinct_id = e.distinct_id
                WHERE u.abi_campaign_id = ?
                  AND u.has_abi_attribution = TRUE
            """, [start_date, end_date, campaign_id])
            
            dashboard_result = cursor.fetchone()
            mixpanel_trials = dashboard_result['mixpanel_trials']
            
            # Tooltip count (after deduplication)
            cursor.execute("""
                SELECT 
                    COUNT(*) as total_records,
                    COUNT(DISTINCT upm.distinct_id) as unique_users
                FROM user_product_metrics upm
                JOIN mixpanel_user u ON upm.distinct_id = u.distinct_id
                WHERE u.abi_campaign_id = ?
                  AND (upm.valid_lifecycle = 1 OR upm.valid_lifecycle IS NULL)
                  AND upm.trial_conversion_rate IS NOT NULL
                  AND EXISTS (
                      SELECT 1 FROM mixpanel_event e 
                      WHERE e.distinct_id = upm.distinct_id 
                      AND e.event_name = 'RC Trial started'
                      AND DATE(e.event_time) BETWEEN ? AND ?
                  )
            """, [campaign_id, start_date, end_date])
            
            tooltip_result = cursor.fetchone()
            tooltip_records = tooltip_result['total_records']
            tooltip_unique = tooltip_result['unique_users']
            
            print(f"   Dashboard trials: {mixpanel_trials}")
            print(f"   Tooltip records: {tooltip_records}")
            print(f"   Tooltip unique users: {tooltip_unique}")
            
            if mixpanel_trials == tooltip_records == tooltip_unique:
                print("   ✅ PERFECT ALIGNMENT: All counts match!")
                print("   🎉 Dashboard discrepancy resolved!")
            elif mixpanel_trials == tooltip_unique:
                print("   ✅ UNIQUE USERS MATCH: Deduplication working")
                print(f"   ⚠️  Still {tooltip_records - tooltip_unique} extra records")
            else:
                print("   ❌ ISSUE REMAINS: Counts still don't match")
                
    except sqlite3.Error as e:
        print(f"❌ Database error: {e}")

if __name__ == "__main__":
    deduplicate_user_lifecycles() 