#!/usr/bin/env python3
"""
Verify if the difference is event-level vs user-level campaign attribution.
Check if CSV users have campaign IDs in their events while excluded users don't.
"""

import sqlite3
import csv
from typing import List, Dict, Any

CAMPAIGN_ID = "120223331225260178"
START_DATE = "2025-07-16"
END_DATE = "2025-07-29"

def get_database_path():
    return "database/mixpanel_data.db"

def get_csv_sample_users(count=5) -> List[str]:
    """Get sample of users from CSV"""
    csv_users = []
    with open("mixpanel_user.csv", 'r') as f:
        reader = csv.DictReader(f)
        for i, row in enumerate(reader):
            if i < count:
                csv_users.append(row.get('Distinct ID', '').strip())
            else:
                break
    return csv_users

def check_event_level_attribution(user_ids: List[str], label: str) -> None:
    """Check if users have campaign attribution at event level"""
    db_path = get_database_path()
    
    with sqlite3.connect(db_path) as conn:
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()
        
        placeholders = ','.join(['?' for _ in user_ids])
        query = f"""
        SELECT 
            e.distinct_id,
            e.event_name,
            e.event_time,
            e.abi_campaign_id as event_campaign_id,
            u.abi_campaign_id as user_campaign_id,
            e.abi_ad_id as event_ad_id,
            u.abi_ad_id as user_ad_id
        FROM mixpanel_event e
        JOIN mixpanel_user u ON e.distinct_id = u.distinct_id
        WHERE e.distinct_id IN ({placeholders})
          AND e.event_name = 'RC Trial started'
          AND DATE(e.event_time) BETWEEN ? AND ?
        ORDER BY e.distinct_id, e.event_time
        """
        
        params = user_ids + [START_DATE, END_DATE]
        cursor.execute(query, params)
        events = [dict(row) for row in cursor.fetchall()]
        
        print(f"\n📊 {label} - Event vs User Level Attribution:")
        print("=" * 60)
        
        users_with_event_attribution = 0
        users_without_event_attribution = 0
        
        for event in events:
            user_id = event['distinct_id'][:20] + "..." if len(event['distinct_id']) > 20 else event['distinct_id']
            event_campaign = event['event_campaign_id'] or "None"
            user_campaign = event['user_campaign_id'] or "None"
            event_ad = event['event_ad_id'] or "None"
            user_ad = event['user_ad_id'] or "None"
            
            print(f"👤 {user_id}")
            print(f"   Event Campaign ID: {event_campaign}")
            print(f"   User Campaign ID:  {user_campaign}")
            print(f"   Event Ad ID: {event_ad}")
            print(f"   User Ad ID:  {user_ad}")
            
            if event['event_campaign_id'] == CAMPAIGN_ID:
                users_with_event_attribution += 1
                print(f"   ✅ Event has correct campaign attribution")
            else:
                users_without_event_attribution += 1
                print(f"   ❌ Event missing campaign attribution")
            print()
        
        print(f"📈 Summary for {label}:")
        print(f"   Users with event-level attribution: {users_with_event_attribution}")
        print(f"   Users without event-level attribution: {users_without_event_attribution}")
        
        return users_with_event_attribution, users_without_event_attribution

def test_dashboard_query_with_event_attribution():
    """Test what the count would be if we require event-level attribution"""
    db_path = get_database_path()
    
    with sqlite3.connect(db_path) as conn:
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()
        
        # Query 1: User-level attribution only (current dashboard logic)
        query1 = """
        SELECT COUNT(DISTINCT u.distinct_id) as count
        FROM mixpanel_user u
        LEFT JOIN mixpanel_event e ON u.distinct_id = e.distinct_id
        WHERE u.abi_campaign_id = ?
          AND u.has_abi_attribution = TRUE
          AND e.event_name = 'RC Trial started'
          AND DATE(e.event_time) BETWEEN ? AND ?
        """
        
        cursor.execute(query1, [CAMPAIGN_ID, START_DATE, END_DATE])
        user_level_count = cursor.fetchone()['count']
        
        # Query 2: Event-level attribution required
        query2 = """
        SELECT COUNT(DISTINCT u.distinct_id) as count
        FROM mixpanel_user u
        LEFT JOIN mixpanel_event e ON u.distinct_id = e.distinct_id
        WHERE u.abi_campaign_id = ?
          AND u.has_abi_attribution = TRUE
          AND e.event_name = 'RC Trial started'
          AND DATE(e.event_time) BETWEEN ? AND ?
          AND e.abi_campaign_id = ?
        """
        
        cursor.execute(query2, [CAMPAIGN_ID, START_DATE, END_DATE, CAMPAIGN_ID])
        event_level_count = cursor.fetchone()['count']
        
        # Query 3: Event-level attribution OR user-level attribution
        query3 = """
        SELECT COUNT(DISTINCT u.distinct_id) as count
        FROM mixpanel_user u
        LEFT JOIN mixpanel_event e ON u.distinct_id = e.distinct_id
        WHERE (u.abi_campaign_id = ? OR e.abi_campaign_id = ?)
          AND u.has_abi_attribution = TRUE
          AND e.event_name = 'RC Trial started'
          AND DATE(e.event_time) BETWEEN ? AND ?
        """
        
        cursor.execute(query3, [CAMPAIGN_ID, CAMPAIGN_ID, START_DATE, END_DATE])
        either_level_count = cursor.fetchone()['count']
        
        return user_level_count, event_level_count, either_level_count

def main():
    print("🔍 INVESTIGATING EVENT-LEVEL vs USER-LEVEL ATTRIBUTION")
    print("=" * 70)
    
    # Test query variations
    print("\n📊 Testing Different Attribution Requirements:")
    user_count, event_count, either_count = test_dashboard_query_with_event_attribution()
    
    print(f"User-level attribution only: {user_count} users")
    print(f"Event-level attribution required: {event_count} users")
    print(f"Either user OR event attribution: {either_count} users")
    
    # Check which count matches what
    if event_count == 41:
        print("🎯 Event-level attribution requirement MATCHES CSV count (41)!")
    if user_count == 45:
        print("🎯 User-level attribution MATCHES database count (45)!")
    
    # Get excluded users
    excluded_users = [
        "$device:61833B32-C2E6-4F11-A5B7-F5C42665AA45",
        "$device:98E17F2E-2836-4C78-9D3F-75D128E16D9E", 
        "$device:D684F83A-B2AB-41DC-B0EC-A19C7980C45E",
        "197a0f6786d8-06993978608dde-497c7f60-59b90-197a0f6786ec48"
    ]
    
    # Check excluded users
    check_event_level_attribution(excluded_users, "EXCLUDED USERS")
    
    # Check CSV users
    csv_users = get_csv_sample_users(5)
    check_event_level_attribution(csv_users, "CSV USERS (SAMPLE)")

if __name__ == "__main__":
    main() 