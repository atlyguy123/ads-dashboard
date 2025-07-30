#!/usr/bin/env python3
"""
Verify ad set numbers:
- CSV should have 47 unique users and 49 events  
- Database should match these numbers
- Find users with duplicate events
"""

import sqlite3
import csv
from collections import Counter
from typing import Set, List, Dict, Any

# Ad Set Configuration
AD_SET_ID = "120223331225270178"
AD_SET_NAME = "ppc_atly_fb_advantage_tier1_ROAS_May_25 Ad set"
START_DATE = "2025-07-16"
END_DATE = "2025-07-29"
CSV_FILE = "mixpanel_user.csv"

def get_database_path():
    return "database/mixpanel_data.db"

def verify_csv_numbers():
    """Verify CSV has 47 unique users and 49 events"""
    print("=== CSV VERIFICATION ===")
    
    distinct_ids = []
    total_events = 0
    
    with open(CSV_FILE, 'r') as f:
        reader = csv.DictReader(f)
        for row in reader:
            distinct_id = row.get('Distinct ID', '').strip()
            if distinct_id:
                distinct_ids.append(distinct_id)
                total_events += 1
    
    unique_users = len(set(distinct_ids))
    user_counts = Counter(distinct_ids)
    duplicates = {user: count for user, count in user_counts.items() if count > 1}
    
    print(f"📊 CSV Results:")
    print(f"   Total events: {total_events}")
    print(f"   Unique users: {unique_users}")
    print(f"   Users with duplicates: {len(duplicates)}")
    
    if duplicates:
        print(f"📋 Duplicate Users:")
        for user, count in duplicates.items():
            print(f"   - {user}: {count} events")
    
    return {
        'total_events': total_events,
        'unique_users': unique_users,
        'duplicates': duplicates
    }

def verify_database_numbers():
    """Verify database has matching numbers for the ad set"""
    print("\n=== DATABASE VERIFICATION ===")
    
    db_path = get_database_path()
    
    with sqlite3.connect(db_path) as conn:
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()
        
        # Query for trial events in the ad set
        query = """
        SELECT 
            u.distinct_id,
            e.event_name,
            e.event_time,
            e.event_uuid,
            u.abi_ad_set_id,
            DATE(e.event_time) as event_date
        FROM mixpanel_user u
        LEFT JOIN mixpanel_event e ON u.distinct_id = e.distinct_id
        WHERE u.abi_ad_set_id = ?
          AND u.has_abi_attribution = TRUE
          AND e.event_name = 'RC Trial started'
          AND DATE(e.event_time) BETWEEN ? AND ?
        ORDER BY e.event_time
        """
        
        cursor.execute(query, (AD_SET_ID, START_DATE, END_DATE))
        results = cursor.fetchall()
        
        distinct_ids = [row['distinct_id'] for row in results]
        unique_users = len(set(distinct_ids))
        total_events = len(results)
        user_counts = Counter(distinct_ids)
        duplicates = {user: count for user, count in user_counts.items() if count > 1}
        
        print(f"📊 Database Results:")
        print(f"   Total events: {total_events}")
        print(f"   Unique users: {unique_users}")
        print(f"   Users with duplicates: {len(duplicates)}")
        
        if duplicates:
            print(f"📋 Duplicate Users:")
            for user, count in duplicates.items():
                print(f"   - {user}: {count} events")
                # Show the specific events for duplicate users
                user_events = [row for row in results if row['distinct_id'] == user]
                for i, event in enumerate(user_events):
                    print(f"     Event {i+1}: {event['event_time']} (UUID: {event['event_uuid']})")
        
        return {
            'total_events': total_events,
            'unique_users': unique_users,
            'duplicates': duplicates,
            'events': results
        }

def main():
    print("🔍 VERIFYING AD SET NUMBERS")
    print(f"Ad Set ID: {AD_SET_ID}")
    print(f"Ad Set Name: {AD_SET_NAME}")
    print(f"Date Range: {START_DATE} to {END_DATE}")
    
    # Verify CSV
    csv_results = verify_csv_numbers()
    
    # Verify Database
    db_results = verify_database_numbers()
    
    # Compare results
    print("\n=== COMPARISON ===")
    print(f"CSV Events vs DB Events: {csv_results['total_events']} vs {db_results['total_events']}")
    print(f"CSV Users vs DB Users: {csv_results['unique_users']} vs {db_results['unique_users']}")
    
    if (csv_results['total_events'] == db_results['total_events'] and 
        csv_results['unique_users'] == db_results['unique_users']):
        print("✅ PERFECT MATCH! CSV and Database numbers align.")
    else:
        print("❌ MISMATCH! Need to investigate discrepancy.")
    
    # Expected numbers check
    print(f"\n=== EXPECTED NUMBERS CHECK ===")
    print(f"Expected: 47 users, 49 events")
    print(f"CSV: {csv_results['unique_users']} users, {csv_results['total_events']} events")
    print(f"DB: {db_results['unique_users']} users, {db_results['total_events']} events")
    
    if csv_results['unique_users'] == 47 and csv_results['total_events'] == 49:
        print("✅ CSV matches expected numbers!")
    else:
        print("❌ CSV doesn't match expected numbers.")
        
    if db_results['unique_users'] == 47 and db_results['total_events'] == 49:
        print("✅ Database matches expected numbers!")
    else:
        print("❌ Database doesn't match expected numbers.")

if __name__ == "__main__":
    main() 