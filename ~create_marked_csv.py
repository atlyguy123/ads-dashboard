#!/usr/bin/env python3
"""
Create a marked version of the expanded CSV that identifies:
1. Users with duplicate events
2. Users that are not part of the original 41 users
"""

import sqlite3
import csv
import json
from typing import Set, List, Dict, Any
from collections import Counter

CAMPAIGN_ID = "120223331225260178"
CAMPAIGN_NAME = "ppc_atly_fb_advantage_tier1_ROAS_May_25 Campaign"
START_DATE = "2025-07-16"
END_DATE = "2025-07-29"

def get_database_path():
    return "database/mixpanel_data.db"

def load_original_csv_users() -> Set[str]:
    """Load distinct IDs from the original CSV"""
    csv_users = set()
    with open("mixpanel_user.csv", 'r') as f:
        reader = csv.DictReader(f)
        for row in reader:
            distinct_id = row.get('Distinct ID', '').strip()
            if distinct_id:
                csv_users.add(distinct_id)
    return csv_users

def get_all_database_users_with_trials() -> List[Dict[str, Any]]:
    """Get all users with trial events from database"""
    db_path = get_database_path()
    
    with sqlite3.connect(db_path) as conn:
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()
        
        query = """
        SELECT 
            e.event_uuid as insert_id,
            e.event_time,
            e.distinct_id,
            u.abi_campaign_id,
            u.abi_ad_id,
            u.abi_ad_set_id,
            u.profile_json,
            DATE(e.event_time) as event_date,
            strftime('%Y-%m-%dT%H:%M:%S', e.event_time) as formatted_time
        FROM mixpanel_user u
        LEFT JOIN mixpanel_event e ON u.distinct_id = e.distinct_id
        WHERE u.abi_campaign_id = ?
          AND u.has_abi_attribution = TRUE
          AND e.event_name = 'RC Trial started'
          AND DATE(e.event_time) BETWEEN ? AND ?
        ORDER BY e.event_time
        """
        
        cursor.execute(query, [CAMPAIGN_ID, START_DATE, END_DATE])
        return [dict(row) for row in cursor.fetchall()]

def extract_user_id_from_profile(profile_json: str) -> str:
    """Extract user_id from profile JSON if available"""
    if not profile_json:
        return ""
    
    try:
        data = json.loads(profile_json)
        return (data.get('properties', {}).get('user_id') or
                data.get('user_id') or
                data.get('properties', {}).get('$user_id') or
                "")
    except json.JSONDecodeError:
        return ""

def create_marked_csv(database_events: List[Dict[str, Any]], original_csv_users: Set[str]) -> None:
    """Create CSV with all database users plus marking columns"""
    
    # Count events per user to identify duplicates
    user_event_counts = Counter(event['distinct_id'] for event in database_events)
    users_with_duplicates = {user for user, count in user_event_counts.items() if count > 1}
    
    # Track event numbers for duplicate users
    user_event_numbers = {}
    for event in database_events:
        user_id = event['distinct_id']
        if user_id not in user_event_numbers:
            user_event_numbers[user_id] = 0
        user_event_numbers[user_id] += 1
        event['event_number'] = user_event_numbers[user_id]
    
    with open("marked_mixpanel_user.csv", 'w', newline='') as f:
        # Extended headers with marking columns
        fieldnames = [
            'Time',
            'Insert ID', 
            'abi_~campaign',
            'abi_~campaign_id',
            'User ID',
            'Distinct ID',
            'Uniques of RC Trial started',
            'NEW_USER_FLAG',           # Mark users not in original 41
            'DUPLICATE_EVENT_FLAG',    # Mark duplicate events
            'EVENT_NUMBER',            # Event sequence for duplicates
            'NOTES'                    # Human readable notes
        ]
        
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        
        for event in database_events:
            user_id = extract_user_id_from_profile(event['profile_json'])
            distinct_id = event['distinct_id']
            
            # Determine flags
            is_new_user = distinct_id not in original_csv_users
            is_duplicate_event = distinct_id in users_with_duplicates
            event_number = event['event_number']
            
            # Create notes
            notes = []
            if is_new_user:
                notes.append("NOT_IN_ORIGINAL_CSV")
            if is_duplicate_event:
                notes.append(f"DUPLICATE_USER_EVENT_{event_number}_OF_{user_event_counts[distinct_id]}")
            
            notes_text = "; ".join(notes) if notes else ""
            
            row = {
                'Time': event['formatted_time'],
                'Insert ID': event['insert_id'],
                'abi_~campaign': CAMPAIGN_NAME,
                'abi_~campaign_id': event['abi_campaign_id'],
                'User ID': user_id,
                'Distinct ID': distinct_id,
                'Uniques of RC Trial started': 1,
                'NEW_USER_FLAG': "YES" if is_new_user else "NO",
                'DUPLICATE_EVENT_FLAG': "YES" if is_duplicate_event else "NO",
                'EVENT_NUMBER': event_number if is_duplicate_event else 1,
                'NOTES': notes_text
            }
            
            writer.writerow(row)

def main():
    print("🏷️ CREATING MARKED CSV FILE")
    print("=" * 40)
    
    # Step 1: Load original CSV users
    print("📄 Step 1: Loading original CSV users...")
    original_csv_users = load_original_csv_users()
    print(f"Original CSV contains: {len(original_csv_users)} users")
    
    # Step 2: Get all database events
    print("\n🗃️ Step 2: Loading database events...")
    db_events = get_all_database_users_with_trials()
    db_users = set(event['distinct_id'] for event in db_events)
    print(f"Database contains: {len(db_events)} events from {len(db_users)} users")
    
    # Step 3: Analyze patterns
    print("\n🔍 Step 3: Analyzing patterns...")
    
    # Find new users
    new_users = db_users - original_csv_users
    print(f"New users (not in original CSV): {len(new_users)}")
    for user in new_users:
        print(f"  - {user}")
    
    # Find users with multiple events
    from collections import Counter
    user_event_counts = Counter(event['distinct_id'] for event in db_events)
    users_with_duplicates = {user: count for user, count in user_event_counts.items() if count > 1}
    
    print(f"\nUsers with multiple events: {len(users_with_duplicates)}")
    for user, count in users_with_duplicates.items():
        in_original = "✅ Original" if user in original_csv_users else "🆕 New"
        print(f"  - {user}: {count} events ({in_original})")
    
    # Step 4: Create marked CSV
    print(f"\n📄 Step 4: Creating marked CSV...")
    create_marked_csv(db_events, original_csv_users)
    print(f"✅ Created 'marked_mixpanel_user.csv' with marking columns")
    
    # Step 5: Summary
    print(f"\n📊 SUMMARY:")
    print(f"Total events in marked CSV: {len(db_events)}")
    print(f"Total unique users: {len(db_users)}")
    print(f"New users (not in original 41): {len(new_users)}")
    print(f"Users with duplicate events: {len(users_with_duplicates)}")
    
    print(f"\n🏷️ MARKING LEGEND:")
    print(f"NEW_USER_FLAG: YES/NO - Whether user was in original CSV")
    print(f"DUPLICATE_EVENT_FLAG: YES/NO - Whether user has multiple events")
    print(f"EVENT_NUMBER: 1,2,3... - Event sequence for duplicate users")
    print(f"NOTES: Human readable explanation")

if __name__ == "__main__":
    main() 