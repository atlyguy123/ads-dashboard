#!/usr/bin/env python3
"""
Analyze Insert ID patterns from CSV vs database to understand Mixpanel's filtering logic.
"""

import sqlite3
import csv
from typing import Set, List, Dict, Any

CAMPAIGN_ID = "120223331225260178"
START_DATE = "2025-07-16"
END_DATE = "2025-07-29"

def get_database_path():
    return "database/mixpanel_data.db"

def load_csv_insert_ids() -> Set[str]:
    """Load Insert IDs from the CSV file"""
    csv_insert_ids = set()
    with open("mixpanel_user.csv", 'r') as f:
        reader = csv.DictReader(f)
        for row in reader:
            insert_id = row.get('Insert ID', '').strip()
            if insert_id:
                csv_insert_ids.add(insert_id)
    return csv_insert_ids

def get_database_trial_events() -> List[Dict[str, Any]]:
    """Get all trial events from database for this campaign"""
    db_path = get_database_path()
    
    with sqlite3.connect(db_path) as conn:
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()
        
        query = """
        SELECT 
            e.event_uuid,
            e.distinct_id,
            e.event_time,
            e.event_json,
            u.abi_campaign_id,
            u.has_abi_attribution
        FROM mixpanel_event e
        JOIN mixpanel_user u ON e.distinct_id = u.distinct_id
        WHERE u.abi_campaign_id = ?
          AND u.has_abi_attribution = TRUE
          AND e.event_name = 'RC Trial started'
          AND DATE(e.event_time) BETWEEN ? AND ?
        ORDER BY e.event_time
        """
        
        cursor.execute(query, [CAMPAIGN_ID, START_DATE, END_DATE])
        return [dict(row) for row in cursor.fetchall()]

def extract_insert_id_from_event_json(event_json: str) -> str:
    """Try to extract insert_id from event JSON"""
    if not event_json:
        return None
    
    try:
        import json
        data = json.loads(event_json)
        # Common places insert_id might be stored
        return (data.get('$insert_id') or 
                data.get('insert_id') or 
                data.get('properties', {}).get('$insert_id') or
                data.get('properties', {}).get('insert_id'))
    except json.JSONDecodeError:
        return None

def main():
    print("🔍 ANALYZING INSERT ID PATTERNS")
    print("=" * 50)
    
    # Step 1: Load CSV Insert IDs
    print("📄 Step 1: Loading CSV Insert IDs...")
    csv_insert_ids = load_csv_insert_ids()
    print(f"CSV contains {len(csv_insert_ids)} Insert IDs")
    print(f"Sample CSV Insert IDs: {list(csv_insert_ids)[:5]}")
    
    # Step 2: Get database events
    print(f"\n🗃️ Step 2: Loading database trial events...")
    db_events = get_database_trial_events()
    print(f"Database contains {len(db_events)} trial events")
    
    # Step 3: Try to match Insert IDs
    print(f"\n🔗 Step 3: Matching Insert IDs...")
    
    # Extract insert IDs from database events
    db_insert_ids = set()
    db_event_uuids = set()
    insert_id_mapping = {}
    
    for event in db_events:
        # Use event_uuid as potential insert_id
        event_uuid = event['event_uuid']
        db_event_uuids.add(event_uuid)
        
        # Try to extract insert_id from JSON
        json_insert_id = extract_insert_id_from_event_json(event['event_json'])
        if json_insert_id:
            db_insert_ids.add(json_insert_id)
            insert_id_mapping[json_insert_id] = {
                'event_uuid': event_uuid,
                'distinct_id': event['distinct_id'],
                'event_time': event['event_time']
            }
    
    print(f"Database event UUIDs: {len(db_event_uuids)}")
    print(f"Database insert IDs from JSON: {len(db_insert_ids)}")
    
    # Step 4: Find matches
    print(f"\n🎯 Step 4: Finding matches...")
    
    # Try matching UUIDs to CSV insert IDs
    uuid_matches = csv_insert_ids & db_event_uuids
    json_matches = csv_insert_ids & db_insert_ids
    
    print(f"CSV Insert IDs matching Event UUIDs: {len(uuid_matches)}")
    print(f"CSV Insert IDs matching JSON Insert IDs: {len(json_matches)}")
    
    if uuid_matches:
        print(f"✅ Found {len(uuid_matches)} UUID matches!")
        print(f"Sample matches: {list(uuid_matches)[:5]}")
        
        # Find users represented in CSV
        csv_users_from_matches = set()
        for event in db_events:
            if event['event_uuid'] in uuid_matches:
                csv_users_from_matches.add(event['distinct_id'])
        
        print(f"Users represented in CSV: {len(csv_users_from_matches)}")
        
        # Find excluded users
        all_db_users = set(event['distinct_id'] for event in db_events)
        excluded_users = all_db_users - csv_users_from_matches
        
        print(f"Users NOT represented in CSV: {len(excluded_users)}")
        print("Excluded users:")
        for user in excluded_users:
            user_events = [e for e in db_events if e['distinct_id'] == user]
            print(f"  {user}: {len(user_events)} events")
            for event in user_events:
                in_csv = "✅" if event['event_uuid'] in csv_insert_ids else "❌"
                print(f"    {event['event_time']}: {event['event_uuid']} {in_csv}")
    
    else:
        print("❌ No UUID matches found")
        print("Sample CSV Insert IDs:", list(csv_insert_ids)[:3])
        print("Sample DB UUIDs:", list(db_event_uuids)[:3])
        
        if json_matches:
            print(f"✅ Found {len(json_matches)} JSON matches!")
        else:
            print("❌ No JSON matches found either")
    
    # Step 5: Summary
    print(f"\n📊 SUMMARY:")
    print(f"CSV Insert IDs: {len(csv_insert_ids)}")
    print(f"Database Events: {len(db_events)}")
    print(f"Database Unique Users: {len(set(e['distinct_id'] for e in db_events))}")
    print(f"Matches found: {len(uuid_matches) if uuid_matches else len(json_matches)}")

if __name__ == "__main__":
    main() 