#!/usr/bin/env python3
"""
🔍 VALIDATE EXACT 1:1 MAPPING
Verify the exact relationship between users and events from CSV
NO ASSUMPTIONS - only validated facts
"""

import json
import csv
from pathlib import Path

def read_csv_data():
    """Read CSV and return exact data structure"""
    csv_data = []
    print("📄 Reading CSV data...")
    
    with open('mixpanel_user.csv', 'r') as f:
        reader = csv.DictReader(f)
        for row in reader:
            csv_data.append({
                'insert_id': row['Insert ID'],
                'distinct_id': row['Distinct ID'], 
                'user_id': row['User ID'],
                'time': row['Time'],
                'campaign': row['abi_~campaign'],
                'campaign_id': row['abi_~campaign_id']
            })
    
    print(f"📊 CSV contains {len(csv_data)} rows")
    return csv_data

def verify_users_in_json(csv_data):
    """Check each CSV user in JSON - NO ASSUMPTIONS"""
    print("\n🔍 CHECKING EACH CSV USER IN JSON")
    print("=" * 60)
    
    user_file = Path("data/users/66ac49f5-ca1d-4b9b-a518-bbd37d73d4fa.json")
    
    # Build set of all users in JSON
    json_users = set()
    print("📄 Reading all users from JSON...")
    
    with open(user_file, 'r') as f:
        for line in f:
            try:
                user = json.loads(line.strip())
                distinct_id = user.get('distinct_id')
                if distinct_id:
                    json_users.add(distinct_id)
            except json.JSONDecodeError:
                continue
    
    print(f"📊 Total users in JSON: {len(json_users)}")
    
    # Check each CSV user
    found_users = []
    missing_users = []
    
    for item in csv_data:
        distinct_id = item['distinct_id']
        if distinct_id in json_users:
            found_users.append(item)
            print(f"✅ FOUND: {distinct_id} (Insert ID: {item['insert_id']})")
        else:
            missing_users.append(item)
            print(f"❌ MISSING: {distinct_id} (Insert ID: {item['insert_id']})")
    
    print(f"\n📊 USER VERIFICATION SUMMARY:")
    print(f"✅ Found: {len(found_users)}/{len(csv_data)} users")
    print(f"❌ Missing: {len(missing_users)}/{len(csv_data)} users")
    
    return found_users, missing_users

def verify_events_in_data(csv_data):
    """Check each CSV event in our data - NO ASSUMPTIONS"""
    print("\n🔍 CHECKING EACH CSV EVENT IN DATA")
    print("=" * 60)
    
    # Check in downloaded event files from S3
    event_files = list(Path("data/events").glob("*.json"))
    
    if not event_files:
        print("❌ No local event files found!")
        print("🔍 Checking in S3 downloaded files instead...")
        event_files = list(Path("data/events").glob("*.json.gz"))
    
    if not event_files:
        print("❌ No event files found at all!")
        return [], csv_data
    
    # Build set of all events
    json_events = set()
    
    for event_file in event_files:
        print(f"📄 Reading events from {event_file.name}...")
        try:
            if event_file.suffix == '.gz':
                import gzip
                with gzip.open(event_file, 'rt') as f:
                    for line in f:
                        try:
                            event = json.loads(line.strip())
                            insert_id = event.get('insert_id')
                            if insert_id:
                                json_events.add(insert_id)
                        except json.JSONDecodeError:
                            continue
            else:
                with open(event_file, 'r') as f:
                    for line in f:
                        try:
                            event = json.loads(line.strip())
                            insert_id = event.get('insert_id')
                            if insert_id:
                                json_events.add(insert_id)
                        except json.JSONDecodeError:
                            continue
        except Exception as e:
            print(f"❌ Error reading {event_file}: {e}")
    
    print(f"📊 Total events in files: {len(json_events)}")
    
    # Check each CSV event
    found_events = []
    missing_events = []
    
    for item in csv_data:
        insert_id = item['insert_id']
        if insert_id in json_events:
            found_events.append(item)
            print(f"✅ FOUND: {insert_id} (User: {item['distinct_id']})")
        else:
            missing_events.append(item)
            print(f"❌ MISSING: {insert_id} (User: {item['distinct_id']})")
    
    print(f"\n📊 EVENT VERIFICATION SUMMARY:")
    print(f"✅ Found: {len(found_events)}/{len(csv_data)} events")
    print(f"❌ Missing: {len(missing_events)}/{len(csv_data)} events")
    
    return found_events, missing_events

def analyze_mapping(csv_data, found_users, missing_users, found_events, missing_events):
    """Analyze the exact mapping relationships"""
    print("\n🔍 ANALYZING 1:1 MAPPING RELATIONSHIPS")
    print("=" * 60)
    
    # Create mappings
    user_to_event = {item['distinct_id']: item['insert_id'] for item in csv_data}
    event_to_user = {item['insert_id']: item['distinct_id'] for item in csv_data}
    
    print(f"📊 Total CSV entries: {len(csv_data)}")
    print(f"📊 Unique users in CSV: {len(user_to_event)}")
    print(f"📊 Unique events in CSV: {len(event_to_user)}")
    
    if len(user_to_event) == len(event_to_user) == len(csv_data):
        print("✅ PERFECT 1:1 MAPPING confirmed in CSV")
    else:
        print("❌ CSV does not have 1:1 mapping!")
        
    # Analyze missing combinations
    missing_user_ids = {item['distinct_id'] for item in missing_users}
    missing_event_ids = {item['insert_id'] for item in missing_events}
    
    print(f"\n🔍 MISSING DATA ANALYSIS:")
    print(f"Missing users: {len(missing_user_ids)}")
    print(f"Missing events: {len(missing_event_ids)}")
    
    # Check if missing users correspond to missing events
    print(f"\n🔍 CHECKING CORRESPONDENCE:")
    
    for missing_user in missing_users:
        user_id = missing_user['distinct_id']
        event_id = missing_user['insert_id']
        
        if event_id in missing_event_ids:
            print(f"✅ CONSISTENT: Missing user {user_id} → Missing event {event_id}")
        else:
            print(f"❌ INCONSISTENT: Missing user {user_id} → Found event {event_id}")
    
    for missing_event in missing_events:
        user_id = missing_event['distinct_id']
        event_id = missing_event['insert_id']
        
        if user_id in missing_user_ids:
            print(f"✅ CONSISTENT: Missing event {event_id} → Missing user {user_id}")
        else:
            print(f"❌ INCONSISTENT: Missing event {event_id} → Found user {user_id}")

def main():
    print("🔍 EXACT 1:1 MAPPING VALIDATION")
    print("=" * 60)
    print("🚨 NO ASSUMPTIONS - ONLY VERIFIED FACTS")
    print()
    
    # Read CSV
    csv_data = read_csv_data()
    
    # Verify users
    found_users, missing_users = verify_users_in_json(csv_data)
    
    # Verify events
    found_events, missing_events = verify_events_in_data(csv_data)
    
    # Analyze mapping
    analyze_mapping(csv_data, found_users, missing_users, found_events, missing_events)
    
    print(f"\n🎯 FINAL VALIDATED FACTS")
    print("=" * 60)
    print(f"📊 CSV entries: {len(csv_data)}")
    print(f"📊 Users found: {len(found_users)}")
    print(f"📊 Users missing: {len(missing_users)}")
    print(f"📊 Events found: {len(found_events)}")
    print(f"📊 Events missing: {len(missing_events)}")
    
    # Print missing data for validation
    if missing_users:
        print(f"\n❌ MISSING USERS:")
        for user in missing_users:
            print(f"   {user['distinct_id']} → {user['insert_id']}")
    
    if missing_events:
        print(f"\n❌ MISSING EVENTS:")
        for event in missing_events:
            print(f"   {event['insert_id']} → {event['distinct_id']}")

if __name__ == "__main__":
    main() 