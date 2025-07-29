#!/usr/bin/env python3
"""
🔍 VERIFY USER-EVENT MAPPING
Check if the 4 missing users exist in our data and investigate processing order issues
"""

import json
import csv
from pathlib import Path

def read_csv_data():
    """Read the CSV to get all users and events"""
    csv_data = []
    with open('mixpanel_user.csv', 'r') as f:
        reader = csv.DictReader(f)
        for row in reader:
            csv_data.append({
                'insert_id': row['Insert ID'],
                'distinct_id': row['Distinct ID'],
                'user_id': row['User ID'],
                'time': row['Time']
            })
    return csv_data

def check_users_in_json(csv_data):
    """Check which users from CSV exist in our JSON"""
    print("🔍 CHECKING USERS FROM CSV IN JSON FILE")
    print("=" * 60)
    
    user_file = Path("data/users/66ac49f5-ca1d-4b9b-a518-bbd37d73d4fa.json")
    
    found_users = set()
    missing_users = []
    
    # Read all users from JSON
    print("📄 Reading user data from JSON...")
    with open(user_file, 'r') as f:
        for line_num, line in enumerate(f, 1):
            try:
                user = json.loads(line.strip())
                distinct_id = user.get('distinct_id')
                if distinct_id:
                    found_users.add(distinct_id)
            except json.JSONDecodeError:
                continue
    
    print(f"📊 Total users in JSON: {len(found_users)}")
    print()
    
    # Check each CSV user
    for item in csv_data:
        distinct_id = item['distinct_id']
        if distinct_id in found_users:
            print(f"✅ USER FOUND: {distinct_id}")
        else:
            print(f"❌ USER MISSING: {distinct_id}")
            missing_users.append(item)
    
    print(f"\n📊 SUMMARY:")
    print(f"✅ Found: {len(csv_data) - len(missing_users)}/{len(csv_data)} users")
    print(f"❌ Missing: {len(missing_users)}/{len(csv_data)} users")
    
    return missing_users

def check_events_in_data(csv_data):
    """Check which events from CSV exist in our data"""
    print("\n🔍 CHECKING EVENTS FROM CSV IN DATA")
    print("=" * 60)
    
    # Check in latest downloaded event files
    event_files = list(Path("data/events").glob("*.json"))
    if not event_files:
        print("❌ No event files found!")
        return []
    
    found_events = set()
    
    # Read all events from JSON files
    for event_file in event_files:
        print(f"📄 Reading events from {event_file.name}...")
        try:
            with open(event_file, 'r') as f:
                for line in f:
                    try:
                        event = json.loads(line.strip())
                        insert_id = event.get('insert_id')
                        if insert_id:
                            found_events.add(insert_id)
                    except json.JSONDecodeError:
                        continue
        except Exception as e:
            print(f"❌ Error reading {event_file}: {e}")
    
    print(f"📊 Total events in JSON files: {len(found_events)}")
    print()
    
    missing_events = []
    
    # Check each CSV event
    for item in csv_data:
        insert_id = item['insert_id']
        if insert_id in found_events:
            print(f"✅ EVENT FOUND: {insert_id}")
        else:
            print(f"❌ EVENT MISSING: {insert_id}")
            missing_events.append(item)
    
    print(f"\n📊 SUMMARY:")
    print(f"✅ Found: {len(csv_data) - len(missing_events)}/{len(csv_data)} events")
    print(f"❌ Missing: {len(missing_events)}/{len(csv_data)} events")
    
    return missing_events

def analyze_processing_order():
    """Analyze the processing order in the pipeline"""
    print("\n🔍 ANALYZING PIPELINE PROCESSING ORDER")
    print("=" * 60)
    
    print("📋 Current Pipeline Steps (from pipelines/mixpanel_pipeline/):")
    print("1. 01_download_update_data.py - Downloads BOTH users AND events")
    print("2. 02_setup_database.py - Sets up database schema")
    print("3. 03_ingest_data.py - Processes data from raw to processed DB")
    
    print("\n🔍 Checking 03_ingest_data.py processing order...")
    
    # Read the ingestion script to understand order
    ingest_file = Path("pipelines/mixpanel_pipeline/03_ingest_data.py")
    if ingest_file.exists():
        with open(ingest_file, 'r') as f:
            content = f.read()
            
        # Look for processing order
        if 'process_users' in content and 'process_events' in content:
            # Find which comes first
            users_pos = content.find('process_users')
            events_pos = content.find('process_events')
            
            if users_pos < events_pos and users_pos != -1:
                print("📊 Processing Order: USERS FIRST, then EVENTS ✅")
                print("   This is CORRECT - users must exist before events can reference them")
            elif events_pos < users_pos and events_pos != -1:
                print("⚠️  Processing Order: EVENTS FIRST, then USERS ❌")
                print("   This could cause issues - events need users to exist first!")
            else:
                print("🤔 Could not determine processing order from code")
        
        # Look for error handling
        if 'foreign key' in content.lower() or 'constraint' in content.lower():
            print("✅ Foreign key constraints detected - should prevent orphaned events")
        else:
            print("⚠️  No foreign key constraint handling found")
    
    else:
        print("❌ Could not find 03_ingest_data.py file")

def main():
    print("🔍 COMPREHENSIVE USER-EVENT MAPPING VERIFICATION")
    print("=" * 60)
    
    # Read CSV data
    csv_data = read_csv_data()
    print(f"📊 CSV contains {len(csv_data)} user-event pairs")
    print()
    
    # Check users
    missing_users = check_users_in_json(csv_data)
    
    # Check events  
    missing_events = check_events_in_data(csv_data)
    
    # Analyze processing order
    analyze_processing_order()
    
    print(f"\n🎯 FINAL ANALYSIS")
    print("=" * 60)
    
    if len(missing_users) == 0 and len(missing_events) == 0:
        print("✅ PERFECT 1:1 MAPPING: All users and events exist in source data")
        print("🚨 Problem is likely PROCESSING ORDER or CONSTRAINT HANDLING")
    else:
        print(f"❌ SOURCE DATA ISSUES:")
        print(f"   Missing users: {len(missing_users)}")
        print(f"   Missing events: {len(missing_events)}")
        
        if missing_users:
            print(f"\n❌ Missing users:")
            for user in missing_users:
                print(f"   {user['distinct_id']} (event: {user['insert_id']})")
                
        if missing_events:
            print(f"\n❌ Missing events:")
            for event in missing_events:
                print(f"   {event['insert_id']} (user: {event['distinct_id']})")

if __name__ == "__main__":
    main() 