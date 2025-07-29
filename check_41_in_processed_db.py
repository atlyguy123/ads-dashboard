#!/usr/bin/env python3
"""
🔍 CHECK 41 USERS/EVENTS IN PROCESSED DATABASE
After full pipeline rebuild, verify what made it through
"""
import sqlite3
import csv
import json
import sys
from pathlib import Path

# Add utils to path
utils_path = str(Path(__file__).resolve().parent / "utils")
sys.path.append(utils_path)
from database_utils import get_database_path

def read_csv_data():
    """Read our CSV with the 41 users/events"""
    csv_data = []
    with open('mixpanel_user.csv', 'r') as f:
        reader = csv.DictReader(f)
        for row in reader:
            csv_data.append({
                'insert_id': row['Insert ID'],
                'distinct_id': row['Distinct ID'], 
                'user_id': row['User ID'],
                'time': row['Time'],
                'date': row['Time'][:10]
            })
    return csv_data

def check_users_in_processed_db(csv_data):
    """Check which of our 41 users made it to processed database"""
    print("1️⃣ CHECKING: Which of the 41 CSV users are in processed database")
    print("=" * 60)
    
    processed_db_path = get_database_path("mixpanel_data")
    conn = sqlite3.connect(processed_db_path)
    cursor = conn.cursor()
    
    users_found = []
    users_missing = []
    
    for item in csv_data:
        distinct_id = item['distinct_id']
        cursor.execute("SELECT COUNT(*) FROM mixpanel_user WHERE distinct_id = ?", [distinct_id])
        exists = cursor.fetchone()[0]
        
        if exists > 0:
            users_found.append(item)
            print(f"✅ USER FOUND: {distinct_id}")
        else:
            users_missing.append(item)
            print(f"❌ USER MISSING: {distinct_id}")
    
    conn.close()
    
    print(f"\n📊 USER SUMMARY:")
    print(f"✅ Found: {len(users_found)}/41")
    print(f"❌ Missing: {len(users_missing)}/41")
    
    return users_found, users_missing

def check_events_in_processed_db(csv_data):
    """Check which of our 41 events made it to processed database"""
    print("\n2️⃣ CHECKING: Which of the 41 CSV events are in processed database")
    print("=" * 60)
    
    processed_db_path = get_database_path("mixpanel_data")
    conn = sqlite3.connect(processed_db_path)
    cursor = conn.cursor()
    
    events_found = []
    events_missing = []
    
    for item in csv_data:
        insert_id = item['insert_id']
        
        # Check if event exists by insert_id in the event_json
        cursor.execute("SELECT COUNT(*) FROM mixpanel_event WHERE event_json LIKE ?", [f'%{insert_id}%'])
        exists = cursor.fetchone()[0]
        
        if exists > 0:
            events_found.append(item)
            print(f"✅ EVENT FOUND: {insert_id} (User: {item['distinct_id']})")
        else:
            events_missing.append(item)
            print(f"❌ EVENT MISSING: {insert_id} (User: {item['distinct_id']})")
    
    conn.close()
    
    print(f"\n📊 EVENT SUMMARY:")
    print(f"✅ Found: {len(events_found)}/41")
    print(f"❌ Missing: {len(events_missing)}/41")
    
    return events_found, events_missing

def analyze_missing_patterns(users_missing, events_missing):
    """Analyze patterns in missing data"""
    print("\n3️⃣ ANALYZING: Patterns in missing data")
    print("=" * 60)
    
    if users_missing:
        print(f"🔍 MISSING USERS ({len(users_missing)}):")
        for user in users_missing:
            print(f"   {user['distinct_id']} → {user['date']}")
    
    if events_missing:
        print(f"\n🔍 MISSING EVENTS ({len(events_missing)}):")
        for event in events_missing:
            print(f"   {event['insert_id']} → {event['distinct_id']} → {event['date']}")
    
    # Check if missing events correspond to missing users
    missing_user_ids = {user['distinct_id'] for user in users_missing}
    missing_event_user_ids = {event['distinct_id'] for event in events_missing}
    
    overlap = missing_user_ids & missing_event_user_ids
    if overlap:
        print(f"\n🎯 CORRELATION: {len(overlap)} users missing from BOTH user and event tables:")
        for user_id in overlap:
            print(f"   {user_id}")
    
    # Check for events missing but users present
    events_missing_users_present = missing_event_user_ids - missing_user_ids
    if events_missing_users_present:
        print(f"\n🚨 CRITICAL: {len(events_missing_users_present)} events missing despite users being present:")
        for user_id in events_missing_users_present:
            print(f"   {user_id}")

def main():
    print("🔍 CHECKING 41 CSV USERS/EVENTS IN PROCESSED DATABASE")
    print("=" * 60)
    print("🎯 After complete pipeline rebuild with Module 2 & 3")
    print()
    
    csv_data = read_csv_data()
    print(f"📄 CSV contains {len(csv_data)} entries")
    
    users_found, users_missing = check_users_in_processed_db(csv_data)
    events_found, events_missing = check_events_in_processed_db(csv_data)
    
    analyze_missing_patterns(users_missing, events_missing)
    
    print(f"\n🎯 FINAL SUMMARY")
    print("=" * 60)
    print(f"👥 Users: {len(users_found)}/41 found, {len(users_missing)}/41 missing")
    print(f"📊 Events: {len(events_found)}/41 found, {len(events_missing)}/41 missing")
    
    if len(users_found) == 41 and len(events_found) == 41:
        print("🎉 SUCCESS: All 41 users and events made it through the pipeline!")
    else:
        print("🚨 ISSUE: Some users/events were lost during pipeline processing")
        print("📋 Next step: Investigate the 'missing users' and 'unimportant events' filtering logic")

if __name__ == "__main__":
    main() 