#!/usr/bin/env python3
"""
🔍 PRECISE COUNT VERIFICATION
Verify exact counts to resolve contradictory information
"""

import json
import csv
import gzip
from pathlib import Path

def read_csv_data():
    """Read CSV data"""
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

def check_users_in_s3(csv_data):
    """Count how many of the 41 CSV users are in S3 user data"""
    print("1️⃣ CHECKING: How many of the 41 users are in S3 data")
    print("=" * 60)
    
    user_file = Path("data/users/66ac49f5-ca1d-4b9b-a518-bbd37d73d4fa.json")
    
    # Get all S3 users
    s3_users = set()
    with open(user_file, 'r') as f:
        for line in f:
            try:
                user = json.loads(line.strip())
                distinct_id = user.get('distinct_id')
                if distinct_id:
                    s3_users.add(distinct_id)
            except json.JSONDecodeError:
                continue
    
    # Check CSV users
    csv_users_in_s3 = []
    csv_users_missing = []
    
    for item in csv_data:
        distinct_id = item['distinct_id']
        if distinct_id in s3_users:
            csv_users_in_s3.append(item)
        else:
            csv_users_missing.append(item)
    
    print(f"📊 CSV users found in S3: {len(csv_users_in_s3)}/41")
    print(f"📊 CSV users missing from S3: {len(csv_users_missing)}/41")
    
    if csv_users_missing:
        print("❌ Missing users:")
        for user in csv_users_missing:
            print(f"   {user['distinct_id']}")
    
    return csv_users_in_s3, csv_users_missing

def check_events_in_s3(csv_data):
    """Count how many of the 41 CSV events are in S3 event data"""
    print("\n2️⃣ CHECKING: How many of the 41 events are in S3 data")
    print("=" * 60)
    
    # Get all S3 events by scanning all date directories
    s3_events = set()
    
    events_dir = Path("data/events")
    date_dirs = [d for d in events_dir.iterdir() if d.is_dir() and d.name.startswith('2025-07')]
    
    for date_dir in date_dirs:
        event_files = list(date_dir.glob("*.json")) + list(date_dir.glob("*.json.gz"))
        
        for event_file in event_files:
            try:
                if event_file.suffix == '.gz':
                    with gzip.open(event_file, 'rt') as f:
                        for line in f:
                            try:
                                event = json.loads(line.strip())
                                insert_id = event.get('insert_id')
                                if insert_id:
                                    s3_events.add(insert_id)
                            except json.JSONDecodeError:
                                continue
                else:
                    with open(event_file, 'r') as f:
                        for line in f:
                            try:
                                event = json.loads(line.strip())
                                insert_id = event.get('insert_id')
                                if insert_id:
                                    s3_events.add(insert_id)
                            except json.JSONDecodeError:
                                continue
            except Exception as e:
                print(f"❌ Error reading {event_file}: {e}")
    
    # Check CSV events
    csv_events_in_s3 = []
    csv_events_missing = []
    
    for item in csv_data:
        insert_id = item['insert_id']
        if insert_id in s3_events:
            csv_events_in_s3.append(item)
        else:
            csv_events_missing.append(item)
    
    print(f"📊 CSV events found in S3: {len(csv_events_in_s3)}/41")
    print(f"📊 CSV events missing from S3: {len(csv_events_missing)}/41")
    
    if csv_events_missing:
        print("❌ Missing events:")
        for event in csv_events_missing:
            print(f"   {event['insert_id']} → {event['distinct_id']}")
    
    return csv_events_in_s3, csv_events_missing

def check_complete_pairs(csv_users_in_s3, csv_events_in_s3):
    """Count how many of the 41 have BOTH user AND event in S3"""
    print("\n3️⃣ CHECKING: How many have BOTH user AND event in S3")
    print("=" * 60)
    
    # Create sets for easy lookup
    users_in_s3_ids = {item['distinct_id'] for item in csv_users_in_s3}
    events_in_s3_ids = {item['insert_id'] for item in csv_events_in_s3}
    
    complete_pairs = []
    incomplete_pairs = []
    
    # Check all 41 CSV entries
    csv_data = read_csv_data()
    for item in csv_data:
        user_in_s3 = item['distinct_id'] in users_in_s3_ids
        event_in_s3 = item['insert_id'] in events_in_s3_ids
        
        if user_in_s3 and event_in_s3:
            complete_pairs.append(item)
            print(f"✅ COMPLETE: {item['distinct_id']} ↔ {item['insert_id']}")
        else:
            incomplete_pairs.append(item)
            status = []
            if not user_in_s3:
                status.append("❌USER")
            if not event_in_s3:
                status.append("❌EVENT")
            print(f"❌ INCOMPLETE: {item['distinct_id']} ↔ {item['insert_id']} ({' + '.join(status)})")
    
    print(f"\n📊 Complete pairs (both user AND event in S3): {len(complete_pairs)}/41")
    print(f"📊 Incomplete pairs: {len(incomplete_pairs)}/41")
    
    return complete_pairs, incomplete_pairs

def main():
    print("🔍 PRECISE COUNT VERIFICATION")
    print("=" * 60)
    print("🎯 RESOLVING CONTRADICTORY INFORMATION")
    print()
    
    csv_data = read_csv_data()
    print(f"📊 Total CSV entries: {len(csv_data)}")
    print()
    
    # Check each requirement
    csv_users_in_s3, csv_users_missing = check_users_in_s3(csv_data)
    csv_events_in_s3, csv_events_missing = check_events_in_s3(csv_data)
    complete_pairs, incomplete_pairs = check_complete_pairs(csv_users_in_s3, csv_events_in_s3)
    
    print(f"\n🎯 FINAL PRECISE COUNTS")
    print("=" * 60)
    print(f"1️⃣ Users in S3: {len(csv_users_in_s3)}/41")
    print(f"2️⃣ Events in S3: {len(csv_events_in_s3)}/41") 
    print(f"3️⃣ Complete pairs (both user AND event): {len(complete_pairs)}/41")
    
    print(f"\n🔍 BREAKDOWN ANALYSIS:")
    print(f"   Missing users only: {len([p for p in incomplete_pairs if p['distinct_id'] not in {u['distinct_id'] for u in csv_users_in_s3} and p['insert_id'] in {e['insert_id'] for e in csv_events_in_s3}])}")
    print(f"   Missing events only: {len([p for p in incomplete_pairs if p['distinct_id'] in {u['distinct_id'] for u in csv_users_in_s3} and p['insert_id'] not in {e['insert_id'] for e in csv_events_in_s3}])}")
    print(f"   Missing both: {len([p for p in incomplete_pairs if p['distinct_id'] not in {u['distinct_id'] for u in csv_users_in_s3} and p['insert_id'] not in {e['insert_id'] for e in csv_events_in_s3}])}")

if __name__ == "__main__":
    main() 