#!/usr/bin/env python3
"""
🔍 VALIDATE EVENTS IN DATE DIRECTORIES
Check each CSV event in the date-organized event files
"""

import json
import csv
import gzip
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
                'date': row['Time'][:10]  # Extract date from ISO timestamp
            })
    
    print(f"📊 CSV contains {len(csv_data)} rows")
    return csv_data

def check_events_in_date_files(csv_data):
    """Check each CSV event in date-organized files"""
    print("\n🔍 CHECKING EVENTS IN DATE-ORGANIZED FILES")
    print("=" * 60)
    
    # Group CSV events by date
    events_by_date = {}
    for item in csv_data:
        date = item['date']
        if date not in events_by_date:
            events_by_date[date] = []
        events_by_date[date].append(item)
    
    print(f"📊 Events distributed across {len(events_by_date)} dates:")
    for date, events in events_by_date.items():
        print(f"  {date}: {len(events)} events")
    
    found_events = []
    missing_events = []
    
    # Check each date
    for date, csv_events in events_by_date.items():
        print(f"\n📅 CHECKING DATE: {date}")
        print("-" * 40)
        
        # Look for event directory
        event_dir = Path(f"data/events/{date}")
        if not event_dir.exists():
            print(f"❌ Event directory {event_dir} not found")
            missing_events.extend(csv_events)
            continue
        
        # Get all event files for this date
        event_files = list(event_dir.glob("*.json")) + list(event_dir.glob("*.json.gz"))
        print(f"📁 Found {len(event_files)} event files")
        
        # Build set of all insert_ids for this date
        date_insert_ids = set()
        
        for event_file in event_files:
            try:
                if event_file.suffix == '.gz':
                    with gzip.open(event_file, 'rt') as f:
                        for line in f:
                            try:
                                event = json.loads(line.strip())
                                insert_id = event.get('insert_id')
                                if insert_id:
                                    date_insert_ids.add(insert_id)
                            except json.JSONDecodeError:
                                continue
                else:
                    with open(event_file, 'r') as f:
                        for line in f:
                            try:
                                event = json.loads(line.strip())
                                insert_id = event.get('insert_id')
                                if insert_id:
                                    date_insert_ids.add(insert_id)
                            except json.JSONDecodeError:
                                continue
            except Exception as e:
                print(f"❌ Error reading {event_file}: {e}")
        
        print(f"📊 Total insert_ids found in {date}: {len(date_insert_ids)}")
        
        # Check each CSV event for this date
        for csv_event in csv_events:
            insert_id = csv_event['insert_id']
            if insert_id in date_insert_ids:
                found_events.append(csv_event)
                print(f"✅ FOUND: {insert_id} (User: {csv_event['distinct_id']})")
            else:
                missing_events.append(csv_event)
                print(f"❌ MISSING: {insert_id} (User: {csv_event['distinct_id']})")
    
    print(f"\n📊 FINAL EVENT VERIFICATION SUMMARY:")
    print(f"✅ Found: {len(found_events)}/{len(csv_data)} events")
    print(f"❌ Missing: {len(missing_events)}/{len(csv_data)} events")
    
    return found_events, missing_events

def main():
    print("🔍 VALIDATE EVENTS IN DATE DIRECTORIES")
    print("=" * 60)
    print("🚨 CHECKING ACTUAL EVENT FILES")
    print()
    
    # Read CSV
    csv_data = read_csv_data()
    
    # Check events
    found_events, missing_events = check_events_in_date_files(csv_data)
    
    print(f"\n🎯 VALIDATED FACTS")
    print("=" * 60)
    print(f"📊 CSV events: {len(csv_data)}")
    print(f"📊 Events found: {len(found_events)}")
    print(f"📊 Events missing: {len(missing_events)}")
    
    if missing_events:
        print(f"\n❌ MISSING EVENTS:")
        for event in missing_events:
            print(f"   {event['insert_id']} → {event['distinct_id']} ({event['date']})")

if __name__ == "__main__":
    main() 