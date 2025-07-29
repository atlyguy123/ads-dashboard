#!/usr/bin/env python3
"""
🕵️ COMPREHENSIVE JULY 21-22 DIAGNOSTIC
Business critical investigation to resolve the last missing events
"""

import sqlite3
import csv
import json
import gzip
from pathlib import Path
import sys
from collections import defaultdict

# Add utils directory to path for database utilities
utils_path = str(Path(__file__).resolve().parent / "utils")
sys.path.append(utils_path)
from database_utils import get_database_path

def main():
    print("🕵️ COMPREHENSIVE JULY 21-22 DIAGNOSTIC")
    print("=" * 60)
    
    # 1. Load CSV events for July 21-22
    csv_july_21_22_events = []
    with open('mixpanel_user.csv', 'r') as f:
        reader = csv.DictReader(f)
        for row in reader:
            event_date = row['Time'][:10]  # Extract YYYY-MM-DD
            if event_date in ['2025-07-21', '2025-07-22']:
                csv_july_21_22_events.append({
                    'insert_id': row['Insert ID'],
                    'time': row['Time'],
                    'distinct_id': row['Distinct ID'],
                    'date': event_date
                })
    
    print(f"📋 CSV Events for July 21-22: {len(csv_july_21_22_events)}")
    for event in csv_july_21_22_events:
        print(f"   {event['date']}: {event['insert_id']} ({event['distinct_id']})")
    
    # 2. Check S3 files for these specific insert IDs
    print(f"\n🔍 CHECKING S3 FILES...")
    s3_findings = defaultdict(list)
    
    for date in ['2025-07-21', '2025-07-22']:
        date_dir = Path(f"data/events/{date}")
        if date_dir.exists():
            for gz_file in date_dir.glob("*.json.gz"):
                print(f"   📂 Analyzing {gz_file.name}...")
                try:
                    with gzip.open(gz_file, 'rt') as f:
                        for line_num, line in enumerate(f, 1):
                            try:
                                event = json.loads(line.strip())
                                
                                # Check if this event matches any of our CSV insert IDs
                                for csv_event in csv_july_21_22_events:
                                    if csv_event['insert_id'] in line:
                                        s3_findings[csv_event['insert_id']].append({
                                            'file': gz_file.name,
                                            'line': line_num,
                                            'event_name': event.get('event', 'unknown'),
                                            'distinct_id': event.get('distinct_id', 'unknown'),
                                            'time': event.get('time', 'unknown')
                                        })
                                        
                            except json.JSONDecodeError:
                                continue
                                
                except Exception as e:
                    print(f"      ❌ Error reading {gz_file}: {e}")
        else:
            print(f"   ❌ Directory {date_dir} does not exist")
    
    print(f"\n📊 S3 FINDINGS:")
    if s3_findings:
        for insert_id, findings in s3_findings.items():
            print(f"   ✅ {insert_id}: Found {len(findings)} times")
            for finding in findings:
                print(f"      📁 {finding['file']}, line {finding['line']}")
                print(f"         Event: {finding['event_name']}, ID: {finding['distinct_id']}")
    else:
        print("   ❌ NO CSV insert IDs found in S3 files!")
    
    # 3. Check what RC Trial started events DO exist in S3 for July 21-22
    print(f"\n🎯 ALL RC TRIAL STARTED EVENTS IN S3 (July 21-22):")
    trial_events = []
    
    for date in ['2025-07-21', '2025-07-22']:
        date_dir = Path(f"data/events/{date}")
        if date_dir.exists():
            for gz_file in date_dir.glob("*.json.gz"):
                try:
                    with gzip.open(gz_file, 'rt') as f:
                        for line in f:
                            try:
                                event = json.loads(line.strip())
                                if event.get('event') == 'RC Trial started':
                                    trial_events.append({
                                        'date': date,
                                        'file': gz_file.name,
                                        'insert_id': event.get('insert_id', 'NO_INSERT_ID'),
                                        'distinct_id': event.get('distinct_id', 'unknown'),
                                        'time': event.get('time', 'unknown'),
                                        'campaign_id': event.get('properties', {}).get('abi_campaign_id', 'NO_CAMPAIGN')
                                    })
                            except json.JSONDecodeError:
                                continue
                except Exception as e:
                    print(f"      ❌ Error reading {gz_file}: {e}")
    
    # Filter for our specific campaign
    campaign_trial_events = [e for e in trial_events if e['campaign_id'] == '120223331225260178']
    
    print(f"   📊 Total RC Trial started events: {len(trial_events)}")
    print(f"   🎯 Our campaign events: {len(campaign_trial_events)}")
    
    if campaign_trial_events:
        print(f"   📋 Our campaign events details:")
        for event in campaign_trial_events:
            print(f"      {event['date']}: {event['insert_id']} ({event['distinct_id']})")
    
    # 4. Check Raw Database
    print(f"\n🗄️ CHECKING RAW DATABASE...")
    try:
        raw_db_path = get_database_path("raw_data.db")
        with sqlite3.connect(raw_db_path) as conn:
            cursor = conn.cursor()
            
            # Check for our specific insert IDs
            for csv_event in csv_july_21_22_events:
                cursor.execute("""
                    SELECT COUNT(*) FROM raw_event_data 
                    WHERE event_data LIKE ?
                """, [f'%{csv_event["insert_id"]}%'])
                count = cursor.fetchone()[0]
                
                if count > 0:
                    print(f"   ✅ Found {csv_event['insert_id']} in raw DB")
                else:
                    print(f"   ❌ Missing {csv_event['insert_id']} from raw DB")
                    
    except Exception as e:
        print(f"   ❌ Error checking raw database: {e}")
    
    # 5. Check Processed Database
    print(f"\n🎯 CHECKING PROCESSED DATABASE...")
    try:
        processed_db_path = get_database_path("mixpanel_data.db")
        with sqlite3.connect(processed_db_path) as conn:
            cursor = conn.cursor()
            
            # Check for July 21-22 RC Trial started events
            cursor.execute("""
                SELECT COUNT(*) FROM mixpanel_event 
                WHERE event_name = 'RC Trial started'
                AND DATE(event_time) IN ('2025-07-21', '2025-07-22')
            """)
            trial_count = cursor.fetchone()[0]
            print(f"   📊 RC Trial started events July 21-22: {trial_count}")
            
            # Check for our specific insert IDs in event_json
            for csv_event in csv_july_21_22_events:
                cursor.execute("""
                    SELECT COUNT(*) FROM mixpanel_event 
                    WHERE event_json LIKE ?
                """, [f'%{csv_event["insert_id"]}%'])
                count = cursor.fetchone()[0]
                
                if count > 0:
                    print(f"   ✅ Found {csv_event['insert_id']} in processed DB")
                else:
                    print(f"   ❌ Missing {csv_event['insert_id']} from processed DB")
                    
    except Exception as e:
        print(f"   ❌ Error checking processed database: {e}")
    
    print(f"\n🎯 DIAGNOSTIC COMPLETE")
    print("If CSV insert IDs are missing from S3, this suggests either:")
    print("1. The CSV data is from a different time period than our S3 data")
    print("2. There's a fundamental mismatch in event identification")
    print("3. The events are in S3 but with different insert IDs than expected")

if __name__ == "__main__":
    main() 