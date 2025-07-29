#!/usr/bin/env python3
"""
🔍 COMPREHENSIVE INSERT ID ANALYSIS
Check all 41 CSV Insert IDs against original unfiltered S3 data (July 16-29, 2025)
Categorize event types and provide complete breakdown
"""

import csv
import json
import gzip
from pathlib import Path
from collections import defaultdict, Counter

def main():
    print("🔍 COMPREHENSIVE INSERT ID ANALYSIS")
    print("=" * 60)
    print("Checking all 41 CSV Insert IDs against original S3 data")
    print("Date range: July 16-29, 2025")
    print()
    
    # Load all CSV Insert IDs
    csv_insert_ids = []
    with open('mixpanel_user.csv', 'r') as f:
        reader = csv.DictReader(f)
        for row in reader:
            csv_insert_ids.append({
                'insert_id': row['Insert ID'],
                'time': row['Time'],
                'distinct_id': row['Distinct ID'],
                'campaign': row['abi_~campaign_id']
            })
    
    print(f"📋 Total CSV Insert IDs to verify: {len(csv_insert_ids)}")
    print()
    
    # Track results
    found_events = {}
    missing_insert_ids = set(item['insert_id'] for item in csv_insert_ids)
    event_type_counts = Counter()
    daily_breakdown = defaultdict(list)
    
    # Check each date directory for .json.gz files
    event_dirs = sorted([d for d in Path('data/events').glob('2025-07-*') if d.is_dir()])
    
    for date_dir in event_dirs:
        date_str = date_dir.name
        print(f"📁 Checking {date_str}...")
        
        # Find .json.gz files in this date directory
        gz_files = list(date_dir.glob('*.json.gz'))
        
        if not gz_files:
            print(f"   ⚠️  No .json.gz files found")
            continue
            
        for gz_file in gz_files:
            print(f"   📄 Processing {gz_file.name}...")
            
            try:
                with gzip.open(gz_file, 'rt') as f:
                    line_count = 0
                    for line in f:
                        line_count += 1
                        try:
                            event = json.loads(line.strip())
                            
                            # Check if this event's insert_id matches any from CSV
                            event_insert_id = event.get('insert_id')
                            if event_insert_id in missing_insert_ids:
                                # Found a match!
                                event_name = event.get('event', 'NULL')
                                distinct_id = event.get('distinct_id', 'UNKNOWN')
                                
                                found_events[event_insert_id] = {
                                    'event_name': event_name,
                                    'distinct_id': distinct_id,
                                    'date': date_str,
                                    'file': gz_file.name,
                                    'full_event': event
                                }
                                
                                missing_insert_ids.remove(event_insert_id)
                                event_type_counts[event_name] += 1
                                daily_breakdown[date_str].append({
                                    'insert_id': event_insert_id,
                                    'event_name': event_name,
                                    'distinct_id': distinct_id
                                })
                                
                                print(f"      ✅ FOUND: {event_insert_id} -> Event: '{event_name}'")
                        
                        except json.JSONDecodeError:
                            continue
                    
                    print(f"      📊 Processed {line_count:,} events")
                    
            except Exception as e:
                print(f"   ❌ Error processing {gz_file}: {e}")
        
        print()
    
    # Generate comprehensive report
    print("🎯 FINAL ANALYSIS RESULTS")
    print("=" * 60)
    
    print(f"📊 SUMMARY:")
    print(f"   Total CSV Insert IDs: {len(csv_insert_ids)}")
    print(f"   Found in S3 data: {len(found_events)}")
    print(f"   Missing from S3 data: {len(missing_insert_ids)}")
    print(f"   Success rate: {len(found_events)/len(csv_insert_ids)*100:.1f}%")
    print()
    
    print(f"📈 EVENT TYPE BREAKDOWN:")
    for event_type, count in event_type_counts.most_common():
        print(f"   '{event_type}': {count} events")
    print()
    
    print(f"📅 DAILY BREAKDOWN:")
    for date_str in sorted(daily_breakdown.keys()):
        events = daily_breakdown[date_str]
        print(f"   {date_str}: {len(events)} events found")
        for event in events:
            print(f"      - {event['insert_id'][:8]}... -> '{event['event_name']}' ({event['distinct_id'][:10]}...)")
    print()
    
    if missing_insert_ids:
        print(f"❌ MISSING INSERT IDs ({len(missing_insert_ids)}):")
        for missing_id in sorted(missing_insert_ids):
            # Find corresponding CSV info
            csv_info = next((item for item in csv_insert_ids if item['insert_id'] == missing_id), None)
            if csv_info:
                print(f"   {missing_id} (Time: {csv_info['time']}, User: {csv_info['distinct_id'][:10]}...)")
        print()
    
    print(f"🔍 DETAILED EVENT ANALYSIS:")
    for insert_id, event_data in found_events.items():
        print(f"   {insert_id}:")
        print(f"      Event: '{event_data['event_name']}'")
        print(f"      User: {event_data['distinct_id']}")
        print(f"      Date: {event_data['date']}")
        print(f"      File: {event_data['file']}")
        
        # Show key properties if available
        props = event_data['full_event'].get('properties', {})
        if isinstance(props, dict):
            key_props = {}
            for key in ['abi_campaign_id', '$os', 'rc_trial_started_at', 'product_identifier']:
                if key in props:
                    key_props[key] = props[key]
            if key_props:
                print(f"      Key Props: {key_props}")
        print()

if __name__ == "__main__":
    main() 