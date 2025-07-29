#!/usr/bin/env python3
"""
🔍 COMPREHENSIVE INSERT ID ANALYSIS WITH JSON EXPORT
Check all 41 CSV Insert IDs against original unfiltered S3 data (July 16-29, 2025)
Export complete JSON event data for manual review
"""

import csv
import json
import gzip
from pathlib import Path
from collections import defaultdict, Counter

def main():
    print("🔍 COMPREHENSIVE INSERT ID ANALYSIS WITH JSON EXPORT")
    print("=" * 60)
    print("Checking all 41 CSV Insert IDs against original S3 data")
    print("Date range: July 16-29, 2025")
    print("Exporting complete JSON events to: found_events_complete.jsonl")
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
    
    # Prepare JSON export file
    json_export_file = open('found_events_complete.jsonl', 'w')
    
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
                                
                                # Export complete JSON to file
                                export_data = {
                                    'insert_id': event_insert_id,
                                    'found_in_date': date_str,
                                    'found_in_file': gz_file.name,
                                    'csv_time': next((item['time'] for item in csv_insert_ids if item['insert_id'] == event_insert_id), 'UNKNOWN'),
                                    'csv_distinct_id': next((item['distinct_id'] for item in csv_insert_ids if item['insert_id'] == event_insert_id), 'UNKNOWN'),
                                    'complete_event_json': event
                                }
                                json_export_file.write(json.dumps(export_data) + '\n')
                                json_export_file.flush()  # Ensure immediate write
                                
                                missing_insert_ids.remove(event_insert_id)
                                event_type_counts[event_name] += 1
                                daily_breakdown[date_str].append({
                                    'insert_id': event_insert_id,
                                    'event_name': event_name,
                                    'distinct_id': distinct_id
                                })
                                
                                print(f"      ✅ FOUND & EXPORTED: {event_insert_id} -> Event: '{event_name}'")
                        
                        except json.JSONDecodeError:
                            continue
                    
                    print(f"      📊 Processed {line_count:,} events")
                    
            except Exception as e:
                print(f"   ❌ Error processing {gz_file}: {e}")
        
        print()
    
    # Close the export file
    json_export_file.close()
    
    # Generate comprehensive report
    print("🎯 FINAL ANALYSIS RESULTS")
    print("=" * 60)
    
    print(f"📊 SUMMARY:")
    print(f"   Total CSV Insert IDs: {len(csv_insert_ids)}")
    print(f"   Found in S3 data: {len(found_events)}")
    print(f"   Missing from S3 data: {len(missing_insert_ids)}")
    print(f"   Success rate: {len(found_events)/len(csv_insert_ids)*100:.1f}%")
    print(f"   Complete JSON export: found_events_complete.jsonl ({len(found_events)} events)")
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
    
    print(f"🔍 JSON EXPORT COMPLETE:")
    print(f"   File: found_events_complete.jsonl")
    print(f"   Format: One JSON object per line")
    print(f"   Contents: CSV metadata + complete S3 event JSON")
    print(f"   Total events exported: {len(found_events)}")
    print()
    print(f"📖 To review the JSON data:")
    print(f"   cat found_events_complete.jsonl | jq '.'")
    print(f"   cat found_events_complete.jsonl | jq '.complete_event_json | keys'")
    print(f"   cat found_events_complete.jsonl | jq '.complete_event_json.properties | keys'")

if __name__ == "__main__":
    main() 