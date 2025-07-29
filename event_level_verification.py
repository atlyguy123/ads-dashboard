#!/usr/bin/env python3
"""
Event-Level Verification Script

Phase 2: Precise event-level verification using Insert ID and event timestamps
from the updated CSV to check if specific RC Trial started events exist in our pipeline.
"""

import sqlite3
import csv
from pathlib import Path
import sys
from datetime import datetime
import json

# Add utils directory to path for database utilities
utils_path = str(Path(__file__).resolve().parent / "utils")
sys.path.append(utils_path)
from database_utils import get_database_path

def main():
    """Main event-level verification process"""
    
    print("🔬 EVENT-LEVEL VERIFICATION WITH FRESH DATA")
    print("=" * 60)
    print("July 16-29, 2025 timeframe")
    print("Using Insert IDs and exact timestamps for precise matching")
    print()
    
    # Read the updated CSV with event details
    csv_events = read_csv_events()
    if not csv_events:
        return 1
    
    print(f"📊 Loaded {len(csv_events)} RC Trial started events from Mixpanel CSV")
    print()
    
    # Phase 1: Check raw event database
    raw_results = check_raw_event_database(csv_events)
    
    print()
    
    # Phase 2: Check processed event database  
    processed_results = check_processed_event_database(csv_events)
    
    print()
    
    # Phase 3: Comprehensive analysis
    analyze_pipeline_effectiveness(csv_events, raw_results, processed_results)
    
    return 0

def read_csv_events():
    """Read event details from the updated CSV"""
    events = []
    
    try:
        with open('mixpanel_user.csv', 'r') as file:
            csv_reader = csv.DictReader(file)
            for row in csv_reader:
                if all(key in row for key in ['Insert ID', 'Time', 'Distinct ID']):
                    events.append({
                        'insert_id': row['Insert ID'].strip(),
                        'timestamp': row['Time'].strip(),
                        'distinct_id': row['Distinct ID'].strip(),
                        'campaign_id': row['abi_~campaign_id'].strip(),
                        'campaign_name': row['abi_~campaign'].strip(),
                        'user_id': row.get('User ID', '').strip()
                    })
        
        print(f"✅ Loaded {len(events)} events with complete data")
        
        # Show sample event for verification
        if events:
            sample = events[0]
            print(f"📋 Sample event:")
            print(f"   Insert ID: {sample['insert_id']}")
            print(f"   Timestamp: {sample['timestamp']}")
            print(f"   Distinct ID: {sample['distinct_id'][:30]}...")
            print(f"   Campaign: {sample['campaign_name'][:40]}...")
        
        return events
        
    except Exception as e:
        print(f"❌ Error reading CSV: {e}")
        return []

def check_raw_event_database(csv_events):
    """Check if the specific events exist in raw_event_data"""
    print("1️⃣ CHECKING RAW EVENT DATABASE...")
    
    found_events = []
    
    try:
        with sqlite3.connect(get_database_path('raw_data')) as conn:
            cursor = conn.cursor()
            
            print(f"   🔍 Searching for {len(csv_events)} specific events...")
            
            for i, event in enumerate(csv_events, 1):
                # Search by multiple criteria for precision
                found = check_event_in_raw_db(cursor, event)
                
                if found:
                    found_events.append({**event, **found})
                    print(f"   ✅ {i:2d}/{len(csv_events)}: Found event {event['insert_id'][:8]}...")
                else:
                    print(f"   ❌ {i:2d}/{len(csv_events)}: Missing event {event['insert_id'][:8]}...")
        
        success_rate = (len(found_events) / len(csv_events) * 100) if csv_events else 0
        print(f"   📊 Raw DB Results: {len(found_events)}/{len(csv_events)} events found ({success_rate:.1f}%)")
        
        return found_events
        
    except Exception as e:
        print(f"   ❌ Error checking raw DB: {e}")
        return []

def check_event_in_raw_db(cursor, event):
    """Check if a specific event exists in raw database using multiple criteria"""
    
    # Method 1: Search by insert_id in event_data JSON
    cursor.execute("""
        SELECT event_data FROM raw_event_data 
        WHERE event_data LIKE ? 
        LIMIT 1
    """, [f'%{event["insert_id"]}%'])
    
    result = cursor.fetchone()
    if result:
        try:
            event_data = json.loads(result[0])
            if (event_data.get('properties', {}).get('$insert_id') == event['insert_id'] and
                event_data.get('distinct_id') == event['distinct_id']):
                return {
                    'found_by': 'insert_id',
                    'raw_data': event_data
                }
        except json.JSONDecodeError:
            pass
    
    # Method 2: Search by distinct_id and event name near timestamp
    # Convert timestamp to date for broader matching
    try:
        event_date = event['timestamp'][:10]  # Extract YYYY-MM-DD
        
        cursor.execute("""
            SELECT event_data FROM raw_event_data 
            WHERE distinct_id = ? 
            AND event_data LIKE '%RC Trial started%'
            AND event_data LIKE ?
            LIMIT 5
        """, [event['distinct_id'], f'%{event_date}%'])
        
        results = cursor.fetchall()
        for result in results:
            try:
                event_data = json.loads(result[0])
                if event_data.get('event') == 'RC Trial started':
                    return {
                        'found_by': 'distinct_id_and_date',
                        'raw_data': event_data
                    }
            except json.JSONDecodeError:
                continue
                
    except Exception:
        pass
    
    return None

def check_processed_event_database(csv_events):
    """Check if the specific events exist in processed mixpanel_event table"""
    print("2️⃣ CHECKING PROCESSED EVENT DATABASE...")
    
    found_events = []
    
    try:
        with sqlite3.connect(get_database_path('mixpanel_data')) as conn:
            cursor = conn.cursor()
            
            print(f"   🔍 Searching for {len(csv_events)} specific events...")
            
            for i, event in enumerate(csv_events, 1):
                found = check_event_in_processed_db(cursor, event)
                
                if found:
                    found_events.append({**event, **found})
                    print(f"   ✅ {i:2d}/{len(csv_events)}: Found event {event['insert_id'][:8]}...")
                else:
                    print(f"   ❌ {i:2d}/{len(csv_events)}: Missing event {event['insert_id'][:8]}...")
        
        success_rate = (len(found_events) / len(csv_events) * 100) if csv_events else 0
        print(f"   📊 Processed DB Results: {len(found_events)}/{len(csv_events)} events found ({success_rate:.1f}%)")
        
        return found_events
        
    except Exception as e:
        print(f"   ❌ Error checking processed DB: {e}")
        return []

def check_event_in_processed_db(cursor, event):
    """Check if a specific event exists in processed database"""
    
    # Method 1: Search by insert_id if available in event_json
    cursor.execute("""
        SELECT * FROM mixpanel_event 
        WHERE distinct_id = ? 
        AND event_name = 'RC Trial started'
        AND event_json LIKE ?
        LIMIT 1
    """, [event['distinct_id'], f'%{event["insert_id"]}%'])
    
    result = cursor.fetchone()
    if result:
        return {
            'found_by': 'insert_id_in_event_json',
            'processed_data': {
                'event_uuid': result[0],
                'event_name': result[1],
                'abi_ad_id': result[2],
                'abi_campaign_id': result[3],
                'abi_ad_set_id': result[4],
                'distinct_id': result[5],
                'event_time': result[6],
                'country': result[7],
                'region': result[8],
                'event_json': result[15] if len(result) > 15 else None
            }
        }
    
    # Method 2: Search by distinct_id and timestamp proximity
    try:
        # Convert Mixpanel timestamp to search range
        event_datetime = datetime.fromisoformat(event['timestamp'].replace('Z', '+00:00'))
        event_date = event_datetime.strftime('%Y-%m-%d')
        
        cursor.execute("""
            SELECT * FROM mixpanel_event 
            WHERE distinct_id = ? 
            AND event_name = 'RC Trial started'
            AND DATE(event_time) = ?
            ORDER BY ABS(
                strftime('%s', event_time) - strftime('%s', ?)
            )
            LIMIT 1
        """, [event['distinct_id'], event_date, event['timestamp']])
        
        result = cursor.fetchone()
        if result:
            return {
                'found_by': 'timestamp_proximity',
                'processed_data': {
                    'event_uuid': result[0],
                    'event_name': result[1],
                    'abi_ad_id': result[2],
                    'abi_campaign_id': result[3],
                    'abi_ad_set_id': result[4],
                    'distinct_id': result[5],
                    'event_time': result[6],
                    'country': result[7],
                    'region': result[8],
                    'event_json': result[15] if len(result) > 15 else None
                }
            }
            
    except Exception:
        pass
    
    return None

def analyze_pipeline_effectiveness(csv_events, raw_results, processed_results):
    """Analyze the effectiveness of the entire pipeline"""
    print("3️⃣ PIPELINE EFFECTIVENESS ANALYSIS...")
    
    total_events = len(csv_events)
    raw_found = len(raw_results)
    processed_found = len(processed_results)
    
    print(f"\n📊 PIPELINE PERFORMANCE:")
    print(f"   🎯 Total Mixpanel events: {total_events}")
    print(f"   📥 Found in Raw DB: {raw_found} ({raw_found/total_events*100:.1f}%)")
    print(f"   ⚡ Found in Processed DB: {processed_found} ({processed_found/total_events*100:.1f}%)")
    
    # Pipeline efficiency calculations
    if raw_found > 0:
        processing_efficiency = (processed_found / raw_found * 100)
        print(f"   🔄 Raw→Processed efficiency: {processing_efficiency:.1f}%")
    
    overall_efficiency = (processed_found / total_events * 100)
    print(f"   🏆 Overall pipeline efficiency: {overall_efficiency:.1f}%")
    
    # Identify failure points
    print(f"\n🔍 FAILURE POINT ANALYSIS:")
    download_failures = total_events - raw_found
    processing_failures = raw_found - processed_found
    
    if download_failures > 0:
        print(f"   📥 Download stage failures: {download_failures} events")
        print(f"      • Events missing from S3→Raw pipeline")
        
    if processing_failures > 0:
        print(f"   ⚙️  Processing stage failures: {processing_failures} events")
        print(f"      • Events lost during Raw→Processed ingestion")
    
    # Success analysis
    if processed_found == total_events:
        print(f"\n🎉 PERFECT PIPELINE: All {total_events} events successfully processed!")
    elif processed_found >= total_events * 0.95:
        print(f"\n✅ EXCELLENT PIPELINE: {overall_efficiency:.1f}% success rate")
    elif processed_found >= total_events * 0.8:
        print(f"\n⚠️  GOOD PIPELINE: {overall_efficiency:.1f}% success rate (room for improvement)")
    else:
        print(f"\n🚨 PIPELINE ISSUES: Only {overall_efficiency:.1f}% success rate")
    
    # Show sample of missing events
    missing_events = []
    for event in csv_events:
        found_in_processed = any(p['distinct_id'] == event['distinct_id'] for p in processed_results)
        if not found_in_processed:
            missing_events.append(event)
    
    if missing_events:
        print(f"\n❌ SAMPLE MISSING EVENTS:")
        for i, event in enumerate(missing_events[:3], 1):
            print(f"   {i}. Insert ID: {event['insert_id']}")
            print(f"      Timestamp: {event['timestamp']}")
            print(f"      Distinct ID: {event['distinct_id'][:30]}...")
    
    # Recommendations
    print(f"\n💡 RECOMMENDATIONS:")
    if download_failures > 0:
        print(f"   🔧 Check S3 download and raw ingestion pipeline")
        print(f"   🔧 Verify event data is being stored correctly in raw_event_data")
        
    if processing_failures > 0:
        print(f"   🔧 Check event filtering logic in processing pipeline")
        print(f"   🔧 Verify event deduplication is not removing valid events")
        
    if overall_efficiency < 90:
        print(f"   🔧 Consider implementing event-level reconciliation")
        print(f"   🔧 Add monitoring for specific event types (RC Trial started)")

if __name__ == "__main__":
    exit(main()) 