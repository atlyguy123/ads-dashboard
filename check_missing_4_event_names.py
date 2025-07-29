#!/usr/bin/env python3
"""
🔍 CHECK ACTUAL EVENT NAMES OF 4 MISSING EVENTS
Investigate what event names these events have in raw database
"""
import sqlite3
import json
import sys
from pathlib import Path

# Add utils to path
utils_path = str(Path(__file__).resolve().parent / "utils")
sys.path.append(utils_path)
from database_utils import get_database_path

def investigate_missing_events():
    """Check the actual event names of the 4 missing events"""
    print("🔍 INVESTIGATING 4 MISSING EVENTS IN RAW DATABASE")
    print("=" * 60)
    
    missing_events = [
        "100b325f-a7ca-4a9f-88c8-4f570e05598d",  # 2025-07-21
        "534ce39d-8fbd-4586-8010-113e8d4898db",  # 2025-07-24  
        "a1ee4830-36b9-4363-a470-56ecb392e638",  # 2025-07-17
        "c51b7896-6686-4e4c-a1a5-d4a43f0e136f"   # 2025-07-23
    ]
    
    raw_db_path = get_database_path("raw_data")
    conn = sqlite3.connect(raw_db_path)
    cursor = conn.cursor()
    
    print("📋 IMPORTANT_EVENTS in pipeline:")
    important_events = {
        "RC Trial started", 
        "RC Trial converted", 
        "RC Cancellation", 
        "RC Initial purchase", 
        "RC Trial cancelled", 
        "RC Renewal"
    }
    for event in important_events:
        print(f"   ✅ '{event}'")
    
    print(f"\n🔍 CHECKING EACH MISSING EVENT:")
    print("-" * 60)
    
    for insert_id in missing_events:
        print(f"\n📊 EVENT: {insert_id}")
        print("=" * 40)
        
        # Find the event in raw database
        cursor.execute("SELECT date_day, event_data FROM raw_event_data WHERE event_data LIKE ?", [f'%{insert_id}%'])
        result = cursor.fetchone()
        
        if not result:
            print("❌ NOT FOUND in raw database!")
            continue
            
        date_day, event_data_str = result
        event_data = json.loads(event_data_str)
        
        # Extract event name using both possible fields
        event_name = event_data.get('event') or event_data.get('event_name')
        distinct_id = event_data.get('distinct_id', 'UNKNOWN')
        event_time = event_data.get('time', 'UNKNOWN')
        
        print(f"📅 Date: {date_day}")
        print(f"👤 User: {distinct_id}")  
        print(f"🕐 Time: {event_time}")
        print(f"🏷️  Event Name: '{event_name}'")
        
        # Check if it's in IMPORTANT_EVENTS
        if event_name in important_events:
            print(f"✅ SHOULD BE PROCESSED: Event name is in IMPORTANT_EVENTS")
            print(f"🚨 CRITICAL: Event exists, is important, but missing from processed DB!")
        else:
            print(f"❌ FILTERED OUT: Event name '{event_name}' NOT in IMPORTANT_EVENTS")
            print(f"🔍 This explains why it was skipped during processing")
        
        # Show additional event properties for debugging
        properties = event_data.get('properties', {})
        if properties:
            print(f"🔧 Properties keys: {list(properties.keys())[:5]}...")  # Show first 5 keys
    
    conn.close()
    
    print(f"\n🎯 SUMMARY")
    print("=" * 60)
    print(f"📊 Total missing events investigated: {len(missing_events)}")
    print(f"📋 All events should be 'RC Trial started' based on CSV")
    print(f"🔍 If any have different event names, that's the root cause!")

if __name__ == "__main__":
    investigate_missing_events() 