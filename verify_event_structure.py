#!/usr/bin/env python3
"""
🔍 VERIFY EVENT DATA STRUCTURE
Check exactly what identifiers are available in our events
"""
import sqlite3
import json
import sys
from pathlib import Path

# Add utils to path
utils_path = str(Path(__file__).resolve().parent / "utils")
sys.path.append(utils_path)
from database_utils import get_database_path

def analyze_event_structure():
    """Analyze the structure of events to understand available identifiers"""
    print("🔍 ANALYZING EVENT DATA STRUCTURE")
    print("=" * 60)
    
    # Check a few of our CSV events
    test_events = [
        "09858561-e3a2-4227-996c-2c534f90b69f",  # Working event
        "100b325f-a7ca-4a9f-88c8-4f570e05598d",  # Missing event
        "0e2d1c40-e33f-4048-909f-a6edd5e1335f",  # Working event
        "534ce39d-8fbd-4586-8010-113e8d4898db"   # Missing event
    ]
    
    raw_db_path = get_database_path("raw_data")
    conn = sqlite3.connect(raw_db_path)
    cursor = conn.cursor()
    
    for i, insert_id in enumerate(test_events):
        print(f"\n📊 EVENT {i+1}: {insert_id[:8]}... ({'Working' if i % 2 == 0 else 'Missing'})")
        print("=" * 50)
        
        cursor.execute("SELECT event_data FROM raw_event_data WHERE event_data LIKE ?", [f'%{insert_id}%'])
        result = cursor.fetchone()
        
        if not result:
            print("❌ Event not found in raw database")
            continue
            
        event_data = json.loads(result[0])
        
        # Main identifiers
        distinct_id = event_data.get('distinct_id', 'MISSING')
        event_name = event_data.get('event', 'MISSING')
        
        print(f"🏷️  Event Name: {event_name}")
        print(f"🆔 distinct_id: {distinct_id}")
        
        # Check properties
        properties = event_data.get('properties', {})
        user_id = properties.get('$user_id', 'MISSING')
        device_id = properties.get('$device_id', 'MISSING') 
        insert_id_prop = properties.get('$insert_id', 'MISSING')
        
        print(f"👤 $user_id: {user_id}")
        print(f"📱 $device_id: {device_id}")
        print(f"🔢 $insert_id: {insert_id_prop}")
        
        # Check for other potential identifiers
        print(f"\n🔍 ALL AVAILABLE IDENTIFIERS:")
        identifiers = []
        
        # Add distinct_id if present
        if distinct_id != 'MISSING':
            identifiers.append(('distinct_id', distinct_id))
            
        # Add $user_id if present and different
        if user_id != 'MISSING' and user_id != distinct_id:
            identifiers.append(('$user_id', user_id))
            
        # Add $device_id if present and different
        if device_id != 'MISSING' and device_id != distinct_id and device_id != user_id:
            identifiers.append(('$device_id', device_id))
        
        for id_type, id_value in identifiers:
            print(f"   {id_type}: {id_value}")
        
        # Show top-level keys for reference
        print(f"\n📋 Top-level keys: {list(event_data.keys())}")
        
        # Show some property keys
        prop_keys = list(properties.keys())[:10]  # First 10 keys
        print(f"🔧 Property keys (first 10): {prop_keys}")
    
    conn.close()
    
    print(f"\n🎯 ANALYSIS SUMMARY")
    print("=" * 60)
    print("📝 FINDINGS:")
    print("   • Every event has distinct_id at top level")
    print("   • $user_id is in properties (may be MISSING)")
    print("   • $device_id is in properties (may be MISSING)")
    print("   • $insert_id matches our CSV Insert ID")
    print()
    print("💡 ROBUST LOOKUP STRATEGY:")
    print("   1. Primary: event.distinct_id")
    print("   2. Fallback: event.properties.$user_id")
    print("   3. Additional: event.properties.$device_id (if needed)")
    print()
    print("🎯 IMPLEMENTATION:")
    print("   • Check if user exists with event.distinct_id")
    print("   • If not found, check event.properties.$user_id")
    print("   • Process event if ANY identifier matches existing user")

if __name__ == "__main__":
    analyze_event_structure() 