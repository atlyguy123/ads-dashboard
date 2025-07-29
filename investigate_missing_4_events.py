#!/usr/bin/env python3
"""
🔍 INVESTIGATE THE 4 MISSING EVENTS
Detailed analysis of why 4 events are missing from processed database
"""

import sqlite3
import json
import sys
from pathlib import Path

# Add utils directory to path for database utilities
utils_path = str(Path(__file__).resolve().parent / "utils")
sys.path.append(utils_path)
from database_utils import get_database_path

def investigate_missing_event(insert_id):
    """Investigate a specific missing event"""
    print(f"\n🔍 ANALYZING: {insert_id}")
    print("=" * 60)
    
    # Connect to databases
    raw_db_path = get_database_path("raw_data")
    processed_db_path = get_database_path("mixpanel_data")
    
    raw_conn = sqlite3.connect(raw_db_path)
    processed_conn = sqlite3.connect(processed_db_path)
    
    # Check raw database
    print("📋 RAW DATABASE CHECK:")
    raw_cursor = raw_conn.cursor()
    raw_cursor.execute("SELECT date_day, event_data FROM raw_event_data WHERE event_data LIKE ? LIMIT 1", [f'%{insert_id}%'])
    raw_result = raw_cursor.fetchone()
    
    if not raw_result:
        print("❌ NOT FOUND in raw database!")
        return
    
    date_day, event_data_str = raw_result
    event_data = json.loads(event_data_str)
    
    print(f"✅ Found in raw database on {date_day}")
    print(f"   Event: {event_data.get('event_name', 'NULL')}")
    print(f"   User: {event_data.get('distinct_id', 'UNKNOWN')}")
    print(f"   Time: {event_data.get('time', 'UNKNOWN')}")
    
    # Check if user exists in processed database
    distinct_id = event_data.get('distinct_id')
    print(f"\n👤 USER CHECK IN PROCESSED DB:")
    processed_cursor = processed_conn.cursor()
    processed_cursor.execute("SELECT COUNT(*) FROM mixpanel_user WHERE distinct_id = ?", [distinct_id])
    user_exists = processed_cursor.fetchone()[0]
    
    if user_exists == 0:
        print(f"❌ USER {distinct_id} NOT FOUND in processed database!")
        print("   🚨 ROOT CAUSE: Event can't be processed because user doesn't exist")
        
        # Check if user is in raw database
        raw_cursor.execute("SELECT COUNT(*) FROM raw_user_data WHERE distinct_id = ?", [distinct_id])
        user_in_raw = raw_cursor.fetchone()[0]
        print(f"   Raw user data: {'✅ EXISTS' if user_in_raw > 0 else '❌ MISSING'}")
        
        if user_in_raw > 0:
            # Get user data to see why it was filtered
            raw_cursor.execute("SELECT user_data FROM raw_user_data WHERE distinct_id = ? LIMIT 1", [distinct_id])
            user_data_result = raw_cursor.fetchone()
            if user_data_result:
                user_data = json.loads(user_data_result[0])
                email = user_data.get('$email', 'No email')
                print(f"   User email: {email}")
                
                # Check for common filtering reasons
                if '@atly.com' in email:
                    print("   🚫 FILTERED: @atly.com email (internal user)")
                elif 'test' in email.lower():
                    print("   🚫 FILTERED: Contains 'test' (test user)")
                elif '@steps.me' in email:
                    print("   🚫 FILTERED: @steps.me email (internal user)")
                else:
                    print("   🤔 UNKNOWN: User should have been processed")
    else:
        print(f"✅ USER {distinct_id} EXISTS in processed database")
        
        # Check if event is in processed database
        processed_cursor.execute("SELECT COUNT(*) FROM mixpanel_event WHERE event_json LIKE ?", [f'%{insert_id}%'])
        event_exists = processed_cursor.fetchone()[0]
        
        if event_exists == 0:
            print("❌ EVENT NOT FOUND in processed database despite user existing!")
            print("   🚨 ROOT CAUSE: Event processing failure during ingestion")
            
            # Check if it's a valid event type
            event_name = event_data.get('event_name')
            valid_events = ['RC Trial started', 'RC Trial converted', 'RC Cancellation', 'RC Initial purchase', 'RC Trial cancelled', 'RC Renewal', 'RC Expiration']
            if event_name in valid_events:
                print(f"   ✅ Event type '{event_name}' is valid")
            else:
                print(f"   🚫 Event type '{event_name}' may be filtered out")
        else:
            print("✅ EVENT EXISTS in processed database (should not be missing!)")
    
    raw_conn.close()
    processed_conn.close()

def main():
    print("🔍 DETAILED INVESTIGATION OF 4 MISSING EVENTS")
    print("=" * 60)
    
    missing_events = [
        "a1ee4830-36b9-4363-a470-56ecb392e638",
        "100b325f-a7ca-4a9f-88c8-4f570e05598d", 
        "c51b7896-6686-4e4c-a1a5-d4a43f0e136f",
        "534ce39d-8fbd-4586-8010-113e8d4898db"
    ]
    
    for insert_id in missing_events:
        investigate_missing_event(insert_id)
    
    print(f"\n🎯 INVESTIGATION COMPLETE")
    print("=" * 60)

if __name__ == "__main__":
    main() 