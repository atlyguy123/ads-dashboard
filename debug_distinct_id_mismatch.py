#!/usr/bin/env python3
"""
🔍 DEBUG DISTINCT_ID MISMATCH
Check if the distinct_id in raw events matches what's in processed user table
"""
import sqlite3
import json
import sys
from pathlib import Path

# Add utils to path
utils_path = str(Path(__file__).resolve().parent / "utils")
sys.path.append(utils_path)
from database_utils import get_database_path

def debug_distinct_id_mismatch():
    """Check for distinct_id mismatches that could cause events to be skipped"""
    print("🔍 DEBUGGING DISTINCT_ID MISMATCH FOR 4 MISSING EVENTS")
    print("=" * 60)
    
    missing_events = [
        ("100b325f-a7ca-4a9f-88c8-4f570e05598d", "$device:34286A65-A2D0-47C7-B813-D7D2B484375A", "C9GeaFRjpfa"),
        ("534ce39d-8fbd-4586-8010-113e8d4898db", "$device:34ac8c5c-b90e-4a14-be7f-cdd567e2edbb", "WhCxnzxApfY"), 
        ("a1ee4830-36b9-4363-a470-56ecb392e638", "190fb4189611aac-0ad8922e77777b8-5866566f-505c8-190fb41896242e", "t9UtN9Zdkzm"),
        ("c51b7896-6686-4e4c-a1a5-d4a43f0e136f", "196b7fddf2d2f05-0608008b3fe90d-540b4e05-51bf4-196b7fddf2e3e6b", "_a1qrFYs55X")
    ]
    
    raw_db_path = get_database_path("raw_data")
    processed_db_path = get_database_path("mixpanel_data")
    
    raw_conn = sqlite3.connect(raw_db_path)
    processed_conn = sqlite3.connect(processed_db_path)
    
    print("🔍 CHECKING EACH MISSING EVENT:")
    print("-" * 60)
    
    for insert_id, csv_distinct_id, raw_distinct_id in missing_events:
        print(f"\n📊 EVENT: {insert_id}")
        print("=" * 40)
        print(f"📄 CSV distinct_id: {csv_distinct_id}")
        print(f"🗄️  Raw distinct_id: {raw_distinct_id}")
        
        # 1. Get actual distinct_id from raw event data
        raw_cursor = raw_conn.cursor()
        raw_cursor.execute("SELECT event_data FROM raw_event_data WHERE event_data LIKE ?", [f'%{insert_id}%'])
        result = raw_cursor.fetchone()
        
        if result:
            event_data = json.loads(result[0])
            actual_distinct_id = event_data.get('distinct_id', 'NOT_FOUND')
            print(f"✅ Actual distinct_id from raw event: {actual_distinct_id}")
            
            # 2. Check if this distinct_id exists in processed user table
            processed_cursor = processed_conn.cursor()
            processed_cursor.execute("SELECT COUNT(*) FROM mixpanel_user WHERE distinct_id = ?", [actual_distinct_id])
            user_exists = processed_cursor.fetchone()[0]
            
            if user_exists > 0:
                print(f"✅ User exists in processed database")
                print(f"🚨 CRITICAL: Event should have been processed but wasn't!")
            else:
                print(f"❌ User NOT found in processed database")
                print(f"🔍 This explains why event was skipped")
                
                # Check if CSV distinct_id exists instead
                processed_cursor.execute("SELECT COUNT(*) FROM mixpanel_user WHERE distinct_id = ?", [csv_distinct_id])
                csv_user_exists = processed_cursor.fetchone()[0]
                
                if csv_user_exists > 0:
                    print(f"🎯 CSV distinct_id EXISTS in processed database!")
                    print(f"🚨 MISMATCH: Raw event has '{actual_distinct_id}' but CSV shows '{csv_distinct_id}'")
                else:
                    print(f"❌ CSV distinct_id also not found in processed database")
            
            # 3. Check mismatches
            if actual_distinct_id != csv_distinct_id:
                print(f"⚠️  DISTINCT_ID MISMATCH:")
                print(f"   CSV: {csv_distinct_id}")
                print(f"   Raw: {actual_distinct_id}")
            else:
                print(f"✅ distinct_id matches between CSV and raw event")
                
        else:
            print(f"❌ Event not found in raw database!")
    
    raw_conn.close()
    processed_conn.close()
    
    print(f"\n🎯 SUMMARY")
    print("=" * 60)
    print(f"📊 Total events investigated: {len(missing_events)}")
    print(f"🔍 If users exist but events are missing, there's a bug in insert_event_batch")
    print(f"🔍 If users don't exist, there's a data inconsistency issue")

if __name__ == "__main__":
    debug_distinct_id_mismatch() 