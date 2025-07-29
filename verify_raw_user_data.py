#!/usr/bin/env python3
"""
🔍 VERIFY RAW USER DATA
Check the raw user data to confirm both distinct_id and user_id are available
"""
import sqlite3
import json
import csv
import sys
from pathlib import Path

# Add utils to path
utils_path = str(Path(__file__).resolve().parent / "utils")
sys.path.append(utils_path)
from database_utils import get_database_path

def verify_raw_user_data():
    """Check raw user data to confirm distinct_id and user_id are both available"""
    print("🔍 VERIFYING RAW USER DATA STRUCTURE")
    print("=" * 60)
    
    # Get test users from our CSV - focus on the missing event users
    test_cases = [
        ("C9GeaFRjpfa", "$device:34286A65-A2D0-47C7-B813-D7D2B484375A"),  # Missing event user
        ("WhCxnzxApfY", "$device:34ac8c5c-b90e-4a14-be7f-cdd567e2edbb"),   # Missing event user  
        ("nvGOajaWruW", "nvGOajaWruW"),                                    # Working user
        ("_0495qKk7Il", "$device:D7284123-54E5-4BD0-91D1-58B769920351")   # Working user
    ]
    
    raw_db_path = get_database_path("raw_data")
    processed_db_path = get_database_path("mixpanel_data")
    
    raw_conn = sqlite3.connect(raw_db_path)
    processed_conn = sqlite3.connect(processed_db_path)
    
    print("🔍 CHECKING RAW USER DATA:")
    print("-" * 60)
    
    for i, (csv_user_id, csv_distinct_id) in enumerate(test_cases):
        print(f"\n👤 TEST CASE {i+1}:")
        print(f"📄 CSV User ID: {csv_user_id}")
        print(f"📄 CSV Distinct ID: {csv_distinct_id}")
        print("=" * 40)
        
        # Check raw user data by CSV distinct_id
        raw_cursor = raw_conn.cursor()
        raw_cursor.execute("SELECT distinct_id, user_data FROM raw_user_data WHERE distinct_id = ?", 
                          [csv_distinct_id])
        result = raw_cursor.fetchone()
        
        if result:
            distinct_id, user_data_str = result
            user_data = json.loads(user_data_str)
            
            print(f"✅ Found in raw_user_data")
            print(f"🆔 Raw distinct_id: {distinct_id}")
            
            # Check for user_id in properties
            properties = user_data.get('properties', {})
            user_id = properties.get('user_id', 'MISSING')
            alt_user_id = properties.get('$user_id', 'MISSING')
            email = properties.get('$email', 'MISSING')
            
            print(f"👤 Properties user_id: {user_id}")
            print(f"👤 Properties $user_id: {alt_user_id}")
            print(f"📧 Properties $email: {email}")
            
            # Show a sample of property keys
            prop_keys = list(properties.keys())[:10]
            print(f"🔧 Property keys (first 10): {prop_keys}")
            
        else:
            print(f"❌ NOT found in raw_user_data with distinct_id: {csv_distinct_id}")
            
            # Try to find by CSV user_id
            raw_cursor.execute("SELECT distinct_id, user_data FROM raw_user_data WHERE distinct_id = ?", 
                              [csv_user_id])
            result = raw_cursor.fetchone()
            if result:
                print(f"✅ Found in raw_user_data with CSV User ID: {csv_user_id}")
                distinct_id, user_data_str = result
                user_data = json.loads(user_data_str)
                properties = user_data.get('properties', {})
                user_id = properties.get('user_id', 'MISSING')
                print(f"👤 Properties user_id: {user_id}")
            else:
                print(f"❌ Also not found with CSV User ID: {csv_user_id}")
        
        # Check what's in processed database
        processed_cursor = processed_conn.cursor()
        processed_cursor.execute("SELECT distinct_id, profile_json FROM mixpanel_user WHERE distinct_id = ?", 
                                [csv_distinct_id])
        processed_result = processed_cursor.fetchone()
        
        if processed_result:
            proc_distinct_id, profile_json = processed_result
            print(f"✅ Found in processed mixpanel_user")
            
            if profile_json:
                profile = json.loads(profile_json)
                properties = profile.get('properties', {})
                proc_user_id = properties.get('user_id', 'MISSING')
                print(f"👤 Processed user_id: {proc_user_id}")
            else:
                print(f"❌ No profile_json in processed user")
        else:
            print(f"❌ NOT found in processed mixpanel_user")
    
    raw_conn.close()
    processed_conn.close()
    
    print(f"\n🎯 VERIFICATION SUMMARY")
    print("=" * 60)
    print("📝 EXPECTED FINDINGS:")
    print("   • Raw users should have BOTH distinct_id and user_id")
    print("   • Processed users should preserve this information")
    print("   • Event matching should check event.distinct_id against BOTH user fields")
    print()
    print("💡 CORRECT MATCHING STRATEGY:")
    print("   • Event distinct_id → User distinct_id (direct match)")
    print("   • Event distinct_id → User properties.user_id (cross-reference)")
    print("   • Process event if EITHER identifier matches")

if __name__ == "__main__":
    verify_raw_user_data() 