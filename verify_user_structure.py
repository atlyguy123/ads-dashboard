#!/usr/bin/env python3
"""
🔍 VERIFY USER DATA STRUCTURE  
Check what identifiers are available in our processed users table
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

def analyze_user_structure():
    """Analyze the structure of users to understand available identifiers"""
    print("🔍 ANALYZING USER DATA STRUCTURE")
    print("=" * 60)
    
    # Get some test users from our CSV
    test_users = []
    with open('mixpanel_user.csv', 'r') as f:
        reader = csv.DictReader(f)
        for i, row in enumerate(reader):
            if i < 4:  # First 4 users
                test_users.append({
                    'csv_distinct_id': row['Distinct ID'],
                    'csv_user_id': row['User ID'],
                    'insert_id': row['Insert ID']
                })
    
    processed_db_path = get_database_path("mixpanel_data")
    conn = sqlite3.connect(processed_db_path)
    cursor = conn.cursor()
    
    # Check the user table schema first
    cursor.execute("PRAGMA table_info(mixpanel_user)")
    columns = cursor.fetchall()
    print("📋 USER TABLE SCHEMA:")
    for col in columns:
        print(f"   {col[1]} ({col[2]})")
    
    print(f"\n🔍 ANALYZING SAMPLE USERS:")
    print("-" * 60)
    
    for i, user_info in enumerate(test_users):
        print(f"\n👤 USER {i+1}: CSV Distinct ID: {user_info['csv_distinct_id'][:20]}...")
        print("=" * 50)
        
        # Check if user exists by CSV distinct_id
        cursor.execute("SELECT distinct_id, profile_json FROM mixpanel_user WHERE distinct_id = ?", 
                      [user_info['csv_distinct_id']])
        result = cursor.fetchone()
        
        if result:
            distinct_id, profile_json = result
            print(f"✅ Found in database")
            print(f"🆔 Stored distinct_id: {distinct_id}")
            
            # Parse profile JSON if available
            if profile_json:
                try:
                    profile = json.loads(profile_json)
                    print(f"📄 Profile keys: {list(profile.keys())[:10]}")
                    
                    # Check for common identifier fields in profile
                    user_id = profile.get('$user_id', 'MISSING')
                    device_id = profile.get('$device_id', 'MISSING')
                    email = profile.get('$email', 'MISSING')
                    
                    print(f"👤 Profile $user_id: {user_id}")
                    print(f"📱 Profile $device_id: {device_id}")
                    print(f"📧 Profile $email: {email}")
                    
                except json.JSONDecodeError:
                    print("❌ Could not parse profile JSON")
            else:
                print("❌ No profile JSON available")
        else:
            print(f"❌ User NOT found with distinct_id: {user_info['csv_distinct_id']}")
            
            # Try to find by CSV user_id
            cursor.execute("SELECT distinct_id FROM mixpanel_user WHERE distinct_id = ?", 
                          [user_info['csv_user_id']])
            result = cursor.fetchone()
            if result:
                print(f"✅ Found with CSV User ID: {user_info['csv_user_id']}")
            else:
                print(f"❌ Also not found with CSV User ID: {user_info['csv_user_id']}")
    
    # Check total distinct_id patterns
    print(f"\n📊 USER TABLE ANALYSIS:")
    print("-" * 60)
    
    cursor.execute("SELECT COUNT(*) FROM mixpanel_user")
    total_users = cursor.fetchone()[0]
    
    cursor.execute("SELECT COUNT(*) FROM mixpanel_user WHERE distinct_id LIKE '$device:%'")
    device_users = cursor.fetchone()[0]
    
    cursor.execute("SELECT COUNT(*) FROM mixpanel_user WHERE distinct_id LIKE '%-%-%-%-%'")
    uuid_users = cursor.fetchone()[0]
    
    print(f"📊 Total users: {total_users}")
    print(f"📱 $device: pattern users: {device_users}")
    print(f"🔢 UUID-like pattern users: {uuid_users}")
    print(f"🆔 Other pattern users: {total_users - device_users - uuid_users}")
    
    conn.close()
    
    print(f"\n🎯 KEY FINDINGS")
    print("=" * 60)
    print("📝 USER STORAGE:")
    print("   • Users are stored with distinct_id as primary key")
    print("   • profile_json contains additional user properties")
    print("   • User lookup must match exactly on distinct_id field")
    print()
    print("💡 MATCHING STRATEGY:")
    print("   • Event distinct_id → User distinct_id (direct match)")
    print("   • Event $user_id → User distinct_id (cross-reference)")
    print("   • Need to check BOTH when processing events")

if __name__ == "__main__":
    analyze_user_structure() 