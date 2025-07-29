#!/usr/bin/env python3
"""
Check Key Mapping Issue

The issue is that users are stored by distinct_id (device ID) but we're searching by $user_id.
Let's check how many of our 40 users are actually present in raw DB under correct keys.
"""

import sqlite3
import csv
import json
from pathlib import Path
import sys

# Add utils directory to path for database utilities
utils_path = str(Path(__file__).resolve().parent / "utils")
sys.path.append(utils_path)
from database_utils import get_database_path

def main():
    """Check key mapping between user_id and distinct_id"""
    
    print("🔍 KEY MAPPING INVESTIGATION")
    print("=" * 50)
    print("🔑 Checking if users exist under device IDs in raw database...")
    print()
    
    # Read CSV users
    csv_users = read_csv_users()
    
    try:
        # Connect to raw database
        raw_db_path = get_database_path('raw_data')
        
        with sqlite3.connect(raw_db_path) as conn:
            conn.row_factory = sqlite3.Row
            cursor = conn.cursor()
            
            # For each CSV user, find their device ID and check raw DB
            found_mapping = []
            missing_completely = []
            
            print("🔍 Searching for users by mapping $user_id to distinct_id...")
            
            for i, user in enumerate(csv_users, 1):
                user_id = user['user_id']
                
                # Search for any record with this $user_id in properties
                cursor.execute("""
                    SELECT distinct_id, user_data 
                    FROM raw_user_data 
                    WHERE user_data LIKE ? 
                    LIMIT 1
                """, [f'%"$user_id":"{user_id}"%'])
                
                result = cursor.fetchone()
                
                if result:
                    distinct_id = result['distinct_id']
                    user_data = json.loads(result['user_data'])
                    properties = user_data.get('properties', {})
                    campaign_id = properties.get('abi_~campaign_id')
                    
                    print(f"   ✅ {i:2d}/40: Found {user_id}")
                    print(f"        Device ID: {distinct_id}")
                    print(f"        Campaign: {campaign_id}")
                    
                    found_mapping.append({
                        'user_id': user_id,
                        'distinct_id': distinct_id,
                        'campaign_id': campaign_id,
                        'campaign_matches': campaign_id == user['campaign_id']
                    })
                else:
                    print(f"   ❌ {i:2d}/40: Missing {user_id}")
                    missing_completely.append(user)
            
            print()
            print("📊 KEY MAPPING RESULTS:")
            print(f"   ✅ Found with device mapping: {len(found_mapping)}/40 users")
            print(f"   ❌ Completely missing: {len(missing_completely)}/40 users")
            print()
            
            # Check campaign attribution accuracy
            correct_campaigns = sum(1 for user in found_mapping if user['campaign_matches'])
            print(f"   🎯 Correct campaign attribution: {correct_campaigns}/{len(found_mapping)} users")
            
            # Now check processed database with device IDs
            if found_mapping:
                check_processed_db_with_device_ids(found_mapping)
            
            if missing_completely:
                print("\n❌ COMPLETELY MISSING USERS:")
                for user in missing_completely:
                    print(f"   - {user['user_id']}")
        
    except Exception as e:
        print(f"❌ Error: {e}")
        return 1
    
    return 0

def read_csv_users():
    """Read users from CSV"""
    users = []
    try:
        with open('mixpanel_user.csv', 'r') as file:
            csv_reader = csv.DictReader(file)
            for row in csv_reader:
                users.append({
                    'user_id': row['User ID'],
                    'campaign_name': row['abi_~campaign'],
                    'campaign_id': row['abi_~campaign_id']
                })
    except Exception as e:
        print(f"❌ Error reading CSV: {e}")
    return users

def check_processed_db_with_device_ids(found_mapping):
    """Check if users with device IDs exist in processed DB"""
    print("\n🔄 CHECKING PROCESSED DATABASE WITH DEVICE IDs...")
    
    try:
        main_db_path = get_database_path('mixpanel_data')
        with sqlite3.connect(main_db_path) as conn:
            conn.row_factory = sqlite3.Row
            cursor = conn.cursor()
            
            processed_found = []
            processed_missing = []
            
            for user_mapping in found_mapping:
                distinct_id = user_mapping['distinct_id']
                
                # Check if device ID exists in processed DB
                cursor.execute("SELECT distinct_id FROM mixpanel_user WHERE distinct_id = ?", [distinct_id])
                result = cursor.fetchone()
                
                if result:
                    processed_found.append(user_mapping)
                else:
                    processed_missing.append(user_mapping)
            
            print(f"   ✅ Found in processed DB: {len(processed_found)}/{len(found_mapping)} users")
            print(f"   ❌ Missing from processed DB: {len(processed_missing)}/{len(found_mapping)} users")
            
            if processed_missing:
                print("\n🚨 INGESTION PIPELINE FAILING FOR:")
                for user in processed_missing[:5]:  # Show first 5
                    print(f"   - {user['user_id']} ({user['distinct_id'][:20]}...)")
                
                print(f"\n💡 SOLUTION NEEDED:")
                print(f"   1. ✅ Raw DB has users under device IDs")
                print(f"   2. ❌ Ingestion pipeline isn't processing them") 
                print(f"   3. 🔧 Need to run ingestion pipeline to fix")
    
    except Exception as e:
        print(f"   ❌ Error checking processed DB: {e}")

if __name__ == "__main__":
    exit(main()) 