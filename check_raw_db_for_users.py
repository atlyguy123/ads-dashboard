#!/usr/bin/env python3
"""
Check Raw Database for Missing Users

Check if our 40 Mixpanel CSV users exist in the raw database.
This will tell us if the issue is:
1. Download issue (users not in raw DB)
2. Ingestion issue (users in raw DB but not processed DB)
"""

import sqlite3
import csv
from pathlib import Path
import sys

# Add utils directory to path for database utilities
utils_path = str(Path(__file__).resolve().parent / "utils")
sys.path.append(utils_path)
from database_utils import get_database_path

def main():
    """Check if CSV users exist in raw database"""
    
    print("🔍 RAW DATABASE USER CHECK")
    print("=" * 50)
    print("📊 Checking if our 40 CSV users exist in raw database...")
    print()
    
    # Read CSV users
    csv_users = read_csv_users()
    if not csv_users:
        print("❌ No users loaded from CSV")
        return 1
    
    print(f"📋 Checking {len(csv_users)} users from CSV")
    print()
    
    try:
        # Connect to raw database
        raw_db_path = get_database_path('raw_data')
        print(f"🗄️  Connecting to raw DB: {raw_db_path}")
        
        with sqlite3.connect(raw_db_path) as raw_conn:
            raw_conn.row_factory = sqlite3.Row
            raw_cursor = raw_conn.cursor()
            
            # Check if raw_user_data table exists
            raw_cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='raw_user_data'")
            if not raw_cursor.fetchone():
                print("❌ raw_user_data table not found!")
                return 1
            
            # Check each user in raw database
            found_users = []
            missing_users = []
            
            print("🔍 Checking users in raw database...")
            for i, user in enumerate(csv_users, 1):
                user_id = user['user_id']
                
                raw_cursor.execute("SELECT distinct_id FROM raw_user_data WHERE distinct_id = ?", [user_id])
                result = raw_cursor.fetchone()
                
                if result:
                    print(f"   ✅ {i:2d}/40: Found {user_id} in raw DB")
                    found_users.append(user)
                else:
                    print(f"   ❌ {i:2d}/40: Missing {user_id} from raw DB")
                    missing_users.append(user)
            
            print()
            print("📊 RAW DATABASE RESULTS:")
            print(f"   ✅ Found in raw DB: {len(found_users)}/40 users")
            print(f"   ❌ Missing from raw DB: {len(missing_users)}/40 users")
            print()
            
            if missing_users:
                print("❌ USERS MISSING FROM RAW DB:")
                for user in missing_users:
                    print(f"   - {user['user_id']}")
                print()
                print("💡 This indicates a DOWNLOAD/INGESTION issue")
                print("   The pipeline isn't getting data from S3 into raw DB")
            
            if found_users:
                print("✅ USERS FOUND IN RAW DB:")
                for user in found_users[:5]:  # Show first 5
                    print(f"   - {user['user_id']}")
                if len(found_users) > 5:
                    print(f"   ... and {len(found_users) - 5} more")
                print()
                
                # If users are in raw DB, check processed DB
                if len(found_users) > 0:
                    check_processed_db(found_users)
            
            # Show raw DB stats
            show_raw_db_stats(raw_cursor)
            
    except Exception as e:
        print(f"❌ Error checking raw database: {e}")
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

def check_processed_db(found_users):
    """Check if users found in raw DB also exist in processed DB"""
    print("🔄 CHECKING PROCESSED DATABASE...")
    
    try:
        # Connect to main database
        main_db_path = get_database_path('mixpanel_data')
        with sqlite3.connect(main_db_path) as main_conn:
            main_conn.row_factory = sqlite3.Row
            main_cursor = main_conn.cursor()
            
            # Check if mixpanel_user table exists
            main_cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='mixpanel_user'")
            if not main_cursor.fetchone():
                print("   ❌ mixpanel_user table not found!")
                return
            
            processed_found = []
            processed_missing = []
            
            for user in found_users:
                user_id = user['user_id']
                main_cursor.execute("SELECT distinct_id FROM mixpanel_user WHERE distinct_id = ?", [user_id])
                result = main_cursor.fetchone()
                
                if result:
                    processed_found.append(user)
                else:
                    processed_missing.append(user)
            
            print(f"   ✅ Found in processed DB: {len(processed_found)}/{len(found_users)} users")
            print(f"   ❌ Missing from processed DB: {len(processed_missing)}/{len(found_users)} users")
            
            if processed_missing:
                print()
                print("🚨 INGESTION PIPELINE ISSUE DETECTED!")
                print(f"   {len(processed_missing)} users exist in raw DB but NOT in processed DB")
                print("   This indicates the ingestion step is failing")
                print()
                print("   Missing from processed DB:")
                for user in processed_missing[:10]:  # Show first 10
                    print(f"   - {user['user_id']}")
                if len(processed_missing) > 10:
                    print(f"   ... and {len(processed_missing) - 10} more")
    
    except Exception as e:
        print(f"   ❌ Error checking processed DB: {e}")

def show_raw_db_stats(raw_cursor):
    """Show raw and processed database statistics"""
    print("📊 DATABASE STATISTICS:")
    
    try:
        # Raw database stats
        raw_cursor.execute("SELECT COUNT(*) FROM raw_user_data")
        total_raw = raw_cursor.fetchone()[0]
        print(f"   📄 Total users in raw DB: {total_raw:,}")
        
        raw_cursor.execute("SELECT MAX(downloaded_at) FROM raw_user_data")
        latest_download = raw_cursor.fetchone()[0]
        print(f"   📅 Latest raw data: {latest_download}")
        
        # Processed database stats
        main_db_path = get_database_path('mixpanel_data')
        with sqlite3.connect(main_db_path) as main_conn:
            main_cursor = main_conn.cursor()
            
            main_cursor.execute("SELECT COUNT(*) FROM mixpanel_user")
            total_processed = main_cursor.fetchone()[0]
            print(f"   🏭 Total users in processed DB: {total_processed:,}")
            
            # Calculate gap
            gap = total_raw - total_processed
            gap_percent = gap/total_raw*100 if total_raw > 0 else 0
            print(f"   📉 Processing gap: {gap:,} users ({gap_percent:.1f}%)")
        
    except Exception as e:
        print(f"   ❌ Error getting stats: {e}")

if __name__ == "__main__":
    exit(main()) 