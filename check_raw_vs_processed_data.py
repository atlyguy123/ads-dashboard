#!/usr/bin/env python3
"""
Check Raw vs Processed Data

Compare data between:
1. Raw database (raw_data.db) - initial S3 ingestion
2. Processed database (mixpanel_data.db) - final processed data

Goal: Find if the 24 missing Mixpanel users exist in raw data but are lost during processing
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
    """Compare raw vs processed databases"""
    
    print("🔍 RAW vs PROCESSED DATABASE COMPARISON")
    print("=" * 60)
    print("🗄️  Checking data pipeline integrity...")
    print()
    
    # Read Mixpanel users from CSV
    mixpanel_users = read_mixpanel_users()
    missing_users = get_missing_users()
    
    print(f"📋 Total Mixpanel users to check: {len(mixpanel_users)}")
    print(f"❌ Users missing from main DB: {len(missing_users)}")
    print()
    
    try:
        # Check raw database
        print("1️⃣ CHECKING RAW DATABASE")
        print("-" * 30)
        raw_db_path = get_database_path('raw_data')
        check_raw_database(raw_db_path, missing_users)
        
        print("\n2️⃣ CHECKING PROCESSED DATABASE DETAILS")
        print("-" * 40)
        processed_db_path = get_database_path('mixpanel_data')
        check_processed_database_details(processed_db_path, missing_users)
        
        print("\n3️⃣ PIPELINE INTEGRITY CHECK")
        print("-" * 30)
        compare_pipeline_integrity(raw_db_path, processed_db_path, mixpanel_users)
        
    except Exception as e:
        print(f"❌ Error: {e}")
        return 1
        
    return 0

def read_mixpanel_users():
    """Read users from CSV"""
    mixpanel_users = []
    try:
        with open('mixpanel_user.csv', 'r') as file:
            csv_reader = csv.DictReader(file)
            for row in csv_reader:
                mixpanel_users.append(row['User ID'])
    except Exception as e:
        print(f"❌ Error reading CSV: {e}")
    return mixpanel_users

def get_missing_users():
    """Get the list of users missing from main database"""
    return [
        '-kld4GPibhI', '0znYAVLEsrF', '1LZSWVV2aFf', '7HP_C27dDJm', 'C9GeaFRjpfa',
        'GZbgc-4UFzS', 'Se8_hbzPxbv', 'Vn433ZFwH-Q', 'VxRGj9dXTO1', 'WhCxnzxApfY',
        'XUzgjy1wOrr', '_0495qKk7Il', '_a1qrFYs55X', 'bEYyrP7LSro', 'caCvDcVf9Do',
        'iwmRwH0QWu6', 'nJQPoy-ycKr', 'pe60vc5po2b', 't9UtN9Zdkzm', 'tgkdL-brJgx',
        'tq6RxxzK7P1', 'undefined', 'y4WnAjiyWPG', 'zCIpa6L4B3b'
    ]

def check_raw_database(raw_db_path, missing_users):
    """Check if missing users exist in raw database"""
    
    try:
        if not Path(raw_db_path).exists():
            print(f"   ⚠️  Raw database not found at: {raw_db_path}")
            print(f"   💡 Checking for alternative raw data locations...")
            
            # Check for other possible raw data locations
            possible_locations = [
                "database/raw_data.db",
                "pipelines/mixpanel_pipeline/downloaded_data/",
                "data/"
            ]
            
            for location in possible_locations:
                if Path(location).exists():
                    print(f"      📁 Found: {location}")
                    if Path(location).is_dir():
                        files = list(Path(location).glob("*.db"))
                        if files:
                            print(f"         Database files: {[f.name for f in files]}")
                else:
                    print(f"      ❌ Not found: {location}")
            return
        
        with sqlite3.connect(raw_db_path) as conn:
            conn.row_factory = sqlite3.Row
            cursor = conn.cursor()
            
            # Check what tables exist in raw database
            cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
            tables = [row['name'] for row in cursor.fetchall()]
            
            print(f"   📊 Raw database tables: {tables}")
            
            if not tables:
                print(f"   ⚠️  Raw database exists but has no tables")
                return
            
            # Check for users in raw database
            found_in_raw = 0
            for table in tables:
                if 'user' in table.lower() or 'mixpanel' in table.lower():
                    print(f"   🔍 Checking table: {table}")
                    
                    try:
                        # Get table schema
                        cursor.execute(f"PRAGMA table_info({table})")
                        columns = [col['name'] for col in cursor.fetchall()]
                        print(f"      Columns: {columns}")
                        
                        # Check for missing users
                        user_col = None
                        for col in columns:
                            if 'distinct_id' in col.lower() or 'user_id' in col.lower():
                                user_col = col
                                break
                        
                        if user_col:
                            for user_id in missing_users[:5]:  # Check first 5 missing users
                                cursor.execute(f"SELECT * FROM {table} WHERE {user_col} = ? LIMIT 1", [user_id])
                                result = cursor.fetchone()
                                if result:
                                    found_in_raw += 1
                                    print(f"      ✅ Found {user_id} in raw data!")
                                else:
                                    print(f"      ❌ {user_id} not in raw data")
                        
                    except Exception as e:
                        print(f"      ❌ Error checking table {table}: {e}")
            
            print(f"   📊 Found {found_in_raw} missing users in raw database")
            
    except Exception as e:
        print(f"   ❌ Error accessing raw database: {e}")

def check_processed_database_details(processed_db_path, missing_users):
    """Check processed database for more details"""
    
    try:
        with sqlite3.connect(processed_db_path) as conn:
            conn.row_factory = sqlite3.Row
            cursor = conn.cursor()
            
            # Check processing pipeline status
            cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name LIKE '%pipeline%'")
            pipeline_tables = [row['name'] for row in cursor.fetchall()]
            
            print(f"   📊 Pipeline-related tables: {pipeline_tables}")
            
            # Check ETL job control
            try:
                cursor.execute("SELECT * FROM etl_job_control ORDER BY last_run_timestamp DESC LIMIT 5")
                etl_jobs = cursor.fetchall()
                
                print(f"   🔄 Recent ETL jobs:")
                for job in etl_jobs:
                    print(f"      {job['job_name']}: {job['status']} at {job['last_run_timestamp']}")
                    if job['error_message']:
                        print(f"         Error: {job['error_message']}")
                        
            except Exception as e:
                print(f"   ⚠️  No ETL job control table or error: {e}")
            
            # Check processed event days
            try:
                cursor.execute("""
                    SELECT date_day, events_processed, status 
                    FROM processed_event_days 
                    WHERE date_day BETWEEN '2025-07-16' AND '2025-07-29'
                    ORDER BY date_day
                """)
                processed_days = cursor.fetchall()
                
                print(f"   📅 Event processing status for July 16-29:")
                for day in processed_days:
                    status_icon = "✅" if day['status'] == 'complete' else "⚠️"
                    print(f"      {status_icon} {day['date_day']}: {day['events_processed']} events ({day['status']})")
                    
            except Exception as e:
                print(f"   ⚠️  No processed event days table or error: {e}")
            
            # Check user ingestion patterns
            cursor.execute("""
                SELECT 
                    DATE(first_seen) as date,
                    COUNT(*) as users_added
                FROM mixpanel_user 
                WHERE abi_campaign_id = '120223331225260178'
                  AND DATE(first_seen) BETWEEN '2025-07-16' AND '2025-07-29'
                GROUP BY DATE(first_seen)
                ORDER BY date
            """)
            
            user_additions = cursor.fetchall()
            print(f"   👥 User additions by date:")
            for addition in user_additions:
                print(f"      {addition['date']}: {addition['users_added']} users")
                
    except Exception as e:
        print(f"   ❌ Error checking processed database: {e}")

def compare_pipeline_integrity(raw_db_path, processed_db_path, mixpanel_users):
    """Compare pipeline integrity between databases"""
    
    print("   🔍 Pipeline integrity analysis...")
    
    # Check if we have any raw data processing
    try:
        # Check for downloaded data files
        download_dir = Path("pipelines/mixpanel_pipeline/downloaded_data/")
        if download_dir.exists():
            files = list(download_dir.glob("*.json")) + list(download_dir.glob("*.db"))
            print(f"   📁 Downloaded data files: {len(files)} files")
            
            if files:
                # Check newest file
                newest_file = max(files, key=lambda f: f.stat().st_mtime)
                print(f"      📄 Newest file: {newest_file.name} ({newest_file.stat().st_size} bytes)")
                
                # If it's a JSON file, check if it contains our missing users
                if newest_file.suffix == '.json':
                    check_json_file_for_users(newest_file, mixpanel_users[:5])
        else:
            print(f"   ❌ No downloaded data directory found")
            
        # Check data directory
        data_dir = Path("data/")
        if data_dir.exists():
            subdirs = [d for d in data_dir.iterdir() if d.is_dir()]
            print(f"   📁 Data subdirectories: {[d.name for d in subdirs]}")
            
            # Check for raw events or users
            events_dir = data_dir / "events"
            users_dir = data_dir / "users"
            
            if events_dir.exists():
                event_files = list(events_dir.glob("*"))
                print(f"      📁 Events directory: {len(event_files)} files")
                
            if users_dir.exists():
                user_files = list(users_dir.glob("*"))
                print(f"      📁 Users directory: {len(user_files)} files")
        else:
            print(f"   ❌ No data directory found")
            
    except Exception as e:
        print(f"   ❌ Error checking pipeline integrity: {e}")

def check_json_file_for_users(file_path, user_ids):
    """Check if JSON file contains missing users"""
    try:
        print(f"      🔍 Checking {file_path.name} for missing users...")
        
        # Read and parse JSON (handle large files carefully)
        with open(file_path, 'r') as f:
            content = f.read(1000000)  # Read first 1MB
            
        found_users = []
        for user_id in user_ids:
            if user_id in content:
                found_users.append(user_id)
        
        if found_users:
            print(f"         ✅ Found users in raw file: {found_users}")
        else:
            print(f"         ❌ No missing users found in sample")
            
    except Exception as e:
        print(f"         ❌ Error reading JSON file: {e}")

if __name__ == "__main__":
    exit(main()) 