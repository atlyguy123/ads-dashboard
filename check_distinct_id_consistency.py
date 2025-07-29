#!/usr/bin/env python3
"""
Check Distinct ID Pipeline Consistency

Now that we have distinct_ids from Mixpanel, let's check the entire pipeline
"""

import sqlite3
import json
import csv
from pathlib import Path
import sys

# Add utils directory to path for database utilities
utils_path = str(Path(__file__).resolve().parent / "utils")
sys.path.append(utils_path)
from database_utils import get_database_path

def main():
    """Run complete distinct_id pipeline check"""
    
    print("🔍 DISTINCT ID PIPELINE CONSISTENCY CHECK")
    print("=" * 50)
    
    # Read distinct_ids from updated CSV
    distinct_ids = read_distinct_ids_from_csv()
    
    if not distinct_ids:
        print("❌ No distinct_ids found in CSV")
        return 1
    
    print(f"📊 Testing {len(distinct_ids)} distinct_ids from Mixpanel export")
    print()
    
    # Step 1: Check JSON file
    json_results = check_json_file(distinct_ids)
    
    print()
    
    # Step 2: Check raw database
    raw_results = check_raw_database(distinct_ids)
    
    print()
    
    # Step 3: Check processed database  
    processed_results = check_processed_database(distinct_ids)
    
    print()
    
    # Summary
    print_summary(distinct_ids, json_results, raw_results, processed_results)
    
    return 0

def read_distinct_ids_from_csv():
    """Read distinct_ids from the updated CSV file"""
    distinct_ids = []
    
    try:
        with open('mixpanel_user.csv', 'r') as file:
            csv_reader = csv.DictReader(file)
            for row in csv_reader:
                if 'Distinct ID' in row and row['Distinct ID'].strip():
                    distinct_ids.append(row['Distinct ID'].strip())
        
        print(f"✅ Loaded {len(distinct_ids)} distinct_ids from CSV")
        return distinct_ids
        
    except Exception as e:
        print(f"❌ Error reading CSV: {e}")
        return []

def check_json_file(distinct_ids):
    """Check if distinct_ids exist in downloaded JSON file"""
    print("1️⃣ CHECKING JSON FILE...")
    
    # Use the latest JSON file
    json_file = 'data/users/66ac49f5-ca1d-4b9b-a518-bbd37d73d4fa.json'
    found_ids = set()
    
    try:
        print(f"   📄 Checking file: {json_file}")
        
        with open(json_file, 'r') as file:
            line_count = 0
            for line in file:
                line_count += 1
                if line_count % 100000 == 0:
                    print(f"   📊 Processed {line_count:,} lines, found {len(found_ids)} matches so far...")
                    
                if line.strip():
                    try:
                        user_data = json.loads(line)
                        distinct_id = user_data.get('distinct_id')
                        
                        if distinct_id in distinct_ids:
                            found_ids.add(distinct_id)
                            print(f"   ✅ Found: {distinct_id}")
                            
                    except json.JSONDecodeError:
                        continue
        
        found_count = len(found_ids)
        total_count = len(distinct_ids)
        success_rate = (found_count / total_count * 100) if total_count > 0 else 0
        
        print(f"   📊 Found {found_count}/{total_count} distinct_ids ({success_rate:.1f}%)")
        
        # Show missing IDs (first few)
        missing_ids = set(distinct_ids) - found_ids
        if missing_ids:
            missing_list = list(missing_ids)[:5]
            print(f"   ❌ Missing from JSON: {missing_list}{'...' if len(missing_ids) > 5 else ''}")
            
        return found_ids
        
    except Exception as e:
        print(f"   ❌ Error checking JSON: {e}")
        return found_ids

def check_raw_database(distinct_ids):
    """Check if distinct_ids exist in raw database"""
    print("2️⃣ CHECKING RAW DATABASE...")
    
    found_ids = set()
    
    try:
        with sqlite3.connect(get_database_path('raw_data')) as conn:
            cursor = conn.cursor()
            
            print(f"   🔍 Checking {len(distinct_ids)} distinct_ids...")
            
            # Check each distinct_id
            for i, distinct_id in enumerate(distinct_ids, 1):
                cursor.execute("SELECT 1 FROM raw_user_data WHERE distinct_id = ? LIMIT 1", [distinct_id])
                if cursor.fetchone():
                    found_ids.add(distinct_id)
                    print(f"   ✅ {i:2d}/{len(distinct_ids)}: Found {distinct_id}")
                else:
                    print(f"   ❌ {i:2d}/{len(distinct_ids)}: Missing {distinct_id}")
        
        found_count = len(found_ids)
        total_count = len(distinct_ids)
        success_rate = (found_count / total_count * 100) if total_count > 0 else 0
        
        print(f"   📊 Found {found_count}/{total_count} distinct_ids ({success_rate:.1f}%)")
            
        return found_ids
        
    except Exception as e:
        print(f"   ❌ Error checking raw DB: {e}")
        return found_ids

def check_processed_database(distinct_ids):
    """Check if distinct_ids exist in processed database"""
    print("3️⃣ CHECKING PROCESSED DATABASE...")
    
    found_ids = set()
    
    try:
        with sqlite3.connect(get_database_path('mixpanel_data')) as conn:
            cursor = conn.cursor()
            
            print(f"   🔍 Checking {len(distinct_ids)} distinct_ids...")
            
            # Check each distinct_id
            for i, distinct_id in enumerate(distinct_ids, 1):
                cursor.execute("SELECT 1 FROM mixpanel_user WHERE distinct_id = ? LIMIT 1", [distinct_id])
                if cursor.fetchone():
                    found_ids.add(distinct_id)
                    print(f"   ✅ {i:2d}/{len(distinct_ids)}: Found {distinct_id}")
                else:
                    print(f"   ❌ {i:2d}/{len(distinct_ids)}: Missing {distinct_id}")
        
        found_count = len(found_ids)
        total_count = len(distinct_ids)
        success_rate = (found_count / total_count * 100) if total_count > 0 else 0
        
        print(f"   📊 Found {found_count}/{total_count} distinct_ids ({success_rate:.1f}%)")
            
        return found_ids
        
    except Exception as e:
        print(f"   ❌ Error checking processed DB: {e}")
        return found_ids

def print_summary(distinct_ids, json_found, raw_found, processed_found):
    """Print pipeline summary"""
    print("📈 PIPELINE SUMMARY")
    print("=" * 30)
    
    total = len(distinct_ids)
    json_count = len(json_found)
    raw_count = len(raw_found)
    processed_count = len(processed_found)
    
    print(f"🎯 Total distinct_ids: {total}")
    print(f"📁 In JSON file: {json_count} ({json_count/total*100:.1f}%)")
    print(f"🗃️  In raw DB: {raw_count} ({raw_count/total*100:.1f}%)")
    print(f"⚡ In processed DB: {processed_count} ({processed_count/total*100:.1f}%)")
    print()
    
    # Pipeline efficiency
    if json_count > 0:
        raw_efficiency = (raw_count / json_count * 100)
        print(f"📥 JSON → Raw DB: {raw_efficiency:.1f}% efficiency")
        
    if raw_count > 0:
        processing_efficiency = (processed_count / raw_count * 100)
        print(f"⚙️  Raw → Processed: {processing_efficiency:.1f}% efficiency")
        
    overall_efficiency = (processed_count / total * 100)
    print(f"🏆 Overall pipeline: {overall_efficiency:.1f}% success")
    
    # Show what we've proven
    if json_count == total:
        print("\n✅ PROVEN: All users exist in source JSON")
    elif json_count > 0:
        print(f"\n⚠️  SOURCE ISSUE: {total - json_count} users missing from JSON")
        
    if raw_count == json_count:
        print("✅ PROVEN: Download pipeline working perfectly")
    elif raw_count > 0:
        print(f"⚠️  DOWNLOAD ISSUE: {json_count - raw_count} users lost in download")
        
    if processed_count == raw_count:
        print("✅ PROVEN: Ingestion pipeline working perfectly")
    elif processed_count > 0:
        print(f"⚠️  INGESTION ISSUE: {raw_count - processed_count} users lost in processing")

if __name__ == "__main__":
    exit(main()) 