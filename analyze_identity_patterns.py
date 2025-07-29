#!/usr/bin/env python3
"""
🔍 ANALYZE IDENTITY MERGING PATTERNS
Understand how distinct_id, $user_id, and User ID relate to each other
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

def analyze_identity_patterns():
    """Analyze identity patterns across CSV, raw events, and processed users"""
    print("🔍 ANALYZING IDENTITY MERGING PATTERNS")
    print("=" * 60)
    
    # Read CSV data first
    csv_data = {}
    with open('mixpanel_user.csv', 'r') as f:
        reader = csv.DictReader(f)
        for row in reader:
            csv_data[row['Insert ID']] = {
                'insert_id': row['Insert ID'],
                'distinct_id': row['Distinct ID'], 
                'user_id': row['User ID'],
                'time': row['Time']
            }
    
    raw_db_path = get_database_path("raw_data")
    processed_db_path = get_database_path("mixpanel_data")
    
    raw_conn = sqlite3.connect(raw_db_path)
    processed_conn = sqlite3.connect(processed_db_path)
    
    print("🔍 ANALYZING ALL 41 EVENTS:")
    print("-" * 60)
    
    patterns = {
        'csv_distinct_equals_raw_distinct': 0,
        'csv_distinct_equals_raw_user_id': 0,
        'csv_user_id_equals_raw_distinct': 0,
        'csv_user_id_equals_raw_user_id': 0,
        'no_match_found': 0,
        'missing_events': []
    }
    
    for insert_id, csv_info in csv_data.items():
        print(f"\n📊 EVENT: {insert_id[:8]}...")
        
        # Get raw event data
        raw_cursor = raw_conn.cursor()
        raw_cursor.execute("SELECT event_data FROM raw_event_data WHERE event_data LIKE ?", [f'%{insert_id}%'])
        result = raw_cursor.fetchone()
        
        if not result:
            print("❌ Event not found in raw database")
            patterns['missing_events'].append(insert_id)
            continue
            
        event_data = json.loads(result[0])
        raw_distinct_id = event_data.get('distinct_id', 'MISSING')
        
        # Check if there's a $user_id in properties
        properties = event_data.get('properties', {})
        raw_user_id = properties.get('$user_id', 'MISSING')
        
        csv_distinct_id = csv_info['distinct_id']
        csv_user_id = csv_info['user_id']
        
        print(f"📄 CSV Distinct ID: {csv_distinct_id}")
        print(f"📄 CSV User ID: {csv_user_id}")
        print(f"🗄️  Raw distinct_id: {raw_distinct_id}")
        print(f"🗄️  Raw $user_id: {raw_user_id}")
        
        # Check matching patterns
        match_found = False
        
        if csv_distinct_id == raw_distinct_id:
            print("✅ MATCH: CSV Distinct ID = Raw distinct_id")
            patterns['csv_distinct_equals_raw_distinct'] += 1
            match_found = True
        
        if csv_distinct_id == raw_user_id:
            print("✅ MATCH: CSV Distinct ID = Raw $user_id")
            patterns['csv_distinct_equals_raw_user_id'] += 1
            match_found = True
            
        if csv_user_id == raw_distinct_id:
            print("✅ MATCH: CSV User ID = Raw distinct_id")
            patterns['csv_user_id_equals_raw_distinct'] += 1
            match_found = True
            
        if csv_user_id == raw_user_id:
            print("✅ MATCH: CSV User ID = Raw $user_id")
            patterns['csv_user_id_equals_raw_user_id'] += 1
            match_found = True
        
        if not match_found:
            print("❌ NO DIRECT MATCH FOUND")
            patterns['no_match_found'] += 1
        
        # Check which ID exists in processed users table
        processed_cursor = processed_conn.cursor()
        
        processed_cursor.execute("SELECT COUNT(*) FROM mixpanel_user WHERE distinct_id = ?", [csv_distinct_id])
        csv_distinct_in_db = processed_cursor.fetchone()[0] > 0
        
        processed_cursor.execute("SELECT COUNT(*) FROM mixpanel_user WHERE distinct_id = ?", [csv_user_id])
        csv_user_in_db = processed_cursor.fetchone()[0] > 0
        
        processed_cursor.execute("SELECT COUNT(*) FROM mixpanel_user WHERE distinct_id = ?", [raw_distinct_id])
        raw_distinct_in_db = processed_cursor.fetchone()[0] > 0
        
        processed_cursor.execute("SELECT COUNT(*) FROM mixpanel_user WHERE distinct_id = ?", [raw_user_id])
        raw_user_in_db = processed_cursor.fetchone()[0] > 0
        
        print(f"🗃️  In processed DB:")
        print(f"   CSV Distinct ID: {'✅' if csv_distinct_in_db else '❌'}")
        print(f"   CSV User ID: {'✅' if csv_user_in_db else '❌'}")
        print(f"   Raw distinct_id: {'✅' if raw_distinct_in_db else '❌'}")
        print(f"   Raw $user_id: {'✅' if raw_user_in_db else '❌'}")
    
    raw_conn.close()
    processed_conn.close()
    
    print(f"\n🎯 PATTERN ANALYSIS SUMMARY")
    print("=" * 60)
    print(f"📊 Total events analyzed: {len(csv_data)}")
    print(f"📊 Missing from raw DB: {len(patterns['missing_events'])}")
    print(f"\n🔍 MATCHING PATTERNS:")
    print(f"   CSV Distinct = Raw distinct_id: {patterns['csv_distinct_equals_raw_distinct']}")
    print(f"   CSV Distinct = Raw $user_id: {patterns['csv_distinct_equals_raw_user_id']}")
    print(f"   CSV User = Raw distinct_id: {patterns['csv_user_id_equals_raw_distinct']}")
    print(f"   CSV User = Raw $user_id: {patterns['csv_user_id_equals_raw_user_id']}")
    print(f"   No direct match: {patterns['no_match_found']}")
    
    print(f"\n💡 RECOMMENDATIONS:")
    if patterns['csv_user_id_equals_raw_distinct'] > 20:
        print("🎯 STRONG PATTERN: CSV User ID matches Raw distinct_id")
        print("💡 SOLUTION: Use CSV User ID as the canonical distinct_id")
    elif patterns['csv_distinct_equals_raw_user_id'] > 20:
        print("🎯 STRONG PATTERN: CSV Distinct ID matches Raw $user_id")
        print("💡 SOLUTION: Try matching against $user_id in event properties")
    else:
        print("🤔 MIXED PATTERNS: Need dual-lookup strategy")
        print("💡 SOLUTION: Try both distinct_id and $user_id when matching events to users")

if __name__ == "__main__":
    analyze_identity_patterns() 