#!/usr/bin/env python3
"""
Analyze Missing Trial Users

Investigate the 12 users who Mixpanel counts but have no trial events
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
    """Analyze the 12 users with no trial events"""
    
    print("🔍 ANALYZING USERS WITH NO TRIAL EVENTS")
    print("=" * 50)
    
    # Get our 41 distinct_ids from CSV
    distinct_ids = read_distinct_ids_from_csv()
    
    # Connect to processed database
    with sqlite3.connect(get_database_path('mixpanel_data')) as conn:
        cursor = conn.cursor()
        
        # Find users with no trial events
        users_no_trials = find_users_without_trial_events(cursor, distinct_ids)
        
        # Analyze what they have instead
        analyze_user_characteristics(cursor, users_no_trials)
        
        # Check if they have other events
        analyze_other_events(cursor, users_no_trials)
        
        # See if there's a pattern
        find_common_patterns(cursor, users_no_trials)
    
    return 0

def read_distinct_ids_from_csv():
    """Read distinct_ids from CSV"""
    distinct_ids = []
    try:
        with open('mixpanel_user.csv', 'r') as file:
            csv_reader = csv.DictReader(file)
            for row in csv_reader:
                if 'Distinct ID' in row and row['Distinct ID'].strip():
                    distinct_ids.append(row['Distinct ID'].strip())
        return distinct_ids
    except Exception as e:
        print(f"❌ Error reading CSV: {e}")
        return []

def find_users_without_trial_events(cursor, distinct_ids):
    """Find users in our database with no trial events"""
    users_no_trials = []
    
    start_date = "2025-07-16"
    end_date = "2025-07-29"
    
    for distinct_id in distinct_ids:
        # Check if user exists in database
        cursor.execute("SELECT * FROM mixpanel_user WHERE distinct_id = ?", [distinct_id])
        user_data = cursor.fetchone()
        
        if user_data:
            # Check for trial events in date range
            cursor.execute("""
                SELECT COUNT(*) FROM mixpanel_event 
                WHERE distinct_id = ? AND event_name = 'RC Trial started'
                AND DATE(event_time) BETWEEN ? AND ?
            """, [distinct_id, start_date, end_date])
            
            trial_count = cursor.fetchone()[0]
            
            if trial_count == 0:
                users_no_trials.append(distinct_id)
                print(f"   ❌ {distinct_id}: No trial events in range")
    
    print(f"\n📊 Found {len(users_no_trials)} users with no trial events")
    return users_no_trials

def analyze_user_characteristics(cursor, users_no_trials):
    """Analyze characteristics of users without trial events"""
    print(f"\n1️⃣ ANALYZING USER CHARACTERISTICS...")
    
    for distinct_id in users_no_trials:
        cursor.execute("""
            SELECT distinct_id, abi_campaign_id, abi_ad_id, abi_ad_set_id, 
                   has_abi_attribution, first_seen, last_updated, country, 
                   economic_tier, valid_user
            FROM mixpanel_user 
            WHERE distinct_id = ?
        """, [distinct_id])
        
        user = cursor.fetchone()
        if user:
            print(f"   👤 {user[0][:20]}...")
            print(f"      📅 First seen: {user[5]}")
            print(f"      🎯 Attribution: {user[4]}")
            print(f"      🌍 Country: {user[7]}")
            print(f"      💰 Economic tier: {user[8]}")
            print(f"      ✅ Valid user: {user[9]}")

def analyze_other_events(cursor, users_no_trials):
    """Check what other events these users have"""
    print(f"\n2️⃣ ANALYZING OTHER EVENTS...")
    
    start_date = "2025-07-16"
    end_date = "2025-07-29"
    
    for distinct_id in users_no_trials:
        # Check for ANY events in date range
        cursor.execute("""
            SELECT event_name, COUNT(*), MIN(event_time), MAX(event_time)
            FROM mixpanel_event 
            WHERE distinct_id = ? AND DATE(event_time) BETWEEN ? AND ?
            GROUP BY event_name
            ORDER BY COUNT(*) DESC
        """, [distinct_id, start_date, end_date])
        
        events_in_range = cursor.fetchall()
        
        # Check for ANY events ever
        cursor.execute("""
            SELECT event_name, COUNT(*), MIN(event_time), MAX(event_time)
            FROM mixpanel_event 
            WHERE distinct_id = ?
            GROUP BY event_name
            ORDER BY COUNT(*) DESC
        """, [distinct_id])
        
        all_events = cursor.fetchall()
        
        print(f"   👤 {distinct_id[:30]}...")
        
        if events_in_range:
            print(f"      📅 Events in July 16-29:")
            for event_name, count, min_time, max_time in events_in_range:
                print(f"         • {event_name}: {count} events ({min_time} to {max_time})")
        else:
            print(f"      ❌ NO events in July 16-29")
            
        if all_events:
            print(f"      🗂️  All-time events (top 3):")
            for event_name, count, min_time, max_time in all_events[:3]:
                print(f"         • {event_name}: {count} events ({min_time} to {max_time})")
        else:
            print(f"      ❌ NO events ever")
        print()

def find_common_patterns(cursor, users_no_trials):
    """Look for common patterns among users without trial events"""
    print(f"3️⃣ LOOKING FOR PATTERNS...")
    
    start_date = "2025-07-16"
    end_date = "2025-07-29"
    
    # Pattern 1: Users with events in date range
    users_with_events_in_range = 0
    users_with_no_events_in_range = 0
    
    # Pattern 2: Users first seen in date range
    users_first_seen_in_range = 0
    
    for distinct_id in users_no_trials:
        # Check for any events in range
        cursor.execute("""
            SELECT COUNT(*) FROM mixpanel_event 
            WHERE distinct_id = ? AND DATE(event_time) BETWEEN ? AND ?
        """, [distinct_id, start_date, end_date])
        
        events_count = cursor.fetchone()[0]
        if events_count > 0:
            users_with_events_in_range += 1
        else:
            users_with_no_events_in_range += 1
            
        # Check first seen date
        cursor.execute("SELECT first_seen FROM mixpanel_user WHERE distinct_id = ?", [distinct_id])
        first_seen = cursor.fetchone()[0]
        
        if first_seen and start_date <= first_seen[:10] <= end_date:
            users_first_seen_in_range += 1
            print(f"   📅 {distinct_id[:30]}: First seen in range ({first_seen})")
    
    print(f"\n🔍 PATTERN ANALYSIS:")
    print(f"   📊 Users with other events in July 16-29: {users_with_events_in_range}/{len(users_no_trials)}")
    print(f"   📊 Users with NO events in July 16-29: {users_with_no_events_in_range}/{len(users_no_trials)}")
    print(f"   📊 Users first seen in July 16-29: {users_first_seen_in_range}/{len(users_no_trials)}")
    
    print(f"\n💡 HYPOTHESIS:")
    if users_with_events_in_range > 0:
        print(f"   🎯 Mixpanel might be counting users who have ANY events in the date range")
        print(f"   🎯 Even if those events are NOT trial events")
    
    if users_first_seen_in_range > 0:
        print(f"   🎯 Mixpanel might also be counting users first seen in the date range")
        print(f"   🎯 This could explain the discrepancy!")

if __name__ == "__main__":
    exit(main()) 