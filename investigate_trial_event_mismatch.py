#!/usr/bin/env python3
"""
Investigate Trial Event Mismatch

We have 39 users in MixpanelDB but dashboard only shows 27 trial events.
Let's find out why.
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
    """Investigate the trial event mismatch"""
    
    print("🔍 INVESTIGATING TRIAL EVENT MISMATCH")
    print("=" * 50)
    
    # Get our 41 distinct_ids from CSV
    distinct_ids = read_distinct_ids_from_csv()
    if not distinct_ids:
        return 1
    
    # Connect to processed database
    with sqlite3.connect(get_database_path('mixpanel_data')) as conn:
        cursor = conn.cursor()
        
        # Step 1: Check which of our 41 users are in the database
        users_in_db = check_users_in_database(cursor, distinct_ids)
        print(f"📊 {len(users_in_db)}/{len(distinct_ids)} users found in MixpanelDB")
        print()
        
        # Step 2: Check trial events for these users  
        trial_events_analysis = analyze_trial_events(cursor, users_in_db)
        print()
        
        # Step 3: Test different query approaches
        test_dashboard_queries(cursor, users_in_db)
        print()
        
        # Step 4: Show detailed breakdown
        show_detailed_breakdown(cursor, users_in_db, distinct_ids)
    
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

def check_users_in_database(cursor, distinct_ids):
    """Check which users exist in our database"""
    users_in_db = []
    
    for distinct_id in distinct_ids:
        cursor.execute("""
            SELECT distinct_id, abi_campaign_id, has_abi_attribution, first_seen
            FROM mixpanel_user 
            WHERE distinct_id = ?
        """, [distinct_id])
        
        result = cursor.fetchone()
        if result:
            users_in_db.append({
                'distinct_id': result[0],
                'campaign_id': result[1], 
                'has_attribution': result[2],
                'first_seen': result[3]
            })
    
    return users_in_db

def analyze_trial_events(cursor, users_in_db):
    """Analyze trial events for our users"""
    print("1️⃣ ANALYZING TRIAL EVENTS...")
    
    campaign_id = "120223331225260178"
    start_date = "2025-07-16"
    end_date = "2025-07-29"
    
    users_with_trials = []
    users_without_trials = []
    
    for user in users_in_db:
        distinct_id = user['distinct_id']
        
        # Check for trial events in date range
        cursor.execute("""
            SELECT COUNT(*), MIN(event_time), MAX(event_time)
            FROM mixpanel_event 
            WHERE distinct_id = ? 
              AND event_name = 'RC Trial started'
              AND DATE(event_time) BETWEEN ? AND ?
        """, [distinct_id, start_date, end_date])
        
        trial_count, min_time, max_time = cursor.fetchone()
        
        # Check for any trial events (all time)
        cursor.execute("""
            SELECT COUNT(*), MIN(event_time), MAX(event_time)
            FROM mixpanel_event 
            WHERE distinct_id = ? 
              AND event_name = 'RC Trial started'
        """, [distinct_id])
        
        total_trials, total_min, total_max = cursor.fetchone()
        
        if trial_count > 0:
            users_with_trials.append({
                **user,
                'trials_in_range': trial_count,
                'trial_times': (min_time, max_time),
                'total_trials': total_trials
            })
            print(f"   ✅ {distinct_id}: {trial_count} trials in range")
        else:
            users_without_trials.append({
                **user,
                'total_trials': total_trials,
                'trial_times': (total_min, total_max) if total_trials > 0 else None
            })
            if total_trials > 0:
                print(f"   ⚠️  {distinct_id}: {total_trials} trials total, but NONE in range ({total_min} to {total_max})")
            else:
                print(f"   ❌ {distinct_id}: NO trial events found at all")
    
    print(f"\n📊 TRIAL EVENT SUMMARY:")
    print(f"   ✅ Users with trials in range: {len(users_with_trials)}")
    print(f"   ⚠️  Users with trials outside range: {len([u for u in users_without_trials if u['total_trials'] > 0])}")
    print(f"   ❌ Users with no trial events: {len([u for u in users_without_trials if u['total_trials'] == 0])}")
    
    return users_with_trials, users_without_trials

def test_dashboard_queries(cursor, users_in_db):
    """Test different query approaches to understand the discrepancy"""
    print("2️⃣ TESTING DASHBOARD QUERIES...")
    
    campaign_id = "120223331225260178"
    start_date = "2025-07-16"
    end_date = "2025-07-29"
    
    # Test 1: Simple trial count (what dashboard probably does)
    cursor.execute("""
        SELECT COUNT(DISTINCT u.distinct_id) as trial_count
        FROM mixpanel_user u
        JOIN mixpanel_event e ON u.distinct_id = e.distinct_id
        WHERE u.abi_campaign_id = ?
          AND u.has_abi_attribution = TRUE
          AND e.event_name = 'RC Trial started'
          AND DATE(e.event_time) BETWEEN ? AND ?
    """, [campaign_id, start_date, end_date])
    
    simple_count = cursor.fetchone()[0]
    print(f"   📊 Simple query (JOIN): {simple_count} trials")
    
    # Test 2: EXISTS approach
    cursor.execute("""
        SELECT COUNT(DISTINCT u.distinct_id) as trial_count
        FROM mixpanel_user u
        WHERE u.abi_campaign_id = ?
          AND u.has_abi_attribution = TRUE
          AND EXISTS (
              SELECT 1 FROM mixpanel_event e 
              WHERE e.distinct_id = u.distinct_id 
              AND e.event_name = 'RC Trial started'
              AND DATE(e.event_time) BETWEEN ? AND ?
          )
    """, [campaign_id, start_date, end_date])
    
    exists_count = cursor.fetchone()[0]
    print(f"   📊 EXISTS query: {exists_count} trials")
    
    # Test 3: Check attribution filtering
    cursor.execute("""
        SELECT COUNT(DISTINCT u.distinct_id) as trial_count
        FROM mixpanel_user u
        JOIN mixpanel_event e ON u.distinct_id = e.distinct_id
        WHERE u.abi_campaign_id = ?
          AND e.event_name = 'RC Trial started'
          AND DATE(e.event_time) BETWEEN ? AND ?
    """, [campaign_id, start_date, end_date])
    
    no_attribution_filter = cursor.fetchone()[0]
    print(f"   📊 Without attribution filter: {no_attribution_filter} trials")
    
    # Test 4: Check attribution status of our users
    cursor.execute("""
        SELECT 
            COUNT(*) as total_users,
            COUNT(CASE WHEN has_abi_attribution = TRUE THEN 1 END) as with_attribution,
            COUNT(CASE WHEN has_abi_attribution = FALSE THEN 1 END) as without_attribution
        FROM mixpanel_user 
        WHERE abi_campaign_id = ?
    """, [campaign_id])
    
    total, with_attr, without_attr = cursor.fetchone()
    print(f"   📊 Attribution status: {with_attr}/{total} have attribution ({without_attr} without)")

def show_detailed_breakdown(cursor, users_in_db, all_distinct_ids):
    """Show detailed breakdown of what's happening"""
    print("3️⃣ DETAILED BREAKDOWN...")
    
    campaign_id = "120223331225260178"
    start_date = "2025-07-16"
    end_date = "2025-07-29"
    
    print(f"\n📋 SUMMARY:")
    print(f"   🎯 Mixpanel export: {len(all_distinct_ids)} users")
    print(f"   🗃️  In our database: {len(users_in_db)} users")
    
    # Check how many have events at all
    users_with_any_events = 0
    users_with_trial_events = 0
    
    for user in users_in_db:
        # Any events?
        cursor.execute("SELECT COUNT(*) FROM mixpanel_event WHERE distinct_id = ?", [user['distinct_id']])
        any_events = cursor.fetchone()[0]
        if any_events > 0:
            users_with_any_events += 1
            
        # Trial events in range?
        cursor.execute("""
            SELECT COUNT(*) FROM mixpanel_event 
            WHERE distinct_id = ? AND event_name = 'RC Trial started' 
            AND DATE(event_time) BETWEEN ? AND ?
        """, [user['distinct_id'], start_date, end_date])
        trial_events = cursor.fetchone()[0]
        if trial_events > 0:
            users_with_trial_events += 1
    
    print(f"   📊 With any events: {users_with_any_events}")
    print(f"   ✅ With trial events in range: {users_with_trial_events}")
    print(f"   ❌ Missing trial events: {len(users_in_db) - users_with_trial_events}")
    
    # The key insight
    print(f"\n🎯 KEY INSIGHT:")
    print(f"   • We have {len(users_in_db)} users in database")
    print(f"   • Only {users_with_trial_events} have trial events in July 16-29 range")
    print(f"   • This explains why dashboard shows ~{users_with_trial_events} instead of {len(users_in_db)}")

if __name__ == "__main__":
    exit(main()) 