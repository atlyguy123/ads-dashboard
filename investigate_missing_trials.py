#!/usr/bin/env python3
"""
Investigate Missing Trials - Deep Dive

Found: 30 trials in our database  
Expected: 39 trials from Mixpanel UI
Missing: 9 trials

Goal: Find out where the 9 missing trials are
"""

import sqlite3
import json
from pathlib import Path
import sys
from datetime import datetime, timedelta

# Add utils directory to path for database utilities
utils_path = str(Path(__file__).resolve().parent / "utils")
sys.path.append(utils_path)
from database_utils import get_database_path

def main():
    """Main investigation function"""
    
    campaign_id = "120223331225260178"
    start_date = "2025-07-16"
    end_date = "2025-07-29"
    
    print("🔍 INVESTIGATING MISSING 9 TRIALS")
    print("=" * 50)
    print(f"📊 Campaign ID: {campaign_id}")
    print(f"📅 Date Range: {start_date} to {end_date}")
    print(f"🎯 Expected: 39 trials (Mixpanel UI)")
    print(f"📊 Found: 30 trials (our database)")
    print(f"❓ Missing: 9 trials")
    print()
    
    try:
        with sqlite3.connect(get_database_path('mixpanel_data')) as conn:
            conn.row_factory = sqlite3.Row
            cursor = conn.cursor()
            
            # Check for timezone boundary issues
            check_timezone_boundaries(cursor, campaign_id, start_date, end_date)
            
            # Check for data pipeline gaps
            check_data_pipeline_gaps(cursor, campaign_id, start_date, end_date)
            
            # Check for event deduplication issues
            check_event_deduplication(cursor, campaign_id, start_date, end_date)
            
            # Check for incomplete user attribution
            check_incomplete_attribution(cursor, campaign_id, start_date, end_date)
            
            # Check for missing events in events table
            check_missing_events(cursor, campaign_id, start_date, end_date)
            
    except Exception as e:
        print(f"❌ Error: {e}")
        return 1
        
    return 0

def check_timezone_boundaries(cursor, campaign_id, start_date, end_date):
    """Check if timezone boundaries could explain missing trials"""
    print("1️⃣ TIMEZONE BOUNDARY CHECK")
    print("-" * 35)
    
    # Expand date range by 1 day on each side to check for timezone issues
    expanded_start = (datetime.strptime(start_date, "%Y-%m-%d") - timedelta(days=1)).strftime("%Y-%m-%d")
    expanded_end = (datetime.strptime(end_date, "%Y-%m-%d") + timedelta(days=1)).strftime("%Y-%m-%d")
    
    cursor.execute("""
        SELECT 
            DATE(e.event_time) as trial_date,
            COUNT(DISTINCT e.distinct_id) as trials_count,
            COUNT(*) as total_events
        FROM mixpanel_event e
        JOIN mixpanel_user u ON e.distinct_id = u.distinct_id
        WHERE u.abi_campaign_id = ?
          AND e.event_name = 'RC Trial started'
          AND DATE(e.event_time) BETWEEN ? AND ?
        GROUP BY DATE(e.event_time)
        ORDER BY trial_date
    """, [campaign_id, expanded_start, expanded_end])
    
    daily_trials = cursor.fetchall()
    
    print(f"   📅 Daily trial breakdown (expanded range {expanded_start} to {expanded_end}):")
    total_in_range = 0
    total_expanded = 0
    
    for row in daily_trials:
        date_str = row['trial_date']
        count = row['trials_count']
        events = row['total_events']
        
        is_in_original_range = start_date <= date_str <= end_date
        if is_in_original_range:
            total_in_range += count
            print(f"      ✅ {date_str}: {count} trials ({events} events) - IN RANGE")
        else:
            print(f"      ⚠️  {date_str}: {count} trials ({events} events) - OUTSIDE RANGE")
        
        total_expanded += count
    
    print(f"   📊 Summary:")
    print(f"      • Trials in original range: {total_in_range}")
    print(f"      • Trials in expanded range: {total_expanded}")
    
    if total_expanded > total_in_range:
        print(f"   💡 Found {total_expanded - total_in_range} additional trials outside date range")
        print(f"      This could explain part of the discrepancy if Mixpanel uses different timezone")
    print()

def check_data_pipeline_gaps(cursor, campaign_id, start_date, end_date):
    """Check for data pipeline processing gaps"""
    print("2️⃣ DATA PIPELINE GAP CHECK")
    print("-" * 35)
    
    # Check when data was last processed for this date range
    cursor.execute("""
        SELECT 
            MAX(last_updated) as last_user_update,
            COUNT(*) as total_users
        FROM mixpanel_user 
        WHERE abi_campaign_id = ?
    """, [campaign_id])
    
    user_update = cursor.fetchone()
    
    # Check event processing dates
    cursor.execute("""
        SELECT 
            MIN(event_time) as earliest_event,
            MAX(event_time) as latest_event,
            COUNT(*) as total_events
        FROM mixpanel_event e
        JOIN mixpanel_user u ON e.distinct_id = u.distinct_id
        WHERE u.abi_campaign_id = ?
          AND e.event_name = 'RC Trial started'
          AND DATE(e.event_time) BETWEEN ? AND ?
    """, [campaign_id, start_date, end_date])
    
    event_range = cursor.fetchone()
    
    print(f"   📊 Pipeline Status:")
    print(f"      • Last user record update: {user_update['last_user_update']}")
    print(f"      • Total users with campaign: {user_update['total_users']}")
    if event_range['total_events'] > 0:
        print(f"      • Earliest trial event: {event_range['earliest_event']}")
        print(f"      • Latest trial event: {event_range['latest_event']}")
        print(f"      • Total trial events in range: {event_range['total_events']}")
    
    # Check for potential processing gaps in processed_event_days table
    cursor.execute("""
        SELECT 
            date_day,
            events_processed,
            status,
            processing_timestamp
        FROM processed_event_days
        WHERE date_day BETWEEN ? AND ?
        ORDER BY date_day
    """, [start_date, end_date])
    
    processing_days = cursor.fetchall()
    
    if processing_days:
        print(f"   📈 Event Processing Status by Day:")
        for day in processing_days:
            status_icon = "✅" if day['status'] == 'complete' else "⚠️"
            print(f"      {status_icon} {day['date_day']}: {day['events_processed']} events, {day['status']}")
    else:
        print(f"   ⚠️  No processing status found for date range")
    
    print()

def check_event_deduplication(cursor, campaign_id, start_date, end_date):
    """Check if event deduplication might be removing legitimate trials"""
    print("3️⃣ EVENT DEDUPLICATION CHECK")
    print("-" * 35)
    
    # Check for multiple trial events per user (potential deduplication)
    cursor.execute("""
        SELECT 
            e.distinct_id,
            COUNT(*) as trial_event_count,
            MIN(e.event_time) as first_trial,
            MAX(e.event_time) as last_trial,
            GROUP_CONCAT(e.event_uuid) as event_uuids
        FROM mixpanel_event e
        JOIN mixpanel_user u ON e.distinct_id = u.distinct_id
        WHERE u.abi_campaign_id = ?
          AND e.event_name = 'RC Trial started'
          AND DATE(e.event_time) BETWEEN ? AND ?
        GROUP BY e.distinct_id
        HAVING COUNT(*) > 1
        ORDER BY trial_event_count DESC
        LIMIT 10
    """, [campaign_id, start_date, end_date])
    
    duplicate_trials = cursor.fetchall()
    
    if duplicate_trials:
        print(f"   🔄 Users with Multiple Trial Events:")
        total_extra_events = 0
        for row in duplicate_trials:
            extra_events = row['trial_event_count'] - 1
            total_extra_events += extra_events
            print(f"      👤 {row['distinct_id'][:12]}...: {row['trial_event_count']} trials")
            print(f"         First: {row['first_trial']}")
            print(f"         Last: {row['last_trial']}")
        
        print(f"   📊 Summary:")
        print(f"      • Users with multiple trials: {len(duplicate_trials)}")
        print(f"      • Extra events that might be deduplicated: {total_extra_events}")
        
        if total_extra_events >= 9:
            print(f"   💡 POTENTIAL CAUSE: Deduplication could explain missing trials")
    else:
        print(f"   ✅ No duplicate trial events found (good)")
    
    print()

def check_incomplete_attribution(cursor, campaign_id, start_date, end_date):
    """Check for trial events that might have campaign attribution but user doesn't"""
    print("4️⃣ INCOMPLETE ATTRIBUTION CHECK")
    print("-" * 40)
    
    # Look for trial events with campaign attribution where user record doesn't have it
    cursor.execute("""
        SELECT DISTINCT
            e.distinct_id,
            e.abi_campaign_id as event_campaign_id,
            u.abi_campaign_id as user_campaign_id,
            e.event_time,
            u.has_abi_attribution
        FROM mixpanel_event e
        LEFT JOIN mixpanel_user u ON e.distinct_id = u.distinct_id
        WHERE e.abi_campaign_id = ?
          AND e.event_name = 'RC Trial started'
          AND DATE(e.event_time) BETWEEN ? AND ?
          AND (u.abi_campaign_id IS NULL OR u.abi_campaign_id != ?)
        LIMIT 10
    """, [campaign_id, start_date, end_date, campaign_id])
    
    mismatched_attribution = cursor.fetchall()
    
    if mismatched_attribution:
        print(f"   ⚠️  Events with campaign attribution but user record mismatch:")
        for row in mismatched_attribution:
            print(f"      👤 {row['distinct_id'][:12]}...: Event has campaign, user doesn't")
            print(f"         Event campaign: {row['event_campaign_id']}")
            print(f"         User campaign: {row['user_campaign_id']}")
        
        print(f"   💡 POTENTIAL CAUSE: {len(mismatched_attribution)} trials with attribution mismatch")
    else:
        print(f"   ✅ No attribution mismatches found")
    
    print()

def check_missing_events(cursor, campaign_id, start_date, end_date):
    """Check for patterns in missing events"""
    print("5️⃣ MISSING EVENTS PATTERN CHECK")
    print("-" * 40)
    
    # Check if there are users with attribution but no trial events at all
    cursor.execute("""
        SELECT 
            u.distinct_id,
            u.first_seen,
            u.last_updated,
            COUNT(e.event_uuid) as total_events,
            COUNT(CASE WHEN e.event_name = 'RC Trial started' THEN 1 END) as trial_events
        FROM mixpanel_user u
        LEFT JOIN mixpanel_event e ON u.distinct_id = e.distinct_id
        WHERE u.abi_campaign_id = ?
        GROUP BY u.distinct_id, u.first_seen, u.last_updated
        HAVING trial_events = 0
        ORDER BY u.first_seen DESC
        LIMIT 10
    """, [campaign_id])
    
    users_without_trials = cursor.fetchall()
    
    if users_without_trials:
        print(f"   👥 Users with campaign attribution but NO trial events:")
        for row in users_without_trials:
            print(f"      👤 {row['distinct_id'][:12]}...: {row['total_events']} total events, 0 trials")
            print(f"         First seen: {row['first_seen']}")
        
        print(f"   📊 Total users with campaign but no trials: {len(users_without_trials)}")
        
        # These users might represent the missing trials if they should have trial events
        if len(users_without_trials) >= 9:
            print(f"   💡 POTENTIAL CAUSE: Missing trial events for attributed users")
    else:
        print(f"   ✅ All attributed users have at least some events")
    
    print()

if __name__ == "__main__":
    exit(main()) 