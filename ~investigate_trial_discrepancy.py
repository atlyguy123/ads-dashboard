#!/usr/bin/env python3
"""
Detective script to investigate trial count discrepancy for campaign:
ppc_atly_fb_advantage_tier1_ROAS_May_25 Campaign (ID: 120223331225260178)

Dashboard shows: 42 trials
CSV shows: 41 users
Date range: July 16-29, 2025
"""

import sqlite3
import json
import csv
from pathlib import Path
from datetime import datetime
from typing import Set, List, Dict, Any

# Configuration
CAMPAIGN_ID = "120223331225260178"
CAMPAIGN_NAME = "ppc_atly_fb_advantage_tier1_ROAS_May_25 Campaign"
START_DATE = "2025-07-16"
END_DATE = "2025-07-29"
CSV_FILE = "mixpanel_user.csv"

def get_database_path():
    """Get the mixpanel database path"""
    # Try common database locations
    possible_paths = [
        "database/mixpanel_data.db",
        "pipelines/mixpanel_pipeline/database/mixpanel_data.db",
        "orchestrator/database/mixpanel_data.db",
        "../database/mixpanel_data.db"
    ]
    
    for path in possible_paths:
        if Path(path).exists():
            return path
    
    # If none found, check environment or use relative path
    return "database/mixpanel_data.db"

def load_csv_users() -> Set[str]:
    """Load distinct IDs from the CSV file"""
    csv_users = set()
    with open(CSV_FILE, 'r') as f:
        reader = csv.DictReader(f)
        for row in reader:
            distinct_id = row.get('Distinct ID', '').strip()
            if distinct_id:
                csv_users.add(distinct_id)
    return csv_users

def query_dashboard_trial_logic(db_path: str) -> List[Dict[str, Any]]:
    """Execute the exact same query the dashboard uses for trial counts"""
    
    with sqlite3.connect(db_path) as conn:
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()
        
        # This is the exact query from analytics_query_service.py line 1189-1200
        query = """
        SELECT 
            u.distinct_id,
            e.event_name,
            e.event_time,
            e.event_uuid,
            DATE(e.event_time) as event_date,
            u.abi_campaign_id
        FROM mixpanel_user u
        LEFT JOIN mixpanel_event e ON u.distinct_id = e.distinct_id
        WHERE u.abi_campaign_id = ?
          AND u.has_abi_attribution = TRUE
          AND e.event_name = 'RC Trial started'
          AND DATE(e.event_time) BETWEEN ? AND ?
        ORDER BY e.event_time
        """
        
        cursor.execute(query, [CAMPAIGN_ID, START_DATE, END_DATE])
        return [dict(row) for row in cursor.fetchall()]

def query_all_campaign_users(db_path: str) -> List[Dict[str, Any]]:
    """Get all users for this campaign regardless of trial events"""
    
    with sqlite3.connect(db_path) as conn:
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()
        
        query = """
        SELECT 
            u.distinct_id,
            u.abi_campaign_id,
            u.has_abi_attribution,
            u.first_seen
        FROM mixpanel_user u
        WHERE u.abi_campaign_id = ?
          AND u.has_abi_attribution = TRUE
        ORDER BY u.first_seen
        """
        
        cursor.execute(query, [CAMPAIGN_ID])
        return [dict(row) for row in cursor.fetchall()]

def main():
    print(f"🔍 INVESTIGATING TRIAL DISCREPANCY")
    print(f"Campaign: {CAMPAIGN_NAME}")
    print(f"ID: {CAMPAIGN_ID}")
    print(f"Date Range: {START_DATE} to {END_DATE}")
    print("=" * 80)
    
    # Step 1: Load CSV data
    print("\n📄 Step 1: Loading CSV data...")
    csv_users = load_csv_users()
    print(f"CSV contains {len(csv_users)} distinct users")
    print(f"Sample CSV users: {list(csv_users)[:5]}")
    
    # Step 2: Query database for trial events (dashboard logic)
    print("\n🗃️ Step 2: Querying database with dashboard logic...")
    db_path = get_database_path()
    print(f"Using database: {db_path}")
    
    if not Path(db_path).exists():
        print(f"❌ Database not found at {db_path}")
        return
    
    trial_events = query_dashboard_trial_logic(db_path)
    print(f"Database trial events: {len(trial_events)}")
    
    # Get unique users from trial events
    db_trial_users = set()
    for event in trial_events:
        db_trial_users.add(event['distinct_id'])
    
    print(f"Unique trial users in database: {len(db_trial_users)}")
    
    # Step 3: Compare datasets
    print("\n🔍 Step 3: Comparing datasets...")
    
    # Users in database but not in CSV
    db_only = db_trial_users - csv_users
    # Users in CSV but not in database  
    csv_only = csv_users - db_trial_users
    # Users in both
    common_users = db_trial_users & csv_users
    
    print(f"Users in both database and CSV: {len(common_users)}")
    print(f"Users in database but NOT in CSV: {len(db_only)}")
    print(f"Users in CSV but NOT in database: {len(csv_only)}")
    
    # Step 4: Detailed analysis
    print("\n📊 Step 4: Detailed Analysis...")
    
    if db_only:
        print(f"\n🚨 EXTRA USERS IN DATABASE (potential cause of +1):")
        for user in db_only:
            # Find events for this user
            user_events = [e for e in trial_events if e['distinct_id'] == user]
            for event in user_events:
                print(f"  - User: {user}")
                print(f"    Event Time: {event['event_time']}")
                print(f"    Event Date: {event['event_date']}")
                print(f"    Event UUID: {event['event_uuid']}")
                print(f"    Campaign ID: {event.get('abi_campaign_id', 'N/A')}")
    
    if csv_only:
        print(f"\n🚨 EXTRA USERS IN CSV (not found in database trials):")
        for user in csv_only:
            print(f"  - User: {user}")
    
    # Step 5: Check all users for this campaign
    print("\n👥 Step 5: All campaign users analysis...")
    all_campaign_users = query_all_campaign_users(db_path)
    print(f"Total users with attribution for this campaign: {len(all_campaign_users)}")
    
    # Compare with CSV total
    all_db_users = set(user['distinct_id'] for user in all_campaign_users)
    print(f"CSV users: {len(csv_users)}")
    print(f"DB users: {len(all_db_users)}")
    
    # Users in DB but not CSV (campaign level)
    campaign_db_only = all_db_users - csv_users
    campaign_csv_only = csv_users - all_db_users
    
    if campaign_db_only:
        print(f"\n🔍 Users in DB campaign but not in CSV:")
        for user in campaign_db_only:
            user_info = next((u for u in all_campaign_users if u['distinct_id'] == user), None)
            if user_info:
                print(f"  - {user} (first_seen: {user_info.get('first_seen', 'N/A')})")
    
    if campaign_csv_only:
        print(f"\n🔍 Users in CSV but not in DB campaign:")
        for user in campaign_csv_only:
            print(f"  - {user}")
    
    # Step 6: Summary and conclusion
    print("\n" + "=" * 80)
    print("📋 SUMMARY:")
    print(f"Dashboard shows: 42 trials")
    print(f"CSV shows: {len(csv_users)} users")
    print(f"Database trial events: {len(trial_events)} events")
    print(f"Database unique trial users: {len(db_trial_users)} users")
    
    if len(db_trial_users) == 42 and len(csv_users) == 41:
        print("✅ Confirmed: Database has 42 trial users, CSV has 41")
        if db_only:
            print(f"🎯 Root cause: User(s) {db_only} exist in database but not in CSV")
        else:
            print("❓ Unexpected: No obvious discrepancy found")
    else:
        print("🤔 Different discrepancy pattern than expected")

if __name__ == "__main__":
    main() 