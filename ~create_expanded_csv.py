#!/usr/bin/env python3
"""
1. Verify if the 41 CSV users are actually within the 45 database users
2. Create an expanded CSV with all 45 users in the same format as the original
"""

import sqlite3
import csv
import json
from typing import Set, List, Dict, Any
from datetime import datetime

CAMPAIGN_ID = "120223331225260178"
CAMPAIGN_NAME = "ppc_atly_fb_advantage_tier1_ROAS_May_25 Campaign"
START_DATE = "2025-07-16"
END_DATE = "2025-07-29"

def get_database_path():
    return "database/mixpanel_data.db"

def load_csv_users() -> Set[str]:
    """Load distinct IDs from the original CSV"""
    csv_users = set()
    with open("mixpanel_user.csv", 'r') as f:
        reader = csv.DictReader(f)
        for row in reader:
            distinct_id = row.get('Distinct ID', '').strip()
            if distinct_id:
                csv_users.add(distinct_id)
    return csv_users

def get_all_database_users_with_trials() -> List[Dict[str, Any]]:
    """Get all users with trial events from database - same query logic as before"""
    db_path = get_database_path()
    
    with sqlite3.connect(db_path) as conn:
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()
        
        # Get all trial events for this campaign within date range
        query = """
        SELECT 
            e.event_uuid as insert_id,
            e.event_time,
            e.distinct_id,
            u.abi_campaign_id,
            u.abi_ad_id,
            u.abi_ad_set_id,
            u.profile_json,
            DATE(e.event_time) as event_date,
            strftime('%Y-%m-%dT%H:%M:%S', e.event_time) as formatted_time
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

def extract_user_id_from_profile(profile_json: str) -> str:
    """Extract user_id from profile JSON if available"""
    if not profile_json:
        return ""
    
    try:
        data = json.loads(profile_json)
        # Look for user_id in various places
        return (data.get('properties', {}).get('user_id') or
                data.get('user_id') or
                data.get('properties', {}).get('$user_id') or
                "")
    except json.JSONDecodeError:
        return ""

def create_expanded_csv(database_events: List[Dict[str, Any]]) -> None:
    """Create CSV with all database users in the same format as original"""
    
    with open("expanded_mixpanel_user.csv", 'w', newline='') as f:
        # Use same headers as original CSV
        fieldnames = [
            'Time',
            'Insert ID', 
            'abi_~campaign',
            'abi_~campaign_id',
            'User ID',
            'Distinct ID',
            'Uniques of RC Trial started'
        ]
        
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        
        for event in database_events:
            # Extract user_id from profile JSON
            user_id = extract_user_id_from_profile(event['profile_json'])
            
            # Format time to match original CSV format (2025-07-16T01:01:06)
            formatted_time = event['formatted_time']
            
            row = {
                'Time': formatted_time,
                'Insert ID': event['insert_id'],
                'abi_~campaign': CAMPAIGN_NAME,
                'abi_~campaign_id': event['abi_campaign_id'],
                'User ID': user_id,
                'Distinct ID': event['distinct_id'],
                'Uniques of RC Trial started': 1
            }
            
            writer.writerow(row)

def main():
    print("🔍 VERIFYING CSV vs DATABASE USER OVERLAP")
    print("=" * 60)
    
    # Step 1: Load original CSV users
    print("📄 Step 1: Loading original CSV users...")
    csv_users = load_csv_users()
    print(f"Original CSV contains: {len(csv_users)} unique users")
    
    # Step 2: Get all database users with trials
    print("\n🗃️ Step 2: Loading database users with trials...")
    db_events = get_all_database_users_with_trials()
    db_users = set(event['distinct_id'] for event in db_events)
    
    print(f"Database contains: {len(db_events)} trial events")
    print(f"Database contains: {len(db_users)} unique users")
    
    # Step 3: Check overlap
    print(f"\n🔍 Step 3: Analyzing overlap...")
    
    # Users in CSV but NOT in database
    csv_only = csv_users - db_users
    # Users in database but NOT in CSV
    db_only = db_users - csv_users
    # Users in both
    overlap = csv_users & db_users
    
    print(f"Users in CSV AND database: {len(overlap)}")
    print(f"Users in CSV but NOT in database: {len(csv_only)}")
    print(f"Users in database but NOT in CSV: {len(db_only)}")
    
    # Step 4: Show the differences
    if csv_only:
        print(f"\n⚠️ Users in CSV but NOT in database:")
        for user in list(csv_only)[:10]:  # Show first 10
            print(f"  - {user}")
        if len(csv_only) > 10:
            print(f"  ... and {len(csv_only) - 10} more")
    
    if db_only:
        print(f"\n⚠️ Users in database but NOT in CSV:")
        for user in list(db_only)[:10]:  # Show first 10
            print(f"  - {user}")
        if len(db_only) > 10:
            print(f"  ... and {len(db_only) - 10} more")
    
    # Step 5: Verification
    print(f"\n✅ VERIFICATION:")
    if len(csv_only) == 0:
        print("✅ All CSV users are found in database")
        if len(overlap) == 41:
            print("✅ Perfect match: 41 CSV users are subset of 45 database users")
        else:
            print(f"⚠️ Unexpected: CSV has {len(csv_users)} users, overlap is {len(overlap)}")
    else:
        print(f"❌ {len(csv_only)} CSV users are NOT in database - this suggests different data sources!")
    
    # Step 6: Create expanded CSV
    print(f"\n📄 Step 6: Creating expanded CSV...")
    create_expanded_csv(db_events)
    print(f"✅ Created 'expanded_mixpanel_user.csv' with {len(db_events)} events ({len(db_users)} unique users)")
    
    # Step 7: Summary
    print(f"\n📊 FINAL SUMMARY:")
    print(f"Original CSV: {len(csv_users)} users")
    print(f"Database query: {len(db_users)} users") 
    print(f"Overlap: {len(overlap)} users")
    print(f"CSV-only: {len(csv_only)} users")
    print(f"Database-only: {len(db_only)} users")
    
    if len(csv_users) == len(overlap):
        print("✅ Confirmed: CSV users are a perfect subset of database users")
    else:
        print("❌ Warning: CSV users are NOT a subset of database users")

if __name__ == "__main__":
    main() 