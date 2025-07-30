#!/usr/bin/env python3
"""
Detailed investigation: Find the 4 extra users in database vs Mixpanel CSV
and examine their events, dates, and attribution in detail.
"""

import sqlite3
import csv
from datetime import datetime
from typing import Set, List, Dict, Any

# Configuration
CAMPAIGN_ID = "120223331225260178"
START_DATE = "2025-07-16"
END_DATE = "2025-07-29"
CSV_FILE = "mixpanel_user.csv"

def get_database_path():
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

def get_all_database_trial_users() -> List[Dict[str, Any]]:
    """Get all users with trial events from database with full details"""
    db_path = get_database_path()
    
    with sqlite3.connect(db_path) as conn:
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()
        
        query = """
        SELECT 
            u.distinct_id,
            u.abi_campaign_id,
            u.has_abi_attribution,
            u.first_seen,
            e.event_uuid,
            e.event_name,
            e.event_time,
            DATE(e.event_time) as event_date,
            TIME(e.event_time) as event_time_only,
            strftime('%Y-%m-%d %H:%M:%S', e.event_time) as formatted_event_time
        FROM mixpanel_user u
        LEFT JOIN mixpanel_event e ON u.distinct_id = e.distinct_id
        WHERE u.abi_campaign_id = ?
          AND u.has_abi_attribution = TRUE
          AND e.event_name = 'RC Trial started'
          AND DATE(e.event_time) BETWEEN ? AND ?
        ORDER BY e.event_time
        """
        
        params = [CAMPAIGN_ID, START_DATE, END_DATE]
        cursor.execute(query, params)
        return [dict(row) for row in cursor.fetchall()]

def test_different_date_boundaries() -> Dict[str, int]:
    """Test different date boundary interpretations"""
    db_path = get_database_path()
    results = {}
    
    with sqlite3.connect(db_path) as conn:
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()
        
        base_query = """
        SELECT COUNT(DISTINCT u.distinct_id) as count
        FROM mixpanel_user u
        LEFT JOIN mixpanel_event e ON u.distinct_id = e.distinct_id
        WHERE u.abi_campaign_id = ?
          AND u.has_abi_attribution = TRUE
          AND e.event_name = 'RC Trial started'
          AND {}
        """
        
        # Test different boundary conditions
        boundary_tests = {
            "BETWEEN inclusive": "DATE(e.event_time) BETWEEN '2025-07-16' AND '2025-07-29'",
            "Greater than/less than inclusive": "DATE(e.event_time) >= '2025-07-16' AND DATE(e.event_time) <= '2025-07-29'",
            "Exclusive end": "DATE(e.event_time) >= '2025-07-16' AND DATE(e.event_time) < '2025-07-29'",
            "Exclusive start": "DATE(e.event_time) > '2025-07-16' AND DATE(e.event_time) <= '2025-07-29'",
            "Both exclusive": "DATE(e.event_time) > '2025-07-16' AND DATE(e.event_time) < '2025-07-29'",
        }
        
        for test_name, condition in boundary_tests.items():
            query = base_query.format(condition)
            cursor.execute(query, [CAMPAIGN_ID])
            result = cursor.fetchone()
            results[test_name] = result['count']
    
    return results

def main():
    print("🔍 DETAILED INVESTIGATION: Finding Extra Users")
    print("Campaign:", CAMPAIGN_ID)
    print("Date Range:", START_DATE, "to", END_DATE)
    print("=" * 80)
    
    # Step 1: Get CSV users and database users
    print("\n📄 Step 1: Loading data...")
    csv_users = load_csv_users()
    db_trial_events = get_all_database_trial_users()
    
    # Get unique users from database
    db_users = set(event['distinct_id'] for event in db_trial_events)
    
    print(f"CSV users: {len(csv_users)}")
    print(f"Database users with trials: {len(db_users)}")
    print(f"Total database trial events: {len(db_trial_events)}")
    
    # Step 2: Find the extra users
    extra_users = db_users - csv_users
    missing_users = csv_users - db_users
    
    print(f"\n🔍 Step 2: User comparison")
    print(f"Users in DB but NOT in CSV: {len(extra_users)}")
    print(f"Users in CSV but NOT in DB: {len(missing_users)}")
    
    # Step 3: Detailed analysis of extra users
    if extra_users:
        print(f"\n🚨 DETAILED ANALYSIS OF {len(extra_users)} EXTRA USERS:")
        print("=" * 60)
        
        for user_id in sorted(extra_users):
            print(f"\n👤 User: {user_id}")
            
            # Find all events for this user
            user_events = [e for e in db_trial_events if e['distinct_id'] == user_id]
            
            for event in user_events:
                print(f"   📅 Event Time: {event['formatted_event_time']}")
                print(f"   📆 Event Date: {event['event_date']}")
                print(f"   🕐 Time Only: {event['event_time_only']}")
                print(f"   🆔 Event UUID: {event['event_uuid']}")
                print(f"   🎯 Campaign ID: {event['abi_campaign_id']}")
                print(f"   ✅ Has Attribution: {event['has_abi_attribution']}")
                print(f"   👁️ First Seen: {event['first_seen']}")
                
                # Check if it's a boundary date
                if event['event_date'] in ['2025-07-16', '2025-07-29']:
                    print(f"   ⚠️  BOUNDARY DATE: {event['event_date']}")
                
                print()
    
    # Step 4: Test different date boundary interpretations
    print("\n📊 Step 4: Testing Different Date Boundary Logic")
    print("=" * 50)
    boundary_results = test_different_date_boundaries()
    
    for test_name, count in boundary_results.items():
        marker = " ← MATCHES DASHBOARD" if count == 42 else " ← MATCHES CSV" if count == 41 else ""
        print(f"{test_name}: {count} users{marker}")
    
    # Step 5: Events by date breakdown
    print("\n📈 Step 5: Events by Date Breakdown")
    print("=" * 40)
    events_by_date = {}
    for event in db_trial_events:
        date = event['event_date']
        if date not in events_by_date:
            events_by_date[date] = []
        events_by_date[date].append(event)
    
    for date in sorted(events_by_date.keys()):
        events = events_by_date[date]
        unique_users = len(set(e['distinct_id'] for e in events))
        print(f"{date}: {len(events)} events, {unique_users} unique users")
        
        # Show boundary dates in detail
        if date in ['2025-07-16', '2025-07-29']:
            print(f"  ⚠️  BOUNDARY DATE DETAILS:")
            for event in events:
                print(f"    Time: {event['formatted_event_time']} | User: {event['distinct_id'][:12]}...")

if __name__ == "__main__":
    main() 