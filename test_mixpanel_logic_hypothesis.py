#!/usr/bin/env python3
"""
Test Mixpanel Logic Hypothesis

Test: Users with trial events OR users first seen in date range = 41?
"""

import sqlite3
from pathlib import Path
import sys

# Add utils directory to path for database utilities
utils_path = str(Path(__file__).resolve().parent / "utils")
sys.path.append(utils_path)
from database_utils import get_database_path

def main():
    """Test our hypothesis about Mixpanel's logic"""
    
    print("🧪 TESTING MIXPANEL LOGIC HYPOTHESIS")
    print("=" * 50)
    
    campaign_id = "120223331225260178"
    start_date = "2025-07-16"
    end_date = "2025-07-29"
    
    with sqlite3.connect(get_database_path('mixpanel_data')) as conn:
        cursor = conn.cursor()
        
        # Test our hypothesis: Trial events OR first seen in range
        test_hypothesis(cursor, campaign_id, start_date, end_date)
        
        # Break down the components
        analyze_components(cursor, campaign_id, start_date, end_date)
        
        # Verify with our CSV data
        verify_with_csv_data(cursor, campaign_id, start_date, end_date)
    
    return 0

def test_hypothesis(cursor, campaign_id, start_date, end_date):
    """Test the hypothesis: trial events OR first seen in range"""
    print("1️⃣ TESTING HYPOTHESIS...")
    
    # Our hypothesis query
    cursor.execute("""
        SELECT COUNT(DISTINCT u.distinct_id) as total_count
        FROM mixpanel_user u
        WHERE u.abi_campaign_id = ?
          AND u.has_abi_attribution = TRUE
          AND (
              -- Users with trial events in date range
              EXISTS (
                  SELECT 1 FROM mixpanel_event e 
                  WHERE e.distinct_id = u.distinct_id 
                  AND e.event_name = 'RC Trial started'
                  AND DATE(e.event_time) BETWEEN ? AND ?
              )
              OR
              -- Users first seen in date range
              DATE(u.first_seen) BETWEEN ? AND ?
          )
    """, [campaign_id, start_date, end_date, start_date, end_date])
    
    hypothesis_count = cursor.fetchone()[0]
    print(f"   🎯 Hypothesis result: {hypothesis_count} users")
    print(f"   🎯 Mixpanel shows: 41 users")
    print(f"   🎯 Match: {'✅ YES!' if hypothesis_count == 41 else '❌ No'}")

def analyze_components(cursor, campaign_id, start_date, end_date):
    """Break down the components of our hypothesis"""
    print(f"\n2️⃣ COMPONENT BREAKDOWN...")
    
    # Component 1: Users with trial events
    cursor.execute("""
        SELECT COUNT(DISTINCT u.distinct_id) as trial_users
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
    
    trial_users = cursor.fetchone()[0]
    print(f"   📊 Users with trial events: {trial_users}")
    
    # Component 2: Users first seen in range
    cursor.execute("""
        SELECT COUNT(DISTINCT u.distinct_id) as first_seen_users
        FROM mixpanel_user u
        WHERE u.abi_campaign_id = ?
          AND u.has_abi_attribution = TRUE
          AND DATE(u.first_seen) BETWEEN ? AND ?
    """, [campaign_id, start_date, end_date])
    
    first_seen_users = cursor.fetchone()[0]
    print(f"   📊 Users first seen in range: {first_seen_users}")
    
    # Component 3: Overlap
    cursor.execute("""
        SELECT COUNT(DISTINCT u.distinct_id) as overlap_users
        FROM mixpanel_user u
        WHERE u.abi_campaign_id = ?
          AND u.has_abi_attribution = TRUE
          AND DATE(u.first_seen) BETWEEN ? AND ?
          AND EXISTS (
              SELECT 1 FROM mixpanel_event e 
              WHERE e.distinct_id = u.distinct_id 
              AND e.event_name = 'RC Trial started'
              AND DATE(e.event_time) BETWEEN ? AND ?
          )
    """, [campaign_id, start_date, end_date, start_date, end_date])
    
    overlap_users = cursor.fetchone()[0]
    print(f"   📊 Users in BOTH categories: {overlap_users}")
    
    # Calculate unique contribution
    unique_trial = trial_users - overlap_users
    unique_first_seen = first_seen_users - overlap_users
    total_unique = unique_trial + unique_first_seen + overlap_users
    
    print(f"\n   🧮 MATH CHECK:")
    print(f"   • Unique trial-only users: {unique_trial}")
    print(f"   • Unique first-seen-only users: {unique_first_seen}")
    print(f"   • Users in both categories: {overlap_users}")
    print(f"   • Total unique users: {total_unique}")

def verify_with_csv_data(cursor, campaign_id, start_date, end_date):
    """Verify our hypothesis explains the CSV data"""
    print(f"\n3️⃣ CSV DATA VERIFICATION...")
    
    # Count users from our CSV who are in the hypothesis result
    csv_distinct_ids = read_csv_distinct_ids()
    
    if not csv_distinct_ids:
        print("   ❌ Could not read CSV data")
        return
    
    # Check how many CSV users match our hypothesis
    found_users = []
    for distinct_id in csv_distinct_ids:
        cursor.execute("""
            SELECT u.distinct_id, 
                   DATE(u.first_seen) BETWEEN ? AND ? as first_seen_in_range,
                   EXISTS (
                       SELECT 1 FROM mixpanel_event e 
                       WHERE e.distinct_id = u.distinct_id 
                       AND e.event_name = 'RC Trial started'
                       AND DATE(e.event_time) BETWEEN ? AND ?
                   ) as has_trial_events
            FROM mixpanel_user u
            WHERE u.distinct_id = ?
              AND u.abi_campaign_id = ?
              AND u.has_abi_attribution = TRUE
        """, [start_date, end_date, start_date, end_date, distinct_id, campaign_id])
        
        result = cursor.fetchone()
        if result:
            found_users.append({
                'distinct_id': result[0],
                'first_seen_in_range': bool(result[1]),
                'has_trial_events': bool(result[2])
            })
    
    print(f"   📊 CSV users found in our DB: {len(found_users)}/{len(csv_distinct_ids)}")
    
    # Categorize the found users
    trial_only = [u for u in found_users if u['has_trial_events'] and not u['first_seen_in_range']]
    first_seen_only = [u for u in found_users if u['first_seen_in_range'] and not u['has_trial_events']]
    both = [u for u in found_users if u['has_trial_events'] and u['first_seen_in_range']]
    neither = [u for u in found_users if not u['has_trial_events'] and not u['first_seen_in_range']]
    
    print(f"   📊 CSV breakdown:")
    print(f"   • Trial events only: {len(trial_only)}")
    print(f"   • First seen only: {len(first_seen_only)}")
    print(f"   • Both criteria: {len(both)}")
    print(f"   • Neither (shouldn't be counted): {len(neither)}")
    
    total_should_count = len(trial_only) + len(first_seen_only) + len(both)
    print(f"   • Total that should count: {total_should_count}")
    
    if neither:
        print(f"   ⚠️  Users that shouldn't count but are in CSV:")
        for user in neither[:3]:
            print(f"      - {user['distinct_id'][:30]}...")

def read_csv_distinct_ids():
    """Read distinct_ids from CSV"""
    import csv
    distinct_ids = []
    try:
        with open('mixpanel_user.csv', 'r') as file:
            csv_reader = csv.DictReader(file)
            for row in csv_reader:
                if 'Distinct ID' in row and row['Distinct ID'].strip():
                    distinct_ids.append(row['Distinct ID'].strip())
        return distinct_ids
    except Exception as e:
        print(f"   ❌ Error reading CSV: {e}")
        return []

if __name__ == "__main__":
    exit(main()) 