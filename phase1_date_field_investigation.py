#!/usr/bin/env python3
"""
PHASE 1: TASK 1.1 - Systematic Date Field Testing

Objective: Test each potential date field to identify which one produces 
the expected 39 results that match Mixpanel UI.

Campaign: ppc_atly_fb_advantage_tier1_ROAS_May_25 Campaign
Campaign ID: 120223331225260178
Date Range: July 16-29, 2025
Expected Result: 39 trials (from Mixpanel UI)
Current Result: 30 trials (from our dashboard)
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
    """Execute systematic date field testing"""
    
    # Test case details
    campaign_id = "120223331225260178"
    start_date = "2025-07-16"
    end_date = "2025-07-29"
    expected_count = 39  # From Mixpanel UI
    current_count = 30   # From our dashboard
    
    print("🔍 PHASE 1: SYSTEMATIC DATE FIELD TESTING")
    print("=" * 70)
    print(f"📊 Campaign ID: {campaign_id}")
    print(f"📅 Date Range: {start_date} to {end_date}")
    print(f"🎯 Expected Count (Mixpanel UI): {expected_count}")
    print(f"📊 Current Count (Our Dashboard): {current_count}")
    print(f"❓ Discrepancy: {expected_count - current_count}")
    print()
    
    try:
        with sqlite3.connect(get_database_path('mixpanel_data')) as conn:
            conn.row_factory = sqlite3.Row
            cursor = conn.cursor()
            
            # Test all potential date fields
            test_results = test_all_date_fields(cursor, campaign_id, start_date, end_date)
            
            # Analyze results
            analyze_results(test_results, expected_count, current_count)
            
            # Additional investigations based on initial results
            perform_additional_investigations(cursor, campaign_id, start_date, end_date, test_results)
            
    except Exception as e:
        print(f"❌ Error: {e}")
        return 1
        
    return 0

def test_all_date_fields(cursor, campaign_id, start_date, end_date):
    """Test all potential date fields that Mixpanel might use"""
    
    print("🧪 TESTING ALL POTENTIAL DATE FIELDS")
    print("-" * 50)
    
    test_queries = {
        'event_time_current': {
            'description': 'Current Logic: Event time filtering (e.event_time)',
            'query': """
                SELECT 
                    COUNT(DISTINCT CASE WHEN e.event_name = 'RC Trial started' 
                                      AND DATE(e.event_time) BETWEEN ? AND ? 
                                      THEN u.distinct_id END) as trial_count
                FROM mixpanel_user u
                LEFT JOIN mixpanel_event e ON u.distinct_id = e.distinct_id
                WHERE u.abi_campaign_id = ?
                  AND u.has_abi_attribution = TRUE
            """,
            'params': [start_date, end_date, campaign_id]
        },
        
        'user_first_seen': {
            'description': 'User First Seen: Filter by when user was first seen (u.first_seen)',
            'query': """
                SELECT 
                    COUNT(DISTINCT CASE WHEN e.event_name = 'RC Trial started' 
                                      AND DATE(u.first_seen) BETWEEN ? AND ? 
                                      THEN u.distinct_id END) as trial_count
                FROM mixpanel_user u
                LEFT JOIN mixpanel_event e ON u.distinct_id = e.distinct_id
                WHERE u.abi_campaign_id = ?
                  AND u.has_abi_attribution = TRUE
            """,
            'params': [start_date, end_date, campaign_id]
        },
        
        'credited_date': {
            'description': 'Credited Date: Filter by credited date from user_product_metrics',
            'query': """
                SELECT 
                    COUNT(DISTINCT u.distinct_id) as trial_count
                FROM user_product_metrics upm
                JOIN mixpanel_user u ON upm.distinct_id = u.distinct_id
                WHERE u.abi_campaign_id = ?
                  AND upm.credited_date BETWEEN ? AND ?
                  AND EXISTS (
                      SELECT 1 FROM mixpanel_event e 
                      WHERE e.distinct_id = u.distinct_id 
                      AND e.event_name = 'RC Trial started'
                  )
            """,
            'params': [campaign_id, start_date, end_date]
        },
        
        'profile_install_date': {
            'description': 'Profile Install Date: Filter by first_install_date from profile JSON',
            'query': """
                SELECT 
                    COUNT(DISTINCT CASE WHEN e.event_name = 'RC Trial started' 
                                      AND DATE(JSON_EXTRACT(u.profile_json, '$.first_install_date')) BETWEEN ? AND ? 
                                      THEN u.distinct_id END) as trial_count
                FROM mixpanel_user u
                LEFT JOIN mixpanel_event e ON u.distinct_id = e.distinct_id
                WHERE u.abi_campaign_id = ?
                  AND u.has_abi_attribution = TRUE
                  AND JSON_EXTRACT(u.profile_json, '$.first_install_date') IS NOT NULL
            """,
            'params': [start_date, end_date, campaign_id]
        },
        
        'user_last_updated': {
            'description': 'User Last Updated: Filter by when user record was last updated',
            'query': """
                SELECT 
                    COUNT(DISTINCT CASE WHEN e.event_name = 'RC Trial started' 
                                      AND DATE(u.last_updated) BETWEEN ? AND ? 
                                      THEN u.distinct_id END) as trial_count
                FROM mixpanel_user u
                LEFT JOIN mixpanel_event e ON u.distinct_id = e.distinct_id
                WHERE u.abi_campaign_id = ?
                  AND u.has_abi_attribution = TRUE
            """,
            'params': [start_date, end_date, campaign_id]
        },
        
        'any_trial_by_user_first_seen': {
            'description': 'Any Trial by First Seen: Users who first appeared in date range and have ANY trial',
            'query': """
                SELECT 
                    COUNT(DISTINCT u.distinct_id) as trial_count
                FROM mixpanel_user u
                WHERE u.abi_campaign_id = ?
                  AND u.has_abi_attribution = TRUE
                  AND DATE(u.first_seen) BETWEEN ? AND ?
                  AND EXISTS (
                      SELECT 1 FROM mixpanel_event e 
                      WHERE e.distinct_id = u.distinct_id 
                      AND e.event_name = 'RC Trial started'
                  )
            """,
            'params': [campaign_id, start_date, end_date]
        },
        
        'any_trial_by_credited_date': {
            'description': 'Any Trial by Credited Date: Users credited in date range who have ANY trial',
            'query': """
                SELECT 
                    COUNT(DISTINCT u.distinct_id) as trial_count
                FROM user_product_metrics upm
                JOIN mixpanel_user u ON upm.distinct_id = u.distinct_id
                WHERE u.abi_campaign_id = ?
                  AND upm.credited_date BETWEEN ? AND ?
                  AND EXISTS (
                      SELECT 1 FROM mixpanel_event e 
                      WHERE e.distinct_id = u.distinct_id 
                      AND e.event_name = 'RC Trial started'
                  )
            """,
            'params': [campaign_id, start_date, end_date]
        }
    }
    
    results = {}
    
    for test_name, test_config in test_queries.items():
        try:
            print(f"   🧪 Testing: {test_name}")
            print(f"      📝 {test_config['description']}")
            
            cursor.execute(test_config['query'], test_config['params'])
            result = cursor.fetchone()
            count = result['trial_count']
            
            results[test_name] = {
                'count': count,
                'description': test_config['description'],
                'query': test_config['query'],
                'params': test_config['params']
            }
            
            # Highlight matches
            if count == 39:
                print(f"      🎯 MATCH! Count: {count} ⭐⭐⭐")
            elif count == 30:
                print(f"      📊 Current: {count}")
            else:
                print(f"      📊 Count: {count}")
            
        except Exception as e:
            print(f"      ❌ Error: {e}")
            results[test_name] = {
                'count': 'ERROR',
                'description': test_config['description'],
                'error': str(e)
            }
        
        print()
    
    return results

def analyze_results(test_results, expected_count, current_count):
    """Analyze the test results and identify potential matches"""
    
    print("📊 RESULTS ANALYSIS")
    print("-" * 30)
    
    matches = []
    near_matches = []
    
    for test_name, result in test_results.items():
        if result['count'] == 'ERROR':
            continue
            
        count = result['count']
        
        if count == expected_count:
            matches.append((test_name, count, result['description']))
        elif abs(count - expected_count) <= 3:  # Within 3 is "close"
            near_matches.append((test_name, count, result['description']))
    
    print(f"🎯 EXACT MATCHES (count = {expected_count}):")
    if matches:
        for test_name, count, description in matches:
            print(f"   ✅ {test_name}: {count}")
            print(f"      💡 {description}")
    else:
        print("   ❌ No exact matches found")
    
    print()
    print(f"🎯 NEAR MATCHES (within 3 of {expected_count}):")
    if near_matches:
        for test_name, count, description in near_matches:
            diff = abs(count - expected_count)
            print(f"   🔸 {test_name}: {count} (off by {diff})")
            print(f"      💡 {description}")
    else:
        print("   ❌ No near matches found")
    
    print()
    print("📊 ALL RESULTS SUMMARY:")
    for test_name, result in test_results.items():
        if result['count'] != 'ERROR':
            count = result['count']
            diff = abs(count - expected_count) if isinstance(count, int) else 'N/A'
            print(f"   {test_name}: {count} (diff: {diff})")

def perform_additional_investigations(cursor, campaign_id, start_date, end_date, test_results):
    """Perform additional investigations based on initial results"""
    
    print("\n🔍 ADDITIONAL INVESTIGATIONS")
    print("-" * 40)
    
    # 1. Timezone boundary investigation
    print("1️⃣ Timezone Boundary Investigation")
    print("   Testing expanded date ranges for timezone effects...")
    
    # Test with expanded date ranges
    expanded_tests = [
        ("2025-07-15", "2025-07-29", "1 day earlier start"),
        ("2025-07-16", "2025-07-30", "1 day later end"),
        ("2025-07-15", "2025-07-30", "1 day expanded both sides")
    ]
    
    for exp_start, exp_end, description in expanded_tests:
        cursor.execute("""
            SELECT 
                COUNT(DISTINCT CASE WHEN e.event_name = 'RC Trial started' 
                                  AND DATE(e.event_time) BETWEEN ? AND ? 
                                  THEN u.distinct_id END) as trial_count
            FROM mixpanel_user u
            LEFT JOIN mixpanel_event e ON u.distinct_id = e.distinct_id
            WHERE u.abi_campaign_id = ?
              AND u.has_abi_attribution = TRUE
        """, [exp_start, exp_end, campaign_id])
        
        result = cursor.fetchone()
        count = result['trial_count']
        
        if count == 39:
            print(f"   🎯 MATCH! {description}: {exp_start} to {exp_end} = {count}")
        else:
            print(f"   📊 {description}: {exp_start} to {exp_end} = {count}")
    
    # 2. Event counting vs user counting
    print("\n2️⃣ Event Counting vs User Counting")
    print("   Checking if Mixpanel counts events vs unique users...")
    
    cursor.execute("""
        SELECT 
            COUNT(CASE WHEN e.event_name = 'RC Trial started' 
                       AND DATE(e.event_time) BETWEEN ? AND ? THEN 1 END) as total_events,
            COUNT(DISTINCT CASE WHEN e.event_name = 'RC Trial started' 
                                 AND DATE(e.event_time) BETWEEN ? AND ? 
                                 THEN u.distinct_id END) as unique_users
        FROM mixpanel_user u
        LEFT JOIN mixpanel_event e ON u.distinct_id = e.distinct_id
        WHERE u.abi_campaign_id = ?
          AND u.has_abi_attribution = TRUE
    """, [start_date, end_date, start_date, end_date, campaign_id])
    
    result = cursor.fetchone()
    total_events = result['total_events']
    unique_users = result['unique_users']
    
    print(f"   📊 Total trial events: {total_events}")
    print(f"   👤 Unique users with trials: {unique_users}")
    
    if total_events == 39:
        print("   🎯 POTENTIAL MATCH: Mixpanel might count total events, not unique users!")
    
    # 3. Field value investigation for matches
    print("\n3️⃣ Field Value Investigation")
    print("   Examining date field values for potential patterns...")
    
    # Sample some users to see their date field values
    cursor.execute("""
        SELECT 
            u.distinct_id,
            u.first_seen,
            u.last_updated,
            upm.credited_date,
            JSON_EXTRACT(u.profile_json, '$.first_install_date') as profile_install_date,
            COUNT(CASE WHEN e.event_name = 'RC Trial started' THEN 1 END) as trial_events
        FROM mixpanel_user u
        LEFT JOIN mixpanel_event e ON u.distinct_id = e.distinct_id
        LEFT JOIN user_product_metrics upm ON u.distinct_id = upm.distinct_id
        WHERE u.abi_campaign_id = ?
          AND u.has_abi_attribution = TRUE
        GROUP BY u.distinct_id, u.first_seen, u.last_updated, upm.credited_date
        ORDER BY u.first_seen DESC
        LIMIT 10
    """, [campaign_id])
    
    sample_users = cursor.fetchall()
    
    print("   📋 Sample users and their date field values:")
    for user in sample_users:
        print(f"      👤 {user['distinct_id'][:12]}...")
        print(f"         First Seen: {user['first_seen']}")
        print(f"         Last Updated: {user['last_updated']}")
        print(f"         Credited Date: {user['credited_date']}")
        print(f"         Profile Install: {user['profile_install_date']}")
        print(f"         Trial Events: {user['trial_events']}")
        print()

if __name__ == "__main__":
    exit(main()) 